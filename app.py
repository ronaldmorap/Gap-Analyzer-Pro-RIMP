from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
from textblob import TextBlob
import requests
import os
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime
import threading
import time

app = Flask(__name__)

# ── CACHÉ EN MEMORIA CON TTL DIFERENCIADO ────────────────────────
_cache      = {}
_cache_lock = threading.Lock()

# TTL por tipo de dato — datos rápidos se refrescan más seguido
CACHE_TTL_MAP = {
    'futures':       30,   # futuros: 30s — tiempo real
    'whale':         120,  # ballenas: 2min — noticias cambian poco
    'high_impact':   60,   # macro noticias: 1min
    'vix':           30,   # VIX: 30s — tiempo real
    'volume':        60,   # volumen: 1min
    'drift':         300,  # drift: 5min — cambia poco
    'sec_':          3600, # SEC Form 4: 1h — solo 2 veces al mes
    'hist_':         3600, # histórico: 1h — no cambia en el día
    'earnings':      3600, # earnings: 1h
    'default':       90    # resto: 90s
}

def _get_ttl(key):
    for prefix, ttl in CACHE_TTL_MAP.items():
        if key.startswith(prefix):
            return ttl
    return CACHE_TTL_MAP['default']

def _cache_get(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (time.time() - entry['ts']) < _get_ttl(key):
            return entry['val']
    return None

def _cache_set(key, val):
    with _cache_lock:
        _cache[key] = {'val': val, 'ts': time.time()}

# ── CONSTANTES ────────────────────────────────────────────────────
MARKET_LEADERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'NFLX', 'RACE']

COMPANY_NAMES = {
    'AAPL': 'Apple', 'MSFT': 'Microsoft', 'GOOGL': 'Google',
    'AMZN': 'Amazon', 'NVDA': 'Nvidia',   'META': 'Meta',
    'TSLA': 'Tesla',  'NFLX': 'Netflix',  'RACE': 'Ferrari'
}

NYSE_STOCKS   = ['RACE']          # SP500
NASDAQ_STOCKS = ['AAPL','MSFT','GOOGL','AMZN','NVDA','META','TSLA','NFLX']  # Nasdaq

SP500_KEYWORDS  = ['S&P', 'SP500', 'S&P500', 'SPX', 'SP 500']
NASDAQ_KEYWORDS = ['NASDAQ', 'QQQ', 'NDX', 'TECH STOCKS', 'NASDAQ 100']

INVESTOR_URLS = {
    'AAPL':  'https://investor.apple.com/investor-relations/default.aspx',
    'MSFT':  'https://www.microsoft.com/en-us/investor',
    'GOOGL': 'https://abc.xyz/investor/',
    'AMZN':  'https://ir.aboutamazon.com/',
    'NVDA':  'https://investor.nvidia.com/',
    'META':  'https://investor.fb.com/',
    'TSLA':  'https://ir.tesla.com/',
    'NFLX':  'https://ir.netflix.net/',
    'RACE':  'https://corporate.ferrari.com/en/investors'
}

HIGH_IMPACT_KEYWORDS = [
    # Macro económico
    'CPI', 'NFP', 'Non-Farm Payroll', 'FOMC', 'GDP',
    'Federal Reserve', 'interest rate decision', 'inflation report', 'jobs report',
    'PCE', 'retail sales', 'unemployment rate',
    # Geopolítica y aranceles — mueven mercado por miedo sistémico
    'tariff', 'tariffs', 'trade war', 'trade deal',
    'Trump tariff', 'Trump tax', 'sanctions', 'embargo',
    'war', 'invasion', 'military strike', 'escalation',
    'NATO', 'Ukraine', 'Russia', 'China trade',
    'geopolitical', 'nuclear', 'oil embargo',
    # Miedo sistémico
    'market crash', 'recession', 'bank failure', 'credit crisis',
    'debt ceiling', 'government shutdown', 'default'
]

# Palabras positivas para geopolítica
GEO_POSITIVE = ['deal', 'ceasefire', 'peace', 'agreement', 'resolved',
                 'lifted', 'cut tariff', 'trade agreement', 'stimulus']
# Palabras negativas para geopolítica  
GEO_NEGATIVE = ['war', 'invasion', 'tariff', 'sanction', 'embargo',
                 'crash', 'recession', 'failure', 'default', 'shutdown',
                 'escalat', 'nuclear', 'strike', 'attack']


# ═══════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════
def _news_is_fresh(pub_date_str, max_hours=24):
    try:
        import calendar
        dt  = parsedate_to_datetime(pub_date_str)
        ts  = calendar.timegm(dt.utctimetuple())
        age = (datetime.utcnow() - datetime.utcfromtimestamp(ts)).total_seconds() / 3600
        return age <= max_hours
    except Exception:
        return True

def _norm_ts(ts):
    try:
        return pd.Timestamp(str(ts)[:10]).normalize()
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════
#  EARNINGS  (triple método, ventana 30 días pasados)
# ═══════════════════════════════════════════════════════════════════
def get_earnings_info(ticker):
    investor_url = INVESTOR_URLS.get(ticker, '')
    stock  = yf.Ticker(ticker)
    today  = pd.Timestamp.now().normalize()

    # Método 1
    try:
        ed = stock.get_earnings_dates(limit=20)
        if ed is not None and not ed.empty:
            days_list = []
            for dt in ed.index:
                norm = _norm_ts(dt)
                if norm is not None:
                    days_list.append(((norm - today).days, norm))
            future = sorted([(d, n) for d, n in days_list if 0 <= d <= 90])
            if future:
                d, n = future[0]
                return {'has_earnings': True, 'days_to_earnings': d,
                        'earnings_date': str(n.date()), 'investor_url': investor_url,
                        'status': 'upcoming'}
            past = sorted([(d, n) for d, n in days_list if -30 <= d < 0], reverse=True)
            if past:
                d, n = past[0]
                return {'has_earnings': True, 'days_to_earnings': d,
                        'earnings_date': str(n.date()), 'investor_url': investor_url,
                        'status': 'published'}
    except Exception:
        pass

    # Método 2: calendar
    try:
        cal = stock.calendar
        raw_dates = []
        if isinstance(cal, dict):
            v = cal.get('Earnings Date') or cal.get('earnings_date')
            raw_dates = (v if isinstance(v, list) else [v]) if v else []
        elif hasattr(cal, 'empty') and not cal.empty:
            for col in ['Earnings Date', 'earnings_date']:
                if col in cal.columns:
                    raw_dates = list(cal[col].dropna()); break
        for raw in raw_dates:
            norm = _norm_ts(raw)
            if norm is None: continue
            d = (norm - today).days
            if -30 <= d <= 90:
                return {'has_earnings': True, 'days_to_earnings': d,
                        'earnings_date': str(norm.date()), 'investor_url': investor_url,
                        'status': 'published' if d < 0 else 'upcoming'}
    except Exception:
        pass

    # Método 3: info
    try:
        info = stock.info
        for key in ['earningsDate', 'earningsTimestamp']:
            raw = info.get(key)
            if not raw: continue
            if isinstance(raw, list): raw = raw[0]
            norm = (pd.Timestamp.fromtimestamp(raw).normalize()
                    if isinstance(raw, (int, float)) else _norm_ts(raw))
            if norm is None: continue
            d = (norm - today).days
            if -30 <= d <= 90:
                return {'has_earnings': True, 'days_to_earnings': d,
                        'earnings_date': str(norm.date()), 'investor_url': investor_url,
                        'status': 'published' if d < 0 else 'upcoming'}
    except Exception:
        pass

    return {'has_earnings': False, 'days_to_earnings': 999,
            'earnings_date': None, 'investor_url': investor_url, 'status': 'unknown'}


# ═══════════════════════════════════════════════════════════════════
#  SEC FORM 4 — insiders reales, gratis, 2 días de delay máximo
# ═══════════════════════════════════════════════════════════════════
def get_sec_insider_activity(ticker):
    """
    Consulta la API pública de la SEC (EDGAR) para obtener transacciones
    reales de insiders (Form 4) de los últimos 30 días.
    Completamente gratuito, sin rate limit abusivo, datos verificados legalmente.
    """
    cache_key = f'sec_{ticker}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    empty = {'score': 0, 'signals': [], 'net_shares': 0, 'summary': 'Sin datos SEC'}

    try:
        # Buscar CIK de la empresa
        search_url = (f'https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22'
                      f'&dateRange=custom&startdt={(datetime.utcnow()-timedelta(days=30)).strftime("%Y-%m-%d")}'
                      f'&enddt={datetime.utcnow().strftime("%Y-%m-%d")}'
                      f'&forms=4')
        resp = requests.get(search_url,
                            headers={'User-Agent': 'GapAnalyzerPro contact@example.com'},
                            timeout=8)
        if resp.status_code != 200:
            _cache_set(cache_key, empty)
            return empty

        hits = resp.json().get('hits', {}).get('hits', [])
        if not hits:
            result = {'score': 0, 'signals': [], 'net_shares': 0,
                      'summary': 'Sin actividad insider reciente'}
            _cache_set(cache_key, result)
            return result

        buys = sells = 0
        signals = []

        for hit in hits[:10]:
            src = hit.get('_source', {})
            form_type = src.get('form_type', '')
            if form_type != '4':
                continue

            display_names = src.get('display_names', [])
            period        = src.get('period_of_report', '')[:10]
            file_date     = src.get('file_date', '')[:10]

            # Detectar dirección por el display name o descripción
            names_str = ' '.join(display_names).upper() if display_names else ''
            desc      = src.get('file_description', '').upper()
            combined  = names_str + ' ' + desc

            # Solo contar si es reciente (últimos 5 días laborables)
            try:
                filed_dt = datetime.strptime(file_date, '%Y-%m-%d')
                days_ago = (datetime.utcnow() - filed_dt).days
                recency_mult = 2.0 if days_ago <= 3 else 1.0
            except Exception:
                recency_mult = 1.0

            # Leer directamente el XML del filing para obtener shares
            acc = src.get('accession_no', '').replace('-', '')
            cik = src.get('entity_id', '')
            if acc and cik:
                try:
                    xml_url = f'https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{acc[10:]}-index.htm'
                    # Usar el filing de forma más ligera — solo contamos buy/sell por ahora
                    pass
                except Exception:
                    pass

            # Clasificar como compra o venta según palabras clave en el filing
            if any(w in combined for w in ['PURCHASE', 'ACQUISITION', 'BUY', 'GRANT']):
                buys += 1
                signals.append({
                    'signal':  '🐋 Insider COMPRA (Form 4)',
                    'detail':  f'{", ".join(display_names[:2])} · {file_date}',
                    'points':  round(3 * recency_mult, 1),
                    'bullish': True
                })
            elif any(w in combined for w in ['SALE', 'DISPOSE', 'SELL']):
                sells += 1
                signals.append({
                    'signal':  '🔴 Insider VENDE (Form 4)',
                    'detail':  f'{", ".join(display_names[:2])} · {file_date}',
                    'points':  round(-2 * recency_mult, 1),
                    'bullish': False
                })

        score = round(sum(s['points'] for s in signals), 1)

        if buys > sells:
            summary = f'🟢 {buys} compras insider vs {sells} ventas (últimos 30d)'
        elif sells > buys:
            summary = f'🔴 {sells} ventas insider vs {buys} compras (últimos 30d)'
        else:
            summary = f'⚪ Actividad insider neutral ({buys} compras, {sells} ventas)'

        result = {'score': score, 'signals': signals[:5],
                  'net_shares': buys - sells, 'summary': summary}
        _cache_set(cache_key, result)
        return result

    except Exception:
        _cache_set(cache_key, empty)
        return empty


# ═══════════════════════════════════════════════════════════════════
#  NOTICIAS DE BALLENAS — solo Google News 24h (hasta tener UW)
# ═══════════════════════════════════════════════════════════════════
def get_vix_level():
    """
    Obtiene el nivel actual del VIX (índice de volatilidad del mercado).
    VIX > 25: mercado nervioso, reducir exposición.
    VIX > 30: alta volatilidad sistémica, forzar amarillo.
    """
    cached = _cache_get('vix_level')
    if cached is not None:
        return cached
    try:
        vix = yf.Ticker('^VIX')
        hist = vix.history(period='2d', interval='1h')
        if hist.empty:
            return 15.0
        val = round(float(hist['Close'].iloc[-1]), 1)
        _cache_set('vix_level', val)
        return val
    except Exception:
        return 15.0


def get_whale_signals(ticker):
    """
    Señales institucionales — 3 fuentes en cascada con decay temporal:
    1. Benzinga RSS     (5-15min)  — más rápida, especializada en mercados
    2. Yahoo Finance RSS(15-30min) — cobertura amplia
    3. Google News RSS  (6-24h)    — fallback final
    Decay: 0-6h=1.0x · 6-24h=0.6x · 24-48h=0.3x · >48h=ignorar
    TODO: reemplazar por Unusual Whales API cuando esté disponible.
    """
    cache_key = f'whale_{ticker}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    score, signals = 0, []
    seen_titles = set()

    LABEL_MAP = {
        'UPGRADE':  '🏦 Banco UPGRADE',
        'DOWNGRADE':'🏦 Banco DOWNGRADE',
        'SHORT':    '⚠️ SHORT SELLER',
        'BLOCK':    '🐋 Block Trade / Insider'
    }

    def _age_hours(pub_str):
        try:
            if not pub_str or not pub_str.strip():
                return 999  # sin fecha = ignorar
            import calendar as c
            dt = parsedate_to_datetime(pub_str)
            ts = c.timegm(dt.utctimetuple())
            h  = (datetime.utcnow() - datetime.utcfromtimestamp(ts)).total_seconds() / 3600
            # Sanity check: si es negativo o >720h (30 días) algo falló
            if h < 0 or h > 720:
                return 999
            return h
        except Exception:
            return 999  # error de parse = ignorar, no mostrar <1h falso

    def _age_weight(h):
        if h <= 6:    return 1.0
        elif h <= 24: return 0.6
        elif h <= 48: return 0.3
        return 0.0

    def _age_label(h):
        if h < 1:      return 'hace <1h'
        elif h < 24:   return f'hace {int(h)}h'
        elif h < 48:   return f'hace {int(h)}h ·0.6x'
        else:          return f'hace {int(h)}h ·0.3x'

    def _classify(title):
        tl = title.lower()
        if any(w in tl for w in ['upgrade','outperform','overweight','strong buy',
                                   'buy rating','raises price target','raises pt','price target increase']):
            return 'UPGRADE', 1.5
        elif any(w in tl for w in ['downgrade','underperform','underweight','sell rating',
                                    'cuts price target','lowers pt','price target cut']):
            return 'DOWNGRADE', -1.5
        elif any(w in tl for w in ['hindenburg','short seller','fraud','citron','muddy waters','short report']):
            return 'SHORT', -3.0
        elif any(w in tl for w in ['block trade','insider buy','purchased shares','insider purchase','insider buying']):
            return 'BLOCK', 2.0
        return None, 0

    def _process_items(items, source_tag=''):
        """Procesa items RSS y añade señales al score/signals."""
        for item in items:
            pub   = item.find('pubDate') or item.find('pubdate')
            pub   = pub.text if pub is not None else ''
            title_el = item.find('title')
            title = title_el.text if title_el is not None else ''
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            h     = _age_hours(pub)
            age_w = _age_weight(h)
            if age_w == 0.0:
                continue
            kind, base_pts = _classify(title)
            if kind:
                pts = round(base_pts * age_w, 1)
                score_delta = pts
                tag = f' [{source_tag}]' if source_tag else ''
                signals.append({
                    'signal': LABEL_MAP[kind],
                    'detail': f'{title[:70]} · {_age_label(h)}{tag}',
                    'points': pts
                })
                return score_delta  # devuelve para acumular fuera
        return 0

    # ── FUENTE 1: Benzinga RSS (5-15min) ──────────────────────────
    try:
        bz_url = f'https://www.benzinga.com/stock/{ticker.lower()}/feed'
        root = ET.fromstring(requests.get(bz_url, timeout=7,
            headers={'User-Agent': 'Mozilla/5.0 (compatible; GapAnalyzer/1.0)'}).content)
        for item in root.findall('.//item')[:15]:
            pub   = item.find('pubDate') or item.find('pubdate')
            pub   = pub.text if pub is not None else ''
            title_el = item.find('title')
            title = title_el.text if title_el is not None else ''
            if not title or title in seen_titles: continue
            seen_titles.add(title)
            h     = _age_hours(pub)
            age_w = _age_weight(h)
            if age_w == 0.0: continue
            kind, base_pts = _classify(title)
            if kind:
                pts = round(base_pts * age_w, 1)
                score += pts
                signals.append({'signal': LABEL_MAP[kind],
                                'detail': f'{title[:70]} · {_age_label(h)} [BZ]',
                                'points': pts})
    except Exception:
        pass

    # ── FUENTE 2: Yahoo Finance RSS (15-30min) ────────────────────
    try:
        yf_url = f'https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US'
        root = ET.fromstring(requests.get(yf_url, timeout=7,
            headers={'User-Agent': 'Mozilla/5.0'}).content)
        for item in root.findall('.//item')[:15]:
            pub   = item.find('pubDate') or item.find('pubdate')
            pub   = pub.text if pub is not None else ''
            title_el = item.find('title')
            title = title_el.text if title_el is not None else ''
            if not title or title in seen_titles: continue
            seen_titles.add(title)
            h     = _age_hours(pub)
            age_w = _age_weight(h)
            if age_w == 0.0: continue
            kind, base_pts = _classify(title)
            if kind:
                pts = round(base_pts * age_w, 1)
                score += pts
                signals.append({'signal': LABEL_MAP[kind],
                                'detail': f'{title[:70]} · {_age_label(h)} [YF]',
                                'points': pts})
    except Exception:
        pass

    # ── FUENTE 3: Google News RSS (6-24h) — fallback final ────────
    if not signals:
        queries = [
            f'{ticker}+Goldman+OR+Morgan+Stanley+OR+JPMorgan+upgrade+OR+downgrade',
            f'{ticker}+short+seller+OR+hindenburg+OR+citron+OR+muddy+waters',
            f'{ticker}+block+trade+OR+insider+buying+OR+insider+purchase'
        ]
        for q in queries:
            try:
                root = ET.fromstring(requests.get(
                    f'https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en',
                    timeout=6).content)
                for item in root.findall('.//item')[:5]:
                    pub   = item.find('pubDate').text or ''
                    title_el = item.find('title')
                    title = title_el.text if title_el is not None else ''
                    if not title or title in seen_titles: continue
                    seen_titles.add(title)
                    h     = _age_hours(pub)
                    age_w = _age_weight(h)
                    if age_w == 0.0: continue
                    kind, base_pts = _classify(title)
                    if kind:
                        pts = round(base_pts * age_w, 1)
                        score += pts
                        signals.append({'signal': LABEL_MAP[kind],
                                        'detail': f'{title[:70]} · {_age_label(h)} [GN]',
                                        'points': pts})
            except Exception:
                continue

    result = round(score, 1), signals[:6]
    _cache_set(cache_key, result)
    return result


# ═══════════════════════════════════════════════════════════════════
#  MACRO — solo eventos de alto impacto (no sentiment genérico)
# ═══════════════════════════════════════════════════════════════════
def check_high_impact_news(ticker='AAPL'):
    """
    Devuelve (activo, titulo_noticia, fecha_pub, hora_pub, sentimiento)
    sentimiento: 'positivo', 'negativo', 'neutro'
    Filtra noticias SP500 para NYSE y Nasdaq para Nasdaq.
    Si no hay evento macro: (False, None, None, None, None)
    """
    cache_key = f'high_impact_news_{ticker}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        q    = 'CPI+OR+NFP+OR+FOMC+OR+GDP+OR+inflation+report+OR+jobs+report+today'
        root = ET.fromstring(requests.get(
            f'https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en',
            timeout=7).content)
        for item in root.findall('.//item')[:10]:
            raw_title = item.find('title').text or ''
            title_up  = raw_title.upper()
            pub       = item.find('pubDate').text or ''
            if not _news_is_fresh(pub, max_hours=48):
                continue
            for kw in HIGH_IMPACT_KEYWORDS:
                if kw.upper() in title_up:
                    # Parsear fecha y hora legible
                    try:
                        import calendar
                        dt  = parsedate_to_datetime(pub)
                        ts  = calendar.timegm(dt.utctimetuple())
                        dtl = datetime.utcfromtimestamp(ts)
                        fecha_str = dtl.strftime('%d/%m/%Y')
                        hora_str  = dtl.strftime('%H:%M') + ' UTC'
                    except Exception:
                        fecha_str = pub[:16]
                        hora_str  = ''
                    # Filtrar por índice: SP500 news solo para NYSE, Nasdaq news solo para Nasdaq
                    title_check = raw_title.upper()
                    is_sp500_news    = any(k in title_check for k in SP500_KEYWORDS)
                    is_nasdaq_news   = any(k in title_check for k in NASDAQ_KEYWORDS)
                    is_nyse_ticker   = ticker in NYSE_STOCKS
                    # Si la noticia es específica de un índice que NO es el del ticker, ignorar
                    if is_sp500_news and not is_nyse_ticker:
                        continue
                    if is_nasdaq_news and is_nyse_ticker:
                        continue
                    # Sentimiento ampliado: económico + geopolítico
                    positive_words = [
                        'SURGE', 'RALLY', 'GAIN', 'RISE', 'JUMP', 'SOAR', 'BOOST',
                        'STRONG', 'BEAT', 'RECORD', 'DEAL', 'CEASEFIRE', 'PEACE',
                        'AGREEMENT', 'RESOLVED', 'LIFTED', 'STIMULUS', 'CUT TARIFF',
                        'TRADE AGREEMENT', 'RECOVERY', 'GROWTH'
                    ]
                    negative_words = [
                        'SLIDE', 'FALL', 'DROP', 'PLUNGE', 'CRASH', 'DECLINE',
                        'WEAK', 'MISS', 'LOSS', 'FEAR', 'INFLATION', 'HOT',
                        'WAR', 'INVASION', 'TARIFF', 'SANCTION', 'EMBARGO',
                        'RECESSION', 'FAILURE', 'DEFAULT', 'SHUTDOWN', 'ESCALAT',
                        'NUCLEAR', 'STRIKE', 'ATTACK', 'CONFLICT', 'TENSION'
                    ]
                    pos = sum(1 for w in positive_words if w in title_check)
                    neg = sum(1 for w in negative_words if w in title_check)
                    sentimiento = 'positivo' if pos > neg else 'negativo' if neg > pos else 'neutro'
                    result = (True, raw_title[:120], fecha_str, hora_str, sentimiento)
                    _cache_set(cache_key, result)
                    return result
        result = (False, None, None, None, None)
        _cache_set(cache_key, result)
        return result
    except Exception:
        return False, None, None, None, None


# ═══════════════════════════════════════════════════════════════════
#  FUTUROS REALES + ÍNDICE — señal primaria y confirmación
# ═══════════════════════════════════════════════════════════════════
def get_futures_sentiment(ticker='AAPL'):
    """
    Combina futuros reales (/NQ=F o /ES=F) como señal primaria
    con el índice (QQQ/SPY) como confirmación secundaria.
    - Ambos coinciden      → señal fuerte (score ±2)
    - Solo futuros         → señal normal (score ±1)
    - Divergen             → aviso de contradicción
    Los futuros operan 23h/día — clave para análisis nocturno.
    """
    use_spy    = ticker in NYSE_STOCKS
    fut_tk     = 'ES=F'  if use_spy else 'NQ=F'
    idx_tk     = 'SPY'   if use_spy else 'QQQ'
    fut_name   = 'S&P 500 Futuros' if use_spy else 'Nasdaq Futuros'
    idx_name   = 'S&P 500' if use_spy else 'Nasdaq'
    cache_key  = f'futures_{ticker}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    def _get_chg(tk):
        """
        Calcula cambio % últimos 30min.
        Usa prepost=True para capturar futuros nocturnos (21:50h análisis).
        Fallback sin prepost si falla.
        """
        for prepost in [True, False]:
            try:
                hist = yf.Ticker(tk).history(
                    period='2d', interval='5m', prepost=prepost)
                if hist.empty or len(hist) < 6:
                    continue
                last30 = hist.tail(6)
                chg = round(float(
                    (last30['Close'].iloc[-1] - last30['Close'].iloc[0])
                    / last30['Close'].iloc[0] * 100), 3)
                return chg
            except Exception:
                continue
        return None

    fut_chg = _get_chg(fut_tk)
    idx_chg = _get_chg(idx_tk)

    # ── Caso 1: Solo futuros disponibles (mercado cerrado) ─────────
    if fut_chg is not None and idx_chg is None:
        emoji = '🟢' if fut_chg >= 0.3 else '🔴' if fut_chg <= -0.3 else '⚪'
        sign  = '+' if fut_chg > 0 else ''
        score = 1 if fut_chg >= 0.3 else -1 if fut_chg <= -0.3 else 0
        signal = f'{emoji} {fut_name} {sign}{fut_chg}% · 30min'
        result = (score, signal, fut_chg, fut_name)

    # ── Caso 2: Solo índice disponible ────────────────────────────
    elif fut_chg is None and idx_chg is not None:
        emoji = '🟢' if idx_chg >= 0.3 else '🔴' if idx_chg <= -0.3 else '⚪'
        sign  = '+' if idx_chg > 0 else ''
        score = 1 if idx_chg >= 0.3 else -1 if idx_chg <= -0.3 else 0
        signal = f'{emoji} {idx_name} {sign}{idx_chg}% · 30min'
        result = (score, signal, idx_chg, idx_name)

    # ── Caso 3: Ambos disponibles ─────────────────────────────────
    elif fut_chg is not None and idx_chg is not None:
        fut_dir = 1 if fut_chg >= 0.3 else -1 if fut_chg <= -0.3 else 0
        idx_dir = 1 if idx_chg >= 0.3 else -1 if idx_chg <= -0.3 else 0
        sign_f  = '+' if fut_chg > 0 else ''
        sign_i  = '+' if idx_chg > 0 else ''

        if fut_dir == idx_dir and fut_dir != 0:
            # Confirmación — ambos en la misma dirección
            score  = 2 * fut_dir  # ±2 por confirmación doble
            emoji  = '🟢🟢' if fut_dir > 0 else '🔴🔴'
            signal = (f'{emoji} {fut_name} {sign_f}{fut_chg}% + '
                      f'{idx_name} {sign_i}{idx_chg}% — CONFIRMACIÓN DOBLE')
        elif fut_dir != 0 and idx_dir != 0 and fut_dir != idx_dir:
            # Divergencia — futuros y índice en direcciones opuestas
            score  = 0  # se anulan
            signal = (f'⚠️ DIVERGENCIA: {fut_name} {sign_f}{fut_chg}% vs '
                      f'{idx_name} {sign_i}{idx_chg}% — precaución')
        else:
            # Uno neutro, el otro con señal
            score  = fut_dir if fut_dir != 0 else idx_dir
            emoji  = '🟢' if score > 0 else '🔴' if score < 0 else '⚪'
            signal = (f'{emoji} {fut_name} {sign_f}{fut_chg}% · '
                      f'{idx_name} {sign_i}{idx_chg}%')

        result = (score, signal, fut_chg, fut_name)

    # ── Caso 4: Sin datos ─────────────────────────────────────────
    else:
        result = (0, 'Sin datos de futuros', 0, fut_name)

    _cache_set(cache_key, result)
    return result


# ═══════════════════════════════════════════════════════════════════
#  PRE-MARKET — precio y movimiento antes de apertura
# ═══════════════════════════════════════════════════════════════════
def get_premarket_data(ticker):
    """
    Obtiene precio pre-market usando múltiples métodos.
    Caché corta de 60s para datos frescos durante ventana pre-market.
    Pre-market americano = 10:00-15:30h hora española.
    """
    cache_key = f'premarket_{ticker}'
    # Caché más corta para pre-market — datos cambian rápido
    with _cache_lock:
        entry = _cache.get(cache_key)
        if entry and (time.time() - entry['ts']) < 60:
            return entry['val']

    empty = {'pre_price': None, 'pre_chg': 0, 'pre_signal': 'Sin datos pre-market',
             'pre_score': 0, 'pre_available': False}
    try:
        tk = yf.Ticker(ticker)

        # Método 1: fast_info
        pre_price  = None
        prev_close = None
        try:
            fi = tk.fast_info
            pre_price  = getattr(fi, 'pre_market_price', None)
            prev_close = getattr(fi, 'previous_close', None) or getattr(fi, 'last_price', None)
        except Exception:
            pass

        # Método 2: historia 1m pre-market si fast_info falla
        if not pre_price:
            try:
                hist = tk.history(period='1d', interval='1m', prepost=True)
                if not hist.empty:
                    now_utc = datetime.utcnow()
                    # Filtrar solo datos pre-market (antes de 13:30 UTC = 15:30h ES)
                    pre = hist[hist.index.hour < 14]
                    if not pre.empty:
                        pre_price = float(pre['Close'].iloc[-1])
                        if not prev_close:
                            # Cierre del día anterior
                            hist_d = tk.history(period='5d', interval='1d')
                            if len(hist_d) >= 2:
                                prev_close = float(hist_d['Close'].iloc[-2])
            except Exception:
                pass

        # Método 3: info dict
        if not pre_price or not prev_close:
            try:
                info = tk.info
                if not pre_price:
                    pre_price = info.get('preMarketPrice')
                if not prev_close:
                    prev_close = info.get('previousClose') or info.get('regularMarketPreviousClose')
            except Exception:
                pass

        if pre_price and prev_close and prev_close > 0:
            chg  = round((pre_price - prev_close) / prev_close * 100, 2)
            sign = '+' if chg > 0 else ''
            if   chg >= 1.0:  score = 2;  emoji = '🚀'; label = f'Pre-market fuerte alcista {sign}{chg}%'
            elif chg >= 0.3:  score = 1;  emoji = '🟢'; label = f'Pre-market alcista {sign}{chg}%'
            elif chg <= -1.0: score = -2; emoji = '🔻'; label = f'Pre-market fuerte bajista {sign}{chg}%'
            elif chg <= -0.3: score = -1; emoji = '🔴'; label = f'Pre-market bajista {sign}{chg}%'
            else:             score = 0;  emoji = '⚪'; label = f'Pre-market plano {sign}{chg}%'
            result = {'pre_price': round(float(pre_price), 2), 'pre_chg': chg,
                      'pre_signal': f'{emoji} {label}', 'pre_score': score,
                      'pre_available': True}
        else:
            result = empty

        _cache_set(cache_key, result)
        return result
    except Exception:
        return empty


# ═══════════════════════════════════════════════════════════════════
#  SHORT INTEREST — presión vendedora institucional
# ═══════════════════════════════════════════════════════════════════
def get_short_interest(ticker):
    """
    Obtiene el short interest (% acciones vendidas en corto) y short ratio.
    Alto short interest + buenas noticias = posible short squeeze (gap alcista amplificado).
    Alto short interest + malas noticias = confirma presión bajista.
    """
    cache_key = f'short_{ticker}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    empty = {'short_pct': None, 'short_ratio': None, 'short_signal': 'Sin datos',
             'short_score': 0, 'squeeze_risk': False}
    try:
        info = yf.Ticker(ticker).info
        short_pct   = info.get('shortPercentOfFloat')
        short_ratio = info.get('shortRatio')  # días para cubrir

        if short_pct is not None:
            short_pct_pct = round(short_pct * 100, 1)
            squeeze_risk  = short_pct_pct >= 15  # >15% = riesgo de squeeze

            if short_pct_pct >= 20:
                score  = 0  # neutro — puede ir en cualquier dirección
                signal = f'⚠️ Short interest muy alto {short_pct_pct}% — riesgo de squeeze'
            elif short_pct_pct >= 10:
                score  = 0
                signal = f'🟡 Short interest elevado {short_pct_pct}% — presión vendedora'
            else:
                score  = 0.5  # bajo short interest = entorno más predecible
                signal = f'✅ Short interest bajo {short_pct_pct}% — entorno limpio'

            ratio_txt = f' · {short_ratio}d para cubrir' if short_ratio else ''
            result = {'short_pct': short_pct_pct, 'short_ratio': short_ratio,
                      'short_signal': signal + ratio_txt,
                      'short_score': score, 'squeeze_risk': squeeze_risk}
        else:
            result = empty
        _cache_set(cache_key, result)
        return result
    except Exception:
        return empty


# ═══════════════════════════════════════════════════════════════════
#  TÉCNICO
# ═══════════════════════════════════════════════════════════════════
def get_fakeout_detector(ticker):
    try:
        hist  = yf.Ticker(ticker).history(period='3mo')
        if hist.empty:
            return False, 'Sin datos', 50
        close = hist['Close']
        gain  = close.diff().clip(lower=0).rolling(14).mean()
        loss  = (-close.diff().clip(upper=0)).rolling(14).mean()
        rsi   = float(100 - (100 / (1 + gain.iloc[-1] / loss.iloc[-1])))
        risk, reasons = 0, []
        if rsi > 72:  risk += 2; reasons.append(f'RSI sobrecomprado ({round(rsi,1)})')
        elif rsi > 68:risk += 1; reasons.append(f'RSI alto ({round(rsi,1)})')
        if rsi < 28:  risk += 2; reasons.append(f'RSI sobrevendido ({round(rsi,1)})')
        lc = float((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100)
        if lc > 4:    risk += 2; reasons.append(f'Subida muy alta (+{round(lc,1)}%)')
        elif lc < -4: risk += 1; reasons.append(f'Caída muy alta ({round(lc,1)}%)')
        return risk >= 3, ' + '.join(reasons) if reasons else 'Sin riesgo detectado', round(rsi, 1)
    except Exception:
        return False, 'Sin datos', 50


def get_volume_analysis(ticker):
    empty = {'rvol': 1.0, 'volume_signal': '⚪ Sin datos', 'volume_score': 0,
             'absorption': False, 'capitulation': False, 'anomaly': False,
             'last_volume': 0, 'avg_volume': 0, 'price_change_pct': 0}
    try:
        hist    = yf.Ticker(ticker).history(period='30d', interval='1d')
        if len(hist) < 10:
            return empty
        avg_vol = float(hist['Volume'][:-1].mean())
        last_vol= float(hist['Volume'].iloc[-1])
        rvol    = round(last_vol / avg_vol, 2) if avg_vol > 0 else 1.0
        pct     = float((hist['Close'].iloc[-1] - hist['Close'].iloc[-2])
                        / hist['Close'].iloc[-2] * 100)
        score   = 0; absorption = capitulation = anomaly = False
        if   rvol >= 2.0 and pct >  0.3: score= 2; vsig=f'⚡ Acumulación institucional (RVOL {rvol}x)'; anomaly=True
        elif rvol >= 2.0 and pct < -0.3: score=-2; vsig=f'🔴 Distribución institucional (RVOL {rvol}x)'; anomaly=True
        elif rvol >= 2.0:                 score=-1; vsig=f'⚠️ Absorción detectada (RVOL {rvol}x)'; absorption=True; anomaly=True
        elif rvol >= 1.5 and pct >  0.2: score= 1; vsig=f'🟡 Volumen elevado alcista (RVOL {rvol}x)'
        elif rvol >= 1.5 and pct < -0.2: score=-1; vsig=f'🟠 Volumen elevado bajista (RVOL {rvol}x)'
        else:                              vsig=f'⚪ Volumen normal (RVOL {rvol}x)'
        return {'rvol': rvol, 'volume_signal': vsig, 'volume_score': round(score, 1),
                'absorption': absorption, 'capitulation': capitulation, 'anomaly': anomaly,
                'last_volume': int(last_vol), 'avg_volume': int(avg_vol),
                'price_change_pct': round(pct, 2)}
    except Exception:
        return empty


def get_overnight_drift(ticker):
    """
    Calcula el overnight drift filtrando por día de la semana.
    Los lunes (weekday=0) tienen efecto weekend → se calculan por separado.
    Los martes-viernes usan la media estándar.
    Devuelve (avg, trend, recent, is_monday_effect)
    """
    try:
        hist = yf.Ticker(ticker).history(period='60d', interval='1d')
        if len(hist) < 5:
            return 0, 'neutral', 0, False
        today_dow = datetime.utcnow().weekday()  # 0=lunes
        is_monday = today_dow == 0

        all_rets    = []
        monday_rets = []
        normal_rets = []

        for i in range(1, len(hist)):
            prev_close = float(hist['Close'].iloc[i-1])
            if prev_close <= 0:
                continue
            ret = (float(hist['Open'].iloc[i]) - prev_close) / prev_close * 100
            all_rets.append(ret)
            dow = hist.index[i].weekday()
            if dow == 0:  # lunes
                monday_rets.append(ret)
            else:
                normal_rets.append(ret)

        # Si hoy es lunes, usar media de lunes anteriores (efecto weekend)
        rets = monday_rets if (is_monday and len(monday_rets) >= 3) else normal_rets
        if not rets:
            rets = all_rets
        if not rets:
            return 0, 'neutral', 0, is_monday

        avg    = round(sum(rets) / len(rets), 3)
        recent = round(sum(rets[-5:]) / min(5, len(rets)), 3)
        trend  = 'alcista' if avg > 0.1 else 'bajista' if avg < -0.1 else 'neutral'
        return avg, trend, recent, is_monday
    except Exception:
        return 0, 'neutral', 0, False


def get_gap_room(ticker):
    empty = {'room_up': 5, 'room_down': 5, 'near_resistance': False,
             'near_support': False, 'resistance': 0, 'support': 0, 'current': 0}
    try:
        hist = yf.Ticker(ticker).history(period='1y', interval='1d')
        if len(hist) < 20:
            return empty
        cur        = float(hist['Close'].iloc[-1])
        resistance = min(float(hist['High'].max()), float(hist['High'].tail(60).max()) * 1.02)
        support    = max(float(hist['Low'].min()),  float(hist['Low'].tail(60).min())  * 0.98)
        return {
            'room_up':          round((resistance - cur) / cur * 100, 2),
            'room_down':        round((cur - support) / cur * 100, 2),
            'near_resistance':  bool((resistance - cur) / cur * 100 < 1.5),
            'near_support':     bool((cur - support) / cur * 100 < 1.5),
            'resistance':       round(resistance, 2),
            'support':          round(support, 2),
            'current':          round(cur, 2)
        }
    except Exception:
        return empty


def get_historical_gap_stats(ticker):
    """
    Histórico adaptativo: empieza con 1y, si hay menos de 50 gaps válidos
    amplía a 2y automáticamente para tickers de bajo volumen (RACE, NFLX).
    """
    MIN_GAPS = 50
    for period in ['1y', '2y']:
        try:
            hist = yf.Ticker(ticker).history(period=period)
            if hist.empty:
                continue
            up = down = 0
            for i in range(1, len(hist)):
                g = (hist['Open'].iloc[i] - hist['Close'].iloc[i-1]) / hist['Close'].iloc[i-1] * 100
                if g > 0.1:    up   += 1
                elif g < -0.1: down += 1
            total = up + down
            if total >= MIN_GAPS or period == '2y':
                return round((up / total) * 100) if total > 0 else 50
        except Exception:
            continue
    return 50


def get_technical_score(ticker):
    try:
        hist  = yf.Ticker(ticker).history(period='3mo')
        if hist.empty:
            return 0
        close = hist['Close']
        ma20  = close.rolling(20).mean().iloc[-1]
        cur   = close.iloc[-1]
        gain  = close.diff().clip(lower=0).rolling(14).mean()
        loss  = (-close.diff().clip(upper=0)).rolling(14).mean()
        rsi   = float(100 - (100 / (1 + gain.iloc[-1] / loss.iloc[-1])))
        score = 0
        if cur > ma20: score += 30
        if rsi > 50:   score += 20
        if rsi > 70:   score -= 10
        if rsi < 30:   score += 20
        return int(score)
    except Exception:
        return 0


# ═══════════════════════════════════════════════════════════════════
#  SEMÁFORO FTMO — el corazón de la app
#  Verde: opera. Amarillo: cuidado. Rojo: NO operar hoy.
# ═══════════════════════════════════════════════════════════════════
def get_ftmo_signal(probability, raw_direction, futures_warning,
                    is_fakeout, near_resistance, earnings_days,
                    sec_score, whale_score, vol_score, macro_event,
                    drift_trend, drift_avg, hist_pct, rsi_val,
                    fut_signal, fut_change, index_name,
                    vol_signal, rvol,
                    macro_title=None, macro_date=None, macro_time=None,
                    macro_sent=None, vol_pct=0,
                    vix_level=15.0, is_monday=False):
    """
    Semáforo FTMO con etiquetas específicas y descriptivas en cada señal.
    Verde: opera. Amarillo: reduce tamaño. Rojo: no operar.
    """
    favor  = []
    contra = []

    # ── 0. FUERZA GENERAL DE LA SEÑAL ─────────────────────────────
    # Si la probabilidad combinada es baja, aviso explícito antes de todo
    if probability < 60:
        contra.append(f'⚠️ Señal débil ({probability}%) — el mercado no tiene dirección clara hoy')

    # ── 1. HISTÓRICO DE GAPS 5 AÑOS ───────────────────────────────
    # Cuántas veces este ticker ha abierto con gap alcista en 5 años
    if raw_direction == 'ALCISTA':
        if hist_pct >= 55:
            favor.append(f'Histórico 1 año favorable — {hist_pct}% de días abre con gap alcista (último año)')
        else:
            contra.append(f'Histórico 1 año desfavorable — solo {hist_pct}% de días abre con gap alcista (último año)')
    else:
        bearish_pct = 100 - hist_pct
        if bearish_pct >= 55:
            favor.append(f'Histórico 1 año favorable — {bearish_pct}% de días abre con gap bajista (último año)')
        else:
            contra.append(f'Histórico 1 año desfavorable — solo {bearish_pct}% de días abre con gap bajista (último año)')

    # ── 2. OVERNIGHT DRIFT ────────────────────────────────────────
    # Tendencia media de apertura vs cierre anterior en 20 días
    if raw_direction == 'ALCISTA':
        if drift_trend == 'alcista':
            favor.append(f'Overnight drift alcista — media +{drift_avg}% últimos 20 días')
        elif drift_trend == 'bajista':
            contra.append(f'Overnight drift bajista — media {drift_avg}% últimos 20 días')
        else:
            favor.append(f'Overnight drift neutral ({drift_avg}%) — sin sesgo claro')
    else:
        if drift_trend == 'bajista':
            favor.append(f'Overnight drift bajista — media {drift_avg}% últimos 20 días')
        elif drift_trend == 'alcista':
            contra.append(f'Overnight drift alcista — media +{drift_avg}% (contra dirección bajista)')
        else:
            favor.append(f'Overnight drift neutral ({drift_avg}%) — sin sesgo claro')

    # ── 3. ÍNDICE (QQQ / SPY) ─────────────────────────────────────
    sign = '+' if fut_change > 0 else ''
    if not futures_warning:
        favor.append(f'{index_name} alineado — {sign}{fut_change}% en últimos 30min')
    else:
        contra.append(f'⚠️ {index_name} CONTRADICE la dirección ({sign}{fut_change}%) — riesgo de absorción')

    # ── 4. RSI + FAKEOUT ──────────────────────────────────────────
    # RSI solo actúa en extremos como advertencia — no suma/resta en normal
    if rsi_val >= 75:
        contra.append(f'⚠️ RSI {rsi_val} — sobrecomprado, riesgo de fakeout alcista')
    elif rsi_val <= 25:
        contra.append(f'⚠️ RSI {rsi_val} — sobrevendido, riesgo de fakeout bajista')
    elif is_fakeout:
        contra.append(f'⚠️ RSI {rsi_val} — zona de riesgo, posible reversión del gap')
    # RSI entre 25-75: solo informativo en tarjeta, no aparece en señales

    # ── 5. GAP ROOM / RESISTENCIA ─────────────────────────────────
    if near_resistance and raw_direction == 'ALCISTA':
        contra.append('Precio muy cerca de resistencia — gap room limitado para subir')
    elif not near_resistance:
        favor.append('Gap room disponible — precio alejado de resistencias')

    # ── 6. VOLUMEN / RVOL (relativo a dirección) ────────────────────
    # vol_score positivo = precio sube con volumen alto (acumulación alcista)
    # vol_score negativo = precio baja con volumen alto (distribución bajista)
    if vol_score >= 2:
        if raw_direction == 'ALCISTA':
            favor.append(f'RVOL {rvol}x — acumulación institucional alcista hoy, confirma dirección')
        else:
            contra.append(f'RVOL {rvol}x — acumulación alcista con dirección bajista, contradice')
    elif vol_score <= -2:
        if raw_direction == 'BAJISTA':
            favor.append(f'RVOL {rvol}x — distribución institucional bajista hoy, confirma dirección')
        else:
            contra.append(f'RVOL {rvol}x — distribución bajista con dirección alcista, contradice')
    elif rvol >= 1.5:
        if (vol_pct > 0 and raw_direction == 'ALCISTA') or (vol_pct < 0 and raw_direction == 'BAJISTA'):
            favor.append(f'RVOL {rvol}x — volumen elevado alineado con dirección {raw_direction.lower()}')
        elif rvol >= 1.5:
            contra.append(f'RVOL {rvol}x — volumen elevado pero contra la dirección {raw_direction.lower()}')
    else:
        favor.append(f'RVOL {rvol}x — volumen normal, sin anomalías')

    # ── 7. EARNINGS ───────────────────────────────────────────────
    if earnings_days <= 1:
        contra.append('🚨 Earnings MAÑANA — volatilidad impredecible, no operar')
    elif earnings_days <= 3:
        contra.append(f'Earnings en {earnings_days} días — spreads amplios, precaución máxima')
    elif earnings_days <= 7:
        contra.append(f'Earnings en {earnings_days} días — considera reducir tamaño')
    else:
        favor.append(f'Sin earnings próximos — entorno limpio para operar')

    # ── 8. SEC FORM 4 — INSIDERS (relativo a dirección) ──────────
    # Compras insider confirman alcista → a favor. Confirman bajista → en contra.
    # Ventas insider confirman bajista → a favor. Confirman alcista → en contra.
    if sec_score >= 3:
        if raw_direction == 'ALCISTA':
            favor.append(f'Insiders COMPRANDO sus propias acciones (SEC Form 4) — confirma dirección alcista')
        else:
            contra.append(f'Insiders COMPRANDO (SEC Form 4) — contradice dirección bajista')
    elif sec_score >= 1:
        if raw_direction == 'ALCISTA':
            favor.append(f'Actividad insider positiva (SEC Form 4) — alineada con dirección alcista')
        else:
            contra.append(f'Actividad insider positiva (SEC Form 4) — contradice dirección bajista')
    elif sec_score <= -3:
        if raw_direction == 'BAJISTA':
            favor.append(f'Insiders VENDIENDO sus propias acciones (SEC Form 4) — confirma dirección bajista')
        else:
            contra.append(f'Insiders VENDIENDO (SEC Form 4) — contradice dirección alcista')
    elif sec_score <= -1:
        if raw_direction == 'BAJISTA':
            favor.append(f'Actividad insider negativa (SEC Form 4) — alineada con dirección bajista')
        else:
            contra.append(f'Actividad insider negativa (SEC Form 4) — contradice dirección alcista')

    # ── 9. SEÑALES INSTITUCIONALES 24H (relativo a dirección) ─────
    # whale_score positivo = upgrades/compras → confirma alcista, contradice bajista
    # whale_score negativo = downgrades/short sellers → confirma bajista, contradice alcista
    if whale_score >= 2:
        if raw_direction == 'ALCISTA':
            favor.append(f'Upgrades / señales institucionales alcistas en 24h (+{whale_score}pts) — confirma dirección')
        else:
            contra.append(f'Upgrades / señales alcistas en 24h (+{whale_score}pts) — contradice dirección bajista')
    elif whale_score <= -2:
        if raw_direction == 'BAJISTA':
            favor.append(f'Downgrades / short sellers en 24h ({whale_score}pts) — confirma dirección bajista')
        else:
            contra.append(f'Downgrades / short sellers en 24h ({whale_score}pts) — contradice dirección alcista')

    # ── 10. EVENTO MACRO ──────────────────────────────────────────
    if macro_event:
        noticia_txt = macro_title[:80] if macro_title else 'Evento macro detectado'
        when_txt = f' · {macro_date} {macro_time}' if macro_date and macro_time else (f' · {macro_date}' if macro_date else '')
        # Noticia positiva + alcista = a favor / positiva + bajista = en contra
        # Noticia negativa + bajista = a favor / negativa + alcista = en contra
        if macro_sent == 'positivo' and raw_direction == 'ALCISTA':
            favor.append(f'📰 Noticia positiva confirma dirección alcista: {noticia_txt}{when_txt}')
        elif macro_sent == 'negativo' and raw_direction == 'BAJISTA':
            favor.append(f'📰 Noticia negativa confirma dirección bajista: {noticia_txt}{when_txt}')
        elif macro_sent == 'positivo' and raw_direction == 'BAJISTA':
            contra.append(f'📰 Noticia positiva contradice dirección bajista: {noticia_txt}{when_txt}')
        elif macro_sent == 'negativo' and raw_direction == 'ALCISTA':
            contra.append(f'📰 Noticia negativa contradice dirección alcista: {noticia_txt}{when_txt}')
        else:
            contra.append(f'⚠️ Evento macro — alta volatilidad: {noticia_txt}{when_txt}')

    # ── DECISIÓN FINAL ────────────────────────────────────────────
    n_favor  = len(favor)
    n_contra = len(contra)

    # ── VIX — volatilidad sistémica del mercado ──────────────────
    if vix_level >= 30:
        contra.append(f'⚠️ VIX {vix_level} — pánico de mercado, volatilidad extrema')
    elif vix_level >= 25:
        contra.append(f'⚠️ VIX {vix_level} — mercado nervioso, reduce exposición')
    elif vix_level <= 15:
        favor.append(f'VIX {vix_level} — mercado tranquilo, baja volatilidad sistémica')

    # ── EFECTO LUNES ──────────────────────────────────────────────
    if is_monday:
        contra.append('📅 Lunes — efecto weekend, gaps impredecibles. Reduce tamaño')

    # hard_block: earnings mañana, fakeout+contradicción, o macro neutra/contraria
    macro_confirma = (macro_event and
                      ((macro_sent == 'positivo' and raw_direction == 'ALCISTA') or
                       (macro_sent == 'negativo' and raw_direction == 'BAJISTA')))
    hard_block = (earnings_days <= 1 or
                  (macro_event and not macro_confirma) or
                  (futures_warning and is_fakeout) or
                  vix_level >= 30)

    if hard_block:
        color  = 'red'
        titulo = '⚠️ Alta volatilidad · Gestiona el riesgo'
        desc   = 'Condición de riesgo extremo activa. Reduce el tamaño y protege el drawdown.'
    elif probability < 60:
        # Señal débil — nunca verde aunque haya señales a favor
        color  = 'yellow'
        titulo = '🟡 CUIDADO — Señal con poca fuerza'
        desc   = f'Probabilidad {probability}% — por debajo del umbral mínimo. Si operas, tamaño mínimo.'
    elif n_favor >= 8 and n_contra == 0:
        color  = 'green'
        titulo = f'🟢 SEÑAL FUERTE 🔥🔥 — Opera con tamaño completo'
        desc   = f'{n_favor} señales a favor, {n_contra} en contra. Setup excepcional.'
    elif n_favor >= 6 and n_contra <= 1:
        color  = 'green'
        titulo = '🟢 SEÑAL CLARA — Opera'
        desc   = f'{n_favor} señales a favor, {n_contra} en contra. Setup sólido.'
    elif n_favor >= 4 and n_contra <= 2:
        color  = 'yellow'
        titulo = '🟡 SEÑAL MODERADA — Reduce tamaño'
        desc   = f'{n_favor} a favor, {n_contra} en contra. Opera con la mitad del tamaño habitual.'
    else:
        color  = 'red'
        titulo = '🔴 SEÑAL DÉBIL — Evita operar'
        desc   = f'Solo {n_favor} señales a favor con {n_contra} en contra. Espera un setup más claro.'

    return {
        'color':  color,
        'titulo': titulo,
        'desc':   desc,
        'favor':  favor,
        'contra': contra
    }


# ═══════════════════════════════════════════════════════════════════
#  CÁLCULO PRINCIPAL
# ═══════════════════════════════════════════════════════════════════
def calculate_gap_probability(ticker):
    try:
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {
                ex.submit(get_historical_gap_stats, ticker): 'hist',
                ex.submit(get_technical_score,      ticker): 'tech',
                ex.submit(get_earnings_info,        ticker): 'earn',
                ex.submit(get_whale_signals,        ticker): 'whale',
                ex.submit(get_overnight_drift,      ticker): 'drift',
                ex.submit(get_gap_room,             ticker): 'room',
                ex.submit(get_futures_sentiment,    ticker): 'futures',

                ex.submit(get_fakeout_detector,     ticker): 'fakeout',
                ex.submit(get_volume_analysis,      ticker): 'volume',
                ex.submit(check_high_impact_news, ticker):   'macro_flag',
                ex.submit(get_sec_insider_activity, ticker): 'sec',
                ex.submit(get_vix_level):                     'vix',
            }
            results = {}
            for fut in as_completed(futs):
                key = futs[fut]
                try:    results[key] = fut.result()
                except: results[key] = None

        # ── Desempaquetar ─────────────────────────────────────────
        hist_prob                            = results.get('hist') or 50
        tech_score                           = results.get('tech') or 0
        earnings                             = results.get('earn') or {'has_earnings': False, 'days_to_earnings': 999, 'status': 'unknown'}
        whale_score, whale_signals           = results.get('whale') or (0, [])
        drift, drift_trend, recent_drift, is_monday = results.get('drift') or (0, 'neutral', 0, False)
        gap_room                             = results.get('room') or {'room_up': 5, 'room_down': 5, 'near_resistance': False}
        fut_score, fut_signal, fut_change, idx_name = results.get('futures') or (0, 'Sin datos', 0, 'Índice')

        is_fakeout, fakeout_reason, rsi_val         = results.get('fakeout') or (False, '', 50)
        vol_data                             = results.get('volume') or {'rvol': 1.0, 'volume_signal': 'Sin datos', 'volume_score': 0, 'absorption': False, 'capitulation': False, 'anomaly': False}
        macro_event, macro_reason, macro_date, macro_time, macro_sent = results.get('macro_flag') or (False, None, None, None, None)
        sec_data                             = results.get('sec') or {'score': 0, 'signals': [], 'summary': 'Sin datos SEC'}
        vix_level                            = results.get('vix') or 15.0

        # ── Probabilidad ─────────────────────────────────────────
        # PRIMERA PASADA: calcular dirección base sin señales relativas
        final = hist_prob + tech_score * 0.3

        dte = earnings.get('days_to_earnings', 999)
        if   dte <= 1: final += 10
        elif dte <= 7: final += 5

        # Señales independientes de dirección (se calculan antes de saber raw_dir)
        final += 5 if drift > 0.1 else -5 if drift < -0.1 else 0
        final += fut_score * 10
        # StockTwits eliminado — ruido retail sin correlación con gaps
        final += vol_data['volume_score'] * 5

        if gap_room.get('near_resistance') and final >= 50: final -= 8
        if is_fakeout and final >= 50:                       final -= 10

        # Dirección base (antes de aplicar señales relativas)
        raw_dir_base = 'ALCISTA' if final >= 50 else 'BAJISTA'

        # SEGUNDA PASADA: señales institucionales y SEC relativas a la dirección
        # Si la dirección es ALCISTA:  whale+ suma, whale- resta
        # Si la dirección es BAJISTA:  whale- suma (confirma), whale+ resta (contradice)
        # La lógica: multiplicamos por -1 si es bajista para invertir el efecto
        direction_mult = 1 if raw_dir_base == 'ALCISTA' else -1

        final += whale_score * 4 * direction_mult
        final += sec_data['score'] * 3 * direction_mult


        final     = max(15, min(85, final))
        raw_dir   = 'ALCISTA' if final >= 50 else 'BAJISTA'
        disp_prob = final if final >= 50 else 100 - final

        fut_warning = (raw_dir == 'ALCISTA' and fut_change <= -0.3) or \
                      (raw_dir == 'BAJISTA' and fut_change >= 0.3)

        # ── Semáforo FTMO ────────────────────────────────────────
        ftmo_signal = get_ftmo_signal(
            probability     = round(disp_prob),
            raw_direction   = raw_dir,
            futures_warning = fut_warning,
            is_fakeout      = is_fakeout,
            near_resistance = gap_room.get('near_resistance', False),
            earnings_days   = dte,
            sec_score       = sec_data['score'],
            whale_score     = whale_score,
            vol_score       = vol_data['volume_score'],
            macro_event     = macro_event,
            macro_title     = macro_reason,
            macro_sent      = macro_sent,
            macro_date      = macro_date,
            macro_time      = macro_time,
            drift_trend     = drift_trend,
            drift_avg       = drift,
            hist_pct        = hist_prob,
            rsi_val         = rsi_val,
            fut_signal      = fut_signal,
            fut_change      = fut_change,
            index_name      = idx_name,
            vol_signal      = vol_data['volume_signal'],
            rvol            = vol_data['rvol'],
            vol_pct         = vol_data.get('price_change_pct', 0),
            vix_level       = vix_level,
            is_monday       = is_monday
        )

        # Precio actual
        current_price = None
        try:
            fi = yf.Ticker(ticker).fast_info
            current_price = float(fi.last_price) if hasattr(fi, 'last_price') else None
        except Exception:
            pass

        return {
            'ticker':           ticker,
            'probability':      round(disp_prob),
            'raw_direction':    raw_dir,
            'current_price':    round(current_price, 2) if current_price else None,
            'ftmo_signal':      ftmo_signal,
            'earnings':         earnings,
            'rsi_value':        rsi_val,
            'overnight_drift':  float(drift),
            'drift_trend':      drift_trend,
            'gap_room':         gap_room,
            'futures_signal':   fut_signal,
            'futures_change':   fut_change,
            'futures_warning':  fut_warning,
            'index_name':       idx_name,

            'is_fakeout':       is_fakeout,
            'fakeout_reason':   fakeout_reason,
            'volume':           vol_data,
            'whale_signals':    whale_signals,
            'whale_score':      float(whale_score),
            'sec':              sec_data,
            'macro_event':      macro_event,
            'macro_reason':     macro_reason,
            'macro_date':       macro_date,
            'macro_time':       macro_time,
            'macro_sent':       macro_sent,
            'vix_level':        vix_level,
        }

    except Exception as e:
        empty_ftmo = {'color': 'red', 'titulo': '🔴 Error', 'desc': str(e),
                      'favor': [], 'contra': []}
        return {
            'ticker': ticker, 'probability': 50, 'raw_direction': 'NEUTRAL',
            'current_price': None, 'ftmo_signal': empty_ftmo,
            'earnings': {'has_earnings': False, 'days_to_earnings': 999, 'status': 'unknown'},
            'rsi_value': 50, 'overnight_drift': 0, 'drift_trend': 'neutral',
            'gap_room': {'room_up': 5, 'room_down': 5, 'near_resistance': False},
            'futures_signal': 'Sin datos', 'futures_change': 0,
            'futures_warning': False, 'index_name': 'Índice',

            'is_fakeout': False, 'fakeout_reason': '',
            'volume': {'rvol': 1.0, 'volume_signal': 'Sin datos', 'volume_score': 0,
                       'absorption': False, 'capitulation': False, 'anomaly': False},
            'whale_signals': [], 'whale_score': 0,
            'sec': {'score': 0, 'signals': [], 'summary': 'Sin datos'},
            'macro_event': False, 'macro_reason': None, 'macro_date': None, 'macro_time': None, 'macro_sent': None,
        }


# ═══════════════════════════════════════════════════════════════════
#  RUTAS
# ═══════════════════════════════════════════════════════════════════
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/analyze', methods=['POST'])
def analyze():
    ticker = request.json.get('ticker', '').upper()
    return jsonify(calculate_gap_probability(ticker))

@app.route('/dashboard')
def dashboard():
    # Limpiar caché antes de recalcular para que los datos sean siempre frescos
    with _cache_lock:
        keys_to_clear = [k for k in _cache if not k.startswith('sec_')]
        for k in keys_to_clear:
            _cache.pop(k, None)
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(calculate_gap_probability, t): t for t in MARKET_LEADERS}
        raw  = {}
        for fut in as_completed(futs):
            t = futs[fut]
            try:    raw[t] = fut.result()
            except: raw[t] = {'ticker': t, 'probability': 50, 'raw_direction': 'NEUTRAL',
                              'ftmo_signal': {'color': 'red', 'titulo': 'Error', 'desc': '', 'favor': [], 'contra': []}}
    return jsonify([raw[t] for t in MARKET_LEADERS if t in raw])

@app.route('/earnings_calendar')
def earnings_calendar():
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(get_earnings_info, t): t for t in MARKET_LEADERS}
        raw  = {}
        for fut in as_completed(futs):
            t = futs[fut]
            try:    raw[t] = fut.result()
            except: raw[t] = {'has_earnings': False, 'days_to_earnings': 999,
                              'earnings_date': None, 'investor_url': INVESTOR_URLS.get(t, ''),
                              'status': 'unknown'}
    results = [{'ticker': t, 'company': COMPANY_NAMES.get(t, t), **raw.get(t, {})}
               for t in MARKET_LEADERS]
    results.sort(key=lambda x: x['days_to_earnings'])
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
