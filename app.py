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

# ── SUPABASE CONFIG ───────────────────────────────────────────────
SUPABASE_URL     = os.environ.get('SUPABASE_URL', '')
SUPABASE_PUB_KEY = os.environ.get('SUPABASE_PUB_KEY', '')
SUPABASE_SECRET  = os.environ.get('SUPABASE_SECRET', '')

def _sb_headers(secret=False):
    key = SUPABASE_SECRET if secret else SUPABASE_PUB_KEY
    return {
        'apikey':        key,
        'Authorization': f'Bearer {key}',
        'Content-Type':  'application/json',
        'Prefer':        'return=representation'
    }

def _sb_get(table, params=None):
    try:
        r = requests.get(f'{SUPABASE_URL}/rest/v1/{table}',
                        headers=_sb_headers(), params=params, timeout=10)
        return r.json() if r.status_code == 200 else []
    except Exception: return []

def _sb_post(table, data):
    try:
        r = requests.post(f'{SUPABASE_URL}/rest/v1/{table}',
                         headers=_sb_headers(secret=True),
                         json=data, timeout=10)
        return r.json() if r.status_code in (200, 201) else None
    except Exception: return None

def _sb_patch(table, row_id, data):
    try:
        r = requests.patch(f'{SUPABASE_URL}/rest/v1/{table}?id=eq.{row_id}',
                          headers=_sb_headers(secret=True),
                          json=data, timeout=10)
        return r.status_code in (200, 204)
    except Exception: return False

def _sb_delete(table, row_id):
    try:
        r = requests.delete(f'{SUPABASE_URL}/rest/v1/{table}?id=eq.{row_id}',
                           headers=_sb_headers(secret=True), timeout=10)
        return r.status_code in (200, 204)
    except Exception: return False


# ── CACHÉ EN MEMORIA CON TTL DIFERENCIADO ────────────────────────
_cache      = {}
_cache_lock = threading.Lock()
_DEPLOY_TS  = time.time()  # invalida entradas anteriores al deploy

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
    'default':       90,   # resto: 90s
    'uw_combined':   90,   # UW combined: 90s
    'uw_flow':       60,   # UW opciones flow: 60s
    'uw_dp':         60,   # UW dark pool: 60s
    'uw_tide':       60,   # UW market tide: 60s
    'uw_congress':   3600, # UW congresistas: 1h
    'uw_oi':         300,  # UW open interest: 5min
}

def _get_ttl(key):
    for prefix, ttl in CACHE_TTL_MAP.items():
        if key.startswith(prefix):
            return ttl
    return CACHE_TTL_MAP['default']

def _cache_get(key, force=False):
    """Retorna None si force=True o si el dato es anterior al deploy actual."""
    if force:
        return None
    with _cache_lock:
        entry = _cache.get(key)
        if not entry:
            return None
        if entry['ts'] < _DEPLOY_TS:   # dato de antes del deploy → obsoleto
            return None
        if (time.time() - entry['ts']) < _get_ttl(key):
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
# Mapa CIK oficial de EDGAR por ticker — evita búsqueda por nombre
SEC_CIK_MAP = {
    'AAPL':  '0000320193',
    'MSFT':  '0000789019',
    'GOOGL': '0001652044',
    'AMZN':  '0001018724',
    'NVDA':  '0001045810',
    'META':  '0001326801',
    'TSLA':  '0001318605',
    'NFLX':  '0001065280',
    'RACE':  '0001673772',
}

def get_sec_insider_activity(ticker):
    """
    Consulta la API pública de la SEC (EDGAR) usando el CIK directo.
    Obtiene Form 4 reales de insiders de los últimos 30 días.
    Usa submissions API oficial: data.sec.gov/submissions/CIK.json
    """
    cache_key = f'sec_{ticker}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    empty = {'score': 0, 'signals': [], 'net_shares': 0, 'summary': 'Sin datos SEC'}

    cik = SEC_CIK_MAP.get(ticker)
    if not cik:
        _cache_set(cache_key, empty)
        return empty

    try:
        # API oficial EDGAR submissions — devuelve filings recientes por CIK
        url  = f'https://data.sec.gov/submissions/{cik}.json'
        resp = requests.get(url,
                            headers={'User-Agent': 'GapAnalyzerPro contact@example.com'},
                            timeout=10)
        if resp.status_code != 200:
            _cache_set(cache_key, empty)
            return empty

        data      = resp.json()
        recent    = data.get('filings', {}).get('recent', {})
        forms     = recent.get('form', [])
        dates     = recent.get('filingDate', [])
        accessions= recent.get('accessionNumber', [])
        reporters = recent.get('reportingOwner', []) if 'reportingOwner' in recent else []

        buys = sells = 0
        signals = []
        cutoff  = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')

        for i, form in enumerate(forms):
            if form not in ('4', '4/A'):
                continue
            file_date = dates[i] if i < len(dates) else ''
            if file_date < cutoff:
                break  # están ordenados por fecha desc, podemos parar

            try:
                filed_dt  = datetime.strptime(file_date, '%Y-%m-%d')
                days_ago  = (datetime.utcnow() - filed_dt).days
                recency_w = 2.0 if days_ago <= 3 else 1.5 if days_ago <= 7 else 1.0
            except Exception:
                recency_w = 1.0

            # Obtener XML del filing para detectar buy/sell real
            acc_clean = accessions[i].replace('-', '') if i < len(accessions) else ''
            acc_fmt   = accessions[i] if i < len(accessions) else ''
            cik_int   = cik.lstrip('0')
            bought = sold = False

            if acc_clean:
                try:
                    xml_url = (f'https://www.sec.gov/Archives/edgar/data/'
                               f'{cik_int}/{acc_clean}/{acc_fmt}.txt')
                    xresp = requests.get(xml_url,
                                         headers={'User-Agent': 'GapAnalyzerPro contact@example.com'},
                                         timeout=6)
                    xtxt = xresp.text.upper()
                    if any(w in xtxt for w in ['P', 'PURCHASE', 'ACQUISITION']):
                        bought = True
                    if any(w in xtxt for w in ['S', 'SALE', 'DISPOSE']):
                        sold = True
                except Exception:
                    # Si no podemos leer el XML, inferir por el índice de filing
                    pass

            # Fallback: usar número de filing como proxy (pares=compra, impares=venta aproximado)
            # Mejor fallback: si no hay info, contar como neutral
            if bought:
                buys += 1
                signals.append({
                    'signal':  '🐋 Insider COMPRA (Form 4)',
                    'detail':  f'{ticker} insider · {file_date} · hace {days_ago}d',
                    'points':  round(3.0 * recency_w, 1),
                    'bullish': True
                })
            elif sold:
                sells += 1
                signals.append({
                    'signal':  '🔴 Insider VENDE (Form 4)',
                    'detail':  f'{ticker} insider · {file_date} · hace {days_ago}d',
                    'points':  round(-2.0 * recency_w, 1),
                    'bullish': False
                })

            if len(signals) >= 5:
                break

        score = round(sum(s['points'] for s in signals), 1)

        if buys > sells:
            summary = f'🟢 {buys} compras insider vs {sells} ventas (últimos 30d)'
        elif sells > buys:
            summary = f'🔴 {sells} ventas insider vs {buys} compras (últimos 30d)'
        elif buys == 0 and sells == 0:
            summary = '⚪ Sin actividad insider en 30 días'
        else:
            summary = f'⚪ Actividad neutral ({buys} compras, {sells} ventas)'

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
    Señales institucionales — 3 fuentes en cascada.
    Clasificación ampliada: upgrades, downgrades, earnings, resultados,
    shorts, blocks, y noticias relevantes de precio.
    
    Lógica de puntuación:
    - Noticias 0-6h:  peso 1.0x — puntuación completa
    - Noticias 6-24h: peso 0.6x — parcialmente descontada
    - Noticias 24-48h: peso 0.3x — mostrar pero no puntuar fuerte
    - Noticias >48h:  peso 0.0x — mostrar con fecha, no puntuar
    """
    cache_key = f'whale_{ticker}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    score, signals = 0, []
    seen_titles = set()

    def _age_hours(pub_str):
        try:
            if not pub_str or not pub_str.strip():
                return 999
            import calendar as c
            dt = parsedate_to_datetime(pub_str)
            ts = c.timegm(dt.utctimetuple())
            h  = (datetime.utcnow() - datetime.utcfromtimestamp(ts)).total_seconds() / 3600
            if h < 0 or h > 720:
                return 999
            return h
        except Exception:
            return 999

    def _age_weight(h):
        """Peso por antigüedad. Noticias >48h no puntúan pero se muestran."""
        if h <= 6:    return 1.0
        elif h <= 24: return 0.6
        elif h <= 48: return 0.3
        elif h <= 96: return 0.0  # mostrar sin puntuar hasta 4 días
        return None  # >4 días: ignorar completamente

    def _age_label(h):
        if h < 1:      return 'hace <1h'
        elif h < 24:   return f'hace {int(h)}h'
        elif h < 48:   return f'hace {int(h)}h ·0.6x'
        elif h < 96:   return f'hace {int(h)}h · sin puntos'
        else:          return f'hace {int(h)}h'

    def _classify(title):
        """
        Clasificación ampliada — captura noticias institucionales reales.
        Devuelve (tipo, puntos_base, es_alcista)
        """
        tl = title.lower()

        # Analistas — upgrades
        if any(w in tl for w in ['upgrade', 'outperform', 'overweight', 'strong buy',
                                   'buy rating', 'raises price target', 'raises pt',
                                   'price target increase', 'initiates', 'starts coverage',
                                   'positive catalyst', 'top pick']):
            return 'UPGRADE', 1.5, True

        # Analistas — downgrades
        if any(w in tl for w in ['downgrade', 'underperform', 'underweight', 'sell rating',
                                   'cuts price target', 'lowers pt', 'price target cut',
                                   'reduce', 'cautious', 'negative catalyst']):
            return 'DOWNGRADE', -1.5, False

        # Short sellers
        if any(w in tl for w in ['hindenburg', 'short seller', 'fraud', 'citron',
                                   'muddy waters', 'short report', 'accounting irregularities']):
            return 'SHORT', -3.0, False

        # Block trades / insiders
        if any(w in tl for w in ['block trade', 'insider buy', 'purchased shares',
                                   'insider purchase', 'insider buying', 'bought shares']):
            return 'BLOCK_BUY', 2.0, True

        if any(w in tl for w in ['insider sell', 'insider sold', 'sold shares', 'insider selling']):
            return 'BLOCK_SELL', -1.5, False

        # Resultados / Earnings
        if any(w in tl for w in ['beats', 'beat estimates', 'beat expectations',
                                   'earnings beat', 'revenue beat', 'tops estimates',
                                   'record revenue', 'record earnings', 'strong results']):
            return 'EARNINGS_BEAT', 2.0, True

        if any(w in tl for w in ['misses', 'miss estimates', 'miss expectations',
                                   'earnings miss', 'revenue miss', 'disappoints',
                                   'weak results', 'cuts guidance', 'lowers guidance']):
            return 'EARNINGS_MISS', -2.0, False

        # Noticias positivas de negocio
        if any(w in tl for w in ['partnership', 'contract', 'deal', 'acquisition',
                                   'buyback', 'dividend increase', 'new product',
                                   'regulatory approval', 'fda approval']):
            return 'POSITIVE_NEWS', 1.0, True

        # Noticias negativas de negocio
        if any(w in tl for w in ['lawsuit', 'investigation', 'fine', 'penalty',
                                   'recall', 'layoffs', 'job cuts', 'warning',
                                   'probe', 'subpoena', 'antitrust']):
            return 'NEGATIVE_NEWS', -1.0, False

        return None, 0, None

    LABEL_MAP = {
        'UPGRADE':       '🏦 Analista UPGRADE',
        'DOWNGRADE':     '🏦 Analista DOWNGRADE',
        'SHORT':         '⚠️ SHORT SELLER',
        'BLOCK_BUY':     '🐋 Insider COMPRA',
        'BLOCK_SELL':    '🔴 Insider VENDE',
        'EARNINGS_BEAT': '💚 Resultados SUPERIORES',
        'EARNINGS_MISS': '🔴 Resultados INFERIORES',
        'POSITIVE_NEWS': '📰 Noticia positiva',
        'NEGATIVE_NEWS': '📰 Noticia negativa',
    }

    def _process_rss(url, source_tag, max_items=15, timeout=7):
        """Procesa un feed RSS y devuelve señales encontradas."""
        nonlocal score
        try:
            root = ET.fromstring(requests.get(url, timeout=timeout,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; GapAnalyzer/1.0)'}).content)
            for item in root.findall('.//item')[:max_items]:
                pub_el   = item.find('pubDate') or item.find('pubdate')
                pub      = pub_el.text if pub_el is not None else ''
                title_el = item.find('title')
                title    = title_el.text.strip() if title_el is not None and title_el.text else ''
                if not title or title in seen_titles:
                    continue
                seen_titles.add(title)
                h     = _age_hours(pub)
                age_w = _age_weight(h)
                if age_w is None:
                    continue  # >4 días, ignorar
                kind, base_pts, _ = _classify(title)
                if kind:
                    pts = round(base_pts * age_w, 1)
                    score += pts  # noticias >48h suman 0 pero aparecen
                    signals.append({
                        'signal': LABEL_MAP[kind],
                        'detail': f'{title[:75]} · {_age_label(h)} [{source_tag}]',
                        'points': pts,
                        'hours':  h
                    })
        except Exception:
            pass

    # ── FUENTE 1: Yahoo Finance RSS (más fiable desde servidor) ──────
    _process_rss(
        f'https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US',
        'YF', max_items=20)

    # ── FUENTE 2: Benzinga RSS ────────────────────────────────────────
    _process_rss(
        f'https://www.benzinga.com/stock/{ticker.lower()}/feed',
        'BZ', max_items=15)

    # ── FUENTE 3: Google News — máxima cobertura ────────────────────
    # 6 queries distintas para capturar todo tipo de noticias relevantes
    gn_queries = [
        f'{ticker} stock analyst upgrade downgrade price target',
        f'{ticker} earnings results beat miss revenue guidance',
        f'{ticker} insider buying selling block trade institutional',
        f'{ticker} stock news today market',
        f'{ticker} partnership deal acquisition contract buyback',
        f'{ticker} lawsuit investigation fine penalty warning layoffs',
    ]
    for q in gn_queries:
        if len(signals) >= 12:
            break
        try:
            root = ET.fromstring(requests.get(
                f'https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en',
                timeout=6).content)
            for item in root.findall('.//item')[:8]:
                pub_el   = item.find('pubDate')
                pub      = pub_el.text if pub_el is not None else ''
                title_el = item.find('title')
                title    = title_el.text.strip() if title_el is not None and title_el.text else ''
                if not title or title in seen_titles:
                    continue
                seen_titles.add(title)
                h     = _age_hours(pub)
                age_w = _age_weight(h)
                if age_w is None:
                    continue
                kind, base_pts, _ = _classify(title)
                if kind:
                    pts = round(base_pts * age_w, 1)
                    score += pts
                    signals.append({
                        'signal': LABEL_MAP[kind],
                        'detail': f'{title[:75]} · {_age_label(h)} [GN]',
                        'points': pts,
                        'hours':  h
                    })
        except Exception:
            continue

    # Ordenar: más recientes primero, luego por puntuación
    signals.sort(key=lambda x: (x.get('hours', 999), -abs(x.get('points', 0))))

    result = round(score, 1), signals[:10]
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
                    vix_level=15.0, is_monday=False,
                    uw_data=None):
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
    # ── UW — UNUSUAL WHALES INSTITUCIONAL ──────────────────────────
    if uw_data:
        uw_score   = uw_data.get('uw_total_score', 0)
        call_k     = uw_data.get('call_premium_k', 0)
        put_k      = uw_data.get('put_premium_k', 0)
        dp_vol     = uw_data.get('dp_volume_m', 0)
        dp_count   = uw_data.get('dp_count', 0)
        tide_pct   = uw_data.get('tide_call_pct', 50)
        tide_bull  = uw_data.get('tide_bullish')
        oi_ratio   = uw_data.get('oi_ratio', 1.0)
        max_pain   = uw_data.get('max_pain', 0)
        cong_buys  = uw_data.get('congress_buys', 0)
        cong_sells = uw_data.get('congress_sells', 0)
        flow_sum   = uw_data.get('flow_summary', '')
        total_k    = uw_data.get('total_flow_k', 0)

        def _uw_is_bullish(val): return val > 0 if raw_direction == 'ALCISTA' else val < 0

        # Premium CALLs vs PUTs
        if call_k > 0 or put_k > 0:
            total_flow = call_k + put_k
            if total_flow > 0:
                call_pct = call_k / total_flow * 100
                flow_str = f'${total_k:.0f}K total' if total_k < 1000 else f'${total_k/1000:.1f}M total'
                if call_pct >= 65:
                    if raw_direction == 'ALCISTA':
                        favor.append(f'🐋 Premium CALLs ${call_k:.0f}K ({call_pct:.0f}%) — flujo institucional alcista ({flow_str})')
                    else:
                        contra.append(f'🐋 Premium CALLs ${call_k:.0f}K ({call_pct:.0f}%) — flujo institucional contra la dirección bajista')
                elif call_pct <= 35:
                    if raw_direction == 'BAJISTA':
                        favor.append(f'🐋 Premium PUTs ${put_k:.0f}K ({100-call_pct:.0f}%) — flujo institucional bajista ({flow_str})')
                    else:
                        contra.append(f'🐋 Premium PUTs ${put_k:.0f}K ({100-call_pct:.0f}%) — flujo institucional contra la dirección alcista')
                else:
                    contra.append(f'🐋 Flujo opciones mixto — {call_pct:.0f}% CALLs vs {100-call_pct:.0f}% PUTs · sin consenso institucional')

        # Dark Pool
        if dp_vol > 0 and dp_count > 0:
            dp_str = f'${dp_vol:.1f}M en {dp_count} prints significativos'
            if uw_score > 0 and raw_direction == 'ALCISTA':
                favor.append(f'🏊 Dark Pool alcista — {dp_str}')
            elif uw_score < 0 and raw_direction == 'BAJISTA':
                favor.append(f'🏊 Dark Pool bajista — {dp_str}')
            elif uw_score > 0 and raw_direction == 'BAJISTA':
                contra.append(f'🏊 Dark Pool comprador ({dp_str}) — contradice dirección bajista')
            elif uw_score < 0 and raw_direction == 'ALCISTA':
                contra.append(f'🏊 Dark Pool vendedor ({dp_str}) — contradice dirección alcista')

        # Market Tide
        if tide_bull is True:
            if raw_direction == 'ALCISTA':
                favor.append(f'🌊 Market Tide alcista — {tide_pct:.0f}% del premium en CALLs (mercado institucional comprador)')
            else:
                contra.append(f'🌊 Market Tide alcista ({tide_pct:.0f}% CALLs) — contradice dirección bajista')
        elif tide_bull is False:
            if raw_direction == 'BAJISTA':
                favor.append(f'🌊 Market Tide bajista — {100-tide_pct:.0f}% del premium en PUTs (mercado institucional vendedor)')
            else:
                contra.append(f'🌊 Market Tide bajista ({100-tide_pct:.0f}% PUTs) — contradice dirección alcista')

        # OI / Max Pain
        if oi_ratio >= 1.5:
            if raw_direction == 'ALCISTA':
                favor.append(f'🎯 OI alcista — {oi_ratio:.1f}x más CALLs que PUTs abiertos{f" · Max Pain ${max_pain:.0f}" if max_pain else ""}')
            else:
                contra.append(f'🎯 OI alcista ({oi_ratio:.1f}x CALLs) — contradice dirección bajista')
        elif oi_ratio <= 0.67:
            if raw_direction == 'BAJISTA':
                favor.append(f'🎯 OI bajista — {1/oi_ratio:.1f}x más PUTs que CALLs abiertos{f" · Max Pain ${max_pain:.0f}" if max_pain else ""}')
            else:
                contra.append(f'🎯 OI bajista ({1/oi_ratio:.1f}x PUTs) — contradice dirección alcista')

        # Congresistas
        if cong_buys > 0:
            if raw_direction == 'ALCISTA':
                favor.append(f'🏛️ {cong_buys} compra{"s" if cong_buys>1 else ""} de congresistas en los últimos 90 días')
            else:
                contra.append(f'🏛️ {cong_buys} compra{"s" if cong_buys>1 else ""} de congresistas — contradice dirección bajista')
        if cong_sells > 0:
            if raw_direction == 'BAJISTA':
                favor.append(f'🏛️ {cong_sells} venta{"s" if cong_sells>1 else ""} de congresistas en los últimos 90 días')
            else:
                contra.append(f'🏛️ {cong_sells} venta{"s" if cong_sells>1 else ""} de congresistas — contradice dirección alcista')

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
# ═══════════════════════════════════════════════════════════════════
#  UNUSUAL WHALES — CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════════
UW_API_KEY  = os.environ.get("UW_API_KEY", "")
UW_BASE     = "https://api.unusualwhales.com"
_uw_env     = os.environ.get("UW_ENABLED", "true").lower()
UW_ENABLED  = bool(UW_API_KEY and UW_API_KEY not in ("", "REEMPLAZA_CON_TU_NUEVA_KEY") and _uw_env != "false")

def _uw_headers():
    return {"Authorization": f"Bearer {UW_API_KEY}", "Accept": "application/json"}

def _uw_get(endpoint, params=None, timeout=8):
    if not UW_ENABLED:
        return None
    try:
        r = requests.get(f"{UW_BASE}{endpoint}", headers=_uw_headers(),
                         params=params or {}, timeout=timeout)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None

def get_uw_options_flow(ticker):
    cache_key = f'uw_flow_{ticker}'
    cached = _cache_get(cache_key)
    if cached is not None: return cached
    empty = {'uw_flow_score':0,'uw_flow_signals':[],'uw_flow_summary':'Sin datos',
             'call_premium':0,'put_premium':0,'total_flow':0,'flow_count':0}
    data = _uw_get(f"/api/stock/{ticker}/flow-recent")
    if not data: _cache_set(cache_key, empty); return empty
    trades = data if isinstance(data, list) else data.get('data', [])
    if not trades: _cache_set(cache_key, empty); return empty
    score=0; signals=[]; total_flow=0; call_prem=0; put_prem=0
    for t in trades[:40]:
        try:
            total_prem = float(t.get('total_premium') or t.get('premium') or 0)
            strike     = float(t.get('strike') or 0)
            underlying = float(t.get('underlying_price') or 0)
            alert_rule = (t.get('alert_rule') or '').upper()
            expiry     = t.get('expiry') or ''
            if total_prem < 30000 or strike == 0 or underlying == 0: continue
            total_flow += total_prem
            if strike > underlying * 1.005:   is_call = True
            elif strike < underlying * 0.995: is_call = False
            else: is_call = True
            is_sweep = 'SWEEP' in alert_rule or 'REPEATED' in alert_rule
            pts = (1.5 if is_sweep else 1.0) if is_call else (-1.5 if is_sweep else -1.0)
            if is_call: call_prem += total_prem
            else:       put_prem  += total_prem
            label = f"{'⚡' if is_sweep else ''}{'📈 CALL' if is_call else '📉 PUT'} ${total_prem/1000:.0f}K · Strike ${strike} · {alert_rule or 'FLOW'}"
            score += pts
            signals.append({'signal':label,'detail':f'{ticker} · ${total_prem/1000:.0f}K premium · {expiry}','points':round(pts,1),'bullish':is_call})
        except Exception: continue
    total_k = total_flow/1000
    if call_prem+put_prem > 0:
        cr = call_prem/(call_prem+put_prem)
        summary = (f'🐋 Flujo alcista — {cr*100:.0f}% CALLs (${total_k:.0f}K)' if cr>=0.65
                   else f'🐋 Flujo bajista — {(1-cr)*100:.0f}% PUTs (${total_k:.0f}K)' if cr<=0.35
                   else f'⚪ Flujo mixto — {cr*100:.0f}% CALLs vs {(1-cr)*100:.0f}% PUTs')
    else: summary = 'Sin flujo significativo'
    result = {'uw_flow_score':round(score,1),'uw_flow_signals':signals[:8],'uw_flow_summary':summary,
              'call_premium':round(call_prem/1000,0),'put_premium':round(put_prem/1000,0),
              'total_flow':round(total_flow/1000,0),'flow_count':len(signals)}
    _cache_set(cache_key, result); return result

def get_uw_darkpool(ticker):
    cache_key = f'uw_dp_{ticker}'
    cached = _cache_get(cache_key)
    if cached is not None: return cached
    empty = {'dp_score':0,'dp_signals':[],'dp_summary':'Sin datos dark pool','dp_volume':0,'dp_count':0}
    data = _uw_get(f"/api/darkpool/{ticker}")
    if not data: _cache_set(cache_key, empty); return empty
    prints = data if isinstance(data, list) else data.get('data', [])
    if not prints: _cache_set(cache_key, empty); return empty
    score=0; signals=[]; total_notional=0; buy_vol=0; sell_vol=0; significant=0
    for p in prints[:50]:
        try:
            price=float(p.get('price') or 0); size=int(p.get('size') or 0)
            nbbo_ask=float(p.get('nbbo_ask') or 0); nbbo_bid=float(p.get('nbbo_bid') or 0)
            if p.get('canceled') or price==0 or size==0: continue
            notional = price*size
            if notional < 250000: continue
            total_notional += notional; significant += 1
            spread = nbbo_ask-nbbo_bid
            ask_pct = (price-nbbo_bid)/spread if spread>0 and nbbo_bid>0 else 0.5
            if ask_pct>=0.65:   pts=1.0;  buy_vol+=size;         sentiment='🟢 Compra'
            elif ask_pct<=0.35: pts=-1.0; sell_vol+=size;        sentiment='🔴 Venta'
            else:               pts=0.2;  buy_vol+=size//2;      sentiment='⚪ Neutro'
            score += pts
            ns = f"${notional/1_000_000:.1f}M" if notional>=1_000_000 else f"${notional/1000:.0f}K"
            signals.append({'signal':f'🏊 Dark Pool {sentiment}','detail':f'{ticker} · {ns} · {size:,} @ ${price:.2f}','points':pts,'bullish':pts>=0})
        except Exception: continue
    tv = buy_vol+sell_vol
    if tv>0 and significant>0:
        br=buy_vol/tv; ns=f"${total_notional/1_000_000:.1f}M"
        summary = (f'🟢 Dark pool alcista — {br*100:.0f}% comprador ({ns}, {significant} prints)' if br>=0.6
                   else f'🔴 Dark pool bajista — {(1-br)*100:.0f}% vendedor ({ns}, {significant} prints)' if br<=0.4
                   else f'⚪ Dark pool neutro ({ns}, {significant} prints)')
    else: summary = 'Sin actividad significativa'
    result = {'dp_score':round(score,1),'dp_signals':signals[:6],'dp_summary':summary,
              'dp_volume':round(total_notional/1_000_000,2),'dp_count':significant}
    _cache_set(cache_key, result); return result

def get_uw_market_tide():
    cache_key = 'uw_tide'
    cached = _cache_get(cache_key)
    if cached is not None: return cached
    empty = {'tide_score':0,'tide_signal':'⚪ Sin datos Market Tide','tide_bullish':None,'call_pct':50,'net_premium':0}
    data = _uw_get("/api/market/market-tide")
    if not data: _cache_set(cache_key, empty); return empty
    try:
        items = data if isinstance(data, list) else data.get('data', [])
        if not items: _cache_set(cache_key, empty); return empty
        total_call=0; total_put=0
        today = datetime.utcnow().strftime('%Y-%m-%d')
        for item in items:
            item_date = (item.get('date') or item.get('timestamp') or '')[:10]
            if item_date == today:
                total_call += abs(float(item.get('net_call_premium') or 0))
                total_put  += abs(float(item.get('net_put_premium')  or 0))
        if total_call==0 and total_put==0:
            for item in items[-20:]:
                total_call += abs(float(item.get('net_call_premium') or 0))
                total_put  += abs(float(item.get('net_put_premium')  or 0))
        if total_call==0 and total_put==0: _cache_set(cache_key, empty); return empty
        total = total_call+total_put
        call_pct = total_call/total*100 if total>0 else 50
        net_prem = round((total_call-total_put)/1_000_000,1)
        if call_pct>=58:   score=1;  bullish=True;  signal=f'🌊 Market Tide ALCISTA — {call_pct:.0f}% CALLs · net ${net_prem}M'
        elif call_pct<=42: score=-1; bullish=False; signal=f'🌊 Market Tide BAJISTA — {100-call_pct:.0f}% PUTs · net ${net_prem}M'
        else:              score=0;  bullish=None;  signal=f'🌊 Market Tide NEUTRO — {call_pct:.0f}% CALLs vs {100-call_pct:.0f}% PUTs'
        result = {'tide_score':score,'tide_signal':signal,'tide_bullish':bullish,'call_pct':round(call_pct,1),'net_premium':net_prem}
        _cache_set(cache_key, result); return result
    except Exception: _cache_set(cache_key, empty); return empty

def get_uw_open_interest(ticker):
    """
    OI por strike usando el endpoint correcto de UW.
    Intenta múltiples endpoints hasta encontrar el que funcione con el plan Basic.
    """
    cache_key = f'uw_oi_{ticker}'
    cached = _cache_get(cache_key)
    if cached is not None: return cached
    empty = {'oi_score':0,'oi_signals':[],'oi_summary':'','max_pain':0,'call_oi':0,'put_oi':0,'oi_ratio':1.0,'oi_expiry':''}

    # Try multiple OI endpoints — Basic plan may have different access
    oi_data = None
    endpoints_tried = []

    # Option 1: options OI summary (most common in Basic)
    for ep in [
        f"/api/stock/{ticker}/options/oi-totals",
        f"/api/stock/{ticker}/option-contracts/stats",
        f"/api/stock/{ticker}/options/volume",
        f"/api/stock/{ticker}/options/summary",
    ]:
        endpoints_tried.append(ep)
        oi_data = _uw_get(ep)
        if oi_data:
            break

    # Option 2: derive from flow-recent — use strike distribution
    if not oi_data:
        flow_raw = _uw_get(f"/api/stock/{ticker}/flow-recent")
        if flow_raw:
            items = flow_raw if isinstance(flow_raw, list) else flow_raw.get('data', [])
            if items:
                call_count=0; put_count=0; call_prem=0; put_prem=0
                underlying=0; strike_volumes = {}
                for t in items[:50]:
                    try:
                        strike    = float(t.get('strike') or 0)
                        und       = float(t.get('underlying_price') or 0)
                        prem      = float(t.get('total_premium') or t.get('premium') or 0)
                        if strike==0 or und==0 or prem<10000: continue
                        if und>0: underlying = und
                        is_call = strike >= und*0.995
                        if strike > und*1.005:   is_call=True
                        elif strike < und*0.995: is_call=False
                        if is_call: call_count+=1; call_prem+=prem
                        else:       put_count+=1;  put_prem+=prem
                        strike_volumes[strike] = strike_volumes.get(strike,0) + prem
                    except Exception: continue

                total = call_count+put_count
                if total > 0:
                    oi_ratio = (call_count/put_count) if put_count>0 else 2.0
                    max_pain = max(strike_volumes, key=strike_volumes.get) if strike_volumes else 0
                    score=0; signals=[]
                    if oi_ratio>=1.5:
                        score+=1.2
                        signals.append({'signal':f'🎯 Flujo alcista — {oi_ratio:.1f}x más CALLs que PUTs','detail':f'{call_count} trades CALL vs {put_count} PUT · ${call_prem/1000:.0f}K vs ${put_prem/1000:.0f}K','points':1.2,'bullish':True})
                    elif oi_ratio<=0.67:
                        score-=1.2
                        signals.append({'signal':f'🎯 Flujo bajista — {1/oi_ratio:.1f}x más PUTs que CALLs','detail':f'{put_count} trades PUT vs {call_count} CALL · ${put_prem/1000:.0f}K vs ${call_prem/1000:.0f}K','points':-1.2,'bullish':False})
                    if max_pain>0 and underlying>0:
                        diff=(max_pain-underlying)/underlying*100
                        if abs(diff)>=0.3:
                            pts=0.6 if diff>0 else -0.6; score+=pts
                            signals.append({'signal':f'🧲 Concentración strike ${max_pain:.0f} — presión {"alcista" if diff>0 else "bajista"}','detail':f'Precio actual ${underlying:.2f} · strike concentrado {diff:+.1f}%','points':pts,'bullish':pts>0})
                    if oi_ratio>=1.5:   summary=f'🎯 Ratio alcista ({oi_ratio:.1f}x calls) · strike concentrado ${max_pain:.0f}'
                    elif oi_ratio<=0.67: summary=f'🎯 Ratio bajista ({1/oi_ratio:.1f}x puts) · strike concentrado ${max_pain:.0f}'
                    else:                summary=f'🎯 Ratio neutro ({oi_ratio:.1f}) · strike concentrado ${max_pain:.0f}'
                    result = {'oi_score':round(score,1),'oi_signals':signals,'oi_summary':summary,
                              'max_pain':max_pain,'call_oi':call_count,'put_oi':put_count,
                              'oi_ratio':round(oi_ratio,2),'oi_expiry':'(flujo reciente)'}
                    _cache_set(cache_key, result); return result

    _cache_set(cache_key, empty); return empty
def get_uw_congress(ticker):
    cache_key = f'uw_congress_{ticker}'
    cached = _cache_get(cache_key)
    if cached is not None: return cached
    empty = {'congress_score':0,'congress_signals':[],'congress_summary':'','congress_buys':0,'congress_sells':0}
    data = _uw_get("/api/congress/recent-trades", params={'ticker':ticker,'limit':50})
    if not data: _cache_set(cache_key, empty); return empty
    trades_list = data if isinstance(data, list) else data.get('data', [])
    score=0; signals=[]; buys=0; sells=0
    for t in trades_list[:10]:
        try:
            txn_type  = (t.get('txn_type') or '').lower()
            politician= t.get('name') or t.get('reporter') or 'Político'
            amounts   = t.get('amounts') or ''
            tx_date   = t.get('transaction_date') or t.get('filed_at') or ''
            days_ago=999
            if tx_date:
                clean=tx_date[:10].strip()
                parts=clean.split('-')
                if len(parts)==3:
                    try: dt_f=datetime(int(parts[0]),int(parts[1]),int(parts[2])); days_ago=(datetime.utcnow()-dt_f).days
                    except Exception: pass
            if days_ago>90: continue
            rw = 1.0 if days_ago<=7 else 0.7 if days_ago<=30 else 0.5 if days_ago<=60 else 0.3
            if 'buy' in txn_type or 'purchase' in txn_type:
                pts=1.5*rw; buys+=1; emoji='🏛️🟢'; action='COMPRA'
            elif 'sell' in txn_type or 'sale' in txn_type or 'exchange' in txn_type:
                pts=-1.0*rw; sells+=1; emoji='🏛️🔴'; action='VENTA'
            else: continue
            score+=pts
            # Color por antigüedad
            if days_ago <= 7:    age_icon = '🟢'
            elif days_ago <= 30: age_icon = '🟡'
            else:                age_icon = '🔘'
            signals.append({'signal':f'{emoji} Congresista {action} {age_icon}','detail':f'{politician} · {amounts} · hace {days_ago}d (peso {rw:.1f}x)','points':round(pts,1),'bullish':pts>0})
        except Exception: continue
    if buys+sells>0: summary=f'🏛️ {buys} compras / {sells} ventas congresistas (90d)'
    elif signals:    summary=f'🏛️ {len(signals)} operaciones detectadas (90d)'
    else:            summary=''
    result={'congress_score':round(score,1),'congress_signals':signals[:5],'congress_summary':summary,'congress_buys':buys,'congress_sells':sells}
    _cache_set(cache_key, result); return result

def get_unusual_whales_data(ticker):
    """Combina todos los módulos UW en paralelo — sin duplicados, con OI/Max Pain."""
    cache_key = f'uw_combined_{ticker}'
    cached = _cache_get(cache_key)
    if cached is not None: return cached
    with ThreadPoolExecutor(max_workers=5) as ex:
        f_flow     = ex.submit(get_uw_options_flow,  ticker)
        f_dp       = ex.submit(get_uw_darkpool,      ticker)
        f_tide     = ex.submit(get_uw_market_tide)
        f_oi       = ex.submit(get_uw_open_interest, ticker)
        f_congress = ex.submit(get_uw_congress,      ticker)
    try: flow_data=f_flow.result()
    except Exception: flow_data={'uw_flow_score':0,'uw_flow_signals':[],'uw_flow_summary':'','call_premium':0,'put_premium':0,'total_flow':0,'flow_count':0}
    try: dp_data=f_dp.result()
    except Exception: dp_data={'dp_score':0,'dp_signals':[],'dp_summary':'','dp_volume':0,'dp_count':0}
    try: tide_data=f_tide.result()
    except Exception: tide_data={'tide_score':0,'tide_signal':'','tide_bullish':None,'call_pct':50,'net_premium':0}
    try: oi_data=f_oi.result()
    except Exception: oi_data={'oi_score':0,'oi_signals':[],'oi_summary':'','max_pain':0,'call_oi':0,'put_oi':0,'oi_ratio':1.0,'oi_expiry':''}
    try: congress_data=f_congress.result()
    except Exception: congress_data={'congress_score':0,'congress_signals':[],'congress_summary':'','congress_buys':0,'congress_sells':0}
    total_score = (flow_data.get('uw_flow_score',0)*1.5 + dp_data.get('dp_score',0)*1.3 +
                   oi_data.get('oi_score',0)*1.2 + tide_data.get('tide_score',0)*0.8 +
                   congress_data.get('congress_score',0)*0.5)
    all_signals = (flow_data.get('uw_flow_signals',[]) + oi_data.get('oi_signals',[]) +
                   dp_data.get('dp_signals',[]) + congress_data.get('congress_signals',[]))
    all_signals.sort(key=lambda x: -abs(x.get('points',0)))
    result = {
        'uw_total_score': round(total_score,1), 'uw_all_signals': all_signals[:12],
        'flow_summary':   flow_data.get('uw_flow_summary',''),
        'dp_summary':     dp_data.get('dp_summary',''),
        'tide_signal':    tide_data.get('tide_signal',''),
        'oi_summary':     oi_data.get('oi_summary',''),
        'congress_summary': congress_data.get('congress_summary',''),
        'call_premium_k': flow_data.get('call_premium',0), 'put_premium_k': flow_data.get('put_premium',0),
        'total_flow_k':   flow_data.get('total_flow',0),   'flow_count':    flow_data.get('flow_count',0),
        'dp_volume_m':    dp_data.get('dp_volume',0),      'dp_count':      dp_data.get('dp_count',0),
        'tide_bullish':   tide_data.get('tide_bullish'),   'tide_call_pct': tide_data.get('call_pct',50),
        'tide_net_premium': tide_data.get('net_premium',0),'max_pain':      oi_data.get('max_pain',0),
        'oi_ratio':       oi_data.get('oi_ratio',1.0),    'call_oi':       oi_data.get('call_oi',0),
        'put_oi':         oi_data.get('put_oi',0),        'oi_expiry':     oi_data.get('oi_expiry',''),
        'congress_buys':  congress_data.get('congress_buys',0), 'congress_sells': congress_data.get('congress_sells',0),
    }
    _cache_set(cache_key, result); return result

# ═══════════════════════════════════════════════════════════════════
#  RUTAS DE DIAGNÓSTICO UW
# ═══════════════════════════════════════════════════════════════════
@app.route('/uw_status')
def uw_status():
    if not UW_ENABLED:
        return jsonify({'status':'DISABLED','uw_connected':False,'uw_enabled':False,'message':'UW desactivado — modo gratuito activo'})
    result = _uw_get("/api/market/market-tide")
    if result:
        return jsonify({'status':'OK','uw_connected':True,'uw_enabled':True})
    return jsonify({'status':'ERROR','uw_connected':False,'uw_enabled':False,'message':'Key inválida o error de conexión'})

@app.route('/uw_mode')
def uw_mode():
    return jsonify({'uw_enabled': UW_ENABLED})

@app.route('/clear_cache', methods=['POST','GET'])
def clear_cache():
    with _cache_lock:
        count=len(_cache); _cache.clear()
    return jsonify({'cleared':True,'entries_removed':count,'message':f'{count} entradas eliminadas'})

@app.route('/clear_ticker_cache/<ticker>')
def clear_ticker_cache(ticker):
    """Limpia solo el caché del ticker especificado — fuerza datos frescos para ese ticker."""
    ticker = ticker.upper()
    with _cache_lock:
        keys = [k for k in _cache if ticker.lower() in k.lower()
                and not k.startswith('sec_')   # mantener SEC (cambia poco)
                and not k.startswith('hist_')  # mantener histórico
                and not k.startswith('drift_') # mantener drift
               ]
        for k in keys:
            _cache.pop(k, None)
    return jsonify({'cleared': True, 'ticker': ticker, 'entries_removed': len(keys)})

@app.route('/cache_status')
def cache_status():
    with _cache_lock:
        entries=[{'key':k,'age_s':round(time.time()-v['ts']),'ttl':_get_ttl(k)} for k,v in _cache.items()]
    entries.sort(key=lambda x: x['age_s'])
    return jsonify({'total':len(entries),'entries':entries[:30]})


def calculate_gap_probability(ticker):
    try:
        with ThreadPoolExecutor(max_workers=5) as ex:
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
                ex.submit(check_high_impact_news,   ticker): 'macro_flag',
                ex.submit(get_sec_insider_activity, ticker): 'sec',
                ex.submit(get_vix_level):                    'vix',
                ex.submit(get_unusual_whales_data,  ticker): 'uw',
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
        uw_data                              = results.get('uw') or {}

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
        direction_mult = 1 if raw_dir_base == 'ALCISTA' else -1

        final += whale_score * 4 * direction_mult
        final += sec_data['score'] * 3 * direction_mult

        # UW score — integrar si está disponible
        uw_score = uw_data.get('uw_total_score', 0)
        if uw_score != 0:
            final += uw_score * 3 * direction_mult
            # Market Tide como filtro sistémico
            tide_bullish = uw_data.get('tide_bullish')
            if tide_bullish is True  and raw_dir_base == 'ALCISTA': final += 5
            elif tide_bullish is False and raw_dir_base == 'BAJISTA': final += 5
            elif tide_bullish is True  and raw_dir_base == 'BAJISTA': final -= 5
            elif tide_bullish is False and raw_dir_base == 'ALCISTA': final -= 5

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
            is_monday       = is_monday,
            uw_data         = uw_data if uw_data else None
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
            'uw_data':          uw_data if uw_data else None,
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
    # Limpiar caché del ticker específico para datos frescos
    # (excepto SEC Form 4 que solo cambia 2 veces/mes)
    with _cache_lock:
        keys_to_clear = [k for k in _cache if (
            ticker.lower() in k.lower() or
            k.startswith('vix') or
            k.startswith('futures_')
        ) and not k.startswith('sec_')]
        for k in keys_to_clear:
            _cache.pop(k, None)
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


@app.route('/debug_analyze/<ticker>')
def debug_analyze(ticker):
    """Debug — muestra errores reales de cada función UW."""
    ticker = ticker.upper()
    results = {}
    
    # Test each UW function individually with real error capture
    def test_fn(name, fn, *args):
        try:
            r = fn(*args)
            results[name] = {'ok': True, 'keys': list(r.keys()) if r else [], 'sample': str(r)[:200]}
        except Exception as e:
            import traceback
            results[name] = {'ok': False, 'error': str(e), 'trace': traceback.format_exc()[-500:]}
    
    test_fn('1_uw_enabled', lambda: UW_ENABLED)
    test_fn('2_api_key_set', lambda: bool(UW_API_KEY and len(UW_API_KEY) > 10))
    test_fn('3_market_tide', get_uw_market_tide)
    test_fn('4_options_flow', get_uw_options_flow, ticker)
    test_fn('5_darkpool', get_uw_darkpool, ticker)
    test_fn('6_congress', get_uw_congress, ticker)
    test_fn('7_combined', get_unusual_whales_data, ticker)
    
    # Also test raw API call
    try:
        import requests as req
        r = req.get(f"{UW_BASE}/api/market/market-tide",
                   headers={"Authorization": f"Bearer {UW_API_KEY}", "Accept": "application/json"},
                   timeout=10)
        results['8_raw_api'] = {'status': r.status_code, 'body': r.text[:300]}
    except Exception as e:
        results['8_raw_api'] = {'error': str(e)}
    
    return jsonify(results)


# ═══════════════════════════════════════════════════════════════════
#  TRADES — CRUD COMPLETO CON SUPABASE
# ═══════════════════════════════════════════════════════════════════

@app.route('/trades', methods=['GET'])
def get_trades():
    """Devuelve todas las operaciones ordenadas por fecha desc."""
    trades = _sb_get('trades', params={'order': 'created_at.desc', 'limit': '500'})
    return jsonify(trades if isinstance(trades, list) else [])

@app.route('/trades', methods=['POST'])
def add_trade():
    """Añade una nueva operación. Guarda también el precio de cierre actual."""
    data = request.json or {}
    ticker = (data.get('ticker') or '').upper()

    # Obtener precio de cierre actual para calcular gap mañana
    close_price = None
    if ticker:
        try:
            tk = yf.Ticker(ticker)
            hist = tk.history(period='1d')
            if not hist.empty:
                close_price = round(float(hist['Close'].iloc[-1]), 4)
        except Exception:
            pass

    trade = {
        'ticker':             ticker,
        'prediction':         data.get('prediction'),
        'system_prediction':  data.get('system_prediction'),
        'system_probability': data.get('system_probability'),
        'contra_signal':      bool(data.get('contra_signal', False)),
        'probability':        data.get('probability'),
        'signal_level':       data.get('signal_level'),
        'vix':                data.get('vix'),
        'futures_dir':        data.get('futures_dir'),
        'gap_real':           data.get('gap_real'),
        'close_price':        close_price,
        'notes':              data.get('notes'),
        'result':             data.get('result', 'pending'),
        'date':               data.get('date'),
        'dow':                data.get('dow'),
        'uw':                 data.get('uw'),
    }
    result = _sb_post('trades', trade)
    if result:
        row = result[0] if isinstance(result, list) else result
        return jsonify({'ok': True, 'trade': row}), 201
    return jsonify({'ok': False, 'error': 'Error guardando en Supabase'}), 500

@app.route('/trades/<int:trade_id>', methods=['PATCH'])
def update_trade(trade_id):
    """Actualiza campos de una operación (resultado, gap_real, notas)."""
    data = request.json or {}
    # Solo permitir campos seguros
    allowed = {'result', 'gap_real', 'notes', 'prediction', 'signal_level', 'vix', 'futures_dir'}
    update = {k: v for k, v in data.items() if k in allowed}
    if not update:
        return jsonify({'ok': False, 'error': 'Sin campos válidos'}), 400
    ok = _sb_patch('trades', trade_id, update)
    return jsonify({'ok': ok})

@app.route('/trades/<int:trade_id>', methods=['DELETE'])
def delete_trade(trade_id):
    """Elimina una operación."""
    ok = _sb_delete('trades', trade_id)
    return jsonify({'ok': ok})

@app.route('/trades/import', methods=['POST'])
def import_trades():
    """Importa operaciones desde localStorage (migración inicial)."""
    trades_list = request.json or []
    if not isinstance(trades_list, list):
        return jsonify({'ok': False}), 400
    imported = 0
    for t in trades_list:
        ticker = (t.get('ticker') or '').upper()
        if not ticker: continue
        trade = {
            'ticker':       ticker,
            'prediction':   t.get('prediction'),
            'probability':  t.get('probability'),
            'signal_level': t.get('signal_level'),
            'vix':          t.get('vix'),
            'futures_dir':  t.get('futures_dir'),
            'gap_real':     t.get('gap_real'),
            'close_price':  t.get('close_price'),
            'notes':        t.get('notes'),
            'result':       t.get('result', 'pending'),
            'date':         t.get('date'),
            'dow':          t.get('dow'),
            'uw':           t.get('uw'),
        }
        if _sb_post('trades', trade):
            imported += 1
    return jsonify({'ok': True, 'imported': imported})

@app.route('/trades/calc_gaps', methods=['POST'])
def calc_gaps():
    """
    Calcula el gap_real automáticamente para operaciones pending sin gap.
    Usa: (precio_apertura_hoy - close_price_guardado) / close_price * 100
    Solo funciona después de las 14:30 UTC (15:30 hora española) — cuando el mercado ya abrió.
    """
    # Validar que el mercado ya abrió hoy (NYSE abre 14:30 UTC)
    now_utc    = datetime.utcnow()
    market_open_utc = now_utc.replace(hour=14, minute=35, second=0, microsecond=0)
    # Si es fin de semana o antes de apertura, bloquear
    weekday = now_utc.weekday()  # 0=lunes, 6=domingo
    if weekday >= 5:
        return jsonify({'ok': False, 'updated': 0,
                       'message': 'El mercado no abre los fines de semana. Usa el botón el lunes después de las 15:35h.'})
    if now_utc < market_open_utc:
        hora_esp = now_utc.hour + 1  # UTC+1 en invierno, aproximado
        faltan   = int((market_open_utc - now_utc).total_seconds() / 60)
        return jsonify({'ok': False, 'updated': 0,
                       'message': f'El mercado aún no ha abierto. Son las ~{hora_esp}:{now_utc.minute:02d}h España. Faltan ~{faltan} minutos para la apertura (15:30h). Vuelve después de las 15:35h.'})

    pending = _sb_get('trades', params={
        'result':      'eq.pending',
        'gap_real':    'is.null',
        'close_price': 'not.is.null',
        'order':       'created_at.desc',
        'limit':       '20'
    })

    if not isinstance(pending, list) or not pending:
        return jsonify({'ok': True, 'updated': 0, 'message': 'No hay operaciones pendientes sin gap'})

    updated = 0
    details = []
    for trade in pending:
        ticker      = (trade.get('ticker') or '').upper()
        close_price = trade.get('close_price')
        trade_id    = trade.get('id')
        trade_date  = (trade.get('created_at') or '')[:10]  # YYYY-MM-DD
        if not ticker or not close_price or not trade_id:
            continue
        try:
            tk   = yf.Ticker(ticker)
            hist = tk.history(period='2d', interval='1d')
            if hist.empty or len(hist) < 1:
                continue

            # Verificar que la apertura es de HOY, no de ayer
            last_date = hist.index[-1].strftime('%Y-%m-%d')
            today_str = now_utc.strftime('%Y-%m-%d')
            if last_date != today_str:
                details.append({'ticker': ticker, 'skip': True,
                               'reason': f'Datos de {last_date}, no de hoy ({today_str}). Mercado aún no actualizó.'})
                continue

            open_price = float(hist['Open'].iloc[-1])
            gap_pct    = round((open_price - float(close_price)) / float(close_price) * 100, 2)

            ok = _sb_patch('trades', trade_id, {'gap_real': gap_pct})
            if ok:
                updated += 1
                details.append({'ticker': ticker, 'close': close_price,
                               'open': open_price, 'gap_pct': gap_pct, 'date': last_date})
        except Exception as e:
            details.append({'ticker': ticker, 'error': str(e)})

    return jsonify({'ok': True, 'updated': updated, 'details': details})


if __name__ == '__main__':
    app.run(debug=True, port=5000)
