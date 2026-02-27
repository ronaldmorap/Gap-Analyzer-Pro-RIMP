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

# ── CACHÉ EN MEMORIA ──────────────────────────────────────────────
_cache      = {}
_cache_lock = threading.Lock()
CACHE_TTL   = 90  # segundos

def _cache_get(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (time.time() - entry['ts']) < CACHE_TTL:
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

NYSE_STOCKS = ['RACE']

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
    'CPI', 'NFP', 'Non-Farm Payroll', 'FOMC', 'GDP',
    'Federal Reserve', 'interest rate decision', 'inflation report', 'jobs report'
]


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
def get_whale_signals(ticker):
    score, signals = 0, []
    queries = [
        f'{ticker}+Goldman+Sachs+OR+Morgan+Stanley+OR+JPMorgan+upgrade+OR+downgrade',
        f'{ticker}+insider+buying+OR+selling+OR+block+trade',
        f'{ticker}+short+seller+OR+hindenburg+OR+citron'
    ]
    for q in queries:
        try:
            root = ET.fromstring(requests.get(
                f'https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en',
                timeout=6).content)
            for item in root.findall('.//item')[:5]:
                pub = item.find('pubDate').text or ''
                if not _news_is_fresh(pub, max_hours=24):
                    continue
                title = item.find('title').text or ''
                tl    = title.lower()
                m     = 1.5 if 'pm' in pub.lower() else 1.0
                if   any(w in tl for w in ['upgrade', 'outperform', 'overweight', 'strong buy', 'buy rating']):
                    pts = round(1.5*m, 1);  score += pts
                    signals.append({'signal': '🏦 Banco UPGRADE', 'detail': title[:80], 'points': pts})
                elif any(w in tl for w in ['downgrade', 'underperform', 'underweight', 'sell rating']):
                    pts = round(-1.5*m, 1); score += pts
                    signals.append({'signal': '🏦 Banco DOWNGRADE', 'detail': title[:80], 'points': pts})
                elif any(w in tl for w in ['hindenburg', 'short seller', 'fraud', 'citron']):
                    pts = round(-3*m, 1);   score += pts
                    signals.append({'signal': '⚠️ SHORT SELLER', 'detail': title[:80], 'points': pts})
                elif any(w in tl for w in ['block trade', 'insider buy', 'purchased shares']):
                    pts = round(2*m, 1);    score += pts
                    signals.append({'signal': '🐋 Block Trade / Insider', 'detail': title[:80], 'points': pts})
        except Exception:
            continue
    return round(score, 1), signals


# ═══════════════════════════════════════════════════════════════════
#  MACRO — solo eventos de alto impacto (no sentiment genérico)
# ═══════════════════════════════════════════════════════════════════
def check_high_impact_news():
    """
    Devuelve (activo, titulo_noticia, fecha_pub, hora_pub)
    Si no hay evento macro: (False, None, None, None)
    """
    cached = _cache_get('high_impact_news')
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
                    result = (True, raw_title[:120], fecha_str, hora_str)
                    _cache_set('high_impact_news', result)
                    return result
        result = (False, None, None, None)
        _cache_set('high_impact_news', result)
        return result
    except Exception:
        return False, None, None, None


# ═══════════════════════════════════════════════════════════════════
#  FUTUROS / ÍNDICE
# ═══════════════════════════════════════════════════════════════════
def get_futures_sentiment(ticker='AAPL'):
    use_spy    = ticker in NYSE_STOCKS
    idx_tk     = 'SPY' if use_spy else 'QQQ'
    index_name = 'S&P 500' if use_spy else 'Nasdaq'
    cache_key  = f'futures_{idx_tk}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        hist = yf.Ticker(idx_tk).history(period='2d', interval='5m')
        if len(hist) < 6:
            return 0, 'Sin datos', 0, index_name
        last30 = hist.tail(6)
        chg    = round(float(
            (last30['Close'].iloc[-1] - last30['Close'].iloc[0])
            / last30['Close'].iloc[0] * 100), 3)
        emoji  = '🟢' if chg >= 0.3 else '🔴' if chg <= -0.3 else '⚪'
        sign   = '+' if chg > 0 else ''
        score  = 1 if chg >= 0.3 else -1 if chg <= -0.3 else 0
        result = (score, f'{emoji} {index_name} {sign}{chg}% últimos 30min', chg, index_name)
        _cache_set(cache_key, result)
        return result
    except Exception:
        return 0, 'Sin datos de futuros', 0, index_name


# ═══════════════════════════════════════════════════════════════════
#  SOCIAL — StockTwits
# ═══════════════════════════════════════════════════════════════════
def get_stocktwits_sentiment(ticker):
    try:
        resp = requests.get(
            f'https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json',
            timeout=7)
        if resp.status_code != 200:
            return 0, 'Sin datos', 50, 0
        messages = resp.json().get('messages', [])
        if not messages:
            return 0, 'Sin datos', 50, 0
        bullish = bearish = 0
        for msg in messages[:30]:
            sd = msg.get('entities', {}).get('sentiment', {})
            if sd:
                if sd.get('basic') == 'Bullish':  bullish += 1
                elif sd.get('basic') == 'Bearish': bearish += 1
        total    = bullish + bearish
        bull_pct = round((bullish / total) * 100) if total > 0 else 50
        if   bull_pct >= 65: return  1,   '🟢 Muy alcista',          bull_pct, len(messages)
        elif bull_pct >= 55: return  0.5, '🟡 Ligeramente alcista',  bull_pct, len(messages)
        elif bull_pct <= 35: return -1,   '🔴 Muy bajista',          bull_pct, len(messages)
        elif bull_pct <= 45: return -0.5, '🟠 Ligeramente bajista',  bull_pct, len(messages)
        return 0, '⚪ Neutral', bull_pct, len(messages)
    except Exception:
        return 0, 'Sin datos StockTwits', 50, 0


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
    try:
        hist = yf.Ticker(ticker).history(period='30d', interval='1d')
        if len(hist) < 5:
            return 0, 'neutral', 0
        rets = [(float(hist['Open'].iloc[i]) - float(hist['Close'].iloc[i-1]))
                / float(hist['Close'].iloc[i-1]) * 100
                for i in range(1, len(hist))
                if float(hist['Close'].iloc[i-1]) > 0]
        if not rets:
            return 0, 'neutral', 0
        avg    = round(sum(rets) / len(rets), 3)
        recent = round(sum(rets[-5:]) / 5, 3)
        trend  = 'alcista' if avg > 0.1 else 'bajista' if avg < -0.1 else 'neutral'
        return avg, trend, recent
    except Exception:
        return 0, 'neutral', 0


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
    try:
        hist = yf.Ticker(ticker).history(period='5y')
        if hist.empty:
            return 50
        up = down = 0
        for i in range(1, len(hist)):
            g = (hist['Open'].iloc[i] - hist['Close'].iloc[i-1]) / hist['Close'].iloc[i-1] * 100
            if g > 0.1:   up   += 1
            elif g < -0.1: down += 1
        total = up + down
        return round((up / total) * 100) if total > 0 else 50
    except Exception:
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
                    macro_title=None, macro_date=None, macro_time=None):
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
            favor.append(f'Histórico 5 años favorable — {hist_pct}% de días abre con gap alcista')
        else:
            contra.append(f'Histórico 5 años desfavorable — solo {hist_pct}% de días abre con gap alcista')
    else:
        bearish_pct = 100 - hist_pct
        if bearish_pct >= 55:
            favor.append(f'Histórico 5 años favorable — {bearish_pct}% de días abre con gap bajista')
        else:
            contra.append(f'Histórico 5 años desfavorable — solo {bearish_pct}% de días abre con gap bajista')

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
    if is_fakeout:
        contra.append(f'⚠️ RSI {rsi_val} — zona extrema, gap puede revertirse (fakeout)')
    elif rsi_val > 60:
        favor.append(f'RSI {rsi_val} — momentum alcista, zona operable')
    elif rsi_val < 40:
        favor.append(f'RSI {rsi_val} — momentum bajista, zona operable')
    else:
        favor.append(f'RSI {rsi_val} — zona neutral, operable')

    # ── 5. GAP ROOM / RESISTENCIA ─────────────────────────────────
    if near_resistance and raw_direction == 'ALCISTA':
        contra.append('Precio muy cerca de resistencia — gap room limitado para subir')
    elif not near_resistance:
        favor.append('Gap room disponible — precio alejado de resistencias')

    # ── 6. VOLUMEN / RVOL ─────────────────────────────────────────
    if vol_score >= 2:
        favor.append(f'RVOL {rvol}x — acumulación institucional detectada hoy')
    elif vol_score <= -2:
        contra.append(f'RVOL {rvol}x — distribución institucional detectada hoy')
    elif rvol >= 1.5:
        favor.append(f'RVOL {rvol}x — volumen elevado, interés institucional')
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

    # ── 8. SEC FORM 4 — INSIDERS ──────────────────────────────────
    if sec_score >= 3:
        favor.append(f'Insiders comprando sus propias acciones (SEC Form 4) — señal alcista fuerte')
    elif sec_score >= 1:
        favor.append(f'Actividad insider ligeramente positiva (SEC Form 4)')
    elif sec_score <= -3:
        contra.append(f'Insiders VENDIENDO sus propias acciones (SEC Form 4) — señal bajista fuerte')
    elif sec_score <= -1:
        contra.append(f'Actividad insider ligeramente negativa (SEC Form 4)')

    # ── 9. SEÑALES INSTITUCIONALES 24H ────────────────────────────
    if whale_score >= 2:
        favor.append(f'Upgrades de bancos / block trades detectados en últimas 24h (+{whale_score}pts)')
    elif whale_score <= -2:
        contra.append(f'Downgrades de bancos / short sellers detectados en últimas 24h ({whale_score}pts)')

    # ── 10. EVENTO MACRO ──────────────────────────────────────────
    if macro_event:
        # Mostrar título real, fecha y hora de la noticia
        noticia_txt = macro_title[:80] if macro_title else 'Evento macro detectado'
        when_txt    = ''
        if macro_date and macro_time:
            when_txt = f' · {macro_date} {macro_time}'
        elif macro_date:
            when_txt = f' · {macro_date}'
        contra.append(f'📰 {noticia_txt}{when_txt} — no operar')

    # ── DECISIÓN FINAL ────────────────────────────────────────────
    n_favor  = len(favor)
    n_contra = len(contra)

    hard_block = (earnings_days <= 1 or macro_event or
                  (futures_warning and is_fakeout))

    if hard_block:
        color  = 'red'
        titulo = '🔴 NO OPERAR HOY'
        desc   = 'Condición de riesgo extremo activa. Protege el drawdown.'
    elif probability < 60:
        # Señal débil — nunca verde aunque haya señales a favor
        color  = 'yellow'
        titulo = '🟡 CUIDADO — Señal con poca fuerza'
        desc   = f'Probabilidad {probability}% — por debajo del umbral mínimo. Si operas, tamaño mínimo.'
    elif n_favor >= 6 and n_contra <= 1:
        color  = 'green'
        titulo = '🟢 SEÑAL CLARA — OPERAR'
        desc   = f'{n_favor} señales a favor, {n_contra} en contra. Setup limpio.'
    elif n_favor >= 4 and n_contra <= 2:
        color  = 'yellow'
        titulo = '🟡 SEÑAL MODERADA — Reducir tamaño'
        desc   = f'{n_favor} a favor, {n_contra} en contra. Opera con la mitad del tamaño habitual.'
    else:
        color  = 'red'
        titulo = '🔴 SEÑAL DÉBIL — NO OPERAR'
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
                ex.submit(get_stocktwits_sentiment, ticker): 'social',
                ex.submit(get_fakeout_detector,     ticker): 'fakeout',
                ex.submit(get_volume_analysis,      ticker): 'volume',
                ex.submit(check_high_impact_news):           'macro_flag',
                ex.submit(get_sec_insider_activity, ticker): 'sec',
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
        drift, drift_trend, recent_drift     = results.get('drift') or (0, 'neutral', 0)
        gap_room                             = results.get('room') or {'room_up': 5, 'room_down': 5, 'near_resistance': False}
        fut_score, fut_signal, fut_change, idx_name = results.get('futures') or (0, 'Sin datos', 0, 'Índice')
        soc_score, soc_trend, bull_pct, msg_count   = results.get('social') or (0, 'Sin datos', 50, 0)
        is_fakeout, fakeout_reason, rsi_val         = results.get('fakeout') or (False, '', 50)
        vol_data                             = results.get('volume') or {'rvol': 1.0, 'volume_signal': 'Sin datos', 'volume_score': 0, 'absorption': False, 'capitulation': False, 'anomaly': False}
        macro_event, macro_reason, macro_date, macro_time = results.get('macro_flag') or (False, None, None, None)
        sec_data                             = results.get('sec') or {'score': 0, 'signals': [], 'summary': 'Sin datos SEC'}

        # ── Probabilidad ─────────────────────────────────────────
        final = hist_prob + tech_score * 0.3

        dte = earnings.get('days_to_earnings', 999)
        if   dte <= 1: final += 10
        elif dte <= 7: final += 5

        final += whale_score * 4
        final += sec_data['score'] * 3
        final += 5 if drift > 0.1 else -5 if drift < -0.1 else 0
        final += fut_score * 10
        final += soc_score * 6
        final += vol_data['volume_score'] * 5

        if gap_room.get('near_resistance') and final >= 50: final -= 8
        if is_fakeout and final >= 50:                       final -= 10

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
            rvol            = vol_data['rvol']
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
            'social_trend':     soc_trend,
            'bull_pct':         bull_pct,
            'msg_count':        msg_count,
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
            'social_trend': 'Sin datos', 'bull_pct': 50, 'msg_count': 0,
            'is_fakeout': False, 'fakeout_reason': '',
            'volume': {'rvol': 1.0, 'volume_signal': 'Sin datos', 'volume_score': 0,
                       'absorption': False, 'capitulation': False, 'anomaly': False},
            'whale_signals': [], 'whale_score': 0,
            'sec': {'score': 0, 'signals': [], 'summary': 'Sin datos'},
            'macro_event': False, 'macro_reason': None, 'macro_date': None, 'macro_time': None,
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
