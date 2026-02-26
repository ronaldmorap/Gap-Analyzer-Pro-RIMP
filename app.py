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

app = Flask(__name__)

# ── CONSTANTES ────────────────────────────────────────────────────
MARKET_LEADERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'NFLX', 'RACE']

COMPANY_NAMES = {
    'AAPL': 'Apple', 'MSFT': 'Microsoft', 'GOOGL': 'Google',
    'AMZN': 'Amazon', 'NVDA': 'Nvidia',   'META': 'Meta',
    'TSLA': 'Tesla',  'NFLX': 'Netflix',  'RACE': 'Ferrari'
}

SECTOR_PEERS = {
    'META': 'GOOGL', 'GOOGL': 'META', 'AAPL': 'MSFT', 'MSFT': 'AAPL',
    'NVDA': 'AMD',   'AMZN':  'MSFT', 'TSLA': 'RIVN',
    'NFLX': 'DIS',   'RACE':  'TSLA'
}

NASDAQ_STOCKS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'NFLX']
NYSE_STOCKS   = ['RACE']

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
    'Federal Reserve meeting', 'interest rate decision',
    'inflation report', 'jobs report'
]


# ═══════════════════════════════════════════════════════════════════
#  HELPER: filtro de fecha RSS  (solo últimas 24 h)
# ═══════════════════════════════════════════════════════════════════
def _news_is_fresh(pub_date_str, max_hours=24):
    """True si la noticia tiene menos de max_hours horas."""
    try:
        dt = parsedate_to_datetime(pub_date_str)
        # Convertir a naive UTC para comparar
        import calendar
        ts = calendar.timegm(dt.utctimetuple())
        age_h = (datetime.utcnow() - datetime.utcfromtimestamp(ts)).total_seconds() / 3600
        return age_h <= max_hours
    except Exception:
        return True   # si no podemos parsear la fecha, la incluimos por defecto


# ═══════════════════════════════════════════════════════════════════
#  EARNINGS  (triple método, ventana 30 días pasados)
# ═══════════════════════════════════════════════════════════════════
def _norm_ts(ts):
    try:
        return pd.Timestamp(str(ts)[:10]).normalize()
    except Exception:
        return None


def get_earnings_info(ticker):
    investor_url = INVESTOR_URLS.get(ticker, '')
    stock        = yf.Ticker(ticker)
    today        = pd.Timestamp.now().normalize()

    # Método 1: get_earnings_dates
    try:
        ed = stock.get_earnings_dates(limit=20)
        if ed is not None and not ed.empty:
            candidates = [(_norm_ts(dt), dt) for dt in ed.index]
            candidates = [(norm, dt) for norm, dt in candidates if norm is not None]
            days_list  = [((norm - today).days, norm) for norm, _ in candidates]
            future = sorted([(d, n) for d, n in days_list if 0 <= d <= 90])
            if future:
                days, norm = future[0]
                return {'has_earnings': True, 'days_to_earnings': days,
                        'earnings_date': str(norm.date()), 'earnings_time': 'After Market (22:00 CET)',
                        'investor_url': investor_url, 'status': 'upcoming'}
            past = sorted([(d, n) for d, n in days_list if -30 <= d < 0], reverse=True)
            if past:
                days, norm = past[0]
                return {'has_earnings': True, 'days_to_earnings': days,
                        'earnings_date': str(norm.date()), 'earnings_time': 'After Market (22:00 CET)',
                        'investor_url': investor_url, 'status': 'published'}
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
            days = (norm - today).days
            if -30 <= days <= 90:
                return {'has_earnings': True, 'days_to_earnings': days,
                        'earnings_date': str(norm.date()), 'earnings_time': 'After Market (22:00 CET)',
                        'investor_url': investor_url,
                        'status': 'published' if days < 0 else 'upcoming'}
    except Exception:
        pass

    # Método 3: info dict
    try:
        info = stock.info
        for key in ['earningsDate', 'earningsTimestamp']:
            raw = info.get(key)
            if not raw: continue
            if isinstance(raw, list): raw = raw[0]
            norm = (pd.Timestamp.fromtimestamp(raw).normalize()
                    if isinstance(raw, (int, float)) else _norm_ts(raw))
            if norm is None: continue
            days = (norm - today).days
            if -30 <= days <= 90:
                return {'has_earnings': True, 'days_to_earnings': days,
                        'earnings_date': str(norm.date()), 'earnings_time': 'After Market (22:00 CET)',
                        'investor_url': investor_url,
                        'status': 'published' if days < 0 else 'upcoming'}
    except Exception:
        pass

    return {'has_earnings': False, 'days_to_earnings': 999, 'earnings_date': None,
            'earnings_time': None, 'investor_url': investor_url, 'status': 'unknown'}


# ═══════════════════════════════════════════════════════════════════
#  NOTICIAS
# ═══════════════════════════════════════════════════════════════════
def check_high_impact_news():
    try:
        q    = 'CPI+OR+NFP+OR+FOMC+OR+GDP+OR+inflation+report+OR+jobs+report+today'
        root = ET.fromstring(requests.get(
            f'https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en',
            timeout=7).content)
        for item in root.findall('.//item')[:10]:
            title = (item.find('title').text or '').upper()
            for kw in HIGH_IMPACT_KEYWORDS:
                if kw.upper() in title:
                    return True, f'Evento macro detectado: {kw}'
        return False, None
    except Exception:
        return False, None


def get_news_sentiment(ticker):
    try:
        company = COMPANY_NAMES.get(ticker, ticker)
        root = ET.fromstring(requests.get(
            f'https://news.google.com/rss/search?q={company}+stock&hl=en-US&gl=US&ceid=US:en',
            timeout=7).content)
        sentiments, news_list = [], []
        for item in root.findall('.//item')[:8]:
            title = item.find('title').text or ''
            date  = (item.find('pubDate').text or '')[:16]
            s     = TextBlob(title).sentiment.polarity
            sentiments.append(s)
            news_list.append({'title': title[:80], 'sentiment': round(s, 2), 'published': date})
        avg = sum(sentiments) / len(sentiments) if sentiments else 0
        return round(avg, 3), news_list
    except Exception:
        return 0, []


def get_macro_sentiment():
    queries = [
        'Federal+Reserve+interest+rates', 'Trump+economy+market+tariffs',
        'stock+market+inflation+GDP', 'hedge+fund+institutional+investors',
        'Warren+Buffett+Berkshire'
    ]
    all_s, all_news = [], []
    for q in queries:
        try:
            root = ET.fromstring(requests.get(
                f'https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en',
                timeout=6).content)
            for item in root.findall('.//item')[:2]:
                title = item.find('title').text or ''
                date  = (item.find('pubDate').text or '')[:16]
                s     = TextBlob(title).sentiment.polarity
                all_s.append(s); all_news.append({'title': title[:80], 'sentiment': round(s,2), 'published': date})
        except Exception:
            continue
    return round(sum(all_s)/len(all_s), 3) if all_s else 0, all_news[:6]


# ═══════════════════════════════════════════════════════════════════
#  BALLENAS  — solo noticias de las últimas 24 h
# ═══════════════════════════════════════════════════════════════════
def get_whale_signals(ticker):
    score, signals = 0, []
    queries = [
        f'{ticker}+insider+buying+OR+selling',
        f'{ticker}+Goldman+Sachs+OR+Morgan+Stanley+upgrade+OR+downgrade',
        f'{ticker}+hindenburg+OR+short+seller'
    ]
    for q in queries:
        try:
            root = ET.fromstring(requests.get(
                f'https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en',
                timeout=6).content)
            for item in root.findall('.//item')[:5]:
                # ── FILTRO DE FRESCURA: descartar noticias >24 h ──
                pub_date_str = item.find('pubDate').text or ''
                if not _news_is_fresh(pub_date_str, max_hours=24):
                    continue

                title = item.find('title').text or ''
                tl    = title.lower()
                m     = 1.5 if 'pm' in pub_date_str.lower() else 1.0

                if   any(w in tl for w in ['insider buy','purchased','acquired']):
                    pts = round(3*m,1);    score += pts; signals.append({'signal':'🐋 Insider COMPRA', 'detail':title[:80],'points':pts})
                elif any(w in tl for w in ['insider sell','disposed']):
                    pts = round(-2.5*m,1); score += pts; signals.append({'signal':'🔴 Insider VENDE',  'detail':title[:80],'points':pts})
                elif any(w in tl for w in ['upgrade','outperform','overweight','strong buy']):
                    pts = round(1.5*m,1);  score += pts; signals.append({'signal':'🏦 Banco UPGRADE',  'detail':title[:80],'points':pts})
                elif any(w in tl for w in ['downgrade','underperform','underweight']):
                    pts = round(-1.5*m,1); score += pts; signals.append({'signal':'🏦 Banco DOWNGRADE','detail':title[:80],'points':pts})
                elif any(w in tl for w in ['hindenburg','short seller','fraud']):
                    pts = round(-3*m,1);   score += pts; signals.append({'signal':'⚠️ SHORT SELLER',  'detail':title[:80],'points':pts})
        except Exception:
            continue
    return round(score, 1), signals


# ═══════════════════════════════════════════════════════════════════
#  FUTUROS / ÍNDICE
# ═══════════════════════════════════════════════════════════════════
def get_futures_sentiment(ticker='AAPL'):
    try:
        use_spy    = ticker in NYSE_STOCKS
        idx_tk     = 'SPY' if use_spy else 'QQQ'
        index_name = 'S&P 500' if use_spy else 'Nasdaq'
        hist       = yf.Ticker(idx_tk).history(period='2d', interval='5m')
        if len(hist) < 6:
            return 0, 'Sin datos', 0, index_name
        last30 = hist.tail(6)
        chg = round(float((last30['Close'].iloc[-1]-last30['Close'].iloc[0])/last30['Close'].iloc[0]*100), 3)
        emoji = '🟢' if chg >= 0.3 else '🔴' if chg <= -0.3 else '⚪'
        sign  = '+' if chg > 0 else ''
        score = 1 if chg >= 0.3 else -1 if chg <= -0.3 else 0
        return score, f'{emoji} {index_name} {sign}{chg}% últimos 30min', chg, index_name
    except Exception:
        return 0, 'Sin datos de futuros', 0, 'Índice'


# ═══════════════════════════════════════════════════════════════════
#  SOCIAL
# ═══════════════════════════════════════════════════════════════════
def get_stocktwits_sentiment(ticker):
    try:
        resp = requests.get(
            f'https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json', timeout=7)
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
        bull_pct = round((bullish/total)*100) if total > 0 else 50
        if   bull_pct >= 65: return  1,   '🟢 Muy alcista',         bull_pct, len(messages)
        elif bull_pct >= 55: return  0.5, '🟡 Ligeramente alcista', bull_pct, len(messages)
        elif bull_pct <= 35: return -1,   '🔴 Muy bajista',         bull_pct, len(messages)
        elif bull_pct <= 45: return -0.5, '🟠 Ligeramente bajista', bull_pct, len(messages)
        return 0, '⚪ Neutral', bull_pct, len(messages)
    except Exception:
        return 0, 'Sin datos StockTwits', 50, 0


# ═══════════════════════════════════════════════════════════════════
#  TÉCNICO
# ═══════════════════════════════════════════════════════════════════
def get_fakeout_detector(ticker):
    try:
        hist  = yf.Ticker(ticker).history(period='3mo')
        if hist.empty: return False, 'Sin datos', 50
        close = hist['Close']
        gain  = close.diff().clip(lower=0).rolling(14).mean()
        loss  = (-close.diff().clip(upper=0)).rolling(14).mean()
        rsi   = float(100 - (100/(1+gain.iloc[-1]/loss.iloc[-1])))
        risk, reasons = 0, []
        if rsi > 72:  risk += 2; reasons.append(f'RSI sobrecomprado ({round(rsi,1)})')
        elif rsi > 68:risk += 1; reasons.append(f'RSI alto ({round(rsi,1)})')
        if rsi < 28:  risk += 2; reasons.append(f'RSI sobrevendido ({round(rsi,1)})')
        lc = float((close.iloc[-1]-close.iloc[-2])/close.iloc[-2]*100)
        if lc > 4:    risk += 2; reasons.append(f'Subida muy alta (+{round(lc,1)}%)')
        elif lc < -4: risk += 1; reasons.append(f'Caída muy alta ({round(lc,1)}%)')
        return risk >= 3, ' + '.join(reasons) if reasons else 'Sin riesgo detectado', round(rsi,1)
    except Exception:
        return False, 'Sin datos', 50


def get_volume_analysis(ticker):
    empty = {'rvol':1.0,'volume_signal':'⚪ Sin datos','volume_score':0,
             'absorption':False,'capitulation':False,'anomaly':False,
             'last_volume':0,'avg_volume':0,'price_change_pct':0}
    try:
        hist = yf.Ticker(ticker).history(period='30d', interval='1d')
        if len(hist) < 10: return empty
        avg_vol  = float(hist['Volume'][:-1].mean())
        last_vol = float(hist['Volume'].iloc[-1])
        rvol     = round(last_vol/avg_vol, 2) if avg_vol > 0 else 1.0
        last_c   = float(hist['Close'].iloc[-1])
        prev_c   = float(hist['Close'].iloc[-2])
        pct      = (last_c-prev_c)/prev_c*100
        score = 0; absorption = capitulation = anomaly = False
        if   rvol >= 2.0 and pct >  0.3: score= 2; vsig=f'⚡ Acumulación institucional (RVOL {rvol}x)'; anomaly=True
        elif rvol >= 2.0 and pct < -0.3: score=-2; vsig=f'🔴 Distribución institucional (RVOL {rvol}x)'; anomaly=True
        elif rvol >= 2.0:                 score=-1; vsig=f'⚠️ Absorción detectada (RVOL {rvol}x)'; absorption=True; anomaly=True
        elif rvol >= 1.5 and pct >  0.2: score= 1; vsig=f'🟡 Volumen elevado alcista (RVOL {rvol}x)'
        elif rvol >= 1.5 and pct < -0.2: score=-1; vsig=f'🟠 Volumen elevado bajista (RVOL {rvol}x)'
        else:                              vsig=f'⚪ Volumen normal (RVOL {rvol}x)'
        try:
            hi = yf.Ticker(ticker).history(period='2d', interval='5m')
            if len(hi) >= 12:
                t3 = hi.tail(3)
                if (float(t3['Volume'].sum()) > float(hi['Volume'].mean())*9 and
                    float((t3['Close'].iloc[-1]-t3['Close'].iloc[0])/t3['Close'].iloc[0]*100) > 0.3):
                    capitulation=True; score+=1.5; vsig+=' + 🚀 Capitulación cortos'
        except Exception:
            pass
        return {'rvol':rvol,'volume_signal':vsig,'volume_score':round(score,1),
                'absorption':absorption,'capitulation':capitulation,'anomaly':anomaly,
                'last_volume':int(last_vol),'avg_volume':int(avg_vol),'price_change_pct':round(pct,2)}
    except Exception:
        return empty


def get_overnight_drift(ticker):
    try:
        hist = yf.Ticker(ticker).history(period='30d', interval='1d')
        if len(hist) < 5: return 0, 'neutral', 0
        rets = [(float(hist['Open'].iloc[i])-float(hist['Close'].iloc[i-1]))/float(hist['Close'].iloc[i-1])*100
                for i in range(1,len(hist)) if float(hist['Close'].iloc[i-1]) > 0]
        if not rets: return 0, 'neutral', 0
        avg    = round(sum(rets)/len(rets), 3)
        recent = round(sum(rets[-5:])/5, 3)
        trend  = 'alcista' if avg > 0.1 else 'bajista' if avg < -0.1 else 'neutral'
        return avg, trend, recent
    except Exception:
        return 0, 'neutral', 0


def get_gap_room(ticker):
    empty = {'room_up':5,'room_down':5,'near_resistance':False,'near_support':False,'resistance':0,'support':0,'current':0}
    try:
        hist = yf.Ticker(ticker).history(period='1y', interval='1d')
        if len(hist) < 20: return empty
        cur        = float(hist['Close'].iloc[-1])
        resistance = min(float(hist['High'].max()), float(hist['High'].tail(60).max())*1.02)
        support    = max(float(hist['Low'].min()),  float(hist['Low'].tail(60).min())*0.98)
        room_up    = round((resistance-cur)/cur*100, 2)
        room_down  = round((cur-support)/cur*100, 2)
        return {'room_up':room_up,'room_down':room_down,
                'near_resistance':bool(room_up<1.5),'near_support':bool(room_down<1.5),
                'resistance':round(resistance,2),'support':round(support,2),'current':round(cur,2)}
    except Exception:
        return empty


def get_sector_correlation(ticker):
    try:
        peer = SECTOR_PEERS.get(ticker)
        if not peer: return 0, None
        ph  = yf.Ticker(peer).history(period='5d', interval='1d')
        if len(ph) < 2: return 0, None
        chg = round(float((ph['Close'].iloc[-1]-ph['Close'].iloc[-2])/ph['Close'].iloc[-2]*100), 2)
        if   chg >  0.5: return  1, {'ticker':peer,'change':chg,'signal':'🟢 Par sectorial ALCISTA'}
        elif chg < -0.5: return -1, {'ticker':peer,'change':chg,'signal':'🔴 Par sectorial BAJISTA'}
        return 0, {'ticker':peer,'change':chg,'signal':'⚪ Par sectorial NEUTRAL'}
    except Exception:
        return 0, None


def get_historical_gap_stats(ticker):
    try:
        hist = yf.Ticker(ticker).history(period="5y")
        if hist.empty: return 50
        up = down = 0
        for i in range(1, len(hist)):
            g = (hist['Open'].iloc[i]-hist['Close'].iloc[i-1])/hist['Close'].iloc[i-1]*100
            if g > 0.1: up += 1
            elif g < -0.1: down += 1
        total = up+down
        return round((up/total)*100) if total > 0 else 50
    except Exception:
        return 50


def get_technical_score(ticker):
    try:
        hist  = yf.Ticker(ticker).history(period="3mo")
        if hist.empty: return 0
        close = hist['Close']
        ma20  = close.rolling(20).mean().iloc[-1]
        cur   = close.iloc[-1]
        gain  = close.diff().clip(lower=0).rolling(14).mean()
        loss  = (-close.diff().clip(upper=0)).rolling(14).mean()
        rsi   = float(100-(100/(1+gain.iloc[-1]/loss.iloc[-1])))
        score = 0
        if cur > ma20: score += 30
        if rsi > 50:   score += 20
        if rsi > 70:   score -= 10
        if rsi < 30:   score += 20
        return int(score)
    except Exception:
        return 0


# ═══════════════════════════════════════════════════════════════════
#  SEMÁFORO
# ═══════════════════════════════════════════════════════════════════
def get_signal_strength(display_prob, direction):
    if display_prob < 60:
        return {'direction_display':'MERCADO INDECISO / NEUTRO','strength':'weak',
                'color':'neutral','description':'Señal débil. No operar.'}
    elif direction == 'ALCISTA':
        return {'direction_display':'ALCISTA','strength':'strong_bullish',
                'color':'bullish','description':'Señal fuerte alcista.'}
    else:
        return {'direction_display':'BAJISTA','strength':'strong_bearish',
                'color':'bearish','description':'Señal fuerte bajista.'}


# ═══════════════════════════════════════════════════════════════════
#  CÁLCULO PRINCIPAL
# ═══════════════════════════════════════════════════════════════════
def calculate_gap_probability(ticker):
    try:
        # ── Llamadas paralelas para máxima velocidad ──────────────
        results = {}
        def run(key, fn, *args):
            results[key] = fn(*args)

        with ThreadPoolExecutor(max_workers=10) as ex:
            futs = {
                ex.submit(get_historical_gap_stats, ticker): 'hist',
                ex.submit(get_technical_score,      ticker): 'tech',
                ex.submit(get_earnings_info,        ticker): 'earn',
                ex.submit(get_news_sentiment,       ticker): 'news',
                ex.submit(get_macro_sentiment):              'macro',
                ex.submit(get_whale_signals,        ticker): 'whale',
                ex.submit(get_overnight_drift,      ticker): 'drift',
                ex.submit(get_gap_room,             ticker): 'room',
                ex.submit(get_sector_correlation,   ticker): 'sector',
                ex.submit(get_futures_sentiment,    ticker): 'futures',
                ex.submit(get_stocktwits_sentiment, ticker): 'social',
                ex.submit(get_fakeout_detector,     ticker): 'fakeout',
                ex.submit(get_volume_analysis,      ticker): 'volume',
                ex.submit(check_high_impact_news):           'macro_news_flag',
            }
            for fut in as_completed(futs):
                key = futs[fut]
                try:    results[key] = fut.result()
                except: results[key] = None

        # ── Desempaquetar resultados ───────────────────────────────
        hist_prob                       = results.get('hist') or 50
        tech_score                      = results.get('tech') or 0
        earnings                        = results.get('earn') or {'has_earnings':False,'days_to_earnings':999,'status':'unknown'}
        news_s, news_list               = results.get('news') or (0, [])
        macro_s, macro_news             = results.get('macro') or (0, [])
        whale_score, whale_signals      = results.get('whale') or (0, [])
        drift, drift_trend, recent_drift= results.get('drift') or (0,'neutral',0)
        gap_room                        = results.get('room') or {'room_up':5,'room_down':5,'near_resistance':False,'near_support':False}
        sec_score, sec_info             = results.get('sector') or (0, None)
        fut_score, fut_signal, fut_change, idx_name = results.get('futures') or (0,'Sin datos',0,'Índice')
        soc_score, soc_trend, bull_pct, msg_count   = results.get('social') or (0,'Sin datos',50,0)
        is_fakeout, fakeout_reason, rsi_val         = results.get('fakeout') or (False,'Sin datos',50)
        vol_data                        = results.get('volume') or {'rvol':1.0,'volume_signal':'Sin datos','volume_score':0,'absorption':False,'capitulation':False,'anomaly':False}
        news_restr, news_restr_reason   = results.get('macro_news_flag') or (False, None)

        # ── Probabilidad ───────────────────────────────────────────
        final = hist_prob + tech_score*0.3 + news_s*15 + macro_s*10

        dte = earnings.get('days_to_earnings', 999)
        if   dte <= 1: final += 10
        elif dte <= 7: final += 5

        final += whale_score * 5
        final += 5 if drift > 0.1 else -5 if drift < -0.1 else 0
        final += sec_score * 8
        final += fut_score * 10
        final += soc_score * 8
        final += vol_data['volume_score'] * 5

        if gap_room.get('near_resistance') and final >= 50: final -= 8
        if is_fakeout and final >= 50:                       final -= 10

        final     = max(15, min(85, final))
        raw_dir   = 'ALCISTA' if final >= 50 else 'BAJISTA'
        disp_prob = final if final >= 50 else 100-final

        fut_warning = (raw_dir=='ALCISTA' and fut_change<=-0.3) or \
                      (raw_dir=='BAJISTA' and fut_change>= 0.3)

        sig = get_signal_strength(disp_prob, raw_dir)

        # Precio actual
        current_price = None
        try:
            fi = yf.Ticker(ticker).fast_info
            current_price = float(fi.last_price) if hasattr(fi,'last_price') else None
        except Exception:
            pass

        return {
            'ticker':          ticker,
            'probability':     round(disp_prob),
            'direction':       sig['direction_display'],
            'raw_direction':   raw_dir,
            'strength':        sig['strength'],
            'signal_color':    sig['color'],
            'current_price':   round(current_price,2) if current_price else None,
            'earnings':        earnings,
            'tech_score':      tech_score,
            'rsi_value':       rsi_val,
            'news_sentiment':  float(news_s),
            'macro_sentiment': float(macro_s),
            'news':            news_list[:5],
            'macro_news':      macro_news[:4],
            'whale_signals':   whale_signals,
            'whale_score':     float(whale_score),
            'overnight_drift': float(drift),
            'drift_trend':     drift_trend,
            'recent_drift':    float(recent_drift),
            'gap_room':        gap_room,
            'sector_info':     sec_info,
            'futures_signal':  fut_signal,
            'futures_change':  fut_change,
            'futures_warning': fut_warning,
            'index_name':      idx_name,
            'social_score':    float(soc_score),
            'social_trend':    soc_trend,
            'bull_pct':        bull_pct,
            'msg_count':       msg_count,
            'is_fakeout':      is_fakeout,
            'fakeout_reason':  fakeout_reason,
            'volume':          vol_data,
            'news_restriction': news_restr,
            'news_restriction_reason': news_restr_reason,
        }

    except Exception as e:
        return {
            'ticker':ticker,'probability':50,'direction':'MERCADO INDECISO / NEUTRO',
            'raw_direction':'NEUTRAL','strength':'weak','signal_color':'neutral',
            'current_price':None,
            'earnings':{'has_earnings':False,'days_to_earnings':999,'earnings_date':None,'investor_url':'','status':'unknown'},
            'tech_score':0,'rsi_value':50,'news_sentiment':0,'macro_sentiment':0,
            'news':[],'macro_news':[],'whale_signals':[],'whale_score':0,
            'overnight_drift':0,'drift_trend':'neutral','recent_drift':0,
            'gap_room':{'room_up':5,'room_down':5,'near_resistance':False,'near_support':False},
            'sector_info':None,'futures_signal':'Sin datos','futures_change':0,
            'futures_warning':False,'index_name':'Índice',
            'social_score':0,'social_trend':'Sin datos','bull_pct':50,'msg_count':0,
            'is_fakeout':False,'fakeout_reason':'',
            'volume':{'rvol':1.0,'volume_signal':'Sin datos','volume_score':0,'absorption':False,'capitulation':False,'anomaly':False},
            'news_restriction':False,'news_restriction_reason':None,
        }


# ═══════════════════════════════════════════════════════════════════
#  RUTAS  (trades eliminados del backend — viven en localStorage)
# ═══════════════════════════════════════════════════════════════════
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/analyze', methods=['POST'])
def analyze():
    ticker = request.json.get('ticker','').upper()
    return jsonify(calculate_gap_probability(ticker))

@app.route('/dashboard')
def dashboard():
    # Paralelo: todos los tickers a la vez
    with ThreadPoolExecutor(max_workers=9) as ex:
        futs = {ex.submit(calculate_gap_probability, t): t for t in MARKET_LEADERS}
        order = {t: i for i, t in enumerate(MARKET_LEADERS)}
        raw   = {}
        for fut in as_completed(futs):
            t = futs[fut]
            try:    raw[t] = fut.result()
            except: raw[t] = {'ticker':t,'probability':50,'direction':'NEUTRO','strength':'weak','signal_color':'neutral'}
    return jsonify([raw[t] for t in MARKET_LEADERS if t in raw])

@app.route('/earnings_calendar')
def earnings_calendar():
    with ThreadPoolExecutor(max_workers=9) as ex:
        futs = {ex.submit(get_earnings_info, t): t for t in MARKET_LEADERS}
        raw  = {}
        for fut in as_completed(futs):
            t = futs[fut]
            try:    raw[t] = fut.result()
            except: raw[t] = {'has_earnings':False,'days_to_earnings':999,'earnings_date':None,'investor_url':INVESTOR_URLS.get(t,''),'status':'unknown'}
    results = [{'ticker':t,'company':COMPANY_NAMES.get(t,t),**raw.get(t,{})} for t in MARKET_LEADERS]
    results.sort(key=lambda x: x['days_to_earnings'])
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
