from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
from textblob import TextBlob
import requests
import json
import os
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import math

app = Flask(__name__)

MARKET_LEADERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'NFLX', 'RACE']

COMPANY_NAMES = {
    'AAPL': 'Apple', 'MSFT': 'Microsoft', 'GOOGL': 'Google',
    'AMZN': 'Amazon', 'NVDA': 'Nvidia', 'META': 'Meta', 'TSLA': 'Tesla',
    'NFLX': 'Netflix', 'RACE': 'Ferrari'
}

SECTOR_PEERS = {
    'META': 'GOOGL', 'GOOGL': 'META', 'AAPL': 'MSFT', 'MSFT': 'AAPL',
    'NVDA': 'AMD',   'AMZN': 'MSFT',  'TSLA': 'RIVN',
    'NFLX': 'DIS',   'RACE': 'TSLA'
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

MT5_LOTS    = 65
TRADES_FILE = 'trades.json'


# ═══════════════════════════════════════════════════════════════════
#  EARNINGS  — ventana ampliada a 30 días pasados
# ═══════════════════════════════════════════════════════════════════

def _norm_ts(ts):
    """Convierte cualquier cosa a Timestamp naive normalizado (medianoche)."""
    try:
        t = pd.Timestamp(str(ts)[:10])   # recorta a YYYY-MM-DD
        return t.normalize()
    except Exception:
        return None


def get_earnings_info(ticker):
    investor_url = INVESTOR_URLS.get(ticker, '')
    stock        = yf.Ticker(ticker)
    today        = pd.Timestamp.now().normalize()   # naive

    # ── MÉTODO 1: get_earnings_dates (limit=20 para cubrir pasados) ──
    try:
        ed = stock.get_earnings_dates(limit=20)
        if ed is not None and not ed.empty:
            candidates = []
            for dt in ed.index:
                norm = _norm_ts(dt)
                if norm is None:
                    continue
                days = (norm - today).days
                candidates.append((days, norm))

            # Próximo futuro 0-90 días
            future = sorted([(d, t) for d, t in candidates if 0 <= d <= 90])
            if future:
                days, norm = future[0]
                return {
                    'has_earnings':     True,
                    'days_to_earnings': days,
                    'earnings_date':    str(norm.date()),
                    'earnings_time':    'After Market (22:00 CET)',
                    'investor_url':     investor_url,
                    'status':           'upcoming'
                }

            # Publicado hace 0-30 días (NVDA caso típico)
            past = sorted([(d, t) for d, t in candidates if -30 <= d < 0], reverse=True)
            if past:
                days, norm = past[0]
                return {
                    'has_earnings':     True,
                    'days_to_earnings': days,
                    'earnings_date':    str(norm.date()),
                    'earnings_time':    'After Market (22:00 CET)',
                    'investor_url':     investor_url,
                    'status':           'published'
                }
    except Exception:
        pass

    # ── MÉTODO 2: calendar (dict o DataFrame) ──
    try:
        cal = stock.calendar
        if cal is not None:
            raw_dates = []
            if isinstance(cal, dict):
                v = cal.get('Earnings Date') or cal.get('earnings_date')
                if v:
                    raw_dates = v if isinstance(v, list) else [v]
            elif hasattr(cal, 'empty') and not cal.empty:
                for col in ['Earnings Date', 'earnings_date']:
                    if col in cal.columns:
                        raw_dates = list(cal[col].dropna())
                        break

            for raw in raw_dates:
                norm = _norm_ts(raw)
                if norm is None:
                    continue
                days = (norm - today).days
                if -30 <= days <= 90:
                    status = 'published' if days < 0 else 'upcoming'
                    return {
                        'has_earnings':     True,
                        'days_to_earnings': days,
                        'earnings_date':    str(norm.date()),
                        'earnings_time':    'After Market (22:00 CET)',
                        'investor_url':     investor_url,
                        'status':           status
                    }
    except Exception:
        pass

    # ── MÉTODO 3: info dict (earningsDate / earningsTimestamp) ──
    try:
        info = stock.info
        for key in ['earningsDate', 'earningsTimestamp']:
            raw = info.get(key)
            if not raw:
                continue
            if isinstance(raw, list):
                raw = raw[0]
            if isinstance(raw, (int, float)):
                norm = pd.Timestamp.fromtimestamp(raw).normalize()
            else:
                norm = _norm_ts(raw)
            if norm is None:
                continue
            days = (norm - today).days
            if -30 <= days <= 90:
                status = 'published' if days < 0 else 'upcoming'
                return {
                    'has_earnings':     True,
                    'days_to_earnings': days,
                    'earnings_date':    str(norm.date()),
                    'earnings_time':    'After Market (22:00 CET)',
                    'investor_url':     investor_url,
                    'status':           status
                }
    except Exception:
        pass

    return {
        'has_earnings':     False,
        'days_to_earnings': 999,
        'earnings_date':    None,
        'earnings_time':    None,
        'investor_url':     investor_url,
        'status':           'unknown'
    }


# ═══════════════════════════════════════════════════════════════════
#  NOTICIAS
# ═══════════════════════════════════════════════════════════════════

def check_high_impact_news():
    try:
        q    = 'CPI+OR+NFP+OR+FOMC+OR+GDP+OR+inflation+report+OR+jobs+report+today'
        root = ET.fromstring(requests.get(
            f'https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en',
            timeout=8).content)
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
            timeout=8).content)
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
                all_s.append(s)
                all_news.append({'title': title[:80], 'sentiment': round(s, 2), 'published': date})
        except Exception:
            continue
    avg = sum(all_s) / len(all_s) if all_s else 0
    return round(avg, 3), all_news[:6]


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
        chg    = float((last30['Close'].iloc[-1] - last30['Close'].iloc[0])
                       / last30['Close'].iloc[0] * 100)
        chg    = round(chg, 3)
        emoji  = '🟢' if chg >= 0.3 else '🔴' if chg <= -0.3 else '⚪'
        sign   = '+' if chg > 0 else ''
        score  = 1 if chg >= 0.3 else -1 if chg <= -0.3 else 0
        return score, f'{emoji} {index_name} {sign}{chg}% últimos 30min', chg, index_name
    except Exception:
        return 0, 'Sin datos de futuros', 0, 'Índice'


# ═══════════════════════════════════════════════════════════════════
#  SOCIAL / BALLENAS
# ═══════════════════════════════════════════════════════════════════

def get_stocktwits_sentiment(ticker):
    try:
        resp = requests.get(
            f'https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json',
            timeout=8)
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
        if   bull_pct >= 65: return  1,   '🟢 Muy alcista',         bull_pct, len(messages)
        elif bull_pct >= 55: return  0.5, '🟡 Ligeramente alcista', bull_pct, len(messages)
        elif bull_pct <= 35: return -1,   '🔴 Muy bajista',         bull_pct, len(messages)
        elif bull_pct <= 45: return -0.5, '🟠 Ligeramente bajista', bull_pct, len(messages)
        return 0, '⚪ Neutral', bull_pct, len(messages)
    except Exception:
        return 0, 'Sin datos StockTwits', 50, 0


def get_whale_signals(ticker):
    score, signals = 0, []
    for q in [f'{ticker}+insider+buying+OR+selling',
              f'{ticker}+Goldman+Sachs+OR+Morgan+Stanley+upgrade+OR+downgrade',
              f'{ticker}+hindenburg+OR+short+seller']:
        try:
            root = ET.fromstring(requests.get(
                f'https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en',
                timeout=6).content)
            for item in root.findall('.//item')[:2]:
                title = item.find('title').text or ''
                tl    = title.lower()
                pub   = item.find('pubDate').text or ''
                m     = 1.5 if 'pm' in pub.lower() else 1.0
                if   any(w in tl for w in ['insider buy', 'purchased', 'acquired']):
                    pts = round(3*m, 1);   score += pts; signals.append({'signal':'🐋 Insider COMPRA', 'detail':title[:80],'points':pts})
                elif any(w in tl for w in ['insider sell', 'disposed']):
                    pts = round(-2.5*m,1); score += pts; signals.append({'signal':'🔴 Insider VENDE',  'detail':title[:80],'points':pts})
                elif any(w in tl for w in ['upgrade','outperform','overweight','strong buy']):
                    pts = round(1.5*m, 1); score += pts; signals.append({'signal':'🏦 Banco UPGRADE',  'detail':title[:80],'points':pts})
                elif any(w in tl for w in ['downgrade','underperform','underweight']):
                    pts = round(-1.5*m,1); score += pts; signals.append({'signal':'🏦 Banco DOWNGRADE','detail':title[:80],'points':pts})
                elif any(w in tl for w in ['hindenburg','short seller','fraud']):
                    pts = round(-3*m,  1); score += pts; signals.append({'signal':'⚠️ SHORT SELLER',  'detail':title[:80],'points':pts})
        except Exception:
            continue
    return round(score, 1), signals


# ═══════════════════════════════════════════════════════════════════
#  TÉCNICO
# ═══════════════════════════════════════════════════════════════════

def get_fakeout_detector(ticker, _=None):
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
    empty = {'rvol':1.0,'volume_signal':'⚪ Sin datos','volume_score':0,
             'absorption':False,'capitulation':False,'anomaly':False,
             'last_volume':0,'avg_volume':0,'price_change_pct':0}
    try:
        hist = yf.Ticker(ticker).history(period='30d', interval='1d')
        if len(hist) < 10:
            return empty
        avg_vol  = float(hist['Volume'][:-1].mean())
        last_vol = float(hist['Volume'].iloc[-1])
        rvol     = round(last_vol / avg_vol, 2) if avg_vol > 0 else 1.0
        last_c   = float(hist['Close'].iloc[-1])
        prev_c   = float(hist['Close'].iloc[-2])
        pct      = (last_c - prev_c) / prev_c * 100
        score, absorption, capitulation, anomaly = 0, False, False, False
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
                if (float(t3['Volume'].sum()) > float(hi['Volume'].mean()) * 9 and
                    float((t3['Close'].iloc[-1]-t3['Close'].iloc[0])/t3['Close'].iloc[0]*100) > 0.3):
                    capitulation = True; score += 1.5; vsig += ' + 🚀 Capitulación cortos'
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
        if len(hist) < 5:
            return 0, 'neutral', 0
        rets = [(float(hist['Open'].iloc[i]) - float(hist['Close'].iloc[i-1]))
                / float(hist['Close'].iloc[i-1]) * 100
                for i in range(1, len(hist)) if float(hist['Close'].iloc[i-1]) > 0]
        if not rets:
            return 0, 'neutral', 0
        avg    = round(sum(rets) / len(rets), 3)
        recent = round(sum(rets[-5:]) / 5, 3)
        trend  = 'alcista' if avg > 0.1 else 'bajista' if avg < -0.1 else 'neutral'
        return avg, trend, recent
    except Exception:
        return 0, 'neutral', 0


def get_gap_room(ticker):
    empty = {'room_up':5,'room_down':5,'near_resistance':False,
             'near_support':False,'resistance':0,'support':0,'current':0}
    try:
        hist = yf.Ticker(ticker).history(period='1y', interval='1d')
        if len(hist) < 20:
            return empty
        cur        = float(hist['Close'].iloc[-1])
        resistance = min(float(hist['High'].max()), float(hist['High'].tail(60).max()) * 1.02)
        support    = max(float(hist['Low'].min()),  float(hist['Low'].tail(60).min())  * 0.98)
        room_up    = round((resistance - cur) / cur * 100, 2)
        room_down  = round((cur - support)    / cur * 100, 2)
        return {'room_up':room_up,'room_down':room_down,
                'near_resistance':bool(room_up < 1.5),'near_support':bool(room_down < 1.5),
                'resistance':round(resistance,2),'support':round(support,2),'current':round(cur,2)}
    except Exception:
        return empty


def get_sector_correlation(ticker):
    try:
        peer = SECTOR_PEERS.get(ticker)
        if not peer:
            return 0, None
        ph  = yf.Ticker(peer).history(period='5d', interval='1d')
        if len(ph) < 2:
            return 0, None
        chg = round(float((ph['Close'].iloc[-1] - ph['Close'].iloc[-2])
                          / ph['Close'].iloc[-2] * 100), 2)
        if   chg >  0.5: return  1, {'ticker':peer,'change':chg,'signal':'🟢 Par sectorial ALCISTA'}
        elif chg < -0.5: return -1, {'ticker':peer,'change':chg,'signal':'🔴 Par sectorial BAJISTA'}
        return 0, {'ticker':peer,'change':chg,'signal':'⚪ Par sectorial NEUTRAL'}
    except Exception:
        return 0, None


def get_historical_gap_stats(ticker):
    try:
        hist = yf.Ticker(ticker).history(period="5y")
        if hist.empty:
            return 50
        up = down = 0
        for i in range(1, len(hist)):
            g = (hist['Open'].iloc[i] - hist['Close'].iloc[i-1]) / hist['Close'].iloc[i-1] * 100
            if g > 0.1:  up   += 1
            elif g < -0.1: down += 1
        total = up + down
        return round((up / total) * 100) if total > 0 else 50
    except Exception:
        return 50


def get_technical_score(ticker):
    try:
        hist  = yf.Ticker(ticker).history(period="3mo")
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
#  CALCULADORA FTMO — 65 lotes fijos con validación de dirección
# ═══════════════════════════════════════════════════════════════════

def calculate_fixed_risk(stop_loss_price, current_price, days_to_earnings, raw_direction):
    """
    Calcula el riesgo monetario con 65 lotes fijos.
    Valida que el Stop Loss tenga sentido según la dirección:
      - ALCISTA  → SL debe estar POR DEBAJO del precio actual
      - BAJISTA  → SL debe estar POR ENCIMA del precio actual
    """
    result = {
        'mt5_volume':       MT5_LOTS,
        'risk_cash':        None,
        'message':          None,
        'earnings_alert':   None,
        'catastrophic_risk':None,
        'sl_error':         None
    }

    # Riesgo catastrófico por earnings (siempre calculado si aplica)
    if current_price is not None and days_to_earnings <= 1:
        try:
            cat = round(float(current_price) * 0.15 * MT5_LOTS, 2)
            result['catastrophic_risk'] = cat
            result['earnings_alert'] = (
                f'⚠️ ALERTA EARNINGS: Un gap en contra del 15% '
                f'te haría perder ${cat:,.2f} con {MT5_LOTS} lotes'
            )
        except Exception:
            pass

    # Riesgo con Stop Loss
    if stop_loss_price is not None and current_price is not None:
        try:
            sl = float(stop_loss_price)
            cp = float(current_price)

            # Validar dirección vs stop loss
            if raw_direction == 'ALCISTA' and sl >= cp:
                result['sl_error'] = (
                    f'⚠️ Stop Loss inválido para posición ALCISTA. '
                    f'El SL debe estar por DEBAJO del precio actual (${cp:,.2f}). '
                    f'Tu SL de ${sl:,.2f} está por encima.'
                )
                return result

            if raw_direction == 'BAJISTA' and sl <= cp:
                result['sl_error'] = (
                    f'⚠️ Stop Loss inválido para posición BAJISTA. '
                    f'El SL debe estar por ENCIMA del precio actual (${cp:,.2f}). '
                    f'Tu SL de ${sl:,.2f} está por debajo.'
                )
                return result

            risk = round(abs(cp - sl) * MT5_LOTS, 2)
            result['risk_cash'] = risk
            result['message']   = f'Operando {MT5_LOTS} lotes. Riesgo asumido: ${risk:,.2f}'
        except Exception:
            pass

    return result


# ═══════════════════════════════════════════════════════════════════
#  SEMÁFORO DE CONFIANZA
# ═══════════════════════════════════════════════════════════════════

def get_signal_strength(display_prob, direction):
    if display_prob < 60:
        return {'direction_display':'MERCADO INDECISO / NEUTRO',
                'strength':'weak','color':'neutral','description':'Señal débil. No operar.'}
    elif direction == 'ALCISTA':
        return {'direction_display':'ALCISTA',
                'strength':'strong_bullish','color':'bullish','description':'Señal fuerte alcista.'}
    else:
        return {'direction_display':'BAJISTA',
                'strength':'strong_bearish','color':'bearish','description':'Señal fuerte bajista.'}


# ═══════════════════════════════════════════════════════════════════
#  CÁLCULO PRINCIPAL
# ═══════════════════════════════════════════════════════════════════

def calculate_gap_probability(ticker, stop_loss_price=None):
    try:
        hist_prob  = get_historical_gap_stats(ticker)
        tech_score = get_technical_score(ticker)
        earnings   = get_earnings_info(ticker)
        news_s, news_list  = get_news_sentiment(ticker)
        macro_s, macro_news = get_macro_sentiment()
        whale_score, whale_signals = get_whale_signals(ticker)
        drift, drift_trend, recent_drift = get_overnight_drift(ticker)
        gap_room   = get_gap_room(ticker)
        sec_score, sec_info = get_sector_correlation(ticker)
        fut_score, fut_signal, fut_change, idx_name = get_futures_sentiment(ticker)
        soc_score, soc_trend, bull_pct, msg_count   = get_stocktwits_sentiment(ticker)
        is_fakeout, fakeout_reason, rsi_val = get_fakeout_detector(ticker)
        vol_data   = get_volume_analysis(ticker)
        news_restr, news_restr_reason = check_high_impact_news()

        final = (hist_prob + tech_score * 0.3 + news_s * 15 + macro_s * 10)

        dte = earnings.get('days_to_earnings', 999)
        if dte <= 1:   final += 10
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
        disp_prob = final if final >= 50 else 100 - final

        fut_warning = (raw_dir == 'ALCISTA' and fut_change <= -0.3) or \
                      (raw_dir == 'BAJISTA' and fut_change >= 0.3)

        sig = get_signal_strength(disp_prob, raw_dir)

        # Precio actual
        current_price = None
        try:
            fi = yf.Ticker(ticker).fast_info
            current_price = float(fi.last_price) if hasattr(fi, 'last_price') else None
        except Exception:
            pass

        ftmo = calculate_fixed_risk(stop_loss_price, current_price, dte, raw_dir)

        return {
            'ticker':              ticker,
            'probability':         round(disp_prob),
            'direction':           sig['direction_display'],
            'raw_direction':       raw_dir,
            'strength':            sig['strength'],
            'signal_color':        sig['color'],
            'signal_description':  sig['description'],
            'current_price':       round(current_price, 2) if current_price else None,
            'earnings':            earnings,
            'tech_score':          tech_score,
            'rsi_value':           rsi_val,
            'news_sentiment':      float(news_s),
            'macro_sentiment':     float(macro_s),
            'news':                news_list[:5],
            'macro_news':          macro_news[:4],
            'whale_signals':       whale_signals,
            'whale_score':         float(whale_score),
            'overnight_drift':     float(drift),
            'drift_trend':         drift_trend,
            'recent_drift':        float(recent_drift),
            'gap_room':            gap_room,
            'sector_info':         sec_info,
            'futures_signal':      fut_signal,
            'futures_change':      fut_change,
            'futures_warning':     fut_warning,
            'index_name':          idx_name,
            'social_score':        float(soc_score),
            'social_trend':        soc_trend,
            'bull_pct':            bull_pct,
            'msg_count':           msg_count,
            'is_fakeout':          is_fakeout,
            'fakeout_reason':      fakeout_reason,
            'volume':              vol_data,
            'news_restriction':    news_restr,
            'news_restriction_reason': news_restr_reason,
            'ftmo':                ftmo
        }

    except Exception as e:
        return {
            'ticker':ticker,'probability':50,
            'direction':'MERCADO INDECISO / NEUTRO','raw_direction':'NEUTRAL',
            'strength':'weak','signal_color':'neutral',
            'signal_description':f'Error: {str(e)}',
            'current_price':None,
            'earnings':{'has_earnings':False,'days_to_earnings':999,'earnings_date':None,
                        'investor_url':'','status':'unknown'},
            'tech_score':0,'rsi_value':50,'news_sentiment':0,'macro_sentiment':0,
            'news':[],'macro_news':[],'whale_signals':[],'whale_score':0,
            'overnight_drift':0,'drift_trend':'neutral','recent_drift':0,
            'gap_room':{'room_up':5,'room_down':5,'near_resistance':False,'near_support':False},
            'sector_info':None,'futures_signal':'Sin datos','futures_change':0,
            'futures_warning':False,'index_name':'Índice',
            'social_score':0,'social_trend':'Sin datos','bull_pct':50,'msg_count':0,
            'is_fakeout':False,'fakeout_reason':'',
            'volume':{'rvol':1.0,'volume_signal':'Sin datos','volume_score':0,
                      'absorption':False,'capitulation':False,'anomaly':False},
            'news_restriction':False,'news_restriction_reason':None,
            'ftmo':{'mt5_volume':MT5_LOTS,'risk_cash':None,'message':None,
                    'earnings_alert':None,'catastrophic_risk':None,'sl_error':None}
        }


# ═══════════════════════════════════════════════════════════════════
#  TRADES
# ═══════════════════════════════════════════════════════════════════

def load_trades():
    if os.path.exists(TRADES_FILE):
        with open(TRADES_FILE, 'r') as f:
            return json.load(f)
    return []

def save_trades(trades):
    with open(TRADES_FILE, 'w') as f:
        json.dump(trades, f, indent=2)


# ═══════════════════════════════════════════════════════════════════
#  RUTAS
# ═══════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/analyze', methods=['POST'])
def analyze():
    data            = request.json
    ticker          = data.get('ticker', '').upper()
    stop_loss_price = data.get('stop_loss_price')
    return jsonify(calculate_gap_probability(ticker, stop_loss_price))

@app.route('/dashboard')
def dashboard():
    return jsonify([calculate_gap_probability(t) for t in MARKET_LEADERS])

@app.route('/earnings_calendar')
def earnings_calendar():
    results = []
    for ticker in MARKET_LEADERS:
        info = get_earnings_info(ticker)
        results.append({
            'ticker':           ticker,
            'company':          COMPANY_NAMES.get(ticker, ticker),
            'has_earnings':     info['has_earnings'],
            'days_to_earnings': info['days_to_earnings'],
            'earnings_date':    info['earnings_date'],
            'earnings_time':    info.get('earnings_time', ''),
            'investor_url':     info.get('investor_url', ''),
            'status':           info.get('status', 'unknown')
        })
    results.sort(key=lambda x: x['days_to_earnings'])
    return jsonify(results)

@app.route('/trades', methods=['GET'])
def get_trades():
    return jsonify(load_trades())

@app.route('/trades', methods=['POST'])
def add_trade():
    trades = load_trades()
    trade  = request.json
    trade['id']   = len(trades) + 1
    trade['date'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    trades.append(trade)
    save_trades(trades)
    return jsonify(trade)

@app.route('/trades/<int:trade_id>', methods=['PUT'])
def update_trade(trade_id):
    trades = load_trades()
    for t in trades:
        if t['id'] == trade_id:
            t.update(request.json)
            break
    save_trades(trades)
    return jsonify({'ok': True})

@app.route('/trades/<int:trade_id>', methods=['DELETE'])
def delete_trade(trade_id):
    trades = [t for t in load_trades() if t['id'] != trade_id]
    save_trades(trades)
    return jsonify({'ok': True})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
