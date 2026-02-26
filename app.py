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
    'NVDA': 'AMD', 'AMZN': 'MSFT', 'TSLA': 'RIVN',
    'NFLX': 'DIS', 'RACE': 'TSLA'
}

NASDAQ_STOCKS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'NFLX']
NYSE_STOCKS = ['RACE']

INVESTOR_URLS = {
    'AAPL': 'https://investor.apple.com/investor-relations/default.aspx',
    'MSFT': 'https://www.microsoft.com/en-us/investor',
    'GOOGL': 'https://abc.xyz/investor/',
    'AMZN': 'https://ir.aboutamazon.com/',
    'NVDA': 'https://investor.nvidia.com/',
    'META': 'https://investor.fb.com/',
    'TSLA': 'https://ir.tesla.com/',
    'NFLX': 'https://ir.netflix.net/',
    'RACE': 'https://corporate.ferrari.com/en/investors'
}

HIGH_IMPACT_KEYWORDS = ['CPI', 'NFP', 'Non-Farm Payroll', 'FOMC', 'GDP', 'Federal Reserve meeting',
                         'interest rate decision', 'inflation report', 'jobs report']

TRADES_FILE = 'trades.json'


# ── EARNINGS ──────────────────────────────────────────────────────────────────

def get_earnings_info(ticker):
    try:
        stock = yf.Ticker(ticker)
        today = pd.Timestamp.now(tz='UTC').normalize()

        # Método 1: get_earnings_dates
        try:
            ed = stock.get_earnings_dates(limit=8)
            if ed is not None and not ed.empty:
                ed = ed.sort_index()
                for dt in ed.index:
                    dt_norm = dt.normalize() if hasattr(dt, 'normalize') else pd.Timestamp(dt).normalize()
                    days_diff = (dt_norm - today).days
                    # Próximos earnings (hasta 90 días)
                    if 0 <= days_diff <= 90:
                        return {
                            'has_earnings': True,
                            'days_to_earnings': int(days_diff),
                            'earnings_date': str(dt_norm.date()),
                            'earnings_time': 'After Market (22:00 CET)',
                            'investor_url': INVESTOR_URLS.get(ticker, ''),
                            'status': 'upcoming'
                        }
                    # Earnings muy recientes (publicados hace 0-3 días)
                    if -3 <= days_diff < 0:
                        return {
                            'has_earnings': True,
                            'days_to_earnings': int(days_diff),
                            'earnings_date': str(dt_norm.date()),
                            'earnings_time': 'After Market (22:00 CET)',
                            'investor_url': INVESTOR_URLS.get(ticker, ''),
                            'status': 'published'
                        }
        except Exception:
            pass

        # Método 2: calendar fallback
        try:
            calendar = stock.calendar
            if calendar is not None and not calendar.empty:
                if 'Earnings Date' in calendar.columns:
                    earnings_date = calendar.iloc[0]['Earnings Date']
                    if earnings_date:
                        dt_norm = pd.Timestamp(earnings_date).normalize()
                        days_diff = (dt_norm - today).days
                        return {
                            'has_earnings': True,
                            'days_to_earnings': int(days_diff),
                            'earnings_date': str(dt_norm.date()),
                            'earnings_time': 'After Market (22:00 CET)',
                            'investor_url': INVESTOR_URLS.get(ticker, ''),
                            'status': 'upcoming'
                        }
        except Exception:
            pass

    except Exception:
        pass

    return {
        'has_earnings': False, 'days_to_earnings': 999,
        'earnings_date': None, 'earnings_time': None,
        'investor_url': INVESTOR_URLS.get(ticker, ''), 'status': 'unknown'
    }


# ── NOTICIAS MACRO ────────────────────────────────────────────────────────────

def check_high_impact_news():
    """Detecta noticias de alto impacto macro (CPI, NFP, FOMC...)"""
    try:
        query = 'CPI+OR+NFP+OR+FOMC+OR+GDP+OR+inflation+report+OR+jobs+report+today'
        url = f'https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en'
        response = requests.get(url, timeout=8)
        root = ET.fromstring(response.content)
        today_str = datetime.now().strftime('%Y-%m-%d')
        tomorrow_str = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        for item in root.findall('.//item')[:10]:
            title = (item.find('title').text or '').upper()
            pub_date = item.find('pubDate')
            date_text = pub_date.text if pub_date is not None else ''
            for kw in HIGH_IMPACT_KEYWORDS:
                if kw.upper() in title:
                    return True, f'Evento macro detectado: {kw}'
        return False, None
    except:
        return False, None


def get_news_sentiment(ticker):
    try:
        company = COMPANY_NAMES.get(ticker, ticker)
        url = f'https://news.google.com/rss/search?q={company}+stock&hl=en-US&gl=US&ceid=US:en'
        response = requests.get(url, timeout=8)
        root = ET.fromstring(response.content)
        sentiments = []
        news_list = []
        for item in root.findall('.//item')[:8]:
            title = item.find('title').text or ''
            pub_date = item.find('pubDate')
            date = pub_date.text[:16] if pub_date is not None else ''
            sentiment = TextBlob(title).sentiment.polarity
            sentiments.append(sentiment)
            news_list.append({'title': title[:80], 'sentiment': round(sentiment, 2), 'published': date})
        avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
        return round(avg_sentiment, 3), news_list
    except:
        return 0, []


def get_macro_sentiment():
    try:
        queries = [
            'Federal+Reserve+interest+rates', 'Trump+economy+market+tariffs',
            'stock+market+inflation+GDP', 'hedge+fund+institutional+investors', 'Warren+Buffett+Berkshire'
        ]
        all_sentiments = []
        all_news = []
        for query in queries:
            try:
                url = f'https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en'
                response = requests.get(url, timeout=6)
                root = ET.fromstring(response.content)
                for item in root.findall('.//item')[:2]:
                    title = item.find('title').text or ''
                    pub_date = item.find('pubDate')
                    date = pub_date.text[:16] if pub_date is not None else ''
                    sentiment = TextBlob(title).sentiment.polarity
                    all_sentiments.append(sentiment)
                    all_news.append({'title': title[:80], 'sentiment': round(sentiment, 2), 'published': date})
            except:
                continue
        avg = sum(all_sentiments) / len(all_sentiments) if all_sentiments else 0
        return round(avg, 3), all_news[:6]
    except:
        return 0, []


# ── FUTUROS / ÍNDICES ─────────────────────────────────────────────────────────

def get_futures_sentiment(ticker='AAPL'):
    try:
        index_ticker = 'SPY' if ticker in NYSE_STOCKS else 'QQQ'
        index_name = 'S&P 500' if ticker in NYSE_STOCKS else 'Nasdaq'
        idx = yf.Ticker(index_ticker)
        hist = idx.history(period='2d', interval='5m')
        if len(hist) < 6:
            return 0, 'Sin datos', 0, index_name
        last_30min = hist.tail(6)
        change_30m = float((last_30min['Close'].iloc[-1] - last_30min['Close'].iloc[0]) / last_30min['Close'].iloc[0] * 100)
        change_30m = round(change_30m, 3)
        emoji = '🟢' if change_30m >= 0.3 else '🔴' if change_30m <= -0.3 else '⚪'
        sign = '+' if change_30m > 0 else ''
        signal = f'{emoji} {index_name} {sign}{change_30m}% últimos 30min'
        score = 1 if change_30m >= 0.3 else -1 if change_30m <= -0.3 else 0
        return score, signal, change_30m, index_name
    except:
        return 0, 'Sin datos de futuros', 0, 'Índice'


# ── SOCIAL / BALLENAS ─────────────────────────────────────────────────────────

def get_stocktwits_sentiment(ticker):
    try:
        url = f'https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json'
        response = requests.get(url, timeout=8)
        if response.status_code != 200:
            return 0, 'Sin datos', 50, 0
        data = response.json()
        messages = data.get('messages', [])
        if not messages:
            return 0, 'Sin datos', 50, 0
        bullish = 0
        bearish = 0
        sentiments = []
        for msg in messages[:30]:
            entities = msg.get('entities', {})
            sentiment_data = entities.get('sentiment', {})
            if sentiment_data:
                if sentiment_data.get('basic') == 'Bullish':
                    bullish += 1
                elif sentiment_data.get('basic') == 'Bearish':
                    bearish += 1
            body = msg.get('body', '')
            if body:
                sentiments.append(TextBlob(body).sentiment.polarity)
        total = bullish + bearish
        bull_pct = round((bullish / total) * 100) if total > 0 else 50
        if bull_pct >= 65:
            return 1, '🟢 Muy alcista', bull_pct, len(messages)
        elif bull_pct >= 55:
            return 0.5, '🟡 Ligeramente alcista', bull_pct, len(messages)
        elif bull_pct <= 35:
            return -1, '🔴 Muy bajista', bull_pct, len(messages)
        elif bull_pct <= 45:
            return -0.5, '🟠 Ligeramente bajista', bull_pct, len(messages)
        return 0, '⚪ Neutral', bull_pct, len(messages)
    except:
        return 0, 'Sin datos StockTwits', 50, 0


def get_whale_signals(ticker):
    try:
        score = 0
        signals = []
        queries = [
            f'{ticker}+insider+buying+OR+selling',
            f'{ticker}+Goldman+Sachs+OR+Morgan+Stanley+upgrade+OR+downgrade',
            f'{ticker}+hindenburg+OR+short+seller'
        ]
        for query in queries:
            try:
                url = f'https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en'
                response = requests.get(url, timeout=6)
                root = ET.fromstring(response.content)
                for item in root.findall('.//item')[:2]:
                    title = item.find('title').text or ''
                    title_lower = title.lower()
                    pub_date = item.find('pubDate').text or ''
                    multiplier = 1.5 if 'pm' in pub_date.lower() else 1.0
                    if any(w in title_lower for w in ['insider buy', 'purchased', 'acquired']):
                        pts = round(3 * multiplier, 1)
                        score += pts
                        signals.append({'signal': '🐋 Insider COMPRA', 'detail': title[:80], 'points': pts})
                    elif any(w in title_lower for w in ['insider sell', 'disposed']):
                        pts = round(-2.5 * multiplier, 1)
                        score += pts
                        signals.append({'signal': '🔴 Insider VENDE', 'detail': title[:80], 'points': pts})
                    elif any(w in title_lower for w in ['upgrade', 'outperform', 'overweight', 'strong buy']):
                        pts = round(1.5 * multiplier, 1)
                        score += pts
                        signals.append({'signal': '🏦 Banco UPGRADE', 'detail': title[:80], 'points': pts})
                    elif any(w in title_lower for w in ['downgrade', 'underperform', 'underweight']):
                        pts = round(-1.5 * multiplier, 1)
                        score += pts
                        signals.append({'signal': '🏦 Banco DOWNGRADE', 'detail': title[:80], 'points': pts})
                    elif any(w in title_lower for w in ['hindenburg', 'short seller', 'fraud']):
                        pts = round(-3 * multiplier, 1)
                        score += pts
                        signals.append({'signal': '⚠️ SHORT SELLER', 'detail': title[:80], 'points': pts})
            except:
                continue
        return round(score, 1), signals
    except:
        return 0, []


# ── TÉCNICO ───────────────────────────────────────────────────────────────────

def get_fakeout_detector(ticker, rsi_score):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='3mo')
        if hist.empty:
            return False, 'Sin datos', 50
        close = hist['Close']
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = -delta.where(delta < 0, 0).rolling(14).mean()
        rs = gain / loss
        rsi = float(100 - (100 / (1 + rs.iloc[-1])))
        fakeout_risk = 0
        reasons = []
        if rsi > 72:
            fakeout_risk += 2
            reasons.append(f'RSI sobrecomprado ({round(rsi, 1)})')
        elif rsi > 68:
            fakeout_risk += 1
            reasons.append(f'RSI alto ({round(rsi, 1)})')
        if rsi < 28:
            fakeout_risk += 2
            reasons.append(f'RSI sobrevendido ({round(rsi, 1)})')
        last_change = float((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100)
        if last_change > 4:
            fakeout_risk += 2
            reasons.append(f'Subida del día muy alta (+{round(last_change, 1)}%)')
        elif last_change < -4:
            fakeout_risk += 1
            reasons.append(f'Caída del día muy alta ({round(last_change, 1)}%)')
        is_fakeout = fakeout_risk >= 3
        return is_fakeout, ' + '.join(reasons) if reasons else 'Sin riesgo detectado', round(rsi, 1)
    except:
        return False, 'Sin datos', 50


def get_volume_analysis(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist_daily = stock.history(period='30d', interval='1d')
        if len(hist_daily) < 10:
            return {'rvol': 1.0, 'volume_signal': '⚪ Normal', 'volume_score': 0,
                    'absorption': False, 'capitulation': False, 'anomaly': False,
                    'last_volume': 0, 'avg_volume': 0, 'price_change_pct': 0}
        avg_volume = float(hist_daily['Volume'][:-1].mean())
        last_volume = float(hist_daily['Volume'].iloc[-1])
        rvol = round(last_volume / avg_volume, 2) if avg_volume > 0 else 1.0
        last_close = float(hist_daily['Close'].iloc[-1])
        prev_close = float(hist_daily['Close'].iloc[-2])
        price_change_pct = (last_close - prev_close) / prev_close * 100
        volume_score = 0
        absorption = False
        capitulation = False
        anomaly = False
        if rvol >= 2.0 and price_change_pct > 0.3:
            volume_score = 2
            volume_signal = f'⚡ Acumulación institucional (RVOL {rvol}x)'
            anomaly = True
        elif rvol >= 2.0 and price_change_pct < -0.3:
            volume_score = -2
            volume_signal = f'🔴 Distribución institucional (RVOL {rvol}x)'
            anomaly = True
        elif rvol >= 2.0 and abs(price_change_pct) <= 0.3:
            volume_score = -1
            volume_signal = f'⚠️ Absorción detectada (RVOL {rvol}x, precio plano)'
            absorption = True
            anomaly = True
        elif rvol >= 1.5 and price_change_pct > 0.2:
            volume_score = 1
            volume_signal = f'🟡 Volumen elevado alcista (RVOL {rvol}x)'
        elif rvol >= 1.5 and price_change_pct < -0.2:
            volume_score = -1
            volume_signal = f'🟠 Volumen elevado bajista (RVOL {rvol}x)'
        else:
            volume_signal = f'⚪ Volumen normal (RVOL {rvol}x)'
        try:
            hist_intra = stock.history(period='2d', interval='5m')
            if len(hist_intra) >= 12:
                last_15min = hist_intra.tail(3)
                avg_5min_vol = float(hist_intra['Volume'].mean())
                last_vol = float(last_15min['Volume'].sum())
                last_price_move = float((last_15min['Close'].iloc[-1] - last_15min['Close'].iloc[0]) / last_15min['Close'].iloc[0] * 100)
                if last_vol > avg_5min_vol * 9 and last_price_move > 0.3:
                    capitulation = True
                    volume_score += 1.5
                    volume_signal += ' + 🚀 Capitulación cortos'
        except:
            pass
        return {
            'rvol': rvol, 'volume_signal': volume_signal, 'volume_score': round(volume_score, 1),
            'absorption': absorption, 'capitulation': capitulation, 'anomaly': anomaly,
            'last_volume': int(last_volume), 'avg_volume': int(avg_volume),
            'price_change_pct': round(price_change_pct, 2)
        }
    except:
        return {'rvol': 1.0, 'volume_signal': '⚪ Sin datos', 'volume_score': 0,
                'absorption': False, 'capitulation': False, 'anomaly': False,
                'last_volume': 0, 'avg_volume': 0, 'price_change_pct': 0}


def get_overnight_drift(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='30d', interval='1d')
        if len(hist) < 5:
            return 0, 'neutral', 0
        overnight_returns = []
        for i in range(1, len(hist)):
            prev_close = float(hist['Close'].iloc[i-1])
            curr_open = float(hist['Open'].iloc[i])
            if prev_close > 0:
                overnight_returns.append((curr_open - prev_close) / prev_close * 100)
        if not overnight_returns:
            return 0, 'neutral', 0
        avg_drift = round(sum(overnight_returns) / len(overnight_returns), 3)
        recent_drift = round(sum(overnight_returns[-5:]) / 5, 3)
        trend = 'alcista' if avg_drift > 0.1 else 'bajista' if avg_drift < -0.1 else 'neutral'
        return avg_drift, trend, recent_drift
    except:
        return 0, 'neutral', 0


def get_gap_room(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='1y', interval='1d')
        if len(hist) < 20:
            return {'room_up': 5, 'room_down': 5, 'near_resistance': False,
                    'near_support': False, 'resistance': 0, 'support': 0, 'current': 0}
        current = float(hist['Close'].iloc[-1])
        resistance = min(float(hist['High'].max()), float(hist['High'].tail(60).max()) * 1.02)
        support = max(float(hist['Low'].min()), float(hist['Low'].tail(60).min()) * 0.98)
        room_up = round((resistance - current) / current * 100, 2)
        room_down = round((current - support) / current * 100, 2)
        return {
            'room_up': room_up, 'room_down': room_down,
            'near_resistance': bool(room_up < 1.5), 'near_support': bool(room_down < 1.5),
            'resistance': round(resistance, 2), 'support': round(support, 2), 'current': round(current, 2)
        }
    except:
        return {'room_up': 5, 'room_down': 5, 'near_resistance': False,
                'near_support': False, 'resistance': 0, 'support': 0, 'current': 0}


def get_sector_correlation(ticker):
    try:
        peer = SECTOR_PEERS.get(ticker)
        if not peer:
            return 0, None
        peer_hist = yf.Ticker(peer).history(period='5d', interval='1d')
        if len(peer_hist) < 2:
            return 0, None
        peer_change = round(float((peer_hist['Close'].iloc[-1] - peer_hist['Close'].iloc[-2]) / peer_hist['Close'].iloc[-2] * 100), 2)
        if peer_change > 0.5:
            return 1, {'ticker': peer, 'change': peer_change, 'signal': '🟢 Par sectorial ALCISTA'}
        elif peer_change < -0.5:
            return -1, {'ticker': peer, 'change': peer_change, 'signal': '🔴 Par sectorial BAJISTA'}
        return 0, {'ticker': peer, 'change': peer_change, 'signal': '⚪ Par sectorial NEUTRAL'}
    except:
        return 0, None


def get_historical_gap_stats(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="5y")
        if hist.empty:
            return 50
        gaps_up = gaps_down = 0
        for i in range(1, len(hist)):
            gap = (hist['Open'].iloc[i] - hist['Close'].iloc[i-1]) / hist['Close'].iloc[i-1] * 100
            if gap > 0.1:
                gaps_up += 1
            elif gap < -0.1:
                gaps_down += 1
        total = gaps_up + gaps_down
        return round((gaps_up / total) * 100) if total > 0 else 50
    except:
        return 50


def get_technical_score(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="3mo")
        if hist.empty:
            return 0
        close = hist['Close']
        ma20 = close.rolling(20).mean().iloc[-1]
        current = close.iloc[-1]
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = -delta.where(delta < 0, 0).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))
        rsi_val = float(rsi.iloc[-1])
        score = 0
        if current > ma20:
            score += 30
        if rsi_val > 50:
            score += 20
        if rsi_val > 70:
            score -= 10
        if rsi_val < 30:
            score += 20
        return int(score)
    except:
        return 0


# ── CALCULADORA FTMO ──────────────────────────────────────────────────────────

def calculate_mt5_volume(balance, risk_pct, entry_price, stop_loss_price, earnings_mode=False, current_price=None):
    """
    Calcula el volumen óptimo para MetaTrader 5 / FTMO.
    En modo earnings usa gap catastrófico del 15%.
    """
    try:
        balance = float(balance)
        risk_pct = float(risk_pct)
        risk_amount = balance * (risk_pct / 100)

        if earnings_mode and current_price:
            # Modo supervivencia: gap catastrófico del 15%
            catastrophic_gap = float(current_price) * 0.15
            volume = risk_amount / catastrophic_gap
            return {
                'mt5_volume': math.floor(volume * 100) / 100,
                'risk_amount': round(risk_amount, 2),
                'earnings_mode': True,
                'alert': '⚠️ Modo Earnings Activado — Stop Loss ignorado. Riesgo calculado con gap catastrófico del 15%.',
                'catastrophic_gap': round(catastrophic_gap, 2)
            }
        else:
            entry_price = float(entry_price)
            stop_loss_price = float(stop_loss_price)
            sl_distance = abs(entry_price - stop_loss_price)
            if sl_distance == 0:
                return {'mt5_volume': 0, 'risk_amount': round(risk_amount, 2),
                        'earnings_mode': False, 'alert': 'Stop Loss igual al precio de entrada'}
            volume = risk_amount / sl_distance
            return {
                'mt5_volume': math.floor(volume * 100) / 100,
                'risk_amount': round(risk_amount, 2),
                'earnings_mode': False,
                'alert': None,
                'sl_distance': round(sl_distance, 4)
            }
    except Exception as e:
        return {'mt5_volume': 0, 'risk_amount': 0, 'earnings_mode': False, 'alert': f'Error: {str(e)}'}


# ── SEMÁFORO DE CONFIANZA ─────────────────────────────────────────────────────

def get_signal_strength(display_prob, direction):
    if display_prob < 60:
        return {
            'direction_display': 'MERCADO INDECISO / NEUTRO',
            'strength': 'weak',
            'color': 'neutral',
            'description': 'Señal débil. No operar.'
        }
    elif direction == 'ALCISTA':
        return {
            'direction_display': 'ALCISTA',
            'strength': 'strong_bullish',
            'color': 'bullish',
            'description': 'Señal fuerte alcista.'
        }
    else:
        return {
            'direction_display': 'BAJISTA',
            'strength': 'strong_bearish',
            'color': 'bearish',
            'description': 'Señal fuerte bajista.'
        }


# ── CÁLCULO PRINCIPAL ─────────────────────────────────────────────────────────

def calculate_gap_probability(ticker, balance=None, risk_pct=None, entry_price=None, stop_loss_price=None):
    try:
        hist_prob = get_historical_gap_stats(ticker)
        tech_score = get_technical_score(ticker)
        earnings_info = get_earnings_info(ticker)
        news_sentiment, news_list = get_news_sentiment(ticker)
        macro_sentiment, macro_news = get_macro_sentiment()
        whale_score, whale_signals = get_whale_signals(ticker)
        overnight_drift, drift_trend, recent_drift = get_overnight_drift(ticker)
        gap_room = get_gap_room(ticker)
        sector_score, sector_info = get_sector_correlation(ticker)
        futures_score, futures_signal, futures_change, index_name = get_futures_sentiment(ticker)
        social_score, social_trend, bull_pct, msg_count = get_stocktwits_sentiment(ticker)
        is_fakeout, fakeout_reason, rsi_value = get_fakeout_detector(ticker, tech_score)
        volume_data = get_volume_analysis(ticker)
        news_restriction, news_restriction_reason = check_high_impact_news()

        final_prob = hist_prob + (tech_score * 0.3) + (news_sentiment * 15) + (macro_sentiment * 10)

        days_to_e = earnings_info.get('days_to_earnings', 999)
        if days_to_e <= 1:
            final_prob += 10
        elif days_to_e <= 7:
            final_prob += 5

        final_prob += whale_score * 5
        final_prob += 5 if overnight_drift > 0.1 else -5 if overnight_drift < -0.1 else 0
        final_prob += sector_score * 8
        final_prob += futures_score * 10
        final_prob += social_score * 8
        final_prob += volume_data['volume_score'] * 5

        if gap_room.get('near_resistance') and final_prob >= 50:
            final_prob -= 8
        if is_fakeout and final_prob >= 50:
            final_prob -= 10

        final_prob = max(15, min(85, final_prob))
        raw_direction = "ALCISTA" if final_prob >= 50 else "BAJISTA"
        display_prob = final_prob if final_prob >= 50 else 100 - final_prob

        futures_warning = (raw_direction == 'ALCISTA' and futures_change <= -0.3) or \
                          (raw_direction == 'BAJISTA' and futures_change >= 0.3)

        signal_info = get_signal_strength(display_prob, raw_direction)

        # Obtener precio actual para calculadora
        current_price = None
        try:
            stock = yf.Ticker(ticker)
            info = stock.fast_info
            current_price = float(info.last_price) if hasattr(info, 'last_price') else None
        except:
            pass

        # Calculadora FTMO
        ftmo_result = None
        if balance and risk_pct:
            earnings_mode = (days_to_e <= 1)
            ep = entry_price or current_price
            ftmo_result = calculate_mt5_volume(balance, risk_pct, ep, stop_loss_price, earnings_mode, current_price)

        return {
            'ticker': ticker,
            'probability': round(display_prob),
            'direction': signal_info['direction_display'],
            'raw_direction': raw_direction,
            'strength': signal_info['strength'],
            'signal_color': signal_info['color'],
            'signal_description': signal_info['description'],
            'current_price': round(current_price, 2) if current_price else None,
            'earnings': earnings_info,
            'tech_score': tech_score,
            'rsi_value': rsi_value,
            'news_sentiment': float(news_sentiment),
            'macro_sentiment': float(macro_sentiment),
            'news': news_list[:5],
            'macro_news': macro_news[:4],
            'whale_signals': whale_signals,
            'whale_score': float(whale_score),
            'overnight_drift': float(overnight_drift),
            'drift_trend': drift_trend,
            'recent_drift': float(recent_drift),
            'gap_room': gap_room,
            'sector_info': sector_info,
            'futures_signal': futures_signal,
            'futures_change': futures_change,
            'futures_warning': futures_warning,
            'index_name': index_name,
            'social_score': float(social_score),
            'social_trend': social_trend,
            'bull_pct': bull_pct,
            'msg_count': msg_count,
            'is_fakeout': is_fakeout,
            'fakeout_reason': fakeout_reason,
            'volume': volume_data,
            'news_restriction': news_restriction,
            'news_restriction_reason': news_restriction_reason,
            'ftmo': ftmo_result
        }
    except Exception as e:
        return {
            'ticker': ticker, 'probability': 50,
            'direction': 'MERCADO INDECISO / NEUTRO', 'raw_direction': 'NEUTRAL',
            'strength': 'weak', 'signal_color': 'neutral',
            'signal_description': 'Error en el análisis',
            'current_price': None,
            'earnings': {'has_earnings': False, 'days_to_earnings': 999, 'earnings_date': None,
                         'investor_url': '', 'status': 'unknown'},
            'tech_score': 0, 'rsi_value': 50, 'news_sentiment': 0, 'macro_sentiment': 0,
            'news': [], 'macro_news': [], 'whale_signals': [], 'whale_score': 0,
            'overnight_drift': 0, 'drift_trend': 'neutral', 'recent_drift': 0,
            'gap_room': {'room_up': 5, 'room_down': 5, 'near_resistance': False, 'near_support': False},
            'sector_info': None, 'futures_signal': 'Sin datos', 'futures_change': 0,
            'futures_warning': False, 'index_name': 'Índice',
            'social_score': 0, 'social_trend': 'Sin datos', 'bull_pct': 50, 'msg_count': 0,
            'is_fakeout': False, 'fakeout_reason': '', 'volume': {'rvol': 1.0, 'volume_signal': 'Sin datos',
            'volume_score': 0, 'absorption': False, 'capitulation': False, 'anomaly': False},
            'news_restriction': False, 'news_restriction_reason': None, 'ftmo': None
        }


# ── TRADES ────────────────────────────────────────────────────────────────────

def load_trades():
    if os.path.exists(TRADES_FILE):
        with open(TRADES_FILE, 'r') as f:
            return json.load(f)
    return []

def save_trades(trades):
    with open(TRADES_FILE, 'w') as f:
        json.dump(trades, f, indent=2)


# ── RUTAS ─────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/analyze', methods=['POST'])
def analyze():
    data = request.json
    ticker = data.get('ticker', '').upper()
    balance = data.get('balance')
    risk_pct = data.get('risk_pct')
    entry_price = data.get('entry_price')
    stop_loss_price = data.get('stop_loss_price')
    result = calculate_gap_probability(ticker, balance, risk_pct, entry_price, stop_loss_price)
    return jsonify(result)

@app.route('/dashboard')
def dashboard():
    return jsonify([calculate_gap_probability(t) for t in MARKET_LEADERS])

@app.route('/earnings_calendar')
def earnings_calendar():
    results = []
    for ticker in MARKET_LEADERS:
        info = get_earnings_info(ticker)
        results.append({
            'ticker': ticker, 'company': COMPANY_NAMES.get(ticker, ticker),
            'has_earnings': info['has_earnings'],
            'days_to_earnings': info['days_to_earnings'],
            'earnings_date': info['earnings_date'],
            'earnings_time': info.get('earnings_time', ''),
            'investor_url': info.get('investor_url', ''),
            'status': info.get('status', 'unknown')
        })
    results.sort(key=lambda x: x['days_to_earnings'])
    return jsonify(results)

@app.route('/trades', methods=['GET'])
def get_trades():
    return jsonify(load_trades())

@app.route('/trades', methods=['POST'])
def add_trade():
    trades = load_trades()
    trade = request.json
    trade['id'] = len(trades) + 1
    trade['date'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    trades.append(trade)
    save_trades(trades)
    return jsonify(trade)

@app.route('/trades/<int:trade_id>', methods=['PUT'])
def update_trade(trade_id):
    trades = load_trades()
    for trade in trades:
        if trade['id'] == trade_id:
            trade.update(request.json)
            break
    save_trades(trades)
    return jsonify({'ok': True})

@app.route('/trades/<int:trade_id>', methods=['DELETE'])
def delete_trade(trade_id):
    trades = load_trades()
    trades = [t for t in trades if t['id'] != trade_id]
    save_trades(trades)
    return jsonify({'ok': True})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
