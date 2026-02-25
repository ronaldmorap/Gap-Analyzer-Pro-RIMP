from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
from textblob import TextBlob
import requests
import json
import os
from datetime import datetime
import xml.etree.ElementTree as ET

app = Flask(__name__)

MAGNIFICENT_7 = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA']

COMPANY_NAMES = {
    'AAPL': 'Apple', 'MSFT': 'Microsoft', 'GOOGL': 'Google',
    'AMZN': 'Amazon', 'NVDA': 'Nvidia', 'META': 'Meta', 'TSLA': 'Tesla'
}

SECTOR_PEERS = {
    'META': 'GOOGL', 'GOOGL': 'META', 'AAPL': 'MSFT', 'MSFT': 'AAPL',
    'NVDA': 'AMD', 'AMZN': 'MSFT', 'TSLA': 'RIVN'
}

STOCKTWITS_MAP = {
    'AAPL': 'AAPL', 'MSFT': 'MSFT', 'GOOGL': 'GOOGL', 'AMZN': 'AMZN',
    'NVDA': 'NVDA', 'META': 'META', 'TSLA': 'TSLA'
}

TRADES_FILE = 'trades.json'


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


def get_stocktwits_sentiment(ticker):
    """Sentimiento social de StockTwits - API gratuita"""
    try:
        url = f'https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json'
        response = requests.get(url, timeout=8)
        if response.status_code != 200:
            return 0, 'Sin datos', 0, 0
        data = response.json()
        messages = data.get('messages', [])
        if not messages:
            return 0, 'Sin datos', 0, 0
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
                blob = TextBlob(body)
                sentiments.append(blob.sentiment.polarity)
        total = bullish + bearish
        bull_pct = round((bullish / total) * 100) if total > 0 else 50
        avg_sent = round(sum(sentiments) / len(sentiments), 3) if sentiments else 0
        if bull_pct >= 65:
            trend = '🟢 Muy alcista'
            score = 1
        elif bull_pct >= 55:
            trend = '🟡 Ligeramente alcista'
            score = 0.5
        elif bull_pct <= 35:
            trend = '🔴 Muy bajista'
            score = -1
        elif bull_pct <= 45:
            trend = '🟠 Ligeramente bajista'
            score = -0.5
        else:
            trend = '⚪ Neutral'
            score = 0
        return score, trend, bull_pct, len(messages)
    except:
        return 0, 'Sin datos StockTwits', 50, 0


def get_fakeout_detector(ticker, rsi_score):
    """Detecta posibles fakeouts: sentimiento eufórico + RSI sobrecomprado"""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='3mo')
        if hist.empty:
            return False, 'Sin datos', 0
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
        last_change = (close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100
        if last_change > 4:
            fakeout_risk += 2
            reasons.append(f'Subida del día muy alta (+{round(last_change, 1)}%)')
        elif last_change < -4:
            fakeout_risk += 1
            reasons.append(f'Caída del día muy alta ({round(last_change, 1)}%)')
        is_fakeout = fakeout_risk >= 3
        reason_text = ' + '.join(reasons) if reasons else 'Sin riesgo detectado'
        return is_fakeout, reason_text, round(rsi, 1)
    except:
        return False, 'Sin datos', 0


def get_volume_analysis(ticker):
    """Análisis completo de volumen: RVOL, absorción y capitulación"""
    try:
        stock = yf.Ticker(ticker)
        hist_daily = stock.history(period='30d', interval='1d')
        if len(hist_daily) < 10:
            return {
                'rvol': 1.0, 'volume_signal': '⚪ Normal', 'volume_score': 0,
                'absorption': False, 'capitulation': False, 'anomaly': False,
                'last_volume': 0, 'avg_volume': 0
            }

        avg_volume = float(hist_daily['Volume'][:-1].mean())
        last_volume = float(hist_daily['Volume'].iloc[-1])
        rvol = round(last_volume / avg_volume, 2) if avg_volume > 0 else 1.0

        last_close = float(hist_daily['Close'].iloc[-1])
        prev_close = float(hist_daily['Close'].iloc[-2])
        price_change_pct = (last_close - prev_close) / prev_close * 100

        volume_score = 0
        volume_signal = '⚪ Volumen normal'
        absorption = False
        capitulation = False
        anomaly = False

        # RVOL alto con precio subiendo = instituciones acumulando
        if rvol >= 2.0 and price_change_pct > 0.3:
            volume_score = 2
            volume_signal = f'⚡ Acumulación institucional (RVOL {rvol}x)'
            anomaly = True
        # RVOL alto con precio bajando = distribución
        elif rvol >= 2.0 and price_change_pct < -0.3:
            volume_score = -2
            volume_signal = f'🔴 Distribución institucional (RVOL {rvol}x)'
            anomaly = True
        # ABSORCIÓN: volumen gigante pero precio no se mueve
        elif rvol >= 2.0 and abs(price_change_pct) <= 0.3:
            volume_score = -1
            volume_signal = f'⚠️ Absorción detectada (RVOL {rvol}x, precio plano)'
            absorption = True
            anomaly = True
        # RVOL moderado
        elif rvol >= 1.5 and price_change_pct > 0.2:
            volume_score = 1
            volume_signal = f'🟡 Volumen elevado alcista (RVOL {rvol}x)'
        elif rvol >= 1.5 and price_change_pct < -0.2:
            volume_score = -1
            volume_signal = f'🟠 Volumen elevado bajista (RVOL {rvol}x)'
        else:
            volume_signal = f'⚪ Volumen normal (RVOL {rvol}x)'

        # CAPITULACIÓN: volumen gigante + precio sube fuerte al final del día
        try:
            hist_intra = stock.history(period='2d', interval='5m')
            if len(hist_intra) >= 12:
                last_15min = hist_intra.tail(3)
                avg_5min_vol = float(hist_intra['Volume'].mean())
                last_vol = float(last_15min['Volume'].sum())
                last_price_move = float((last_15min['Close'].iloc[-1] - last_15min['Close'].iloc[0]) / last_15min['Close'].iloc[0] * 100)
                if last_vol > avg_5min_vol * 3 * 3 and last_price_move > 0.3:
                    capitulation = True
                    volume_score += 1.5
                    volume_signal += ' + 🚀 Capitulación cortos detectada'
        except:
            pass

        return {
            'rvol': rvol,
            'volume_signal': volume_signal,
            'volume_score': round(volume_score, 1),
            'absorption': absorption,
            'capitulation': capitulation,
            'anomaly': anomaly,
            'last_volume': int(last_volume),
            'avg_volume': int(avg_volume),
            'price_change_pct': round(price_change_pct, 2)
        }
    except:
        return {
            'rvol': 1.0, 'volume_signal': '⚪ Sin datos de volumen', 'volume_score': 0,
            'absorption': False, 'capitulation': False, 'anomaly': False,
            'last_volume': 0, 'avg_volume': 0, 'price_change_pct': 0
        }


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


def get_overnight_drift(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='30d', interval='1d')
        if len(hist) < 5:
            return 0, 'neutral', 0
        overnight_returns = []
        for i in range(1, len(hist)):
            prev_close = hist['Close'].iloc[i-1]
            curr_open = hist['Open'].iloc[i]
            if prev_close > 0:
                overnight_ret = (curr_open - prev_close) / prev_close * 100
                overnight_returns.append(overnight_ret)
        if not overnight_returns:
            return 0, 'neutral', 0
        avg_drift = round(sum(overnight_returns) / len(overnight_returns), 3)
        recent_drift = round(sum(overnight_returns[-5:]) / 5, 3)
        if avg_drift > 0.1:
            trend = 'alcista'
        elif avg_drift < -0.1:
            trend = 'bajista'
        else:
            trend = 'neutral'
        return avg_drift, trend, recent_drift
    except:
        return 0, 'neutral', 0


def get_gap_room(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='1y', interval='1d')
        if len(hist) < 20:
            return {'room_up': 5, 'room_down': 5, 'near_resistance': False, 'near_support': False, 'resistance': 0, 'support': 0, 'current': 0}
        current = float(hist['Close'].iloc[-1])
        high_52w = float(hist['High'].max())
        low_52w = float(hist['Low'].min())
        high_3m = float(hist['High'].tail(60).max())
        low_3m = float(hist['Low'].tail(60).min())
        resistance = min(high_52w, high_3m * 1.02)
        support = max(low_52w, low_3m * 0.98)
        room_up = round((resistance - current) / current * 100, 2)
        room_down = round((current - support) / current * 100, 2)
        return {
            'room_up': room_up, 'room_down': room_down,
            'near_resistance': bool(room_up < 1.5), 'near_support': bool(room_down < 1.5),
            'resistance': round(resistance, 2), 'support': round(support, 2), 'current': round(current, 2)
        }
    except:
        return {'room_up': 5, 'room_down': 5, 'near_resistance': False, 'near_support': False, 'resistance': 0, 'support': 0, 'current': 0}


def get_sector_correlation(ticker):
    try:
        peer = SECTOR_PEERS.get(ticker)
        if not peer:
            return 0, None
        peer_stock = yf.Ticker(peer)
        peer_hist = peer_stock.history(period='5d', interval='1d')
        if len(peer_hist) < 2:
            return 0, None
        peer_change = float((peer_hist['Close'].iloc[-1] - peer_hist['Close'].iloc[-2]) / peer_hist['Close'].iloc[-2] * 100)
        peer_change = round(peer_change, 2)
        if peer_change > 0.5:
            return 1, {'ticker': peer, 'change': peer_change, 'signal': '🟢 Par sectorial ALCISTA'}
        elif peer_change < -0.5:
            return -1, {'ticker': peer, 'change': peer_change, 'signal': '🔴 Par sectorial BAJISTA'}
        return 0, {'ticker': peer, 'change': peer_change, 'signal': '⚪ Par sectorial NEUTRAL'}
    except:
        return 0, None


def get_futures_sentiment():
    try:
        qqq = yf.Ticker('QQQ')
        hist = qqq.history(period='2d', interval='5m')
        if len(hist) < 6:
            return 0, 'Sin datos', 0
        last_30min = hist.tail(6)
        change_30m = float((last_30min['Close'].iloc[-1] - last_30min['Close'].iloc[0]) / last_30min['Close'].iloc[0] * 100)
        change_30m = round(change_30m, 3)
        if change_30m >= 0.3:
            return 1, f'🟢 Nasdaq +{change_30m}% ultimos 30min', change_30m
        elif change_30m <= -0.3:
            return -1, f'🔴 Nasdaq {change_30m}% ultimos 30min', change_30m
        else:
            return 0, f'⚪ Nasdaq {change_30m}% ultimos 30min', change_30m
    except:
        return 0, 'Sin datos de futuros', 0


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


def get_historical_gap_stats(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="5y")
        if hist.empty:
            return 50
        gaps_up = 0
        gaps_down = 0
        for i in range(1, len(hist)):
            prev_close = hist['Close'].iloc[i-1]
            curr_open = hist['Open'].iloc[i]
            gap = (curr_open - prev_close) / prev_close * 100
            if gap > 0.1:
                gaps_up += 1
            elif gap < -0.1:
                gaps_down += 1
        total = gaps_up + gaps_down
        if total == 0:
            return 50
        return round((gaps_up / total) * 100)
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
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs.iloc[-1]))
        score = 0
        if current > ma20:
            score += 30
        if rsi > 50:
            score += 20
        if rsi > 70:
            score -= 10
        if rsi < 30:
            score += 20
        return int(score)
    except:
        return 0


def get_earnings_info(ticker):
    try:
        stock = yf.Ticker(ticker)
        calendar = stock.calendar
        if calendar is not None and not calendar.empty:
            earnings_date = calendar.iloc[0]['Earnings Date'] if 'Earnings Date' in calendar.columns else None
            if earnings_date:
                days_to_earnings = (pd.Timestamp(earnings_date) - pd.Timestamp.now()).days
                return {'has_earnings': True, 'days_to_earnings': int(days_to_earnings), 'earnings_date': str(earnings_date)[:10]}
    except:
        pass
    return {'has_earnings': False, 'days_to_earnings': 999, 'earnings_date': None}


def calculate_gap_probability(ticker):
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
        futures_score, futures_signal, futures_change = get_futures_sentiment()
        social_score, social_trend, bull_pct, msg_count = get_stocktwits_sentiment(ticker)
        is_fakeout, fakeout_reason, rsi_value = get_fakeout_detector(ticker, tech_score)
        volume_data = get_volume_analysis(ticker)

        final_prob = hist_prob + (tech_score * 0.3) + (news_sentiment * 15) + (macro_sentiment * 10)

        if earnings_info['days_to_earnings'] <= 1:
            final_prob += 10
        elif earnings_info['days_to_earnings'] <= 7:
            final_prob += 5

        final_prob += whale_score * 5
        if overnight_drift > 0.1:
            final_prob += 5
        elif overnight_drift < -0.1:
            final_prob -= 5

        final_prob += sector_score * 8
        final_prob += futures_score * 10
        final_prob += social_score * 8
        final_prob += volume_data['volume_score'] * 5

        if gap_room.get('near_resistance') and final_prob >= 50:
            final_prob -= 8

        # Fakeout penaliza si la señal es alcista pero hay riesgo
        if is_fakeout and final_prob >= 50:
            final_prob -= 10

        final_prob = max(15, min(85, final_prob))
        direction = "ALCISTA" if final_prob >= 50 else "BAJISTA"
        display_prob = final_prob if final_prob >= 50 else 100 - final_prob

        # Alerta de contradicción futuros
        futures_warning = False
        if direction == 'ALCISTA' and futures_change <= -0.3:
            futures_warning = True
        elif direction == 'BAJISTA' and futures_change >= 0.3:
            futures_warning = True

        return {
            'ticker': ticker,
            'probability': round(display_prob),
            'direction': direction,
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
            'social_score': float(social_score),
            'social_trend': social_trend,
            'bull_pct': bull_pct,
            'msg_count': msg_count,
            'is_fakeout': is_fakeout,
            'fakeout_reason': fakeout_reason,
            'volume': volume_data
        }
    except Exception as e:
        return {
            'ticker': ticker, 'probability': 50, 'direction': 'NEUTRAL',
            'earnings': {'has_earnings': False, 'days_to_earnings': 999, 'earnings_date': None},
            'tech_score': 0, 'rsi_value': 50, 'news_sentiment': 0, 'macro_sentiment': 0,
            'news': [], 'macro_news': [], 'whale_signals': [], 'whale_score': 0,
            'overnight_drift': 0, 'drift_trend': 'neutral', 'recent_drift': 0,
            'gap_room': {'room_up': 5, 'room_down': 5, 'near_resistance': False, 'near_support': False},
            'sector_info': None, 'futures_signal': 'Sin datos', 'futures_change': 0,
            'futures_warning': False, 'social_score': 0, 'social_trend': 'Sin datos',
            'bull_pct': 50, 'msg_count': 0, 'is_fakeout': False, 'fakeout_reason': '',
            'volume': {'rvol': 1.0, 'volume_signal': 'Sin datos', 'volume_score': 0,
                       'absorption': False, 'capitulation': False, 'anomaly': False}
        }


def load_trades():
    if os.path.exists(TRADES_FILE):
        with open(TRADES_FILE, 'r') as f:
            return json.load(f)
    return []


def save_trades(trades):
    with open(TRADES_FILE, 'w') as f:
        json.dump(trades, f, indent=2)


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/analyze', methods=['POST'])
def analyze():
    data = request.json
    ticker = data.get('ticker', '').upper()
    result = calculate_gap_probability(ticker)
    return jsonify(result)

@app.route('/dashboard')
def dashboard():
    results = []
    for ticker in MAGNIFICENT_7:
        result = calculate_gap_probability(ticker)
        results.append(result)
    return jsonify(results)

@app.route('/earnings_calendar')
def earnings_calendar():
    results = []
    for ticker in MAGNIFICENT_7:
        info = get_earnings_info(ticker)
        results.append({
            'ticker': ticker, 'company': COMPANY_NAMES.get(ticker, ticker),
            'has_earnings': info['has_earnings'], 'days_to_earnings': info['days_to_earnings'],
            'earnings_date': info['earnings_date']
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
