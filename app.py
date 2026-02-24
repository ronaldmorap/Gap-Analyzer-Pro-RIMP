from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
from textblob import TextBlob
import requests
import json
import os
from datetime import datetime
import xml.etree.ElementTree as ET
import numpy as np

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
            news_list.append({
                'title': title[:80],
                'sentiment': round(sentiment, 2),
                'published': date
            })
        avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
        return round(avg_sentiment, 3), news_list
    except:
        return 0, []


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
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period='20d', interval='1d')
            if len(hist) >= 10:
                avg_volume = hist['Volume'][:-1].mean()
                last_volume = hist['Volume'].iloc[-1]
                ratio = last_volume / avg_volume if avg_volume > 0 else 1
                if ratio >= 3:
                    last_close = hist['Close'].iloc[-1]
                    prev_close = hist['Close'].iloc[-2]
                    price_change = (last_close - prev_close) / prev_close
                    if price_change > 0.005:
                        score += 1
                        signals.append({'signal': '📊 Volumen COMPRA anormal', 'detail': f'Volumen {ratio:.1f}x la media', 'points': 1})
                    elif price_change < -0.005:
                        score -= 1
                        signals.append({'signal': '📊 Volumen VENTA anormal', 'detail': f'Volumen {ratio:.1f}x la media', 'points': -1})
        except:
            pass
        return round(score, 1), signals
    except:
        return 0, []


def get_overnight_drift(ticker):
    """Calcula el rendimiento medio overnight de los últimos 20 días"""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='30d', interval='1d')
        if len(hist) < 5:
            return 0, 'neutral'
        overnight_returns = []
        for i in range(1, len(hist)):
            prev_close = hist['Close'].iloc[i-1]
            curr_open = hist['Open'].iloc[i]
            if prev_close > 0:
                overnight_ret = (curr_open - prev_close) / prev_close * 100
                overnight_returns.append(overnight_ret)
        if not overnight_returns:
            return 0, 'neutral'
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
    """Calcula el espacio hasta la resistencia/soporte más cercano"""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='1y', interval='1d')
        if len(hist) < 20:
            return {'room_up': 0, 'room_down': 0, 'near_resistance': False}
        current = hist['Close'].iloc[-1]
        high_52w = hist['High'].max()
        low_52w = hist['Low'].min()
        high_3m = hist['High'].tail(60).max()
        low_3m = hist['Low'].tail(60).min()
        resistance = min(high_52w, high_3m * 1.02)
        support = max(low_52w, low_3m * 0.98)
        room_up = round((resistance - current) / current * 100, 2)
        room_down = round((current - support) / current * 100, 2)
        near_resistance = room_up < 1.5
        near_support = room_down < 1.5
        return {
            'room_up': room_up,
            'room_down': room_down,
            'near_resistance': near_resistance,
            'near_support': near_support,
            'resistance': round(resistance, 2),
            'support': round(support, 2),
            'current': round(current, 2)
        }
    except:
        return {'room_up': 5, 'room_down': 5, 'near_resistance': False, 'near_support': False}


def get_sector_correlation(ticker):
    """Compara con el par sectorial para confirmar señal"""
    try:
        peer = SECTOR_PEERS.get(ticker)
        if not peer:
            return 0, None
        peer_stock = yf.Ticker(peer)
        peer_hist = peer_stock.history(period='5d', interval='1d')
        if len(peer_hist) < 2:
            return 0, None
        peer_change = (peer_hist['Close'].iloc[-1] - peer_hist['Close'].iloc[-2]) / peer_hist['Close'].iloc[-2] * 100
        if peer_change > 0.5:
            return 1, {'ticker': peer, 'change': round(peer_change, 2), 'signal': '🟢 Par sectorial ALCISTA'}
        elif peer_change < -0.5:
            return -1, {'ticker': peer, 'change': round(peer_change, 2), 'signal': '🔴 Par sectorial BAJISTA'}
        return 0, {'ticker': peer, 'change': round(peer_change, 2), 'signal': '⚪ Par sectorial NEUTRAL'}
    except:
        return 0, None


def get_futures_sentiment():
    """Obtiene el sentimiento de futuros via Yahoo Finance (QQQ como proxy del NQ)"""
    try:
        qqq = yf.Ticker('QQQ')
        hist = qqq.history(period='2d', interval='5m')
        if len(hist) < 6:
            return 0, 'neutral'
        last_30min = hist.tail(6)
        change_30m = (last_30min['Close'].iloc[-1] - last_30min['Close'].iloc[0]) / last_30min['Close'].iloc[0] * 100
        change_30m = round(change_30m, 3)
        if change_30m >= 0.3:
            return 1, f'🟢 Nasdaq Futures +{change_30m}% (últimos 30min)'
        elif change_30m <= -0.3:
            return -1, f'🔴 Nasdaq Futures {change_30m}% (últimos 30min)'
        else:
            return 0, f'⚪ Nasdaq Futures {change_30m}% (últimos 30min)'
    except:
        return 0, 'Sin datos de futuros'


def get_macro_sentiment():
    try:
        queries = [
            'Federal+Reserve+interest+rates',
            'Trump+economy+market+tariffs',
            'stock+market+inflation+GDP',
            'hedge+fund+institutional+investors',
            'Warren+Buffett+Berkshire'
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
                    all_news.append({
                        'title': title[:80],
                        'sentiment': round(sentiment, 2),
                        'published': date
                    })
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
        return score
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
                return {
                    'has_earnings': True,
                    'days_to_earnings': days_to_earnings,
                    'earnings_date': str(earnings_date)[:10]
                }
    except:
        pass
    return {'has_earnings': False, 'days_to_earnings': 999, 'earnings_date': None}


def calculate_gap_probability(ticker):
    hist_prob = get_historical_gap_stats(ticker)
    tech_score = get_technical_score(ticker)
    earnings_info = get_earnings_info(ticker)
    news_sentiment, news_list = get_news_sentiment(ticker)
    macro_sentiment, macro_news = get_macro_sentiment()
    whale_score, whale_signals = get_whale_signals(ticker)
    overnight_drift, drift_trend, recent_drift = get_overnight_drift(ticker)
    gap_room = get_gap_room(ticker)
    sector_score, sector_info = get_sector_correlation(ticker)
    futures_score, futures_signal = get_futures_sentiment()

    final_prob = hist_prob + (tech_score * 0.3) + (news_sentiment * 15) + (macro_sentiment * 10)

    if earnings_info['days_to_earnings'] <= 1:
        final_prob += 10
    elif earnings_info['days_to_earnings'] <= 7:
        final_prob += 5

    # Señales de ballenas
    final_prob += whale_score * 5

    # Overnight drift
    if overnight_drift > 0.1:
        final_prob += 5
    elif overnight_drift < -0.1:
        final_prob -= 5

    # Correlación sectorial
    final_prob += sector_score * 8

    # Futuros
    final_prob += futures_score * 10

    # Gap room - si está cerca de resistencia bajamos probabilidad alcista
    if gap_room['near_resistance'] and final_prob >= 50:
        final_prob -= 8

    final_prob = max(15, min(85, final_prob))
    direction = "ALCISTA" if final_prob >= 50 else "BAJISTA"
    display_prob = final_prob if final_prob >= 50 else 100 - final_prob

    return {
        'ticker': ticker,
        'probability': round(display_prob),
        'direction': direction,
        'earnings': earnings_info,
        'tech_score': tech_score,
        'news_sentiment': news_sentiment,
        'macro_sentiment': macro_sentiment,
        'news': news_list[:5],
        'macro_news': macro_news[:4],
        'whale_signals': whale_signals,
        'whale_score': whale_score,
        'overnight_drift': overnight_drift,
        'drift_trend': drift_trend,
        'recent_drift': recent_drift,
        'gap_room': gap_room,
        'sector_info': sector_info,
        'futures_signal': futures_signal
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
            'ticker': ticker,
            'company': COMPANY_NAMES.get(ticker, ticker),
            'has_earnings': info['has_earnings'],
            'days_to_earnings': info['days_to_earnings'],
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
