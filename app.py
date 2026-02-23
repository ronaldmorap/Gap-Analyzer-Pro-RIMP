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

TRADES_FILE = 'C:\\GapAnalyzer\\trades.json'

def get_news_sentiment(ticker):
    try:
        company = COMPANY_NAMES.get(ticker, ticker)
        url = f'https://news.google.com/rss/search?q={company}+stock&hl=en-US&gl=US&ceid=US:en'
        response = requests.get(url, timeout=10)
        root = ET.fromstring(response.content)
        
        sentiments = []
        news_list = []
        
        for item in root.findall('.//item')[:10]:
            title = item.find('title').text or ''
            description = item.find('description')
            desc = description.text if description is not None else ''
            pub_date = item.find('pubDate')
            date = pub_date.text[:16] if pub_date is not None else ''
            
            text = title + ' ' + (desc or '')
            blob = TextBlob(text)
            sentiment = blob.sentiment.polarity
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
def get_insider_activity(ticker):
    """Detecta compras/ventas de insiders via SEC EDGAR"""
    try:
        headers = {'User-Agent': 'GapAnalyzer contact@gmail.com'}
        url = f'https://efts.sec.gov/LATEST/search-index?q="{ticker}"&dateRange=custom&startdt={datetime.now().strftime("%Y-%m-%d")}&forms=4'
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()
        
        score = 0
        signals = []
        
        if data.get('hits', {}).get('hits'):
            for hit in data['hits']['hits'][:5]:
                filing = hit.get('_source', {})
                form_type = filing.get('form_type', '')
                
                if form_type == '4':
                    # Buscar en noticias si hay compras o ventas de insiders
                    news_url = f'https://news.google.com/rss/search?q={ticker}+insider+buying+SEC&hl=en-US&gl=US&ceid=US:en'
                    news_response = requests.get(news_url, timeout=10)
                    news_root = ET.fromstring(news_response.content)
                    
                    for item in news_root.findall('.//item')[:3]:
                        title = item.find('title').text or ''
                        title_lower = title.lower()
                        sentiment = TextBlob(title).sentiment.polarity
                        
                        if any(word in title_lower for word in ['bought', 'purchased', 'acquired', 'buys']):
                            score += 3
                            signals.append({'signal': '🐋 Insider COMPRA', 'detail': title[:80], 'points': +3})
                        elif any(word in title_lower for word in ['sold', 'sells', 'disposed', 'selling']):
                            score -= 2.5
                            signals.append({'signal': '🔴 Insider VENDE', 'detail': title[:80], 'points': -2.5})
        
        # Buscar también en noticias directamente
        for query in [f'{ticker}+insider+buying', f'{ticker}+insider+selling', f'{ticker}+SEC+form+4']:
            news_url = f'https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en'
            news_response = requests.get(news_url, timeout=10)
            news_root = ET.fromstring(news_response.content)
            
            for item in news_root.findall('.//item')[:2]:
                title = item.find('title').text or ''
                title_lower = title.lower()
                pub_date = item.find('pubDate').text or ''
                
                # Multiplicador after-hours x1.5
                multiplier = 1.0
                if any(word in pub_date.lower() for word in ['after', 'pm']):
                    multiplier = 1.5
                
                if any(word in title_lower for word in ['bought', 'purchased', 'acquired', 'insider buy']):
                    pts = round(3 * multiplier, 1)
                    score += pts
                    signals.append({'signal': '🐋 Insider COMPRA', 'detail': title[:80], 'points': pts})
                elif any(word in title_lower for word in ['sold', 'sells', 'insider sell', 'disposed']):
                    pts = round(-2.5 * multiplier, 1)
                    score += pts
                    signals.append({'signal': '🔴 Insider VENDE', 'detail': title[:80], 'points': pts})
                elif any(word in title_lower for word in ['hindenburg', 'short seller', 'fraud', 'citron']):
                    pts = round(-3 * multiplier, 1)
                    score += pts
                    signals.append({'signal': '⚠️ SHORT SELLER ATAQUE', 'detail': title[:80], 'points': pts})
        
        return round(score, 1), signals
    except:
        return 0, []


def get_bank_ratings(ticker):
    """Detecta upgrades/downgrades de grandes bancos"""
    try:
        score = 0
        signals = []
        banks = ['Goldman+Sachs', 'Morgan+Stanley', 'JP+Morgan', 'Bank+of+America', 'Citigroup', 'Wells+Fargo']
        
        for bank in banks:
            url = f'https://news.google.com/rss/search?q={ticker}+{bank}+rating&hl=en-US&gl=US&ceid=US:en'
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)
            
            for item in root.findall('.//item')[:2]:
                title = item.find('title').text or ''
                title_lower = title.lower()
                pub_date = item.find('pubDate').text or ''
                
                multiplier = 1.5 if any(h in pub_date for h in ['after', 'PM']) else 1.0
                
                if any(word in title_lower for word in ['upgrade', 'buy', 'outperform', 'overweight', 'strong buy']):
                    pts = round(1.5 * multiplier, 1)
                    score += pts
                    signals.append({'signal': f'🏦 {bank.replace("+", " ")} UPGRADE', 'detail': title[:80], 'points': pts})
                elif any(word in title_lower for word in ['downgrade', 'sell', 'underperform', 'underweight', 'reduce']):
                    pts = round(-1.5 * multiplier, 1)
                    score += pts
                    signals.append({'signal': f'🏦 {bank.replace("+", " ")} DOWNGRADE', 'detail': title[:80], 'points': pts})
        
        return round(score, 1), signals
    except:
        return 0, []


def get_volume_anomaly(ticker):
    """Detecta volumen anormal al cierre"""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='20d', interval='1d')
        
        if len(hist) < 10:
            return 0, []
        
        avg_volume = hist['Volume'][:-1].mean()
        last_volume = hist['Volume'].iloc[-1]
        
        score = 0
        signals = []
        
        ratio = last_volume / avg_volume if avg_volume > 0 else 1
        
        if ratio >= 3:
            last_close = hist['Close'].iloc[-1]
            prev_close = hist['Close'].iloc[-2]
            price_change = (last_close - prev_close) / prev_close
            
            if price_change > 0.005:
                score += 1
                signals.append({'signal': '📊 Volumen COMPRA anormal', 'detail': f'Volumen {ratio:.1f}x la media con subida de precio', 'points': +1})
            elif price_change < -0.005:
                score -= 1
                signals.append({'signal': '📊 Volumen VENTA anormal', 'detail': f'Volumen {ratio:.1f}x la media con bajada de precio', 'points': -1})
            else:
                signals.append({'signal': '📊 Block Trade detectado', 'detail': f'Volumen {ratio:.1f}x la media sin movimiento claro (trampa posible)', 'points': 0})
        
        return round(score, 1), signals
    except:
        return 0, []
def get_macro_sentiment():
    try:
        queries = [
    'Federal+Reserve+interest+rates',
    'stock+market+economy',
    'inflation+GDP',
    'Trump+economy+market',
    'Trump+tariffs+trade',
    'Trump+policy+stocks',
    'institutional+investors+buying+stocks',
    'hedge+fund+market+moves',
    'Warren+Buffett+Berkshire+investing'
]
        all_sentiments = []
        all_news = []
        
        for query in queries:
            url = f'https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en'
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)
            
            for item in root.findall('.//item')[:4]:
                title = item.find('title').text or ''
                pub_date = item.find('pubDate')
                date = pub_date.text[:16] if pub_date is not None else ''
                blob = TextBlob(title)
                sentiment = blob.sentiment.polarity
                all_sentiments.append(sentiment)
                all_news.append({
                    'title': title[:80],
                    'sentiment': round(sentiment, 2),
                    'published': date,
                    'type': 'MACRO'
                })
        
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

    final_prob = hist_prob + (tech_score * 0.3) + (news_sentiment * 15) + (macro_sentiment * 10)

    if earnings_info['days_to_earnings'] <= 1:
        final_prob += 10
    elif earnings_info['days_to_earnings'] <= 7:
        final_prob += 5
# FASE 1: Señales de Ballenas
        insider_score, insider_signals = get_insider_activity(ticker)
        bank_score, bank_signals = get_bank_ratings(ticker)
        volume_score, volume_signals = get_volume_anomaly(ticker)
        
        whale_score = insider_score + bank_score + volume_score
        final_prob += whale_score * 5
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
        'macro_news': macro_news,'whale_signals': insider_signals + bank_signals + volume_signals,
        'whale_score': whale_score[:4]
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