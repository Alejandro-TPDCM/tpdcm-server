"""
TPDCM-IA - Trading Platform Deep Claude Machine Intelligence
EUR/USD Institucional - Prop Firm System
"""
import asyncio
import json
import os
import time
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict
from typing import Optional

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger('TPDCM')

ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
OANDA_TOKEN       = os.environ.get('OANDA_API_TOKEN', '')
OANDA_ACCOUNT     = os.environ.get('OANDA_ACCOUNT_ID', '')
OANDA_ENV         = os.environ.get('OANDA_ENVIRONMENT', 'practice')
AUTO_EXECUTE      = os.environ.get('AUTO_EXECUTE', 'false').lower() == 'true'
RISK_PCT          = float(os.environ.get('RISK_PCT', '1.0'))
MIN_CONFIDENCE    = float(os.environ.get('MIN_CONFIDENCE', '0.75'))

PAIR             = 'EUR_USD'
CLAUDE_MODEL     = 'claude-sonnet-4-6'
SESSION_START_ET = 3
SESSION_END_ET   = 12
FRIDAY_CLOSE_ET  = 14
MAX_DAILY_LOSS   = 1.0
CONSEC_PAUSE     = 2
NEWS_BEFORE      = 15
NEWS_AFTER       = 30
MIN_SCORE        = 58
MIN_SCORE_WEDTHU = 65
ADR_MIN          = 0.20
ATR_MIN_PIPS     = 8
ATR_MAX_PIPS     = 80

OANDA_BASE = (
    'https://api-fxpractice.oanda.com'
    if OANDA_ENV == 'practice'
    else 'https://api-fxtrade.oanda.com'
)

app = FastAPI(title='TPDCM-IA', version='1.0.0')
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

HISTORY_FILE = '/tmp/tpdcm_history.json'
TRADES_FILE  = '/tmp/tpdcm_trades.json'
MEMORY_FILE  = '/tmp/tpdcm_memory.json'

def load_file(path, default):
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception:
        return default

def save_file(path, data):
    try:
        with open(path, 'w') as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        log.warning(f'[SAVE] {path}: {e}')

state = {
    'balance':            0.0,
    'open_trades':        [],
    'last_analysis':      None,
    'last_decision':      None,
    'last_update':        None,
    'history':            load_file(HISTORY_FILE, []),
    'live_trades':        load_file(TRADES_FILE, []),
    'daily_loss_usd':     0.0,
    'daily_loss_date':    None,
    'consecutive_losses': 0,
    'risk_pct_current':   RISK_PCT,
    'trading_paused':     False,
    'pause_reason':       '',
    'active_trades_meta': {},
    'defensive_mode':     False,
    'defensive_reason':   '',
}

memory = load_file(MEMORY_FILE, {
    'recent_trades':      [],
    'session_stats':      {},
    'sweep_quality_hist': [],
    'edge_score':         100.0,
    'last_updated':       None,
})

bt_state = {
    'trades':   [],
    'summary':  None,
    'last_run': None,
    'running':  False,
    'log':      [],
}

SCORE_WEIGHTS = {
    'htf_alignment':   18,
    'sweep_quality':   16,
    'displacement':    15,
    'inducement':      12,
    'bos_quality':     10,
    'fvg_ob_quality':  10,
    'session_quality':  8,
    'adr_remaining':    5,
    'volatility':       4,
    'consolidation':    2,
}

def adjust_weights(trade_log):
    global SCORE_WEIGHTS
    if len(trade_log) < 10:
        return
    failures = defaultdict(int)
    sl_count = 0
    for t in trade_log[-30:]:
        if t.get('outcome') in ('SL', 'BE'):
            sl_count += 1
            for f in t.get('failure_factors', []):
                failures[f] += 1
    if sl_count < 3:
        return
    for factor, count in failures.items():
        if factor in SCORE_WEIGHTS and count / sl_count > 0.5:
            SCORE_WEIGHTS[factor] = min(SCORE_WEIGHTS[factor] * 1.25, SCORE_WEIGHTS[factor] + 3)
    total = sum(SCORE_WEIGHTS.values())
    if total != 100:
        scale = 100 / total
        for k in SCORE_WEIGHTS:
            SCORE_WEIGHTS[k] = round(SCORE_WEIGHTS[k] * scale, 1)
    log.info(f'[LEARN] Pesos ajustados tras {len(trade_log)} trades')

def now_et():
    return datetime.now(ZoneInfo('America/New_York'))

def now_utc():
    return datetime.now(timezone.utc)

def is_session():
    et = now_et()
    if et.weekday() == 6:
        return False
    if et.weekday() == 4 and et.hour >= FRIDAY_CLOSE_ET:
        return False
    return SESSION_START_ET <= et.hour < SESSION_END_ET

def get_killzone(hour):
    if 3 <= hour < 5:
        return 'LONDON_OPEN'
    if 8 <= hour < 11:
        return 'NY_OPEN'
    if 11 <= hour < 12:
        return 'NY_LATE'
    return None

HIGH_IMPACT_EVENTS = [
    {'date': '2025-01-10', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2025-02-07', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2025-03-07', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2025-04-04', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2025-05-02', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2025-06-06', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2025-07-03', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2025-08-01', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2025-09-05', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2025-10-03', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2025-11-07', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2025-12-05', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2026-01-09', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2026-02-06', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2026-03-06', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2026-04-03', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2026-05-01', 'time': '08:30', 'title': 'NFP',  'currency': 'USD'},
    {'date': '2025-01-15', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2025-02-12', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2025-03-12', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2025-04-10', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2025-05-13', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2025-06-11', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2025-07-15', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2025-08-12', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2025-09-10', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2025-10-15', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2025-11-12', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2025-12-10', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2026-01-15', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2026-02-12', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2026-03-11', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2026-04-10', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2026-05-13', 'time': '08:30', 'title': 'CPI',  'currency': 'USD'},
    {'date': '2025-01-29', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2025-03-19', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2025-05-07', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2025-06-18', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2025-07-30', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2025-09-17', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2025-11-05', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2025-12-17', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2026-01-28', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2026-03-18', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2026-05-06', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2025-01-30', 'time': '13:15', 'title': 'ECB',  'currency': 'EUR'},
    {'date': '2025-03-06', 'time': '13:15', 'title': 'ECB',  'currency': 'EUR'},
    {'date': '2025-04-17', 'time': '13:15', 'title': 'ECB',  'currency': 'EUR'},
    {'date': '2025-06-05', 'time': '13:15', 'title': 'ECB',  'currency': 'EUR'},
    {'date': '2025-07-24', 'time': '13:15', 'title': 'ECB',  'currency': 'EUR'},
    {'date': '2025-09-11', 'time': '13:15', 'title': 'ECB',  'currency': 'EUR'},
    {'date': '2025-10-30', 'time': '13:15', 'title': 'ECB',  'currency': 'EUR'},
    {'date': '2025-12-18', 'time': '13:15', 'title': 'ECB',  'currency': 'EUR'},
    {'date': '2026-01-29', 'time': '13:15', 'title': 'ECB',  'currency': 'EUR'},
    {'date': '2026-03-05', 'time': '13:15', 'title': 'ECB',  'currency': 'EUR'},
    {'date': '2026-04-16', 'time': '13:15', 'title': 'ECB',  'currency': 'EUR'},
]

async def fetch_ff_calendar():
    try:
        url = 'https://nfs.faireconomy.media/ff_calendar_thisweek.json'
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
            if r.is_success:
                return [
                    e for e in r.json()
                    if e.get('impact', '').lower() == 'high'
                    and e.get('currency', '').upper() in ('USD', 'EUR')
                ]
    except Exception:
        pass
    return []

def is_news_blocked(candle_dt, events):
    for evt in events:
        try:
            dt_str = f"{evt.get('date','')} {evt.get('time','')}"
            try:
                evt_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M')
            except Exception:
                continue
            naive = candle_dt.replace(tzinfo=None)
            diff  = (evt_dt - naive).total_seconds() / 60
            if -NEWS_AFTER <= diff <= NEWS_BEFORE:
                return True, f"{evt.get('currency','')} {evt.get('title','')} ({diff:.0f}min)"
        except Exception:
            continue
    return False, ''

async def oanda_get(path):
    url = f'{OANDA_BASE}{path}'
    headers = {'Authorization': f'Bearer {OANDA_TOKEN}'}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        return r.json()

async def oanda_post(path, body):
    url = f'{OANDA_BASE}{path}'
    headers = {'Authorization': f'Bearer {OANDA_TOKEN}', 'Content-Type': 'application/json'}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, headers=headers, json=body)
        r.raise_for_status()
        return r.json()

async def oanda_put(path, body):
    url = f'{OANDA_BASE}{path}'
    headers = {'Authorization': f'Bearer {OANDA_TOKEN}', 'Content-Type': 'application/json'}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.put(url, headers=headers, json=body)
        r.raise_for_status()
        return r.json()

async def get_candles(granularity='H1', count=100):
    data = await oanda_get(f'/v3/instruments/{PAIR}/candles?granularity={granularity}&count={count}&price=M')
    return data.get('candles', [])

async def get_candles_to(granularity='H1', count=500, to_dt=None):
    path = f'/v3/instruments/{PAIR}/candles?granularity={granularity}&count={count}&price=M'
    if to_dt:
        ts = to_dt.split('.')[0] + 'Z'
        path += f'&to={ts}'
    data = await oanda_get(path)
    return data.get('candles', [])

async def get_account():
    data = await oanda_get(f'/v3/accounts/{OANDA_ACCOUNT}/summary')
    return data.get('account', {})

async def get_open_trades():
    data = await oanda_get(f'/v3/accounts/{OANDA_ACCOUNT}/openTrades')
    return data.get('trades', [])

async def get_price():
    data = await oanda_get(f'/v3/accounts/{OANDA_ACCOUNT}/pricing?instruments={PAIR}')
    p = data['prices'][0]
    return (float(p['bids'][0]['price']) + float(p['asks'][0]['price'])) / 2

async def place_order(units, sl, tp, action):
    u = str(-abs(units)) if action == 'SELL' else str(abs(units))
    order = {'type': 'MARKET', 'instrument': PAIR, 'units': u, 'timeInForce': 'FOK'}
    if sl > 0: order['stopLossOnFill']   = {'price': f'{sl:.5f}'}
    if tp > 0: order['takeProfitOnFill'] = {'price': f'{tp:.5f}'}
    return await oanda_post(f'/v3/accounts/{OANDA_ACCOUNT}/orders', {'order': order})

async def close_trade(trade_id, partial=None):
    body = {'units': str(abs(partial))} if partial else {}
    return await oanda_put(f'/v3/accounts/{OANDA_ACCOUNT}/trades/{trade_id}/close', body)

async def modify_sl(trade_id, new_sl):
    return await oanda_put(
        f'/v3/accounts/{OANDA_ACCOUNT}/trades/{trade_id}/orders',
        {'stopLoss': {'price': f'{new_sl:.5f}', 'timeInForce': 'GTC'}}
    )

def compute_atr(candles, period=14):
    if len(candles) < period:
        return 0.0010
    trs = [float(c['mid']['h']) - float(c['mid']['l']) for c in candles[-period:]]
    return sum(trs) / len(trs)

def identify_liquidity_levels(h1_window, d1=None):
    levels = {
        'pdh': 0.0, 'pdl': 0.0,
        'weekly_high': 0.0, 'weekly_low': 0.0,
        'h4_eqh': [], 'h4_eql': [],
        'swing_highs': [], 'swing_lows': [],
    }
    if not h1_window:
        return levels
    if d1 and len(d1) >= 2:
        levels['pdh'] = float(d1[-2]['mid']['h'])
        levels['pdl'] = float(d1[-2]['mid']['l'])
    elif len(h1_window) >= 48:
        prev = h1_window[-48:-24]
        levels['pdh'] = max(float(c['mid']['h']) for c in prev)
        levels['pdl'] = min(float(c['mid']['l']) for c in prev)
    wk = h1_window[-120:] if len(h1_window) >= 120 else h1_window
    levels['weekly_high'] = max(float(c['mid']['h']) for c in wk)
    levels['weekly_low']  = min(float(c['mid']['l']) for c in wk)
    recent = h1_window[-40:] if len(h1_window) >= 40 else h1_window
    for i in range(2, len(recent) - 2):
        h = float(recent[i]['mid']['h'])
        l = float(recent[i]['mid']['l'])
        if (h > float(recent[i-1]['mid']['h']) and h > float(recent[i-2]['mid']['h']) and
                h > float(recent[i+1]['mid']['h']) and h > float(recent[i+2]['mid']['h'])):
            levels['swing_highs'].append(round(h, 5))
        if (l < float(recent[i-1]['mid']['l']) and l < float(recent[i-2]['mid']['l']) and
                l < float(recent[i+1]['mid']['l']) and l < float(recent[i+2]['mid']['l'])):
            levels['swing_lows'].append(round(l, 5))
    return levels

def detect_htf_bias(d1=None, h1=None):
    if d1 and len(d1) >= 5:
        closes = [float(c['mid']['c']) for c in d1[-5:]]
        bias   = 'bullish' if closes[-1] > closes[0] else 'bearish'
        strength = min(1.0, abs(closes[-1] - closes[0]) / closes[0] * 100)
        return bias, round(strength, 2)
    if h1 and len(h1) >= 20:
        closes = [float(c['mid']['c']) for c in h1[-20:]]
        ema_f  = sum(closes[-5:]) / 5
        ema_s  = sum(closes) / len(closes)
        bias   = 'bullish' if ema_f > ema_s else 'bearish'
        diff   = abs(ema_f - ema_s) / ema_s * 100
        return bias, round(min(1.0, diff * 10), 2)
    return 'neutral', 0.3

def detect_inducement(candles, lookback=12):
    if len(candles) < lookback:
        return False, 'none'
    window = candles[-lookback:]
    highs  = [float(c['mid']['h']) for c in window]
    lows   = [float(c['mid']['l']) for c in window]
    atr    = compute_atr(candles)
    rng    = max(highs) - min(lows)
    compressed = rng < atr * lookback * 0.35
    p70h   = sorted(highs)[int(len(highs) * 0.70)]
    p30l   = sorted(lows)[int(len(lows)  * 0.30)]
    false_breaks = 0
    for c in window[1:]:
        h = float(c['mid']['h'])
        l = float(c['mid']['l'])
        cl = float(c['mid']['c'])
        if h > p70h and cl < p70h: false_breaks += 1
        if l < p30l and cl > p30l: false_breaks += 1
    tol  = atr * 0.3
    eq_h = sum(1 for i in range(len(highs)) for j in range(i+1, len(highs)) if abs(highs[i]-highs[j]) < tol)
    eq_l = sum(1 for i in range(len(lows))  for j in range(i+1, len(lows))  if abs(lows[i]-lows[j])   < tol)
    score = (2 if compressed else 0) + min(3, false_breaks) + (2 if (eq_h >= 2 or eq_l >= 2) else 0)
    if score >= 5: return True, 'strong'
    if score >= 3: return True, 'medium'
    if score >= 2: return True, 'weak'
    return False, 'none'

def detect_sweep(candles, levels, atr):
    if len(candles) < 6:
        return {'detected': False}
    c    = candles[-1]
    ch   = float(c['mid']['h'])
    cl   = float(c['mid']['l'])
    co   = float(c['mid']['o'])
    cc   = float(c['mid']['c'])
    rng  = max(ch - cl, 0.00001)
    buf  = atr * 0.15
    bear = []
    bull = []
    if levels.get('pdh', 0) > 0:         bear.append(('PDH',         levels['pdh']))
    if levels.get('weekly_high', 0) > 0: bear.append(('WEEKLY_HIGH', levels['weekly_high']))
    for v in levels.get('h4_eqh', []):   bear.append(('H4_EQH',      v))
    for v in levels.get('swing_highs', [])[-3:]: bear.append(('SWING_HIGH', v))
    if levels.get('pdl', 0) > 0:         bull.append(('PDL',         levels['pdl']))
    if levels.get('weekly_low', 0) > 0:  bull.append(('WEEKLY_LOW',  levels['weekly_low']))
    for v in levels.get('h4_eql', []):   bull.append(('H4_EQL',      v))
    for v in levels.get('swing_lows', [])[-3:]:  bull.append(('SWING_LOW',  v))
    if not bear and not bull and len(candles) >= 8:
        prev = candles[-8:-1]
        bear = [('SWING_H1', max(float(x['mid']['h']) for x in prev))]
        bull = [('SWING_H1', min(float(x['mid']['l']) for x in prev))]
    for lt, lv in bear:
        if lv <= 0: continue
        if ch > lv - buf and cc < lv:
            wick = ch - max(co, cc)
            wp   = wick / rng
            ext  = ch - lv
            if wp >= 0.40 and ext >= atr * 0.05:
                q = 'high' if wp >= 0.65 and ext >= atr * 0.15 else 'medium' if wp >= 0.50 else 'low'
                return {'detected': True, 'direction': 'bearish', 'level': round(lv, 5),
                        'level_type': lt, 'wick_pct': round(wp, 3), 'quality': q,
                        'sweep_high': round(ch, 5)}
    for lt, lv in bull:
        if lv <= 0: continue
        if cl < lv + buf and cc > lv:
            wick = min(co, cc) - cl
            wp   = wick / rng
            ext  = lv - cl
            if wp >= 0.40 and ext >= atr * 0.05:
                q = 'high' if wp >= 0.65 and ext >= atr * 0.15 else 'medium' if wp >= 0.50 else 'low'
                return {'detected': True, 'direction': 'bullish', 'level': round(lv, 5),
                        'level_type': lt, 'wick_pct': round(wp, 3), 'quality': q,
                        'sweep_low': round(cl, 5)}
    return {'detected': False}

def detect_displacement(candles, action, atr):
    if len(candles) < 4:
        return False, 'none', {}
    hits = []
    for c in candles[-3:]:
        o  = float(c['mid']['o'])
        cl = float(c['mid']['c'])
        h  = float(c['mid']['h'])
        l  = float(c['mid']['l'])
        rng  = max(h - l, 0.00001)
        body = abs(cl - o)
        bp   = body / rng
        ok   = (action == 'SELL' and cl < o) or (action == 'BUY' and cl > o)
        if ok and bp >= 0.55:
            hits.append({'body': body, 'bp': bp})
    if not hits:
        return False, 'none', {}
    best  = max(hits, key=lambda x: x['bp'])
    total = sum(h['body'] for h in hits)
    if len(hits) >= 2 and best['bp'] >= 0.65: strength = 'strong'
    elif best['bp'] >= 0.60 or total >= atr * 0.7: strength = 'medium'
    else: strength = 'weak'
    return True, strength, {'count': len(hits), 'best_bp': round(best['bp'], 2)}

def detect_bos(candles, action, atr):
    if len(candles) < 15:
        return False, 'none', 0.0
    recent = candles[-15:]
    highs  = [float(c['mid']['h']) for c in recent]
    lows   = [float(c['mid']['l']) for c in recent]
    lc     = float(recent[-1]['mid']['c'])
    lo     = float(recent[-1]['mid']['o'])
    lrng   = max(float(recent[-1]['mid']['h']) - float(recent[-1]['mid']['l']), 0.00001)
    lbody  = abs(lc - lo) / lrng
    if action == 'SELL':
        sl = min(lows[2:-3])
        if lc < sl:
            d = sl - lc
            if d >= atr * 0.3 and lbody >= 0.55:
                q = 'strong' if d >= atr * 0.5 and lbody >= 0.65 else 'medium'
                return True, q, round(sl, 5)
            elif d >= atr * 0.15:
                return True, 'weak', round(sl, 5)
    else:
        sh = max(highs[2:-3])
        if lc > sh:
            d = lc - sh
            if d >= atr * 0.3 and lbody >= 0.55:
                q = 'strong' if d >= atr * 0.5 and lbody >= 0.65 else 'medium'
                return True, q, round(sh, 5)
            elif d >= atr * 0.15:
                return True, 'weak', round(sh, 5)
    return False, 'none', 0.0

def detect_fvg_ob(candles, action, atr):
    result = {'ob': None, 'fvg': None, 'entry_zone': {'high': 0, 'low': 0}, 'valid': False}
    if len(candles) < 8:
        return result
    min_fvg = atr * 0.15
    recent  = candles[-8:]
    for i in range(len(recent) - 3):
        c1h = float(recent[i]['mid']['h'])
        c1l = float(recent[i]['mid']['l'])
        c3h = float(recent[i+2]['mid']['h'])
        c3l = float(recent[i+2]['mid']['l'])
        if action == 'BUY' and c3l > c1h:
            gap = c3l - c1h
            if gap >= min_fvg:
                result['fvg'] = {'high': round(c3l, 5), 'low': round(c1h, 5),
                                 'size': round(gap, 5), 'type': 'bull',
                                 'quality': 'strong' if gap >= atr * 0.3 else 'medium'}
                result['entry_zone'] = {'high': round(c3l, 5), 'low': round(c1h, 5)}
                result['valid'] = True
                break
        elif action == 'SELL' and c3h < c1l:
            gap = c1l - c3h
            if gap >= min_fvg:
                result['fvg'] = {'high': round(c1l, 5), 'low': round(c3h, 5),
                                 'size': round(gap, 5), 'type': 'bear',
                                 'quality': 'strong' if gap >= atr * 0.3 else 'medium'}
                result['entry_zone'] = {'high': round(c1l, 5), 'low': round(c3h, 5)}
                result['valid'] = True
                break
    for i in range(len(recent) - 3, 0, -1):
        cv   = recent[i]
        co   = float(cv['mid']['o'])
        cc_v = float(cv['mid']['c'])
        ch   = float(cv['mid']['h'])
        cl   = float(cv['mid']['l'])
        nxt_o = float(recent[i+1]['mid']['o'])
        nxt_c = float(recent[i+1]['mid']['c'])
        is_ob = (action == 'BUY' and cc_v < co) or (action == 'SELL' and cc_v > co)
        confirms = (action == 'BUY' and nxt_c > nxt_o) or (action == 'SELL' and nxt_c < nxt_o)
        size = abs(ch - cl)
        if is_ob and confirms and size >= atr * 0.2:
            result['ob'] = {'high': round(ch, 5), 'low': round(cl, 5), 'size': round(size, 5),
                            'quality': 'strong' if size >= atr * 0.4 else 'medium'}
            if not result['valid']:
                result['entry_zone'] = {'high': round(ch, 5), 'low': round(cl, 5)}
                result['valid'] = True
            break
    return result

def compute_liquidity_target(action, levels, price, atr):
    targets = []
    if action == 'SELL':
        for name, val in [('PDL', levels.get('pdl', 0)), ('WEEKLY_LOW', levels.get('weekly_low', 0))]:
            if val > 0 and val < price and price - val >= atr * 0.5:
                targets.append((name, val, price - val))
        for v in levels.get('h4_eql', []):
            if v < price and price - v >= atr * 0.5:
                targets.append(('H4_EQL', v, price - v))
    else:
        for name, val in [('PDH', levels.get('pdh', 0)), ('WEEKLY_HIGH', levels.get('weekly_high', 0))]:
            if val > 0 and val > price and val - price >= atr * 0.5:
                targets.append((name, val, val - price))
        for v in levels.get('h4_eqh', []):
            if v > price and v - price >= atr * 0.5:
                targets.append(('H4_EQH', v, v - price))
    if not targets:
        return 0.0, 'NONE', 0.0
    targets.sort(key=lambda x: x[2])
    best = targets[0]
    return best[1], best[0], round(best[2] / atr, 1)

def compute_score(htf_bias, htf_strength, sweep, inducement, disp_strength, bos_data, fvg_ob,
                  killzone, adr_pct, atr_pips, consol_ok, has_target):
    W      = SCORE_WEIGHTS
    score  = 0.0
    factors = {}
    action = 'SELL' if sweep.get('direction') == 'bearish' else 'BUY'
    aligned = (action == 'SELL' and htf_bias == 'bearish') or (action == 'BUY' and htf_bias == 'bullish')
    if htf_strength >= 0.7:   pts = W['htf_alignment'] * (1.0 if aligned else 0.2); tag = 'fuerte' if aligned else 'CONTRA HTF'
    elif htf_strength >= 0.4: pts = W['htf_alignment'] * (0.6 if aligned else 0.15); tag = 'moderado' if aligned else 'CONTRA HTF'
    else:                     pts = W['htf_alignment'] * (0.3 if aligned else 0.1); tag = 'debil'
    score += pts; factors['htf_alignment'] = {'pts': round(pts,1), 'max': W['htf_alignment'], 'tag': tag}
    sq  = sweep.get('quality', 'low'); lt = sweep.get('level_type', '')
    sp  = {'high': 1.0, 'medium': 0.65, 'low': 0.25}.get(sq, 0.25)
    bon = 1.2 if lt in ('PDH','PDL','WEEKLY_HIGH','WEEKLY_LOW') else 1.1 if lt in ('H4_EQH','H4_EQL') else 1.0
    pts = min(W['sweep_quality'], W['sweep_quality'] * sp * bon)
    score += pts; factors['sweep_quality'] = {'pts': round(pts,1), 'max': W['sweep_quality'], 'tag': f'{sq} {lt}'}
    ds_map = {'strong': 1.0, 'medium': 0.65, 'weak': 0.30, 'none': 0.0}
    pts = W['displacement'] * ds_map.get(disp_strength, 0)
    score += pts; factors['displacement'] = {'pts': round(pts,1), 'max': W['displacement'], 'tag': disp_strength}
    ind_found, ind_q = inducement
    iq_map = {'strong': 1.0, 'medium': 0.6, 'weak': 0.3, 'none': 0.0}
    pts = W['inducement'] * (iq_map.get(ind_q, 0) if ind_found else 0)
    score += pts; factors['inducement'] = {'pts': round(pts,1), 'max': W['inducement'], 'tag': ind_q if ind_found else 'ausente'}
    bos_found, bos_q, _ = bos_data
    bq_map = {'strong': 1.0, 'medium': 0.65, 'weak': 0.3, 'none': 0.0}
    pts = W['bos_quality'] * (bq_map.get(bos_q, 0) if bos_found else 0)
    score += pts; factors['bos_quality'] = {'pts': round(pts,1), 'max': W['bos_quality'], 'tag': bos_q if bos_found else 'ausente'}
    fvg_q = (fvg_ob.get('fvg') or {}).get('quality', ''); ob_q = (fvg_ob.get('ob') or {}).get('quality', '')
    if fvg_q == 'strong' or ob_q == 'strong':   pts = W['fvg_ob_quality']; tag = 'strong'
    elif fvg_q == 'medium' or ob_q == 'medium': pts = W['fvg_ob_quality'] * 0.65; tag = 'medium'
    elif fvg_ob.get('valid'):                   pts = W['fvg_ob_quality'] * 0.3; tag = 'weak'
    else:                                        pts = 0; tag = 'ausente'
    score += pts; factors['fvg_ob_quality'] = {'pts': round(pts,1), 'max': W['fvg_ob_quality'], 'tag': tag}
    sess_map = {'LONDON_OPEN': 1.0, 'NY_OPEN': 1.0, 'NY_LATE': 0.6}
    pts = W['session_quality'] * sess_map.get(killzone, 0)
    score += pts; factors['session_quality'] = {'pts': round(pts,1), 'max': W['session_quality'], 'tag': killzone or 'fuera'}
    if adr_pct <= ADR_MIN:   pts = 0; tag = f'agotado {adr_pct:.0%}'
    elif adr_pct >= 0.60:    pts = W['adr_remaining']; tag = f'{adr_pct:.0%}'
    else:                    pts = W['adr_remaining'] * (adr_pct / 0.60); tag = f'{adr_pct:.0%}'
    score += pts; factors['adr_remaining'] = {'pts': round(pts,1), 'max': W['adr_remaining'], 'tag': tag}
    if ATR_MIN_PIPS <= atr_pips <= ATR_MAX_PIPS: pts = W['volatility']; tag = f'{atr_pips:.1f}pips'
    elif atr_pips < ATR_MIN_PIPS:                pts = 0; tag = f'comprimido {atr_pips:.1f}pips'
    else:                                         pts = W['volatility'] * 0.3; tag = f'extremo {atr_pips:.1f}pips'
    score += pts; factors['volatility'] = {'pts': round(pts,1), 'max': W['volatility'], 'tag': tag}
    pts = W['consolidation'] if consol_ok else 0
    score += pts; factors['consolidation'] = {'pts': round(pts,1), 'max': W['consolidation'], 'tag': 'OK' if consol_ok else 'rango muerto'}
    if not has_target: score *= 0.70
    min_req    = MIN_SCORE + (10 if state.get('defensive_mode') else 0)
    executable = score >= min_req
    confidence = round(min(0.95, max(0.50, 0.55 + (score - 50) / 150)), 2)
    reasons    = [f"{k}: {v['pts']:.0f}/{v['max']:.0f} ({v['tag']})" for k, v in factors.items()]
    return {'total': round(score,1), 'executable': executable, 'confidence': confidence,
            'factors': factors, 'reasons': reasons, 'action': action}

def compute_levels(sweep, fvg_ob, target_level, price, balance, risk_pct, atr):
    action  = 'SELL' if sweep['direction'] == 'bearish' else 'BUY'
    buf     = atr * 0.20
    if action == 'SELL':
        sl      = sweep.get('sweep_high', sweep['level']) + buf
        sl_dist = abs(sl - price)
        tp1     = price - sl_dist * 1.5
        tp2     = target_level if target_level > 0 else price - sl_dist * 2.5
    else:
        sl      = sweep.get('sweep_low', sweep['level']) - buf
        sl_dist = abs(price - sl)
        tp1     = price + sl_dist * 1.5
        tp2     = target_level if target_level > 0 else price + sl_dist * 2.5
    if sl_dist <= 0 or sl_dist > price * 0.012:
        return None
    risk_usd = balance * (risk_pct / 100)
    pos_size = max(1000, int(risk_usd / sl_dist))
    rr1 = round(abs(tp1 - price) / sl_dist, 2)
    rr2 = round(abs(tp2 - price) / sl_dist, 2)
    return {'action': action, 'sl': round(sl,5), 'tp1': round(tp1,5), 'tp2': round(tp2,5),
            'sl_dist': round(sl_dist,5), 'pos_size': pos_size, 'rr1': rr1, 'rr2': rr2,
            'entry_zone': fvg_ob.get('entry_zone', {'high': 0, 'low': 0})}

def run_ict_pipeline(h1, h4, d1, price, balance, risk_pct, hour=None):
    if len(h1) < 30:
        return {'sweep': {'detected': False},
                'score': {'total': 0, 'executable': False, 'confidence': 0, 'factors': {},
                          'reasons': ['Velas insuficientes'], 'action': None},
                'levels': None}
    atr      = compute_atr(h1)
    atr_pips = atr * 10000
    h        = hour if hour is not None else now_et().hour
    kill     = get_killzone(h)
    levels   = identify_liquidity_levels(h1, d1[-5:] if d1 and len(d1) >= 5 else None)
    htf_bias, htf_str = detect_htf_bias(d1, h1)
    inducement = detect_inducement(h1)
    sweep      = detect_sweep(h1, levels, atr)
    if not sweep.get('detected'):
        return {'sweep': sweep,
                'score': {'total': 0, 'executable': False, 'confidence': 0, 'factors': {},
                          'reasons': ['Sin sweep institucional detectado'], 'action': None},
                'levels': None, 'htf_bias': htf_bias, 'htf_strength': htf_str,
                'liq_levels': levels, 'atr': round(atr,5), 'atr_pips': round(atr_pips,1),
                'killzone': kill, 'adr_pct': 0.5, 'inducement': {'found': False, 'quality': 'none'},
                'displacement': {'found': False, 'strength': 'none'},
                'structure': {'bos': False, 'bos_quality': 'none', 'bos_level': 0.0},
                'fvg_ob': {'ob': None, 'fvg': None, 'entry_zone': {'high': 0, 'low': 0}, 'valid': False},
                'liq_target': {'level': 0.0, 'type': 'NONE', 'rr': 0.0}}
    action  = 'SELL' if sweep['direction'] == 'bearish' else 'BUY'
    disp_found, disp_str, _ = detect_displacement(h1, action, atr)
    bos_data = detect_bos(h1, action, atr)
    fvg_ob   = detect_fvg_ob(h1, action, atr)
    tl, tt, tr = compute_liquidity_target(action, levels, price, atr)
    adr_pct  = 0.5
    today    = h1[-1].get('time', '')[:10]
    day_c    = [c for c in h1[-24:] if c.get('time', '')[:10] == today]
    if len(day_c) >= 3:
        dh = max(float(c['mid']['h']) for c in day_c)
        dl = min(float(c['mid']['l']) for c in day_c)
        adr_pct = max(0, 1 - (dh - dl) / (atr * 8))
    rh = [float(c['mid']['h']) for c in h1[-8:]]
    rl = [float(c['mid']['l']) for c in h1[-8:]]
    consol_ok = (max(rh) - min(rl)) >= atr * 0.8
    score     = compute_score(htf_bias, htf_str, sweep, inducement, disp_str, bos_data, fvg_ob,
                              kill, adr_pct, atr_pips, consol_ok, tl > 0)
    levels_out = compute_levels(sweep, fvg_ob, tl, price, balance, risk_pct, atr)
    return {'sweep': sweep,
            'structure': {'bos': bos_data[0], 'bos_quality': bos_data[1], 'bos_level': bos_data[2]},
            'fvg_ob': fvg_ob, 'inducement': {'found': inducement[0], 'quality': inducement[1]},
            'displacement': {'found': disp_found, 'strength': disp_str},
            'score': score, 'levels': levels_out,
            'htf_bias': htf_bias, 'htf_strength': htf_str, 'liq_levels': levels,
            'liq_target': {'level': tl, 'type': tt, 'rr': tr},
            'atr': round(atr,5), 'atr_pips': round(atr_pips,1),
            'killzone': kill, 'adr_pct': round(adr_pct,2), 'consol_ok': consol_ok}

async def call_claude(system_prompt, user_msg, max_tokens=600):
    if not ANTHROPIC_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=40) as client:
            r = await client.post(
                'https://api.anthropic.com/v1/messages',
                headers={'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_API_KEY,
                         'anthropic-version': '2023-06-01'},
                json={'model': CLAUDE_MODEL, 'max_tokens': max_tokens,
                      'system': system_prompt, 'messages': [{'role': 'user', 'content': user_msg}]}
            )
            if r.is_success:
                return r.json()['content'][0]['text']
    except Exception as e:
        log.warning(f'[CLAUDE] {e}')
    return None

def parse_json_safe(text):
    if not text: return None
    text = text.replace('```json', '').replace('```', '').strip()
    s = text.find('{')
    if s == -1: return None
    depth = 0
    for i, ch in enumerate(text[s:], s):
        if ch == '{': depth += 1
        elif ch == '}': depth -= 1
        if depth == 0:
            try: return json.loads(text[s:i+1])
            except Exception: return None
    return None

CEO_SYSTEM = """You are the CEO of TPDCM-IA, an institutional EUR/USD analyst for prop firms.
The ICT local engine already calculated the setup. Your role: evaluate institutional context.

Respond ONLY in JSON:
{"action":"BUY|SELL|HOLD","confidence":0.0,"reason":"analysis","macro_context":"EUR/USD context","risk_note":"risk note","recommendation":"recommendation"}

RULES: score<58=HOLD, against HTF=HOLD, ADR<20%=HOLD, outside killzone=HOLD"""

async def run_ceo(ict, recent_history=None):
    score  = ict['score']
    sweep  = ict['sweep']
    levels = ict.get('levels')
    if not sweep.get('detected') or not score.get('executable'):
        top = score.get('reasons', ['Score insuficiente'])[0]
        return {'action': 'HOLD', 'confidence': 0.0, 'reason': top, 'score': score['total'],
                'macro_context': '', 'risk_note': '', 'recommendation': 'Esperar setup de mayor calidad',
                'source': 'local', 'sl': 0, 'tp1': 0, 'tp2': 0, 'pos_size': 0, 'rr1': 0, 'rr2': 0,
                'entry_zone': {'high': 0, 'low': 0}, 'liq_target': ict.get('liq_target', {}),
                'killzone': ict.get('killzone'), 'htf_bias': ict.get('htf_bias')}
    base_conf   = score['confidence']
    base_action = score['action']
    claude_r    = None
    if ANTHROPIC_API_KEY:
        msg = json.dumps({
            'pair': 'EUR/USD', 'score': score['total'], 'action': base_action,
            'sweep': {'level': sweep.get('level'), 'type': sweep.get('level_type'), 'quality': sweep.get('quality')},
            'htf_bias': ict.get('htf_bias'), 'htf_strength': ict.get('htf_strength'),
            'killzone': ict.get('killzone'), 'adr_pct': ict.get('adr_pct'),
            'liq_target': ict.get('liq_target'), 'bos': ict['structure']['bos_quality'],
            'displacement': ict['displacement']['strength'], 'inducement': ict['inducement']['quality'],
            'levels': levels, 'recent_decisions': (recent_history or [])[-5:],
            'consecutive_losses': state.get('consecutive_losses', 0),
            'defensive_mode': state.get('defensive_mode', False),
        }, ensure_ascii=False)
        raw = await call_claude(CEO_SYSTEM, msg)
        claude_r = parse_json_safe(raw)
    if claude_r and claude_r.get('action') in ('BUY', 'SELL', 'HOLD'):
        action     = claude_r['action']
        confidence = round(min(0.95, max(0.50, float(claude_r.get('confidence', base_conf)))), 2)
        reason     = claude_r.get('reason', '')
        macro      = claude_r.get('macro_context', '')
        risk_note  = claude_r.get('risk_note', '')
        rec        = claude_r.get('recommendation', '')
        source     = 'ict+claude'
    else:
        action = base_action; confidence = base_conf
        reason = ' | '.join(score.get('reasons', [])[:4])
        macro  = risk_note = rec = ''
        source = 'ict_local'
    log.info(f'[CEO] {action} conf:{confidence:.0%} score:{score["total"]}/100 kill:{ict.get("killzone")} [{source}]')
    return {'action': action, 'confidence': confidence, 'reason': reason,
            'score': score['total'], 'macro_context': macro, 'risk_note': risk_note,
            'recommendation': rec, 'source': source,
            'sl':       levels['sl']       if levels else 0,
            'tp1':      levels['tp1']      if levels else 0,
            'tp2':      levels['tp2']      if levels else 0,
            'pos_size': levels['pos_size'] if levels else 0,
            'rr1':      levels['rr1']      if levels else 0,
            'rr2':      levels['rr2']      if levels else 0,
            'entry_zone': levels['entry_zone'] if levels else {'high': 0, 'low': 0},
            'liq_target': ict.get('liq_target', {}),
            'killzone':   ict.get('killzone'),
            'htf_bias':   ict.get('htf_bias')}

def update_memory(trade_result=None):
    global memory
    if trade_result:
        memory['recent_trades'].append(trade_result)
        memory['recent_trades'] = memory['recent_trades'][-50:]
        kill = trade_result.get('killzone', 'UNKNOWN')
        if kill not in memory['session_stats']:
            memory['session_stats'][kill] = {'trades': 0, 'wins': 0, 'pnl': 0}
        memory['session_stats'][kill]['trades'] += 1
        if trade_result.get('result', 0) > 0:
            memory['session_stats'][kill]['wins'] += 1
        memory['session_stats'][kill]['pnl'] += trade_result.get('result', 0)
    recent = memory['recent_trades'][-20:]
    if len(recent) >= 5:
        wins = sum(1 for t in recent if t.get('result', 0) > 0)
        memory['edge_score'] = round(wins / len(recent) * 100, 1)
    recent10 = memory['recent_trades'][-10:]
    if len(recent10) >= 5:
        sl_count = sum(1 for t in recent10 if t.get('outcome') == 'SL')
        if sl_count >= 4:
            state['defensive_mode']   = True
            state['defensive_reason'] = f'{sl_count} SL en ultimos 10 trades'
            log.warning(f'[DEFENSE] Modo defensivo activado')
        elif sl_count <= 1 and state.get('defensive_mode'):
            state['defensive_mode']   = False
            state['defensive_reason'] = ''
            log.info('[DEFENSE] Modo defensivo desactivado')
    memory['last_updated'] = now_utc().isoformat()
    save_file(MEMORY_FILE, memory)

async def run_analysis(auto_execute=False):
    log.info('=== ANALISIS EUR/USD ===')
    try:
        acc = await get_account()
        state['balance'] = float(acc.get('balance', state['balance']))
    except Exception as e:
        log.error(f'[BALANCE] {e}')
    try:
        state['open_trades'] = await get_open_trades()
    except Exception:
        pass
    live_news = []
    try:
        live_news = await fetch_ff_calendar()
    except Exception:
        pass
    all_news = HIGH_IMPACT_EVENTS + live_news
    et = now_et()
    news_blocked, news_reason = is_news_blocked(et, all_news)
    if news_blocked:
        log.info(f'[NEWS] Bloqueado: {news_reason}')
    h1 = h4 = d1 = []
    try:
        h1 = await get_candles('H1', 80)
        h4 = await get_candles('H4', 50)
        d1 = await get_candles('D', 10)
        log.info(f'[VELAS] H1:{len(h1)} H4:{len(h4)} D1:{len(d1)}')
    except Exception as e:
        log.error(f'[VELAS] {e}')
    price = 0.0
    try:
        price = await get_price()
    except Exception:
        if h1: price = float(h1[-1]['mid']['c'])
    ict = run_ict_pipeline(h1, h4, d1, price, state['balance'], state['risk_pct_current'])
    log.info(f'[ICT] sweep:{ict["sweep"].get("detected")} score:{ict["score"]["total"]}/100 htf:{ict.get("htf_bias")} kill:{ict.get("killzone")}')
    decision = await run_ceo(ict, state.get('history', [])[-10:])
    state['last_analysis'] = {'ict': ict, 'price': price}
    state['last_decision']  = decision
    state['last_update']    = now_utc().isoformat()
    et_now = now_et()
    entry  = {
        'timestamp':    now_utc().isoformat(),
        'date':         et_now.strftime('%Y-%m-%d'),
        'time':         et_now.strftime('%H:%M ET'),
        'day_of_week':  et_now.strftime('%A'),
        'month':        et_now.strftime('%B %Y'),
        'killzone':     ict.get('killzone', '--'),
        'action':       decision['action'],
        'confidence':   decision['confidence'],
        'score':        decision['score'],
        'source':       decision.get('source', ''),
        'sweep_detected': ict['sweep'].get('detected', False),
        'sweep_level':  ict['sweep'].get('level', 0),
        'sweep_type':   ict['sweep'].get('level_type', ''),
        'sweep_quality':ict['sweep'].get('quality', ''),
        'htf_bias':     ict.get('htf_bias', ''),
        'htf_strength': ict.get('htf_strength', 0),
        'bos_quality':  ict['structure']['bos_quality'],
        'displacement': ict['displacement']['strength'],
        'inducement':   ict['inducement']['quality'],
        'adr_pct':      ict.get('adr_pct', 0),
        'atr_pips':     ict.get('atr_pips', 0),
        'sl':           decision.get('sl', 0),
        'tp1':          decision.get('tp1', 0),
        'tp2':          decision.get('tp2', 0),
        'rr1':          decision.get('rr1', 0),
        'rr2':          decision.get('rr2', 0),
        'price':        price,
        'ceo_reason':   decision.get('reason', ''),
        'ceo_macro':    decision.get('macro_context', ''),
        'ceo_rec':      decision.get('recommendation', ''),
        'ceo_obs':      '', 'outcome': '', 'result_usd': 0, 'trade_id': '',
        'news_blocked': news_blocked, 'news_reason': news_reason,
        'score_factors': ' | '.join(f"{k}:{v['pts']:.0f}" for k, v in ict['score'].get('factors', {}).items()),
        'defensive_mode': state.get('defensive_mode', False),
    }
    state['history'].append(entry)
    if len(state['history']) > 2000:
        state['history'] = state['history'][-2000:]
    save_file(HISTORY_FILE, state['history'][-2000:])
    log.info(f'[DECISION] {decision["action"]} ({decision["confidence"]:.0%}) score:{decision["score"]}/100')
    if auto_execute and is_session() and not news_blocked:
        await execute_signal(decision)
    log.info('=== FIN ANALISIS ===')

def update_risk(win, pnl):
    if not win and pnl < 0:
        state['consecutive_losses'] += 1
        today = str(now_et().date())
        if state.get('daily_loss_date') != today:
            state['daily_loss_date'] = today
            state['daily_loss_usd']  = 0.0
        state['daily_loss_usd'] += abs(pnl)
        bal = state.get('balance', 100000.0)
        if state['daily_loss_usd'] >= bal * (MAX_DAILY_LOSS / 100):
            state['trading_paused'] = True
            state['pause_reason']   = f'Limite perdida diaria ${state["daily_loss_usd"]:.0f}'
        if state['consecutive_losses'] >= CONSEC_PAUSE:
            state['risk_pct_current'] = max(0.25, state['risk_pct_current'] / 2)
    else:
        state['consecutive_losses'] = 0
        if state['risk_pct_current'] < RISK_PCT:
            state['risk_pct_current'] = RISK_PCT

async def execute_signal(decision):
    if state['trading_paused'] or decision['action'] == 'HOLD': return
    if decision['confidence'] < MIN_CONFIDENCE: return
    sl = decision.get('sl', 0); tp = decision.get('tp1', 0); sz = max(1000, int(decision.get('pos_size', 1000)))
    if sl <= 0 or tp <= 0: return
    try:
        result   = await place_order(sz, sl, tp, decision['action'])
        fill     = result.get('orderFillTransaction', {})
        trade_id = fill.get('tradeOpened', {}).get('tradeID', '')
        if trade_id:
            state['active_trades_meta'][trade_id] = {
                'open_time': now_utc().isoformat(), 'action': decision['action'],
                'entry_price': float(fill.get('price', 0)),
                'tp1': tp, 'tp2': decision.get('tp2', 0), 'sl_original': sl, 'sl_current': sl,
                'partial_closed': False, 'sl_breakeven': False,
            }
            state['live_trades'].append({
                'trade_id': trade_id, 'date': now_et().strftime('%Y-%m-%d'),
                'time': now_et().strftime('%H:%M ET'), 'day_of_week': now_et().strftime('%A'),
                'month': now_et().strftime('%B %Y'), 'action': decision['action'],
                'entry_price': float(fill.get('price', 0)), 'sl': sl, 'tp1': tp,
                'tp2': decision.get('tp2', 0), 'rr1': decision.get('rr1', 0), 'rr2': decision.get('rr2', 0),
                'pos_size': sz, 'score': decision.get('score', 0), 'confidence': decision.get('confidence', 0),
                'killzone': decision.get('killzone', ''), 'htf_bias': decision.get('htf_bias', ''),
                'ceo_reason': decision.get('reason', ''), 'outcome': 'OPEN',
                'result_usd': 0, 'close_time': '', 'gestion': '',
                'sl_moved_be': False, 'partial_closed': False, 'ceo_obs': '',
            })
            save_file(TRADES_FILE, state['live_trades'][-500:])
        log.info(f'[EXEC] {decision["action"]} id:{trade_id}')
    except Exception as e:
        log.error(f'[EXEC] {e}')

_prev_trades = {}

async def monitor_trades():
    global _prev_trades
    et    = now_et()
    fuera  = et.hour >= SESSION_END_ET or et.hour < SESSION_START_ET
    finde  = et.weekday() in (5, 6)
    vierne = et.weekday() == 4 and et.hour >= FRIDAY_CLOSE_ET
    try:
        trades = await get_open_trades()
        state['open_trades'] = trades
    except Exception:
        return
    cur = {t['id']: t for t in trades}
    for tid, trade in _prev_trades.items():
        if tid not in cur:
            pnl  = float(trade.get('unrealizedPL', 0))
            meta = state['active_trades_meta'].pop(tid, {})
            update_risk(pnl >= 0, pnl)
            sl_be = meta.get('sl_breakeven', False); par = meta.get('partial_closed', False)
            if sl_be and par and pnl > 0: outcome = 'TP2'
            elif sl_be and par:            outcome = 'BE'
            elif par and pnl > 0:          outcome = 'TP'
            elif sl_be and pnl == 0:       outcome = 'BE'
            elif pnl < 0:                  outcome = 'SL'
            else:                          outcome = 'TP'
            gestion = []
            if par:   gestion.append('Parcial 50% en TP1')
            if sl_be: gestion.append('SL a BE')
            if outcome == 'SL':   gestion.append('SL hit')
            elif outcome == 'BE': gestion.append('Cerrado en BE')
            elif outcome in ('TP', 'TP2'): gestion.append(f'{outcome} alcanzado')
            ceo_obs = (f'SL hit. P&L: ${pnl:.0f}.' if outcome == 'SL' else
                       f'TP2 alcanzado. P&L: ${pnl:.0f}.' if outcome == 'TP2' else
                       f'BE. Capital protegido. P&L: ${pnl:.0f}.' if outcome == 'BE' else
                       f'TP alcanzado. P&L: ${pnl:.0f}.')
            for lt in reversed(state['live_trades']):
                if lt.get('trade_id') == tid:
                    lt.update({'outcome': outcome, 'result_usd': round(pnl, 2),
                               'close_time': now_et().strftime('%H:%M ET'),
                               'gestion': ' | '.join(gestion), 'sl_moved_be': sl_be,
                               'partial_closed': par, 'ceo_obs': ceo_obs})
                    break
            for h in reversed(state['history']):
                if h.get('trade_id') == tid:
                    h['outcome'] = outcome; h['result_usd'] = round(pnl, 2); break
            update_memory({'outcome': outcome, 'result': round(pnl, 2), 'killzone': meta.get('action', ''), 'sweep_quality': ''})
            adjust_weights(bt_state.get('log', []) + [{'outcome': outcome, 'failure_factors': []}])
            save_file(TRADES_FILE,  state['live_trades'][-500:])
            save_file(HISTORY_FILE, state['history'][-2000:])
            log.info(f'[MONITOR] {tid} cerrado: {outcome} P&L:${pnl:.2f}')
    _prev_trades = cur
    if (fuera or finde or vierne) and trades:
        for t in trades:
            try:
                await close_trade(t['id'])
                log.info(f'[MONITOR] Cerrado fuera sesion: {t["id"]}')
            except Exception as e:
                log.error(f'[MONITOR] {e}')
        return
    for trade in trades:
        tid    = str(trade['id'])
        units  = int(trade.get('currentUnits', 0))
        action = 'BUY' if units > 0 else 'SELL'
        meta   = state['active_trades_meta'].get(tid, {})
        tp1    = meta.get('tp1', 0)
        entry  = meta.get('entry_price', float(trade.get('price', 0)))
        if meta.get('partial_closed') and not meta.get('sl_breakeven'):
            try:
                h1_ = await get_candles('H1', 20)
                atr_ = compute_atr(h1_)
                b, bq, _ = detect_bos(h1_, action, atr_)
                if b and bq in ('strong', 'medium'):
                    await modify_sl(tid, entry)
                    meta['sl_breakeven'] = True
                    state['active_trades_meta'][tid] = meta
                    log.info(f'[MONITOR] BE activado {tid}')
            except Exception as e:
                log.error(f'[MONITOR] BE error: {e}')
        if tp1 and not meta.get('partial_closed'):
            try:
                cp  = await get_price()
                hit = (action == 'BUY' and cp >= tp1) or (action == 'SELL' and cp <= tp1)
                if hit:
                    partial = abs(units) // 2
                    if partial >= 1:
                        await close_trade(tid, partial=partial)
                        meta['partial_closed'] = True
                        state['active_trades_meta'][tid] = meta
                        log.info(f'[MONITOR] Parcial 50% {tid}')
            except Exception as e:
                log.error(f'[MONITOR] Parcial error: {e}')

def generate_ceo_analysis(td):
    outcome = td.get('outcome', ''); action = td.get('action', ''); score = td.get('score', 0)
    pnl = td.get('result', 0); disp = td.get('displacement_strength', '')
    ind = td.get('inducement_quality', ''); bos = td.get('bos_quality', '')
    htf = td.get('htf_bias', ''); adr = td.get('adr_pct', 0); news = td.get('news_blocked', False)
    analysis = ''; rec = ''; fail_factors = []
    if outcome in ('TP', 'TP2'):
        analysis = f'Trade ganador. {action} score {score}/100. Displacement {disp}. BOS {bos}. HTF {htf}. Objetivo liquidez alcanzado.'
        rec = 'Setup valido. Mantener criterios actuales.'
    elif outcome == 'BE':
        analysis = f'BE. {action} score {score}/100. TP1 alcanzado sin continuacion hacia TP2. Gestion correcta.'
        rec = 'Revisar si objetivo TP2 era alcanzable con ADR disponible.'
    elif outcome == 'SL':
        analysis = f'SL hit. {action} score {score}/100. P&L: ${pnl:.0f}. '
        if news:
            analysis += 'Afectado por noticia alto impacto.'; fail_factors.append('news_filter')
            rec = 'Filtro de noticias debe bloquear esta entrada.'
        elif disp in ('weak', 'none'):
            analysis += 'Displacement debil. Sin impulso institucional confirmado.'; fail_factors.append('displacement')
            rec = 'Exigir displacement strong antes de entrar.'
        elif ind in ('weak', 'none'):
            analysis += 'Induccion insuficiente. Sweep sin liquidez acumulada previa.'; fail_factors.append('inducement')
            rec = 'Verificar build-up de liquidez antes de validar sweep.'
        elif bos == 'weak':
            analysis += 'BOS debil. Sin cambio estructural real confirmado.'; fail_factors.append('bos_quality')
            rec = 'Solo aceptar BOS strong o medium.'
        elif adr < ADR_MIN:
            analysis += f'ADR muy consumido ({adr:.0%}). Sin espacio para expandir.'; fail_factors.append('adr_remaining')
            rec = 'Bloquear entradas con ADR < 20%.'
        else:
            analysis += 'Setup tecnicamente valido. Mercado no continuo.'; fail_factors.append('market_noise')
            rec = 'Loss aceptable dentro de parametros correctos.'
    return analysis, rec, fail_factors

def simulate_trade(candles, entry_idx, action, sl, tp1, tp2, balance, atr):
    entry_p = float(candles[entry_idx]['mid']['c'])
    spread  = 0.00015; slip = 0.00003
    fill    = entry_p + (spread + slip) if action == 'BUY' else entry_p - (spread + slip)
    sl_dist = abs(fill - sl)
    if sl_dist <= 0: return None
    risk_usd  = balance * (RISK_PCT / 100)
    units     = max(1000, int(risk_usd / sl_dist))
    sl_cur    = sl; units_cur = units; sl_be = False; par = False; pnl_par = 0.0
    tp1_vela  = None; be_vela = None; be_reason = ''; gestion = []; MAX_V = 25
    for v, fc in enumerate(candles[entry_idx+1:entry_idx+MAX_V+1], 1):
        fh = float(fc['mid']['h']); fl = float(fc['mid']['l'])
        if not par:
            if (action == 'BUY' and fh >= tp1) or (action == 'SELL' and fl <= tp1):
                tp1_vela  = v; half = units_cur // 2; pnl_par = half * abs(tp1 - fill)
                units_cur -= half; par = True; gestion.append(f'V{v}: Parcial 50% @ {tp1:.5f} +${pnl_par:.0f}')
        if par and not sl_be and tp1_vela and v >= tp1_vela + 2:
            wi = candles[entry_idx+v-2:entry_idx+v+1]
            if len(wi) >= 3:
                hs = [float(c['mid']['h']) for c in wi]; ls = [float(c['mid']['l']) for c in wi]
                bos_ok = ((action == 'BUY' and hs[-1] > hs[0] + atr * 0.3) or
                          (action == 'SELL' and ls[-1] < ls[0] - atr * 0.3))
                if bos_ok:
                    sl_cur = fill; sl_be = True; be_vela = v; be_reason = 'BOS post-TP1'
                    gestion.append(f'V{v}: SL->BE @ {fill:.5f}')
        sl_hit = (action == 'BUY' and fl <= sl_cur) or (action == 'SELL' and fh >= sl_cur)
        if sl_hit:
            pnl_rest = 0.0 if sl_be else -(units_cur * sl_dist)
            outcome  = 'BE' if sl_be else 'SL'
            gestion.append(f'V{v}: SL hit @ {sl_cur:.5f}')
            total = pnl_par + pnl_rest
            return _sim_r(total, outcome, units, fill, sl_dist, tp2, sl_be, be_vela, be_reason, par, pnl_par, tp1_vela, gestion, v)
        if (action == 'BUY' and fh >= tp2) or (action == 'SELL' and fl <= tp2):
            pnl_rest = units_cur * abs(tp2 - fill)
            total    = pnl_par + pnl_rest
            gestion.append(f'V{v}: TP2 @ {tp2:.5f} +${pnl_rest:.0f}')
            return _sim_r(total, 'TP2', units, fill, sl_dist, tp2, sl_be, be_vela, be_reason, par, pnl_par, tp1_vela, gestion, v)
    last_p   = float(candles[min(entry_idx+MAX_V, len(candles)-1)]['mid']['c'])
    pnl_rest = units_cur * (last_p - fill) if action == 'BUY' else units_cur * (fill - last_p)
    total    = pnl_par + pnl_rest
    outcome  = 'TP' if ((action == 'BUY' and last_p > fill) or (action == 'SELL' and last_p < fill)) else 'TIMEOUT'
    gestion.append(f'V{MAX_V}: timeout @ {last_p:.5f}')
    return _sim_r(total, outcome, units, fill, sl_dist, tp2, sl_be, be_vela, be_reason, par, pnl_par, tp1_vela, gestion, MAX_V)

def _sim_r(total, outcome, units, fill, sl_dist, tp2, sl_be, be_vela, be_reason, par, pnl_par, tp1_vela, gestion, dur):
    return {'result': round(total,2), 'outcome': outcome, 'units': units, 'fill': round(fill,5),
            'rr_real': round(abs(tp2-fill)/sl_dist,2) if sl_dist > 0 else 0,
            'sl_moved_be': sl_be, 'be_vela': be_vela, 'be_reason': be_reason,
            'partial_closed': par, 'pnl_parcial': round(pnl_par,2), 'tp1_vela': tp1_vela,
            'gestion': ' | '.join(gestion) if gestion else 'Sin eventos', 'velas_duration': dur}

async def run_backtesting():
    if bt_state['running']: return
    bt_state['running'] = True
    log.info('[BT] BACKTESTING EUR/USD 1 ANIO INICIANDO')
    all_trades = []; trade_log = []; balance = state.get('balance', 110000.0)
    ET = ZoneInfo('America/New_York')
    live_news = []
    try: live_news = await fetch_ff_calendar()
    except Exception: pass
    all_news = HIGH_IMPACT_EVENTS + live_news
    log.info(f'[BT] {len(all_news)} eventos noticias')
    try:
        h1 = await get_candles_to('H1', 500)
        for req in range(1, 17):
            if len(h1) >= 8500: break
            oldest = h1[0].get('time', '')
            if not oldest: break
            batch = await get_candles_to('H1', 500, to_dt=oldest)
            if not batch or len(batch) < 2: break
            h1 = batch[:-1] + h1
            await asyncio.sleep(0.3)
            log.info(f'[BT] {len(h1)} velas H1 (req {req+1})')
        log.info(f'[BT] {len(h1)} velas H1 totales')
        d1_all = []
        try: d1_all = await get_candles_to('D', 300)
        except Exception: pass
        cnt = defaultdict(int); last_day = ''; last_idx = -999
        d1_by_date = {c.get('time','')[:10]: i for i, c in enumerate(d1_all)}
        for i in range(30, len(h1) - 26):
            c_time  = h1[i].get('time', '')
            c_price = float(h1[i]['mid']['c'])
            try:
                cdt   = datetime.fromisoformat(c_time.replace('Z', '+00:00')).astimezone(ET)
                c_h   = cdt.hour; c_dow = cdt.weekday(); c_day = cdt.strftime('%Y-%m-%d')
                if c_dow in (5, 6):                               cnt['weekend'] += 1; continue
                if not (SESSION_START_ET <= c_h < SESSION_END_ET): cnt['sesion'] += 1;  continue
            except Exception:
                c_day = c_time[:10]; c_h = 8; cdt = None
            kill = get_killzone(c_h)
            if not kill: cnt['sesion'] += 1; continue
            if cdt:
                nb, _ = is_news_blocked(cdt, all_news)
                if nb: cnt['news'] += 1; continue
            else: nb = False
            if c_day == last_day:  cnt['cooldown'] += 1; continue
            if i - last_idx < 5:   cnt['cooldown'] += 1; continue
            h1_w = h1[max(0, i-60):i+1]
            atr  = compute_atr(h1_w); atr_p = atr * 10000
            if atr_p < ATR_MIN_PIPS: cnt['vol'] += 1; continue
            if atr_p > ATR_MAX_PIPS: cnt['vol'] += 1; continue
            d1_idx = d1_by_date.get(c_day, -1)
            d1_w   = d1_all[:d1_idx+1] if d1_idx >= 0 else []
            liq    = identify_liquidity_levels(h1_w, d1_w[-5:] if len(d1_w) >= 5 else None)
            htf_b, htf_s = detect_htf_bias(d1_w[-5:] if len(d1_w) >= 5 else None, h1_w)
            ind    = detect_inducement(h1_w)
            sweep  = detect_sweep(h1_w, liq, atr)
            if not sweep.get('detected'): cnt['no_sweep'] += 1; continue
            action = 'SELL' if sweep['direction'] == 'bearish' else 'BUY'
            df, ds, _ = detect_displacement(h1_w, action, atr)
            bos_data  = detect_bos(h1_w, action, atr)
            fvg_ob    = detect_fvg_ob(h1_w, action, atr)
            tl, tt, tr = compute_liquidity_target(action, liq, c_price, atr)
            adr_pct   = 0.5
            day_c     = [c for c in h1_w[-24:] if c.get('time','')[:10] == c_day]
            if len(day_c) >= 3:
                dh = max(float(c['mid']['h']) for c in day_c); dl = min(float(c['mid']['l']) for c in day_c)
                adr_pct = max(0, 1 - (dh - dl) / (atr * 8))
            if adr_pct < ADR_MIN: cnt['adr'] += 1; continue
            rh = [float(c['mid']['h']) for c in h1_w[-8:]]; rl = [float(c['mid']['l']) for c in h1_w[-8:]]
            consol_ok = (max(rh) - min(rl)) >= atr * 0.8
            c_dow_n   = cdt.weekday() if cdt else 2
            min_sc    = MIN_SCORE_WEDTHU if c_dow_n in (2, 3) else MIN_SCORE
            min_sc   += 10 if state.get('defensive_mode') else 0
            score     = compute_score(htf_b, htf_s, sweep, ind, ds, bos_data, fvg_ob,
                                      kill, adr_pct, atr_p, consol_ok, tl > 0)
            if score['total'] < min_sc: cnt['score'] += 1; continue
            buf = atr * 0.20
            if action == 'SELL':
                sl = sweep.get('sweep_high', sweep['level']) + buf; sl_dist = abs(sl - c_price)
                tp1 = c_price - sl_dist * 1.5; tp2 = tl if tl > 0 else c_price - sl_dist * 2.5
            else:
                sl = sweep.get('sweep_low', sweep['level']) - buf; sl_dist = abs(c_price - sl)
                tp1 = c_price + sl_dist * 1.5; tp2 = tl if tl > 0 else c_price + sl_dist * 2.5
            if sl_dist <= 0 or sl_dist > c_price * 0.012: cnt['sl_dist'] += 1; continue
            sim = simulate_trade(h1, i, action, sl, tp1, tp2, balance, atr)
            if not sim: continue
            last_day = c_day; last_idx = i
            td = {'action': action, 'outcome': sim['outcome'], 'result': sim['result'],
                  'score': score['total'], 'killzone': kill, 'displacement_strength': ds,
                  'inducement_quality': ind[1], 'bos_quality': bos_data[1],
                  'htf_bias': htf_b, 'adr_pct': adr_pct, 'news_blocked': nb, 'sweep_quality': sweep.get('quality','')}
            analysis, rec, fail_factors = generate_ceo_analysis(td)
            trade_log.append({'outcome': sim['outcome'], 'score': score['total'], 'failure_factors': fail_factors})
            if len(trade_log) % 20 == 0: adjust_weights(trade_log)
            conf = round(min(0.95, max(0.70, 0.65 + score['total'] / 250)), 2)
            all_trades.append({
                'pair': 'EUR/USD', 'date': c_day, 'session': f'{c_h:02d}:00 ET', 'killzone': kill,
                'action': action, 'entry': round(c_price,5), 'fill_price': sim['fill'],
                'sl': round(sl,5), 'tp1': round(tp1,5), 'tp2': round(tp2,5),
                'rr_tp1': round(abs(tp1-c_price)/sl_dist,2), 'rr_tp2': sim['rr_real'],
                'result': sim['result'], 'outcome': sim['outcome'], 'confidence': conf,
                'score': score['total'], 'sweepLevel': sweep.get('level',0),
                'sweep_level_type': sweep.get('level_type',''), 'sweep_quality': sweep.get('quality',''),
                'wick_pct': sweep.get('wick_pct',0), 'htf_bias': htf_b, 'htf_strength': round(htf_s,2),
                'bos_quality': bos_data[1], 'displacement_strength': ds, 'inducement_quality': ind[1],
                'liq_obj_level': round(tl,5) if tl else 0, 'liq_obj_type': tt,
                'adr_pct': round(adr_pct,2), 'atr_pips': round(atr_p,1),
                'velas_duration': sim['velas_duration'], 'sl_moved_be': sim['sl_moved_be'],
                'be_vela': sim['be_vela'], 'be_reason': sim.get('be_reason',''),
                'partial_closed': sim['partial_closed'], 'pnl_parcial': sim['pnl_parcial'],
                'tp1_vela': sim['tp1_vela'], 'gestion': sim['gestion'], 'news_blocked': nb,
                'score_factors': ' | '.join(f"{k}:{v['pts']:.0f}" for k, v in score['factors'].items()),
                'confirmaciones': (f"Sweep {sweep.get('quality','')} @ {sweep.get('level_type','')} | HTF {htf_b} | BOS {bos_data[1]} | Disp {ds} | Ind {ind[1]} | Score {score['total']}/100"),
                'ceo_analysis': analysis, 'ceo_recommendation': rec,
                'failure_factors': ', '.join(fail_factors) if fail_factors else 'N/A',
            })
        log.info(f'[BT] {len(all_trades)} trades | sesion={cnt["sesion"]} news={cnt["news"]} no_sweep={cnt["no_sweep"]} score={cnt["score"]}')
    except Exception as e:
        log.error(f'[BT] Error: {e}', exc_info=True)
    if all_trades:
        wins = [t for t in all_trades if t['result'] > 0]; bes = [t for t in all_trades if t['outcome'] == 'BE']
        pnl  = sum(t['result'] for t in all_trades); wr = len(wins) / len(all_trades)
        gw   = sum(t['result'] for t in all_trades if t['result'] > 0)
        gl   = abs(sum(t['result'] for t in all_trades if t['result'] < 0))
        pf   = round(gw / gl, 2) if gl > 0 else 0
        equity = balance; peak = equity; max_dd = 0.0
        for t in sorted(all_trades, key=lambda x: x['date']):
            equity += t['result']
            if equity > peak: peak = equity
            dd = (peak - equity) / peak
            if dd > max_dd: max_dd = dd
        bt_state['summary'] = {
            'total_trades': len(all_trades), 'wins': len(wins),
            'losses': len(all_trades) - len(wins) - len(bes), 'breakeven': len(bes),
            'win_rate': round(wr,4), 'total_pnl': round(pnl,2), 'profit_factor': pf,
            'max_drawdown': round(max_dd*100,2), 'trades_per_week': round(len(all_trades)/52,1),
        }
        all_trades.sort(key=lambda x: x['date'], reverse=True)
        bt_state['trades']   = all_trades[:300]
        bt_state['last_run'] = now_utc().isoformat()
        bt_state['log']      = trade_log
        log.info(f'[BT] COMPLETADO: {len(all_trades)} trades | WR:{wr:.1%} | PnL:${pnl:.0f} | PF:{pf} | DD:{max_dd:.1%} | {len(all_trades)/52:.1f} t/sem')
    bt_state['running'] = False

@app.get('/health')
async def health():
    return {'status': 'ok', 'version': '1.0', 'pair': 'EUR/USD',
            'trading_paused': state['trading_paused'], 'pause_reason': state['pause_reason'],
            'consecutive_losses': state['consecutive_losses'], 'daily_loss_usd': state['daily_loss_usd'],
            'risk_pct_current': state['risk_pct_current'], 'balance': state['balance'],
            'defensive_mode': state.get('defensive_mode', False),
            'defensive_reason': state.get('defensive_reason', ''),
            'edge_score': memory.get('edge_score', 100.0),
            'min_score': MIN_SCORE, 'score_weights': SCORE_WEIGHTS}

@app.get('/dashboard')
async def dashboard():
    ict   = state.get('last_analysis', {}).get('ict', {})
    dec   = state.get('last_decision', {}) or {}
    score = ict.get('score', {}); sweep = ict.get('sweep', {}); struct = ict.get('structure', {})
    fvgob = ict.get('fvg_ob', {})
    return {'ts': state.get('last_update', now_utc().isoformat()),
            'balance': state['balance'], 'current_price': state.get('last_analysis', {}).get('price', 0),
            'server': 'online',
            'decision': {'action': dec.get('action','HOLD'), 'confidence': dec.get('confidence',0),
                         'score': dec.get('score',0), 'reason': dec.get('reason',''),
                         'macro_context': dec.get('macro_context',''), 'risk_note': dec.get('risk_note',''),
                         'recommendation': dec.get('recommendation',''),
                         'sl': dec.get('sl',0), 'tp1': dec.get('tp1',0), 'tp2': dec.get('tp2',0),
                         'rr1': dec.get('rr1',0), 'rr2': dec.get('rr2',0), 'pos_size': dec.get('pos_size',0),
                         'source': dec.get('source',''), 'killzone': dec.get('killzone',''),
                         'htf_bias': dec.get('htf_bias',''), 'liq_target': dec.get('liq_target',{})},
            'ict': {'sweep_detected': sweep.get('detected',False), 'sweep_direction': sweep.get('direction',''),
                    'sweep_level': sweep.get('level',0), 'sweep_level_type': sweep.get('level_type',''),
                    'sweep_quality': sweep.get('quality',''), 'sweep_wick': sweep.get('wick_pct',0),
                    'structure_bos': struct.get('bos',False), 'structure_bos_q': struct.get('bos_quality',''),
                    'score_total': score.get('total',0), 'score_exec': score.get('executable',False),
                    'score_reasons': score.get('reasons',[]), 'score_factors': score.get('factors',{}),
                    'ob': fvgob.get('ob'), 'fvg': fvgob.get('fvg'),
                    'entry_zone': fvgob.get('entry_zone',{'high':0,'low':0}),
                    'atr': ict.get('atr',0), 'atr_pips': ict.get('atr_pips',0),
                    'htf_bias': ict.get('htf_bias',''), 'htf_strength': ict.get('htf_strength',0),
                    'killzone': ict.get('killzone',''), 'adr_pct': ict.get('adr_pct',0),
                    'liq_target': ict.get('liq_target',{}), 'inducement': ict.get('inducement',{}),
                    'displacement': ict.get('displacement',{})},
            'history': state.get('history',[])[-20:], 'open_trades': state.get('open_trades',[]),
            'risk_status': {'trading_paused': state['trading_paused'], 'pause_reason': state['pause_reason'],
                            'consecutive_losses': state['consecutive_losses'],
                            'daily_loss_usd': state['daily_loss_usd'], 'risk_pct_current': state['risk_pct_current']},
            'memory': {'edge_score': memory.get('edge_score',100.0), 'session_stats': memory.get('session_stats',{}),
                       'recent_trades_count': len(memory.get('recent_trades',[])),
                       'defensive_mode': state.get('defensive_mode',False),
                       'defensive_reason': state.get('defensive_reason','')},
            'learning': {'trades_analyzed': len(bt_state.get('log',[])), 'current_weights': SCORE_WEIGHTS}}

@app.get('/prices')
async def prices():
    try:
        price = await get_price(); ot = await get_open_trades()
        return {'price': price, 'open_trades': ot, 'balance': state['balance']}
    except Exception as e:
        return {'price': 0, 'open_trades': [], 'error': str(e)}

@app.get('/trigger-analysis')
@app.get('/trigger-report')
async def trigger():
    asyncio.create_task(run_analysis(auto_execute=AUTO_EXECUTE))
    return {'status': 'ok', 'message': 'Analisis iniciado'}

@app.get('/run-backtesting')
async def trigger_bt():
    if bt_state['running']:
        return {'status': 'running', 'message': 'Backtesting en progreso'}
    asyncio.create_task(run_backtesting())
    return {'status': 'ok', 'message': 'Backtesting EUR/USD iniciado'}

@app.get('/backtesting')
async def backtesting():
    return {'trades': bt_state.get('trades',[]), 'summary': bt_state.get('summary'),
            'last_run': bt_state.get('last_run'), 'running': bt_state.get('running',False)}

@app.get('/signal-history')
async def signal_history(outcome: Optional[str]=None, month: Optional[str]=None,
                          day: Optional[str]=None, killzone: Optional[str]=None, limit: int=500):
    hist = state.get('history', [])
    if outcome:
        o = outcome.upper()
        hist = [h for h in hist if h.get('outcome','').upper() == o
                or (o == 'TRADED' and h.get('trade_id'))
                or (o == 'HOLD' and h.get('action') == 'HOLD')]
    if month:    hist = [h for h in hist if month.lower()    in h.get('month','').lower()]
    if day:      hist = [h for h in hist if day.lower()      in h.get('day_of_week','').lower()]
    if killzone: hist = [h for h in hist if killzone.upper() in h.get('killzone','').upper()]
    return {'history': list(reversed(hist))[:limit], 'total': len(hist)}

@app.get('/live-trades')
async def live_trades(outcome: Optional[str]=None, month: Optional[str]=None, day: Optional[str]=None):
    trades = state.get('live_trades', [])
    if outcome: trades = [t for t in trades if t.get('outcome','').upper() == outcome.upper()]
    if month:   trades = [t for t in trades if month.lower() in t.get('month','').lower()]
    if day:     trades = [t for t in trades if day.lower()   in t.get('day_of_week','').lower()]
    wins = [t for t in trades if t.get('result_usd',0) > 0]
    pnl  = sum(t.get('result_usd',0) for t in trades)
    return {'trades': list(reversed(trades))[:200], 'total': len(trades),
            'summary': {'total': len(trades), 'wins': len(wins),
                        'losses': len([t for t in trades if t.get('result_usd',0) < 0]),
                        'breakeven': len([t for t in trades if t.get('outcome') == 'BE']),
                        'open': len([t for t in trades if t.get('outcome') == 'OPEN']),
                        'total_pnl': round(pnl,2)}}

@app.get('/export-backtesting')
async def export_backtesting():
    trades = bt_state.get('trades', [])
    if not trades: return JSONResponse({'error': 'Sin trades'})
    s = bt_state.get('summary', {}); wr = s.get('win_rate',0); pnl = s.get('total_pnl',0)
    pf = s.get('profit_factor',0); dd = s.get('max_drawdown',0); tpw = s.get('trades_per_week',0)
    def cc(v):
        s2 = '' if v is None else str(v)
        if any(x in s2 for x in [',','"',chr(10)]): s2 = '"' + s2.replace('"','""') + '"'
        return s2
    rows = [
        'TPDCM-IA - Backtesting EUR/USD Institucional - Prop Firm System',
        f'Total:{len(trades)} | WR:{wr:.1%} | PnL:${pnl:,.0f} | PF:{pf} | MaxDD:{dd:.1f}% | {tpw:.1f} t/sem',
        'Sesiones: Londres (3-5AM ET) + NY (8-11AM ET) | Noticias: NFP/CPI/FOMC/ECB filtrados', '',
        ','.join(['Fecha','Sesion','Killzone','Tipo','Entrada','Fill Real','SL','TP1 (50%)',
                  'TP2 (Objetivo)','RR TP1','RR TP2','Resultado ($)','Outcome','PnL Parcial ($)',
                  'Confianza','Score ICT','CONFIRMACIONES DE ENTRADA',
                  'Sweep Level','Tipo Sweep','Calidad Sweep','Mecha %',
                  'HTF Bias','HTF Fuerza','BOS Calidad','Displacement','Induccion',
                  'Liq Objetivo Level','Liq Objetivo Tipo','ADR %','ATR pips','Velas Duracion',
                  'SL a Break-Even','Vela BE','Razon BE','Cierre Parcial 50%','Vela TP1',
                  'MANEJO DE LA OPERACION','Factores Score ICT',
                  'CEO ANALISIS - Que paso','CEO RECOMENDACION','Factores que fallaron']),
    ]
    for t in trades:
        rows.append(','.join(cc(v) for v in [
            t.get('date',''), t.get('session',''), t.get('killzone',''), t.get('action',''),
            t.get('entry',''), t.get('fill_price',''), t.get('sl',''), t.get('tp1',''), t.get('tp2',''),
            t.get('rr_tp1',''), t.get('rr_tp2',''), t.get('result',''), t.get('outcome',''), t.get('pnl_parcial',0),
            f"{t.get('confidence',0)*100:.0f}%" if t.get('confidence') else '', t.get('score',''),
            t.get('confirmaciones',''), t.get('sweepLevel',''), t.get('sweep_level_type',''),
            t.get('sweep_quality',''), f"{t.get('wick_pct',0)*100:.0f}%" if t.get('wick_pct') else '',
            t.get('htf_bias',''), t.get('htf_strength',''), t.get('bos_quality',''),
            t.get('displacement_strength',''), t.get('inducement_quality',''),
            t.get('liq_obj_level',''), t.get('liq_obj_type',''),
            f"{t.get('adr_pct',0)*100:.0f}%" if t.get('adr_pct') else '', t.get('atr_pips',''),
            t.get('velas_duration',''), 'SI' if t.get('sl_moved_be') else 'No',
            f"Vela {t['be_vela']}" if t.get('be_vela') else '--', t.get('be_reason','--'),
            'SI' if t.get('partial_closed') else 'No',
            f"Vela {t['tp1_vela']}" if t.get('tp1_vela') else '--',
            t.get('gestion',''), t.get('score_factors',''),
            t.get('ceo_analysis',''), t.get('ceo_recommendation',''), t.get('failure_factors','N/A'),
        ]))
    csv_bytes = ('\ufeff' + '\n'.join(rows)).encode('utf-8')
    fname = f'TPDCM_BT_EURUSD_{now_et().strftime("%Y-%m-%d")}.csv'
    return Response(content=csv_bytes, media_type='text/csv',
                    headers={'Content-Disposition': f'attachment; filename="{fname}"'})

@app.get('/export-live-trades')
async def export_live_trades():
    trades = state.get('live_trades', [])
    if not trades: return JSONResponse({'error': 'Sin trades ejecutados'})
    wins = [t for t in trades if t.get('result_usd',0) > 0]
    pnl  = sum(t.get('result_usd',0) for t in trades); wr = len(wins)/len(trades) if trades else 0
    def cc(v):
        s2 = '' if v is None else str(v)
        if any(x in s2 for x in [',','"',chr(10)]): s2 = '"' + s2.replace('"','""') + '"'
        return s2
    rows = ['TPDCM-IA - Trades Reales Ejecutados - EUR/USD',
            f'Total:{len(trades)} | WR:{wr:.1%} | PnL:${pnl:,.0f}', '',
            ','.join(['Fecha','Hora','Dia','Mes','Killzone','Tipo','Entrada','SL','TP1','TP2',
                      'RR TP1','RR TP2','Score','Confianza','HTF Bias','Outcome','Resultado ($)',
                      'Hora Cierre','SL a BE','Parcial 50%','MANEJO DE LA OPERACION',
                      'CEO Analisis Entrada','CEO Observacion Cierre'])]
    for t in trades:
        rows.append(','.join(cc(v) for v in [
            t.get('date',''), t.get('time',''), t.get('day_of_week',''), t.get('month',''),
            t.get('killzone',''), t.get('action',''), t.get('entry_price',''),
            t.get('sl',''), t.get('tp1',''), t.get('tp2',''), t.get('rr1',''), t.get('rr2',''),
            t.get('score',''), f"{t.get('confidence',0)*100:.0f}%", t.get('htf_bias',''),
            t.get('outcome',''), t.get('result_usd',''), t.get('close_time',''),
            'SI' if t.get('sl_moved_be') else 'No', 'SI' if t.get('partial_closed') else 'No',
            t.get('gestion',''), t.get('ceo_reason',''), t.get('ceo_obs',''),
        ]))
    csv_bytes = ('\ufeff' + '\n'.join(rows)).encode('utf-8')
    fname = f'TPDCM_Trades_{now_et().strftime("%Y-%m-%d")}.csv'
    return Response(content=csv_bytes, media_type='text/csv',
                    headers={'Content-Disposition': f'attachment; filename="{fname}"'})

@app.on_event('startup')
async def startup():
    try:
        acc = await get_account()
        state['balance'] = float(acc.get('balance', 110000.0))
        log.info(f'[STARTUP] Balance: ${state["balance"]:,.2f}')
    except Exception as e:
        log.error(f'[STARTUP] Balance: {e}')
    try:
        trades = await get_open_trades()
        state['open_trades'] = trades
        et = now_et()
        if (et.hour >= SESSION_END_ET or et.hour < SESSION_START_ET or et.weekday() in (5,6)) and trades:
            for t in trades:
                try: await close_trade(t['id']); log.info(f'[STARTUP] Cerrado: {t["id"]}')
                except Exception as e: log.error(f'[STARTUP] {e}')
        else:
            log.info(f'[STARTUP] {len(trades)} trades abiertos')
    except Exception as e:
        log.error(f'[STARTUP] Trades: {e}')
    sched = AsyncIOScheduler(timezone=ZoneInfo('America/New_York'))
    sched.add_job(run_analysis,    'interval', hours=1,   id='analysis', args=[AUTO_EXECUTE])
    sched.add_job(monitor_trades,  'interval', minutes=5, id='monitor')
    sched.add_job(run_backtesting, CronTrigger(hour=3, minute=30, timezone=ZoneInfo('America/New_York')), id='bt_daily')
    sched.add_job(run_analysis, CronTrigger(hour=3,  minute=15, timezone=ZoneInfo('America/New_York')), id='london', args=[False])
    sched.add_job(run_analysis, CronTrigger(hour=8,  minute=0,  timezone=ZoneInfo('America/New_York')), id='ny',     args=[AUTO_EXECUTE])
    sched.add_job(run_analysis, CronTrigger(hour=10, minute=0,  timezone=ZoneInfo('America/New_York')), id='ny2',    args=[AUTO_EXECUTE])
    sched.start()
    log.info('[SCHEDULER] Activo - analisis/hora | monitor/5min | BT 3:30AM')
    async def delayed_start():
        await asyncio.sleep(5)
        log.info('[INIT] Analisis inicial...')
        try: await run_analysis(auto_execute=AUTO_EXECUTE)
        except Exception as e: log.error(f'[INIT] {e}')
        bt_flag = '/tmp/tpdcm_bt_done.txt'; should_bt = True
        try:
            with open(bt_flag) as f:
                last = float(f.read().strip())
            if time.time() - last < 21600: should_bt = False; log.info('[INIT] BT reciente - saltando')
        except Exception: pass
        if should_bt:
            await asyncio.sleep(3)
            log.info('[INIT] Backtesting institucional...')
            await run_backtesting()
            try:
                with open(bt_flag, 'w') as f: f.write(str(time.time()))
            except Exception: pass
    asyncio.create_task(delayed_start())
    log.info('TPDCM-IA v1.0 - EUR/USD Institucional - Sistema activo')
