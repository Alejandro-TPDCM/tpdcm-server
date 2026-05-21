"""
═══════════════════════════════════════════════════════════════════════════════
TPDCM-IA v2.6.0-beta-fixed — Trading Platform Deep Claude Machine Intelligence
EUR/USD + GBP/USD Institucional · Prop Firm System
═══════════════════════════════════════════════════════════════════════════════

CAMBIOS v2.6.0-beta -> v2.6.0-beta-fixed (FIX FASE 3 backtest):
  + run_backtesting_pair() REESCRITA para usar run_ict_pipeline()
  + Elimina 7 funciones inexistentes (build_h4_from_h1, htf_bias, etc.)
  + Elimina 2 llamadas con firma incorrecta (detect_sweep, detect_fvg_ob)
  + Backtest ahora usa MISMA logica que sistema en vivo
  + Mantiene 100% de los filtros (caution days, killzones, 2 trades/dia)
  + Anade filtros de regime + anomalies en caution days
  + FASE 2 (run_analysis_pair) NO se toca (ya funciona)
  + Resto del archivo IDENTICO a v2.6.0-beta

CAMBIOS v2.6.0-alpha -> v2.6.0-beta (FASE 2: Run analysis multi-par):
  + Nueva funcion run_analysis_pair(pair) - procesa UN par
  + run_analysis() ahora itera sobre PAIRS activos
  + execute_signal(decision, pair=None) acepta par especifico
  + Tracking de today_trades SEPARADO por par
  + Cada par tiene su limite independiente de 2 trades/dia
  + active_trades_meta incluye campo 'pair'
  + News se obtiene UNA vez y se comparte entre pares (eficiencia)
  + Audit record incluye campo 'pair'
  + EUR/USD funciona igual que antes (compatibilidad)
  + GBP/USD ahora se analiza automaticamente cada ciclo
  + Trades GBP/USD pueden ejecutarse si hay setup valido

CAMBIOS v2.5.1 -> v2.6.0-alpha (FASE 1: Estructura base):
  + Estructura PAIRS = ['EUR_USD', 'GBP_USD']
  + PAIR_CONFIG con configuracion independiente por par
  + State separado por par (pair_state)
  + Funciones OANDA aceptan pair como parametro
  + Endpoint /pairs para monitoreo de estado multi-par
═══════════════════════════════════════════════════════════════════════════════
"""

import asyncio
import json
import os
import time
import uuid
import logging
import fcntl
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
from typing import Optional, List, Literal
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pydantic import BaseModel, Field, field_validator, ConfigDict

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger('TPDCM')


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 1: CONFIGURACION
# ═══════════════════════════════════════════════════════════════════════════════

ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
OANDA_TOKEN       = os.environ.get('OANDA_API_TOKEN', '')
OANDA_ACCOUNT     = os.environ.get('OANDA_ACCOUNT_ID', '')
OANDA_ENV         = os.environ.get('OANDA_ENVIRONMENT', 'practice')
AUTO_EXECUTE      = os.environ.get('AUTO_EXECUTE', 'false').lower() == 'true'
RISK_PCT          = float(os.environ.get('RISK_PCT', '1.0'))
MIN_CONFIDENCE    = float(os.environ.get('MIN_CONFIDENCE', '0.75'))

DATA_PATH         = os.environ.get('DATA_VOLUME_PATH', '/data')

COGNITIVE_TIMEOUT_SEC          = float(os.environ.get('COGNITIVE_TIMEOUT_SEC', '30'))
COGNITIVE_FAILURE_THRESHOLD    = float(os.environ.get('COGNITIVE_FAILURE_THRESHOLD', '0.30'))
ELITE_SCORE_THRESHOLD          = float(os.environ.get('ELITE_SCORE_THRESHOLD', '95'))
ELITE_NO_COGNITIVE_PENALTY     = float(os.environ.get('ELITE_NO_COGNITIVE_PENALTY', '0.80'))

RESEND_API_KEY        = os.environ.get('RESEND_API_KEY', '')
NOTIFY_EMAIL_TO       = os.environ.get('NOTIFY_EMAIL_TO', 'tpdcmia@gmail.com')
NOTIFY_EMAIL_FROM     = os.environ.get('NOTIFY_EMAIL_FROM', 'TPDCM-IA <onboarding@resend.dev>')
NOTIFICATIONS_ENABLED = os.environ.get('NOTIFICATIONS_ENABLED', 'true').lower() == 'true'

PAIR             = 'EUR_USD'

PAIRS = ['EUR_USD', 'GBP_USD', 'AUD_USD', 'USD_CAD', 'NZD_USD', 'USD_CHF', 'EUR_GBP', 'EUR_CHF',
         'USD_JPY', 'GBP_JPY', 'EUR_JPY', 'XAU_USD']

PAIR_CONFIG = {
    'EUR_USD': {
        'display':       'EUR/USD',
        'enabled':       True,
        'min_score':     58,
        'min_score_wed': 65,
        'risk_pct':      float(os.environ.get('RISK_PCT_EUR', '1.2')),
        'atr_min_pips':  8,
        'atr_max_pips':  80,
        'adr_min':       0.20,
        'spread_pips':   1.5,
        'slippage_pips': 0.3,
        'pip_value':     0.0001,
        'tier':          'A',
        # v2.6.0-beta-fixed3: filtros adicionales (vacios = sin filtros extra)
        'extra_caution_days':    [],
        'block_regimes_always':  [],
    },
    'GBP_USD': {
        'display':       'GBP/USD',
        'enabled':       True,
        'min_score':     65,
        'min_score_wed': 70,
        'risk_pct':      float(os.environ.get('RISK_PCT_GBP', '1.0')),
        'atr_min_pips':  10,
        'atr_max_pips':  100,
        'adr_min':       0.25,
        'spread_pips':   2.0,
        'slippage_pips': 0.5,
        'pip_value':     0.0001,
        'tier':          'B',
        # v2.6.0-beta-fixed3: filtros especificos GBP (basados en backtest 24 trades)
        'extra_caution_days':    ['Tuesday'],         # martes WR 25% PnL -$4,388
        'block_regimes_always':  ['ranging'],          # ranging 0% WR PnL -$3,950
    },
    # ═══════════════════════════════════════════════════════════════════
    # v2.6.0-beta-fixed12: FASE 1 - 6 pares nuevos de 4-decimales
    # Todos con enabled=False hasta validar con backtest individual.
    # Misma logica de pips que EUR/USD (pip_value 0.0001).
    # ═══════════════════════════════════════════════════════════════════
    'AUD_USD': {
        'display':       'AUD/USD',
        'enabled':       True,
        'min_score':     62,
        'min_score_wed': 68,
        'risk_pct':      float(os.environ.get('RISK_PCT_AUD', '1.0')),
        'atr_min_pips':  8,
        'atr_max_pips':  90,
        'adr_min':       0.22,
        'spread_pips':   1.8,
        'slippage_pips': 0.4,
        'pip_value':     0.0001,
        'tier':          'B',
        'extra_caution_days':    [],
        'block_regimes_always':  [],
        # v2.6.0-beta-fixed12: AUD solo opera London (NY pierde -$5,832)
        # Backtest London-only: WR 56%, PF 2.72, +$9,066
        'allowed_killzones':     ['LONDON_OPEN'],
    },
    'USD_CAD': {
        'display':       'USD/CAD',
        'enabled':       True,
        'min_score':     62,
        'min_score_wed': 68,
        'risk_pct':      float(os.environ.get('RISK_PCT_CAD', '1.0')),
        'atr_min_pips':  8,
        'atr_max_pips':  90,
        'adr_min':       0.22,
        'spread_pips':   2.0,
        'slippage_pips': 0.4,
        'pip_value':     0.0001,
        'tier':          'B',
        'extra_caution_days':    [],
        'block_regimes_always':  [],
        # v2.6.0-beta-fixed12: CAD solo opera London (NY pierde -$1,365)
        # Backtest London-only: WR 80%, PF 6.59, +$7,363
        'allowed_killzones':     ['LONDON_OPEN'],
    },
    'NZD_USD': {
        'display':       'NZD/USD',
        'enabled':       False,
        'min_score':     63,
        'min_score_wed': 68,
        'risk_pct':      float(os.environ.get('RISK_PCT_NZD', '0.8')),
        'atr_min_pips':  7,
        'atr_max_pips':  80,
        'adr_min':       0.22,
        'spread_pips':   2.2,
        'slippage_pips': 0.5,
        'pip_value':     0.0001,
        'tier':          'C',
        'extra_caution_days':    [],
        'block_regimes_always':  [],
    },
    'USD_CHF': {
        'display':       'USD/CHF',
        'enabled':       False,
        'min_score':     63,
        'min_score_wed': 68,
        'risk_pct':      float(os.environ.get('RISK_PCT_CHF', '0.8')),
        'atr_min_pips':  8,
        'atr_max_pips':  85,
        'adr_min':       0.22,
        'spread_pips':   2.0,
        'slippage_pips': 0.5,
        'pip_value':     0.0001,
        'tier':          'C',
        'extra_caution_days':    [],
        'block_regimes_always':  [],
    },
    'EUR_GBP': {
        'display':       'EUR/GBP',
        'enabled':       False,
        'min_score':     64,
        'min_score_wed': 70,
        'risk_pct':      float(os.environ.get('RISK_PCT_EURGBP', '0.8')),
        'atr_min_pips':  6,
        'atr_max_pips':  60,
        'adr_min':       0.20,
        'spread_pips':   2.0,
        'slippage_pips': 0.4,
        'pip_value':     0.0001,
        'tier':          'C',
        'extra_caution_days':    [],
        'block_regimes_always':  [],
    },
    'EUR_CHF': {
        'display':       'EUR/CHF',
        'enabled':       False,
        'min_score':     64,
        'min_score_wed': 70,
        'risk_pct':      float(os.environ.get('RISK_PCT_EURCHF', '0.8')),
        'atr_min_pips':  6,
        'atr_max_pips':  60,
        'adr_min':       0.20,
        'spread_pips':   2.0,
        'slippage_pips': 0.4,
        'pip_value':     0.0001,
        'tier':          'C',
        'extra_caution_days':    [],
        'block_regimes_always':  [],
    },
    # ═══════════════════════════════════════════════════════════════════
    # v2.6.0-beta-fixed12: FASE 2 - pares volatiles (JPY + oro)
    # AHORA POSIBLES gracias al refactor de pip_value.
    # JPY: pip_value 0.01 (2 decimales) | XAU: pip_value 0.1
    # Todos enabled=False hasta validar con backtest individual.
    # ═══════════════════════════════════════════════════════════════════
    'USD_JPY': {
        'display':       'USD/JPY',
        'enabled':       False,
        'min_score':     60,
        'min_score_wed': 66,
        'risk_pct':      float(os.environ.get('RISK_PCT_USDJPY', '1.0')),
        'atr_min_pips':  8,
        'atr_max_pips':  100,
        'adr_min':       0.20,
        'spread_pips':   1.5,
        'slippage_pips': 0.3,
        'pip_value':     0.01,
        'tier':          'B',
        'extra_caution_days':    [],
        'block_regimes_always':  [],
    },
    'GBP_JPY': {
        'display':       'GBP/JPY',
        'enabled':       True,
        'min_score':     62,
        'min_score_wed': 68,
        'risk_pct':      float(os.environ.get('RISK_PCT_GBPJPY', '0.8')),
        'atr_min_pips':  12,
        'atr_max_pips':  130,
        'adr_min':       0.25,
        'spread_pips':   2.5,
        'slippage_pips': 0.6,
        'pip_value':     0.01,
        'tier':          'B',
        'extra_caution_days':    [],
        'block_regimes_always':  [],
        # v2.6.0-beta-fixed12: GBP/JPY solo opera London (NY pierde -$2,085)
        # Backtest London-only: WR 62%, PF 2.52, +$6,009 (8 trades en 16 meses)
        'allowed_killzones':     ['LONDON_OPEN'],
    },
    'EUR_JPY': {
        'display':       'EUR/JPY',
        'enabled':       False,
        'min_score':     61,
        'min_score_wed': 67,
        'risk_pct':      float(os.environ.get('RISK_PCT_EURJPY', '0.9')),
        'atr_min_pips':  10,
        'atr_max_pips':  110,
        'adr_min':       0.22,
        'spread_pips':   2.0,
        'slippage_pips': 0.4,
        'pip_value':     0.01,
        'tier':          'B',
        'extra_caution_days':    [],
        'block_regimes_always':  [],
    },
    'XAU_USD': {
        'display':       'XAU/USD',
        'enabled':       False,
        'min_score':     60,
        'min_score_wed': 66,
        'risk_pct':      float(os.environ.get('RISK_PCT_XAU', '0.8')),
        'atr_min_pips':  20,
        'atr_max_pips':  300,
        'adr_min':       0.20,
        'spread_pips':   3.0,
        'slippage_pips': 0.5,
        'pip_value':     0.1,
        'tier':          'B',
        'extra_caution_days':    [],
        'block_regimes_always':  [],
    },
}

def get_pair_config(pair):
    return PAIR_CONFIG.get(pair, PAIR_CONFIG['EUR_USD'])

def get_active_pairs():
    return [p for p in PAIRS if PAIR_CONFIG.get(p, {}).get('enabled', False)]

SONNET_MODEL     = 'claude-sonnet-4-6'
OPUS_MODEL       = 'claude-opus-4-7'

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

CAUTION_DAYS              = ['Monday', 'Friday']
CAUTION_RISK_MULTIPLIER   = 0.5
CAUTION_MIN_SCORE         = 70
CAUTION_MIN_CLAUDE_MULT   = 0.85
CAUTION_MIN_HTF_STRENGTH  = 0.50
CAUTION_BLOCKED_REGIMES   = ['choppy', 'compression']
CAUTION_BLOCKED_ANOMALIES = ['medium', 'high']

MAX_TRADES_PER_DAY        = 2
SECOND_TRADE_RISK_MULT    = 0.7
MIN_HOURS_BETWEEN_TRADES  = 2   # v2.6.0-beta-fixed12: 3->2 (ajuste fino, mas trades)

OANDA_BASE = ('https://api-fxpractice.oanda.com' if OANDA_ENV == 'practice'
              else 'https://api-fxtrade.oanda.com')


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 2: PERSISTENCIA EN VOLUMEN
# ═══════════════════════════════════════════════════════════════════════════════

def _ensure_data_dirs():
    base = Path(DATA_PATH)
    for d in ['trades', 'audit', 'memory', 'legacy', 'backtest', 'cognitive', 'regime', 'notifications', 'claude_conversations']:
        try: (base / d).mkdir(parents=True, exist_ok=True)
        except Exception as e: log.warning(f'[STORAGE] No se pudo crear {d}: {e}')

def storage_write_json(relpath: str, data) -> bool:
    full = Path(DATA_PATH) / relpath
    try:
        full.parent.mkdir(parents=True, exist_ok=True)
        with open(full, 'w') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            json.dump(data, f, ensure_ascii=False, default=str)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        return True
    except Exception as e:
        log.warning(f'[STORAGE-W] {relpath}: {e}'); return False

def storage_read_json(relpath: str, default=None):
    full = Path(DATA_PATH) / relpath
    try:
        with open(full, 'r') as f: return json.load(f)
    except Exception: return default

def storage_append_jsonl(relpath: str, record: dict) -> bool:
    full = Path(DATA_PATH) / relpath
    try:
        full.parent.mkdir(parents=True, exist_ok=True)
        with open(full, 'a') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            f.write(json.dumps(record, ensure_ascii=False, default=str) + '\n')
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        return True
    except Exception as e:
        log.warning(f'[STORAGE-A] {relpath}: {e}'); return False

def storage_read_jsonl(relpath: str, limit: int = None) -> List[dict]:
    full = Path(DATA_PATH) / relpath
    try:
        with open(full, 'r') as f: lines = f.readlines()
        if limit: lines = lines[-limit:]
        return [json.loads(l) for l in lines if l.strip()]
    except Exception: return []

def audit_decision(record: dict):
    et = datetime.now(ZoneInfo('America/New_York'))
    month_file = f'audit/decisions_{et.strftime("%Y-%m")}.jsonl'
    record['decision_id']    = record.get('decision_id', str(uuid.uuid4()))
    record['timestamp_utc']  = record.get('timestamp_utc', datetime.now(timezone.utc).isoformat())
    record['timestamp_et']   = record.get('timestamp_et', et.isoformat())
    storage_append_jsonl(month_file, record)
# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 3: NOTIFICATION LAYER (Resend) - NUEVO v2.1
# ═══════════════════════════════════════════════════════════════════════════════

RESEND_API_URL = 'https://api.resend.com/emails'

EMAIL_BASE_STYLE = """
<style>
  body { margin:0; padding:0; background:#070a0d; font-family:'IBM Plex Mono','SF Mono',Consolas,monospace;
         color:#c4d8cc; line-height:1.6; }
  .wrap { max-width:640px; margin:0 auto; padding:24px; background:#0c1017; }
  .header { border-bottom:1px solid rgba(0,232,122,0.2); padding-bottom:12px; margin-bottom:20px; }
  .logo { font-size:14px; font-weight:700; color:#00e87a; letter-spacing:0.1em; }
  .logo span { color:#5a7a68; font-weight:400; font-size:11px; margin-left:6px; }
  h1 { font-size:18px; color:#c4d8cc; margin:0 0 8px 0; font-weight:600; }
  .sub { color:#5a7a68; font-size:11px; letter-spacing:0.05em; margin-bottom:20px; }
  .card { background:#111820; border:1px solid rgba(255,255,255,0.07); border-radius:6px;
          padding:14px 16px; margin-bottom:12px; }
  .label { font-size:9px; color:#5a7a68; letter-spacing:0.15em; text-transform:uppercase; margin-bottom:6px; }
  .value { font-size:13px; color:#c4d8cc; }
  .big { font-size:22px; font-weight:700; }
  .row { display:flex; justify-content:space-between; padding:5px 0;
         border-bottom:1px solid rgba(255,255,255,0.04); font-size:12px; }
  .row:last-child { border-bottom:none; }
  .k { color:#5a7a68; }
  .v { color:#c4d8cc; font-weight:600; }
  .green { color:#00e87a !important; }
  .red { color:#ff3355 !important; }
  .gold { color:#f0c040 !important; }
  .blue { color:#4a9eff !important; }
  .footer { margin-top:24px; padding-top:14px; border-top:1px solid rgba(255,255,255,0.05);
            font-size:10px; color:#3a5a48; text-align:center; }
  .badge { display:inline-block; padding:4px 10px; border-radius:3px; font-size:10px;
           letter-spacing:0.1em; font-weight:700; }
  .badge-green { background:rgba(0,232,122,0.15); color:#00e87a; }
  .badge-red { background:rgba(255,51,85,0.15); color:#ff3355; }
  .badge-gold { background:rgba(240,192,64,0.12); color:#f0c040; }
</style>
"""

def _email_wrapper(title: str, body_html: str) -> str:
    et = datetime.now(ZoneInfo('America/New_York'))
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">{EMAIL_BASE_STYLE}</head>
<body><div class="wrap">
<div class="header"><div class="logo">TPDCM-IA<span>EUR/USD INSTITUCIONAL · v2.6</span></div></div>
<h1>{title}</h1>
<div class="sub">{et.strftime('%A %d %B %Y · %H:%M:%S ET')}</div>
{body_html}
<div class="footer">Sistema automatizado · Decision Gate Architecture<br>Python authority + Claude cognitive validation</div>
</div></body></html>"""

async def send_email(subject: str, html: str) -> bool:
    if not NOTIFICATIONS_ENABLED:
        log.info(f'[NOTIFY] disabled - skip: {subject}'); return False
    if not RESEND_API_KEY:
        log.warning('[NOTIFY] RESEND_API_KEY no configurada'); return False
    if not NOTIFY_EMAIL_TO:
        log.warning('[NOTIFY] NOTIFY_EMAIL_TO no configurada'); return False
    payload = {'from': NOTIFY_EMAIL_FROM, 'to': [NOTIFY_EMAIL_TO],
               'subject': subject, 'html': html}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(RESEND_API_URL,
                                  headers={'Authorization': f'Bearer {RESEND_API_KEY}',
                                           'Content-Type': 'application/json'},
                                  json=payload)
        if r.is_success:
            email_id = r.json().get('id', '')
            log.info(f'[NOTIFY] OK "{subject[:60]}" id:{email_id}')
            storage_append_jsonl('notifications/sent.jsonl', {
                'ts': datetime.now(timezone.utc).isoformat(),
                'subject': subject, 'to': NOTIFY_EMAIL_TO,
                'id': email_id, 'status': 'sent'})
            return True
        else:
            log.warning(f'[NOTIFY] HTTP {r.status_code}: {r.text[:200]}')
            storage_append_jsonl('notifications/sent.jsonl', {
                'ts': datetime.now(timezone.utc).isoformat(),
                'subject': subject, 'status': 'failed',
                'error': f'HTTP {r.status_code} - {r.text[:150]}'})
            return False
    except Exception as e:
        log.warning(f'[NOTIFY] Exception: {e}'); return False


def build_pre_london_report():
    ict = state.get('last_analysis', {}).get('ict', {})
    et = datetime.now(ZoneInfo('America/New_York'))
    today_str = et.strftime('%Y-%m-%d')
    todays_news = [e for e in HIGH_IMPACT_EVENTS if e.get('date') == today_str]
    liq = ict.get('liq_levels', {})
    htf_bias_v = ict.get('htf_bias', 'neutral')
    htf_color = 'green' if htf_bias_v == 'bullish' else 'red' if htf_bias_v == 'bearish' else 'gold'
    defensive = state.get('defensive_mode', False)
    paused = state.get('trading_paused', False)
    system_status = 'badge-red' if paused else ('badge-gold' if defensive else 'badge-green')
    system_text = 'PAUSADO' if paused else ('DEFENSIVO' if defensive else 'OPERATIVO')
    if todays_news:
        items = "".join(f'<div class="row"><span class="k">⚠️ {e.get("time","")} ET</span>'
                        f'<span class="v gold">{e.get("currency","")} {e.get("title","")}</span></div>'
                        for e in todays_news)
        news_html = f'<div class="card"><div class="label">Noticias High Impact Hoy</div>{items}</div>'
    else:
        news_html = '<div class="card"><div class="label">Noticias High Impact Hoy</div><div class="value" style="color:#5a7a68">Sin noticias programadas</div></div>'
    body = f"""
<div class="card"><div class="label">Estado del Sistema</div>
<div style="display:flex;align-items:center;gap:12px;margin-top:4px">
<span class="badge {system_status}">{system_text}</span>
<span class="value">Balance: <strong class="green">${state['balance']:,.2f}</strong></span></div></div>
<div class="card"><div class="label">Mercado EUR/USD</div>
<div class="row"><span class="k">Precio actual</span><span class="v">{state.get('last_analysis', {}).get('price', 0):.5f}</span></div>
<div class="row"><span class="k">HTF Bias</span><span class="v {htf_color}">{htf_bias_v.upper()} ({ict.get('htf_strength', 0)*100:.0f}%)</span></div>
<div class="row"><span class="k">ATR H1</span><span class="v">{ict.get('atr_pips', 0):.1f} pips</span></div>
<div class="row"><span class="k">ADR restante</span><span class="v">{ict.get('adr_pct', 0)*100:.0f}%</span></div></div>
<div class="card"><div class="label">Regimen de Mercado</div>
<div class="row"><span class="k">Tipo</span><span class="v gold">{ict.get('regime', {}).get('type', 'unknown').upper()}</span></div>
<div class="row"><span class="k">Calidad</span><span class="v">{ict.get('regime', {}).get('regime_quality', 'unknown').upper()}</span></div>
<div class="row"><span class="k">Volatilidad Z</span><span class="v">{ict.get('regime', {}).get('volatility_z', 0):+.2f}</span></div>
<div class="row"><span class="k">Anomalias</span><span class="v">{ict.get('anomalies', {}).get('severity', 'none').upper()}</span></div>
</div>
<div class="card"><div class="label">Niveles del Dia</div>
<div class="row"><span class="k">PDH</span><span class="v gold">{liq.get('pdh', 0):.5f}</span></div>
<div class="row"><span class="k">PDL</span><span class="v gold">{liq.get('pdl', 0):.5f}</span></div>
<div class="row"><span class="k">Weekly High</span><span class="v blue">{liq.get('weekly_high', 0):.5f}</span></div>
<div class="row"><span class="k">Weekly Low</span><span class="v blue">{liq.get('weekly_low', 0):.5f}</span></div></div>
{news_html}
<div class="card"><div class="label">Salud del Sistema</div>
<div class="row"><span class="k">Edge Score</span><span class="v green">{memory.get('edge_score', 100):.0f}/100</span></div>
<div class="row"><span class="k">Defensivo</span><span class="v {'red' if defensive else 'green'}">{ 'ACTIVO' if defensive else 'NO' }</span></div>
<div class="row"><span class="k">Risk actual</span><span class="v">{state.get('risk_pct_current', 1.0):.2f}%</span></div>
<div class="row"><span class="k">Cognitive</span><span class="v {'red' if cognitive_is_disabled() else 'green'}">{'DEGRADED' if cognitive_is_disabled() else 'OK'}</span></div>
<div class="row"><span class="k">Auto Execute</span><span class="v {'green' if AUTO_EXECUTE else 'gold'}">{'ACTIVO' if AUTO_EXECUTE else 'SOLO SENAL'}</span></div></div>"""
    subject = f'TPDCM-IA · Briefing 7AM · EUR/USD · {htf_bias_v.upper()}'
    return subject, _email_wrapper('Pre-Londres Briefing', body)


def build_ny_open_report():
    ict = state.get('last_analysis', {}).get('ict', {})
    dec = state.get('last_decision', {}) or {}
    sweep = ict.get('sweep', {})
    action = dec.get('action', 'HOLD')
    action_color = 'green' if action == 'BUY' else 'red' if action == 'SELL' else 'gold'
    source = dec.get('source', '')
    source_explain = {
        'hold_technical': 'Python decidio HOLD',
        'validated':      'Setup validado por Claude',
        'vetoed':         'Setup vetado por Claude',
        'cognitive_down_elite':     'Claude down · setup elite',
        'cognitive_down_non_elite': 'Claude down · no elite · HOLD',
    }.get(source, source)
    confidence_pct = dec.get('confidence', 0) * 100
    conf_color = 'green' if confidence_pct >= 70 else 'gold' if confidence_pct >= 50 else 'red'
    setup_html = ""
    if sweep.get('detected'):
        setup_html = f"""<div class="card"><div class="label">Setup Tecnico</div>
<div class="row"><span class="k">Sweep</span><span class="v">{sweep.get('quality','').upper()} @ {sweep.get('level_type','')}</span></div>
<div class="row"><span class="k">Nivel</span><span class="v gold">{sweep.get('level',0):.5f}</span></div>
<div class="row"><span class="k">Mecha %</span><span class="v">{sweep.get('wick_pct',0)*100:.0f}%</span></div>
<div class="row"><span class="k">BOS</span><span class="v">{ict.get('structure',{}).get('bos_quality','--').upper()}</span></div>
<div class="row"><span class="k">Displacement</span><span class="v">{ict.get('displacement',{}).get('strength','--').upper()}</span></div></div>"""
    cognitive_html = ""
    if dec.get('narrative'):
        veto = dec.get('cognitive_veto', False)
        mult = dec.get('cognitive_multiplier')
        cognitive_html = f"""<div class="card"><div class="label">Validacion Cognitiva</div>
<div class="row"><span class="k">Veto</span><span class="v {'red' if veto else 'green'}">{ 'SI' if veto else 'NO' }</span></div>
<div class="row"><span class="k">Multiplier</span><span class="v">{mult if mult else '--'}</span></div>
<div style="margin-top:10px;padding:10px;background:rgba(0,0,0,0.3);border-radius:4px;font-size:11px;color:#c4d8cc;font-style:italic">"{dec.get('narrative','--')}"</div></div>"""
    levels_html = ""
    if dec.get('sl', 0) > 0 and action != 'HOLD':
        levels_html = f"""<div class="card"><div class="label">Niveles Operativos</div>
<div class="row"><span class="k">Entry</span><span class="v blue">{state.get('last_analysis',{}).get('price',0):.5f}</span></div>
<div class="row"><span class="k">SL</span><span class="v red">{dec.get('sl',0):.5f}</span></div>
<div class="row"><span class="k">TP1</span><span class="v green">{dec.get('tp1',0):.5f}</span></div>
<div class="row"><span class="k">TP2</span><span class="v green">{dec.get('tp2',0):.5f}</span></div>
<div class="row"><span class="k">RR</span><span class="v">{dec.get('rr1',0)} / {dec.get('rr2',0)}</span></div>
<div class="row"><span class="k">Position size</span><span class="v">{dec.get('pos_size',0):,} units</span></div></div>"""
    body = f"""<div class="card"><div class="label">Decision Final</div>
<div style="display:flex;align-items:center;gap:16px;margin-top:4px">
<span class="big {action_color}">{action}</span>
<span class="value">Confianza: <strong class="{conf_color}">{confidence_pct:.0f}%</strong></span>
<span class="value">Score: <strong class="gold">{dec.get('score',0)}/100</strong></span></div>
<div style="margin-top:10px;color:#5a7a68;font-size:11px">{source_explain}</div></div>
{setup_html}{cognitive_html}{levels_html}"""
    subject = f'TPDCM-IA · NY 9AM · {action} ({confidence_pct:.0f}%)'
    return subject, _email_wrapper('NY Open Analysis', body)


def build_trade_open_email(trade_record: dict):
    action = trade_record.get('action', '')
    action_color = 'green' if action == 'BUY' else 'red'
    body = f"""<div class="card"><div class="label">Orden Ejecutada</div>
<div style="display:flex;align-items:center;gap:12px;margin:10px 0">
<span class="big {action_color}">{action}</span>
<span class="value">@ <strong>{trade_record.get('entry_price', 0):.5f}</strong></span></div>
<div style="font-size:11px;color:#5a7a68">Trade ID: {trade_record.get('trade_id', '--')}</div></div>
<div class="card"><div class="label">Niveles</div>
<div class="row"><span class="k">Entry</span><span class="v blue">{trade_record.get('entry_price',0):.5f}</span></div>
<div class="row"><span class="k">SL</span><span class="v red">{trade_record.get('sl',0):.5f}</span></div>
<div class="row"><span class="k">TP1</span><span class="v green">{trade_record.get('tp1',0):.5f}</span></div>
<div class="row"><span class="k">TP2</span><span class="v green">{trade_record.get('tp2',0):.5f}</span></div>
<div class="row"><span class="k">RR</span><span class="v">{trade_record.get('rr1',0)} / {trade_record.get('rr2',0)}</span></div>
<div class="row"><span class="k">Size</span><span class="v">{trade_record.get('pos_size',0):,} units</span></div></div>"""
    emoji = '🟢' if action == 'BUY' else '🔴'
    subject = f'{emoji} TPDCM-IA · TRADE OPEN · {action} EUR/USD @ {trade_record.get("entry_price",0):.5f}'
    return subject, _email_wrapper(f'{emoji} Trade Abierto', body)


def build_trade_close_email(trade: dict, outcome: str, pnl: float, gestion: list):
    pnl_color = 'green' if pnl > 0 else 'red' if pnl < 0 else 'gold'
    outcome_color = {'TP':'green','TP2':'green','SL':'red','BE':'gold','TIMEOUT':'gold'}.get(outcome,'gold')
    outcome_emoji = {'TP':'✅','TP2':'🎯','SL':'❌','BE':'⚖️','TIMEOUT':'⏱️'}.get(outcome,'•')
    gestion_html = ""
    if gestion:
        items = "".join(f'<div style="padding:4px 0;font-size:11px">✓ {g}</div>' for g in gestion)
        gestion_html = f'<div class="card"><div class="label">Gestion Aplicada</div>{items}</div>'
    body = f"""<div class="card"><div class="label">Resultado</div>
<div style="display:flex;align-items:center;gap:14px;margin:10px 0">
<span style="font-size:32px">{outcome_emoji}</span>
<div><div class="big {outcome_color}">{outcome}</div></div>
<div style="margin-left:auto;text-align:right">
<div class="big {pnl_color}">{('+' if pnl > 0 else '')}${pnl:,.2f}</div></div></div></div>
{gestion_html}"""
    subject = f'{outcome_emoji} TPDCM-IA · {outcome} · {("+" if pnl > 0 else "")}${pnl:,.0f}'
    return subject, _email_wrapper(f'{outcome_emoji} Trade Cerrado', body)


def build_cognitive_veto_email(decision: dict):
    technical_action = decision.get('technical_action_that_would_have_been', '--')
    body = f"""<div class="card"><div class="label">VETO COGNITIVO</div>
<div class="row"><span class="k">Accion</span><span class="v">{technical_action}</span></div>
<div class="row"><span class="k">Score</span><span class="v gold">{decision.get('score',0)}/100</span></div>
<div style="margin-top:10px;padding:10px;background:rgba(240,192,64,0.08);border-left:3px solid #f0c040;border-radius:4px;color:#c4d8cc;font-style:italic">"{decision.get('reason','--')}"</div></div>"""
    subject = f'🟡 TPDCM-IA · VETO · {technical_action}'
    return subject, _email_wrapper('Cognitive Veto', body)


def build_critical_alert_email(alert_type: str, message: str, details: dict):
    body = f"""<div class="card" style="border-color:rgba(255,51,85,0.3)">
<div class="label" style="color:#ff3355">Alerta Critica</div>
<div style="margin:10px 0"><span class="big red">{alert_type}</span></div>
<div style="font-size:12px;color:#c4d8cc;line-height:1.6;padding:10px;background:rgba(255,51,85,0.05);border-radius:4px;border-left:3px solid #ff3355">{message}</div></div>
<div class="card"><div class="label">Detalles</div>
{"".join(f'<div class="row"><span class="k">{k}</span><span class="v">{v}</span></div>' for k, v in details.items())}</div>"""
    subject = f'⚠️ TPDCM-IA · {alert_type}'
    return subject, _email_wrapper(f'{alert_type}', body)
# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 4: APP FASTAPI + ESTADO GLOBAL
# ═══════════════════════════════════════════════════════════════════════════════

app = FastAPI(title='TPDCM-IA', version='2.6.0-beta-fixed12')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=True,
                   allow_methods=['*'], allow_headers=['*'])

state = {
    'balance': 0.0, 'open_trades': [], 'last_analysis': None,
    'last_decision': None, 'last_update': None,
    'history': storage_read_json('legacy/history.json', []),
    'live_trades': storage_read_json('legacy/live_trades.json', []),
    'daily_loss_usd': 0.0, 'daily_loss_date': None, 'consecutive_losses': 0,
    'risk_pct_current': RISK_PCT, 'trading_paused': False, 'pause_reason': '',
    'active_trades_meta': {}, 'defensive_mode': False, 'defensive_reason': '',
}

def _init_pair_state():
    return {
        'last_analysis': None,
        'last_decision': None,
        'recent_trades': [],
        'today_trades': [],
        'consecutive_losses': 0,
        'daily_pnl': 0.0,
        'open_trades': {},
    }

pair_state = storage_read_json('state/pair_state.json', {
    pair: _init_pair_state() for pair in PAIRS
})
for p in PAIRS:
    if p not in pair_state:
        pair_state[p] = _init_pair_state()

memory = storage_read_json('memory/edge_tracker.json', {
    'recent_trades': [], 'session_stats': {}, 'sweep_quality_hist': [],
    'edge_score': 100.0, 'last_updated': None,
})

bt_state = {'trades': [], 'summary': None, 'last_run': None, 'running': False, 'log': []}
_candles_cache = {}
_CANDLES_CACHE_TTL = {'M5': 60, 'M15': 120, 'H1': 300, 'H4': 600, 'D': 1800}


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 5: SCORING WEIGHTS
# ═══════════════════════════════════════════════════════════════════════════════

SCORE_WEIGHTS = storage_read_json('memory/adaptive_weights.json', {
    'htf_alignment': 18, 'sweep_quality': 16, 'displacement': 15,
    'inducement': 12, 'bos_quality': 10, 'fvg_ob_quality': 10,
    'session_quality': 8, 'adr_remaining': 5, 'volatility': 4, 'consolidation': 2,
})

def adjust_weights(trade_log):
    global SCORE_WEIGHTS
    if len(trade_log) < 10: return
    failures = defaultdict(int); sl_count = 0
    for t in trade_log[-30:]:
        if t.get('outcome') in ('SL', 'BE'):
            sl_count += 1
            for f in t.get('failure_factors', []): failures[f] += 1
    if sl_count < 3: return
    for factor, count in failures.items():
        if factor in SCORE_WEIGHTS and count / sl_count > 0.5:
            SCORE_WEIGHTS[factor] = min(SCORE_WEIGHTS[factor] * 1.25, SCORE_WEIGHTS[factor] + 3)
    total = sum(SCORE_WEIGHTS.values())
    if total != 100:
        scale = 100 / total
        for k in SCORE_WEIGHTS: SCORE_WEIGHTS[k] = round(SCORE_WEIGHTS[k] * scale, 1)
    storage_write_json('memory/adaptive_weights.json', SCORE_WEIGHTS)


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 6: TIEMPO / SESION / NOTICIAS
# ═══════════════════════════════════════════════════════════════════════════════

def now_et():  return datetime.now(ZoneInfo('America/New_York'))
def now_utc(): return datetime.now(timezone.utc)

def is_session():
    et = now_et()
    if et.weekday() == 6: return False
    if et.weekday() == 4 and et.hour >= FRIDAY_CLOSE_ET: return False
    return SESSION_START_ET <= et.hour < SESSION_END_ET

def get_killzone(hour):
    if 3 <= hour < 5:   return 'LONDON_OPEN'
    if 8 <= hour < 11:  return 'NY_OPEN'
    if 11 <= hour < 12: return 'NY_LATE'
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
    {'date': '2025-05-07', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2025-06-18', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2025-07-30', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2025-09-17', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2025-11-05', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2025-12-17', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2026-01-28', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2026-03-18', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
    {'date': '2026-05-06', 'time': '14:00', 'title': 'FOMC', 'currency': 'USD'},
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
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get('https://nfs.faireconomy.media/ff_calendar_thisweek.json')
            if r.is_success:
                return [e for e in r.json() if e.get('impact', '').lower() == 'high'
                        and e.get('currency', '').upper() in ('USD', 'EUR')]
    except Exception: pass
    return []

def is_news_blocked(candle_dt, events):
    for evt in events:
        try:
            dt_str = f"{evt.get('date','')} {evt.get('time','')}"
            try: evt_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M')
            except Exception: continue
            naive = candle_dt.replace(tzinfo=None)
            diff = (evt_dt - naive).total_seconds() / 60
            if -NEWS_AFTER <= diff <= NEWS_BEFORE:
                return True, f"{evt.get('currency','')} {evt.get('title','')} ({diff:.0f}min)"
        except Exception: continue
    return False, ''


# ═══════════════════════════════════════════════════════════════════════════════
# DAYS OF CAUTION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def is_caution_day(dt=None):
    if dt is None:
        dt = now_et()
    day_name = dt.strftime('%A')
    return day_name in CAUTION_DAYS


def _get_caution_day_warning():
    day = now_et().strftime('%A')
    if day == 'Monday':
        return {
            'day': 'Monday',
            'historical_wr': '20%',
            'historical_pnl': '-$1,585 over 10 trades',
            'recommendation': 'BE EXTREMELY STRICT.',
            'requirements': [
                'Setup must be ELITE quality',
                'Regime must be TRENDING or EXPANSION',
                'HTF bias must be strong (>0.5)',
                'No anomalies should be present',
            ]
        }
    elif day == 'Friday':
        return {
            'day': 'Friday',
            'historical_wr': '33%',
            'historical_pnl': '-$1,665 over 12 trades',
            'recommendation': 'BE EXTREMELY STRICT.',
            'requirements': [
                'Avoid trades after 12:00 ET',
                'Setup must be ELITE quality',
                'Strong HTF alignment required',
            ]
        }
    return None


def days_of_caution_filter(signal, regime, anomalies, claude_response, dt=None):
    if dt is None:
        dt = now_et()
    day_name = dt.strftime('%A')
    if day_name not in CAUTION_DAYS:
        return {
            'allow': True, 'risk_multiplier': 1.0, 'caution_mode': False,
            'day': day_name, 'detail': 'Dia normal'
        }
    filters_passed = []
    regime_type = (regime or {}).get('type', 'unknown')
    if regime_type in CAUTION_BLOCKED_REGIMES:
        return {
            'allow': False, 'veto_reason': 'caution_day_unfavorable_regime',
            'risk_multiplier': 0, 'caution_mode': True, 'day': day_name,
            'detail': f'{day_name} + regime {regime_type}',
            'filters_passed': filters_passed
        }
    filters_passed.append('regime_ok')
    anomaly_severity = (anomalies or {}).get('severity', 'none')
    if anomaly_severity in CAUTION_BLOCKED_ANOMALIES:
        return {
            'allow': False, 'veto_reason': 'caution_day_anomaly_detected',
            'risk_multiplier': 0, 'caution_mode': True, 'day': day_name,
            'detail': f'{day_name} con anomalia {anomaly_severity}',
            'filters_passed': filters_passed
        }
    filters_passed.append('no_anomalies')
    score = signal.get('score', 0)
    if score < CAUTION_MIN_SCORE:
        return {
            'allow': False, 'veto_reason': 'caution_day_score_too_low',
            'risk_multiplier': 0, 'caution_mode': True, 'day': day_name,
            'detail': f'Score {score:.1f} < {CAUTION_MIN_SCORE}',
            'filters_passed': filters_passed
        }
    filters_passed.append('score_elite')
    htf_strength = signal.get('htf_strength', 0)
    if htf_strength < CAUTION_MIN_HTF_STRENGTH:
        return {
            'allow': False, 'veto_reason': 'caution_day_weak_htf',
            'risk_multiplier': 0, 'caution_mode': True, 'day': day_name,
            'detail': f'HTF {htf_strength:.2f} < {CAUTION_MIN_HTF_STRENGTH}',
            'filters_passed': filters_passed
        }
    filters_passed.append('htf_strong')
    if claude_response and claude_response.get('cognitive_veto'):
        return {
            'allow': False, 'veto_reason': 'caution_day_claude_veto',
            'risk_multiplier': 0, 'caution_mode': True, 'day': day_name,
            'detail': 'Claude veto en caution day',
            'filters_passed': filters_passed
        }
    filters_passed.append('claude_ok')
    claude_mult = (claude_response or {}).get('multiplier', 1.0)
    if claude_mult < CAUTION_MIN_CLAUDE_MULT:
        return {
            'allow': False, 'veto_reason': 'caution_day_low_claude_confidence',
            'risk_multiplier': 0, 'caution_mode': True, 'day': day_name,
            'detail': f'Mult {claude_mult:.2f} < {CAUTION_MIN_CLAUDE_MULT}',
            'filters_passed': filters_passed
        }
    filters_passed.append('claude_confident')
    return {
        'allow': True, 'risk_multiplier': CAUTION_RISK_MULTIPLIER,
        'caution_mode': True, 'day': day_name,
        'detail': 'CAUTION DAY ELITE PASS',
        'filters_passed': filters_passed, 'reason': 'elite_setup_caution_day'
    }
# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 7: OANDA CLIENT
# ═══════════════════════════════════════════════════════════════════════════════

async def oanda_get(path):
    headers = {'Authorization': f'Bearer {OANDA_TOKEN}'}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f'{OANDA_BASE}{path}', headers=headers); r.raise_for_status()
        return r.json()

async def oanda_post(path, body):
    headers = {'Authorization': f'Bearer {OANDA_TOKEN}', 'Content-Type': 'application/json'}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(f'{OANDA_BASE}{path}', headers=headers, json=body); r.raise_for_status()
        return r.json()

async def oanda_put(path, body):
    headers = {'Authorization': f'Bearer {OANDA_TOKEN}', 'Content-Type': 'application/json'}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.put(f'{OANDA_BASE}{path}', headers=headers, json=body); r.raise_for_status()
        return r.json()

async def get_candles(granularity='H1', count=100, pair=None):
    instrument = pair if pair else PAIR
    data = await oanda_get(f'/v3/instruments/{instrument}/candles?granularity={granularity}&count={count}&price=M')
    return data.get('candles', [])

async def get_candles_to(granularity='H1', count=500, to_dt=None, pair=None):
    instrument = pair if pair else PAIR
    path = f'/v3/instruments/{instrument}/candles?granularity={granularity}&count={count}&price=M'
    if to_dt: path += f'&to={to_dt.split(".")[0]}Z'
    data = await oanda_get(path)
    return data.get('candles', [])

async def get_account():
    data = await oanda_get(f'/v3/accounts/{OANDA_ACCOUNT}/summary')
    return data.get('account', {})

async def get_open_trades():
    data = await oanda_get(f'/v3/accounts/{OANDA_ACCOUNT}/openTrades')
    return data.get('trades', [])

async def get_price(pair=None):
    instrument = pair if pair else PAIR
    data = await oanda_get(f'/v3/accounts/{OANDA_ACCOUNT}/pricing?instruments={instrument}')
    p = data['prices'][0]
    return (float(p['bids'][0]['price']) + float(p['asks'][0]['price'])) / 2

async def place_order(units, sl, tp, action, pair=None):
    instrument = pair if pair else PAIR
    u = str(-abs(units)) if action == 'SELL' else str(abs(units))
    order = {'type': 'MARKET', 'instrument': instrument, 'units': u, 'timeInForce': 'FOK'}
    if sl > 0: order['stopLossOnFill'] = {'price': f'{sl:.5f}'}
    if tp > 0: order['takeProfitOnFill'] = {'price': f'{tp:.5f}'}
    return await oanda_post(f'/v3/accounts/{OANDA_ACCOUNT}/orders', {'order': order})

async def close_trade(trade_id, partial=None):
    body = {'units': str(abs(partial))} if partial else {}
    return await oanda_put(f'/v3/accounts/{OANDA_ACCOUNT}/trades/{trade_id}/close', body)

async def modify_sl(trade_id, new_sl):
    return await oanda_put(f'/v3/accounts/{OANDA_ACCOUNT}/trades/{trade_id}/orders',
                           {'stopLoss': {'price': f'{new_sl:.5f}', 'timeInForce': 'GTC'}})


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 8: TECHNICAL ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def compute_atr(candles, period=14):
    if len(candles) < period: return 0.0010
    return sum(float(c['mid']['h']) - float(c['mid']['l']) for c in candles[-period:]) / period

def identify_liquidity_levels(h1_window, d1=None):
    levels = {'pdh': 0.0, 'pdl': 0.0, 'weekly_high': 0.0, 'weekly_low': 0.0,
              'h4_eqh': [], 'h4_eql': [], 'swing_highs': [], 'swing_lows': []}
    if not h1_window: return levels
    if d1 and len(d1) >= 2:
        levels['pdh'] = float(d1[-2]['mid']['h']); levels['pdl'] = float(d1[-2]['mid']['l'])
    elif len(h1_window) >= 48:
        prev = h1_window[-48:-24]
        levels['pdh'] = max(float(c['mid']['h']) for c in prev)
        levels['pdl'] = min(float(c['mid']['l']) for c in prev)
    wk = h1_window[-120:] if len(h1_window) >= 120 else h1_window
    levels['weekly_high'] = max(float(c['mid']['h']) for c in wk)
    levels['weekly_low']  = min(float(c['mid']['l']) for c in wk)
    recent = h1_window[-40:] if len(h1_window) >= 40 else h1_window
    for i in range(2, len(recent) - 2):
        h = float(recent[i]['mid']['h']); l = float(recent[i]['mid']['l'])
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
        bias = 'bullish' if closes[-1] > closes[0] else 'bearish'
        strength = min(1.0, abs(closes[-1] - closes[0]) / closes[0] * 100)
        return bias, round(strength, 2)
    if h1 and len(h1) >= 20:
        closes = [float(c['mid']['c']) for c in h1[-20:]]
        ema_f = sum(closes[-5:]) / 5; ema_s = sum(closes) / len(closes)
        bias = 'bullish' if ema_f > ema_s else 'bearish'
        return bias, round(min(1.0, abs(ema_f - ema_s) / ema_s * 100 * 10), 2)
    return 'neutral', 0.3

def compute_directional_bias(ict):
    """v2.7: combina HTF bias + direccion de velas + regimen en un sesgo 0-100.
    NO es probabilidad del futuro; es un resumen ponderado de lo que los
    indicadores actuales muestran. >50 = sesgo alcista, <50 = bajista."""
    htf_bias = ict.get('htf_bias', 'neutral')
    htf_str = ict.get('htf_strength', 0) or 0
    htf_push = min(0.5, htf_str * 0.5)
    if htf_bias == 'bullish':
        htf_score = 0.5 + htf_push
    elif htf_bias == 'bearish':
        htf_score = 0.5 - htf_push
    else:
        htf_score = 0.5

    regime = ict.get('regime', {})
    trending = regime.get('trending_score', 0.5) or 0.5
    vela_dir = 0.5
    if htf_bias == 'bullish':
        vela_dir = 0.5 + (trending * 0.4)
    elif htf_bias == 'bearish':
        vela_dir = 0.5 - (trending * 0.4)

    regime_type = regime.get('type', 'unknown')
    momentum = regime.get('momentum_consistency', 0) or 0
    chop = regime.get('chop_penalty', 0) or 0
    if regime_type in ('choppy', 'unknown'):
        regime_score = 0.5
    else:
        regime_dir = momentum * 0.4
        if htf_bias == 'bullish':
            regime_score = 0.5 + regime_dir
        elif htf_bias == 'bearish':
            regime_score = 0.5 - regime_dir
        else:
            regime_score = 0.5

    combined = htf_score * 0.5 + vela_dir * 0.3 + regime_score * 0.2
    if chop > 0.4:
        combined = 0.5 + (combined - 0.5) * (1 - min(0.6, chop))
    combined = max(0.05, min(0.95, combined))

    bull_pct = round(combined * 100)
    bear_pct = 100 - bull_pct
    if bull_pct >= 60:
        label = 'alcista'
    elif bull_pct <= 40:
        label = 'bajista'
    else:
        label = 'neutral'
    dist = abs(bull_pct - 50)
    if dist >= 30:
        strength_label = 'fuerte'
    elif dist >= 15:
        strength_label = 'moderado'
    else:
        strength_label = 'debil'

    return {
        'bull_pct': bull_pct,
        'bear_pct': bear_pct,
        'label': label,
        'strength_label': strength_label,
        'regime_type': regime_type,
    }

def detect_inducement(candles, lookback=12):
    if len(candles) < lookback: return False, 'none'
    window = candles[-lookback:]
    highs = [float(c['mid']['h']) for c in window]; lows = [float(c['mid']['l']) for c in window]
    atr = compute_atr(candles); rng = max(highs) - min(lows)
    compressed = rng < atr * lookback * 0.35
    p70h = sorted(highs)[int(len(highs) * 0.70)]; p30l = sorted(lows)[int(len(lows) * 0.30)]
    false_breaks = 0
    for c in window[1:]:
        h = float(c['mid']['h']); l = float(c['mid']['l']); cl = float(c['mid']['c'])
        if h > p70h and cl < p70h: false_breaks += 1
        if l < p30l and cl > p30l: false_breaks += 1
    tol = atr * 0.3
    eq_h = sum(1 for i in range(len(highs)) for j in range(i+1, len(highs)) if abs(highs[i]-highs[j]) < tol)
    eq_l = sum(1 for i in range(len(lows))  for j in range(i+1, len(lows))  if abs(lows[i]-lows[j])  < tol)
    score = (2 if compressed else 0) + min(3, false_breaks) + (2 if (eq_h >= 2 or eq_l >= 2) else 0)
    if score >= 5: return True, 'strong'
    if score >= 3: return True, 'medium'
    if score >= 2: return True, 'weak'
    return False, 'none'

def detect_sweep(candles, levels, atr):
    if len(candles) < 6: return {'detected': False}
    c = candles[-1]
    ch = float(c['mid']['h']); cl = float(c['mid']['l'])
    co = float(c['mid']['o']); cc = float(c['mid']['c'])
    rng = max(ch - cl, 0.00001); buf = atr * 0.15
    bear = []; bull = []
    if levels.get('pdh', 0) > 0:         bear.append(('PDH',         levels['pdh']))
    if levels.get('weekly_high', 0) > 0: bear.append(('WEEKLY_HIGH', levels['weekly_high']))
    for v in levels.get('h4_eqh', []):   bear.append(('H4_EQH', v))
    for v in levels.get('swing_highs', [])[-3:]: bear.append(('SWING_HIGH', v))
    if levels.get('pdl', 0) > 0:         bull.append(('PDL',         levels['pdl']))
    if levels.get('weekly_low', 0) > 0:  bull.append(('WEEKLY_LOW',  levels['weekly_low']))
    for v in levels.get('h4_eql', []):   bull.append(('H4_EQL', v))
    for v in levels.get('swing_lows', [])[-3:]:  bull.append(('SWING_LOW', v))
    if not bear and not bull and len(candles) >= 8:
        prev = candles[-8:-1]
        bear = [('SWING_H1', max(float(x['mid']['h']) for x in prev))]
        bull = [('SWING_H1', min(float(x['mid']['l']) for x in prev))]
    for lt, lv in bear:
        if lv <= 0: continue
        if ch > lv - buf and cc < lv:
            wick = ch - max(co, cc); wp = wick / rng; ext = ch - lv
            if wp >= 0.40 and ext >= atr * 0.05:
                q = 'high' if wp >= 0.65 and ext >= atr * 0.15 else 'medium' if wp >= 0.50 else 'low'
                return {'detected': True, 'direction': 'bearish', 'level': round(lv, 5),
                        'level_type': lt, 'wick_pct': round(wp, 3), 'quality': q,
                        'sweep_high': round(ch, 5)}
    for lt, lv in bull:
        if lv <= 0: continue
        if cl < lv + buf and cc > lv:
            wick = min(co, cc) - cl; wp = wick / rng; ext = lv - cl
            if wp >= 0.40 and ext >= atr * 0.05:
                q = 'high' if wp >= 0.65 and ext >= atr * 0.15 else 'medium' if wp >= 0.50 else 'low'
                return {'detected': True, 'direction': 'bullish', 'level': round(lv, 5),
                        'level_type': lt, 'wick_pct': round(wp, 3), 'quality': q,
                        'sweep_low': round(cl, 5)}
    return {'detected': False}

def detect_displacement(candles, action, atr):
    if len(candles) < 4: return False, 'none', {}
    hits = []
    for c in candles[-3:]:
        o = float(c['mid']['o']); cl = float(c['mid']['c'])
        h = float(c['mid']['h']); l = float(c['mid']['l'])
        rng = max(h - l, 0.00001); body = abs(cl - o); bp = body / rng
        ok = (action == 'SELL' and cl < o) or (action == 'BUY' and cl > o)
        if ok and bp >= 0.55: hits.append({'body': body, 'bp': bp})
    if not hits: return False, 'none', {}
    best = max(hits, key=lambda x: x['bp']); total = sum(h['body'] for h in hits)
    if len(hits) >= 2 and best['bp'] >= 0.65: strength = 'strong'
    elif best['bp'] >= 0.60 or total >= atr * 0.7: strength = 'medium'
    else: strength = 'weak'
    return True, strength, {'count': len(hits), 'best_bp': round(best['bp'], 2)}

def detect_bos(candles, action, atr):
    if len(candles) < 15: return False, 'none', 0.0
    recent = candles[-15:]
    highs = [float(c['mid']['h']) for c in recent]
    lows  = [float(c['mid']['l']) for c in recent]
    lc = float(recent[-1]['mid']['c']); lo = float(recent[-1]['mid']['o'])
    lrng = max(float(recent[-1]['mid']['h']) - float(recent[-1]['mid']['l']), 0.00001)
    lbody = abs(lc - lo) / lrng
    if action == 'SELL':
        sl = min(lows[2:-3])
        if lc < sl:
            d = sl - lc
            if d >= atr * 0.3 and lbody >= 0.55:
                q = 'strong' if d >= atr * 0.5 and lbody >= 0.65 else 'medium'
                return True, q, round(sl, 5)
            elif d >= atr * 0.15: return True, 'weak', round(sl, 5)
    else:
        sh = max(highs[2:-3])
        if lc > sh:
            d = lc - sh
            if d >= atr * 0.3 and lbody >= 0.55:
                q = 'strong' if d >= atr * 0.5 and lbody >= 0.65 else 'medium'
                return True, q, round(sh, 5)
            elif d >= atr * 0.15: return True, 'weak', round(sh, 5)
    return False, 'none', 0.0

def detect_fvg_ob(candles, action, atr):
    result = {'ob': None, 'fvg': None, 'entry_zone': {'high': 0, 'low': 0}, 'valid': False}
    if len(candles) < 8: return result
    min_fvg = atr * 0.15
    recent = candles[-8:]
    for i in range(len(recent) - 3):
        c1h = float(recent[i]['mid']['h']); c1l = float(recent[i]['mid']['l'])
        c3h = float(recent[i+2]['mid']['h']); c3l = float(recent[i+2]['mid']['l'])
        if action == 'BUY' and c3l > c1h:
            gap = c3l - c1h
            if gap >= min_fvg:
                result['fvg'] = {'high': round(c3l, 5), 'low': round(c1h, 5),
                                 'size': round(gap, 5), 'type': 'bull',
                                 'quality': 'strong' if gap >= atr * 0.3 else 'medium'}
                result['entry_zone'] = {'high': round(c3l, 5), 'low': round(c1h, 5)}
                result['valid'] = True; break
        elif action == 'SELL' and c3h < c1l:
            gap = c1l - c3h
            if gap >= min_fvg:
                result['fvg'] = {'high': round(c1l, 5), 'low': round(c3h, 5),
                                 'size': round(gap, 5), 'type': 'bear',
                                 'quality': 'strong' if gap >= atr * 0.3 else 'medium'}
                result['entry_zone'] = {'high': round(c1l, 5), 'low': round(c3h, 5)}
                result['valid'] = True; break
    for i in range(len(recent) - 3, 0, -1):
        cv = recent[i]
        co = float(cv['mid']['o']); cc_v = float(cv['mid']['c'])
        ch = float(cv['mid']['h']); cl = float(cv['mid']['l'])
        nxt_o = float(recent[i+1]['mid']['o']); nxt_c = float(recent[i+1]['mid']['c'])
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
            if v < price and price - v >= atr * 0.5: targets.append(('H4_EQL', v, price - v))
    else:
        for name, val in [('PDH', levels.get('pdh', 0)), ('WEEKLY_HIGH', levels.get('weekly_high', 0))]:
            if val > 0 and val > price and val - price >= atr * 0.5:
                targets.append((name, val, val - price))
        for v in levels.get('h4_eqh', []):
            if v > price and v - price >= atr * 0.5: targets.append(('H4_EQH', v, v - price))
    if not targets: return 0.0, 'NONE', 0.0
    targets.sort(key=lambda x: x[2]); best = targets[0]
    return best[1], best[0], round(best[2] / atr, 1)


# ═══════════════════════════════════════════════════════════════════════════════
# REGIME DETECTOR
# ═══════════════════════════════════════════════════════════════════════════════

def detect_regime(candles_h1, candles_d1=None):
    if len(candles_h1) < 30:
        return {
            'type': 'unknown', 'volatility_z': 0.0, 'trending_score': 0.0,
            'regime_quality': 'insufficient_data', 'momentum_consistency': 0.0,
        }
    window_recent = candles_h1[-12:]
    window_long   = candles_h1[-50:]
    atr_recent = compute_atr(window_recent, period=12)
    atr_long   = compute_atr(window_long, period=50)
    if atr_long > 0:
        volatility_z = (atr_recent - atr_long) / atr_long
    else:
        volatility_z = 0.0
    volatility_z = max(-3.0, min(3.0, volatility_z * 3))
    closes = [float(c['mid']['c']) for c in window_recent]
    opens  = [float(c['mid']['o']) for c in window_recent]
    bullish_candles = sum(1 for c, o in zip(closes, opens) if c > o)
    bearish_candles = sum(1 for c, o in zip(closes, opens) if c < o)
    total = len(closes)
    directional_bias = max(bullish_candles, bearish_candles) / total if total > 0 else 0.5
    direction_changes = sum(
        1 for i in range(1, len(closes))
        if (closes[i] - opens[i]) * (closes[i-1] - opens[i-1]) < 0
    )
    chop_penalty = direction_changes / max(1, total - 1)
    trending_score = round(max(0.0, min(1.0, directional_bias - chop_penalty * 0.5)), 2)
    high_recent = max(float(c['mid']['h']) for c in window_recent)
    low_recent  = min(float(c['mid']['l']) for c in window_recent)
    net_move    = abs(closes[-1] - closes[0])
    total_range = high_recent - low_recent
    momentum_consistency = round(net_move / total_range, 2) if total_range > 0 else 0.0
    if volatility_z < -1.0 and total_range < atr_long * 4:
        regime_type = 'compression'
    elif volatility_z > 1.5 and trending_score > 0.65:
        regime_type = 'expansion'
    elif trending_score >= 0.70 and momentum_consistency >= 0.40:
        regime_type = 'trending'
    elif chop_penalty > 0.40 and trending_score < 0.55:
        regime_type = 'choppy'
    else:
        regime_type = 'ranging'
    if chop_penalty < 0.20 and momentum_consistency >= 0.35:
        regime_quality = 'clean'
    elif chop_penalty < 0.40:
        regime_quality = 'noisy'
    else:
        regime_quality = 'choppy'
    return {
        'type': regime_type, 'volatility_z': round(volatility_z, 2),
        'trending_score': trending_score, 'momentum_consistency': momentum_consistency,
        'regime_quality': regime_quality, 'chop_penalty': round(chop_penalty, 2),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ANOMALY FEATURES
# ═══════════════════════════════════════════════════════════════════════════════

def detect_anomalies(candles_h1, news_events=None, candle_dt=None, pip_value=0.0001):
    if len(candles_h1) < 10:
        return {
            'consecutive_long_wicks': 0, 'mechas_consecutivas': 0,
            'gap_post_news': False, 'sweep_without_retest': False,
            'volume_dissonance': False, 'displacement_velocity_pips': 0.0,
            'anomaly_count': 0, 'severity': 'none',
        }
    recent = candles_h1[-8:]
    atr = compute_atr(candles_h1)
    long_wick_streak = 0
    max_streak = 0
    for c in recent:
        h = float(c['mid']['h']); l = float(c['mid']['l'])
        o = float(c['mid']['o']); cc = float(c['mid']['c'])
        rng = max(h - l, 0.00001)
        upper_wick = h - max(o, cc)
        lower_wick = min(o, cc) - l
        max_wick = max(upper_wick, lower_wick)
        wick_pct = max_wick / rng
        if wick_pct > 0.55:
            long_wick_streak += 1
            max_streak = max(max_streak, long_wick_streak)
        else:
            long_wick_streak = 0
    mechas_alternantes = 0
    for i in range(1, len(recent)):
        c_prev = recent[i-1]; c_curr = recent[i]
        h_p, l_p = float(c_prev['mid']['h']), float(c_prev['mid']['l'])
        o_p, cc_p = float(c_prev['mid']['o']), float(c_prev['mid']['c'])
        h_c, l_c = float(c_curr['mid']['h']), float(c_curr['mid']['l'])
        o_c, cc_c = float(c_curr['mid']['o']), float(c_curr['mid']['c'])
        upper_p = h_p - max(o_p, cc_p); upper_c = h_c - max(o_c, cc_c)
        lower_p = min(o_p, cc_p) - l_p; lower_c = min(o_c, cc_c) - l_c
        if upper_p > atr * 0.3 and lower_c > atr * 0.3:
            mechas_alternantes += 1
        elif lower_p > atr * 0.3 and upper_c > atr * 0.3:
            mechas_alternantes += 1
    gap_post_news = False
    if news_events and candle_dt:
        for i in range(1, len(recent)):
            prev_close = float(recent[i-1]['mid']['c'])
            curr_open = float(recent[i]['mid']['o'])
            gap = abs(curr_open - prev_close)
            if gap > atr * 0.5:
                try:
                    candle_time = recent[i].get('time', '')
                    candle_dt_check = datetime.fromisoformat(
                        candle_time.replace('Z', '+00:00')
                    ).replace(tzinfo=None)
                    for evt in news_events:
                        try:
                            evt_dt = datetime.strptime(
                                f"{evt.get('date','')} {evt.get('time','')}",
                                '%Y-%m-%d %H:%M'
                            )
                            diff_min = abs((candle_dt_check - evt_dt).total_seconds() / 60)
                            if diff_min <= 120:
                                gap_post_news = True
                                break
                        except Exception:
                            continue
                    if gap_post_news:
                        break
                except Exception:
                    pass
    sweep_without_retest = False
    if len(candles_h1) >= 10:
        last_5 = candles_h1[-5:]
        prev_5 = candles_h1[-10:-5]
        prev_high = max(float(c['mid']['h']) for c in prev_5)
        prev_low  = min(float(c['mid']['l']) for c in prev_5)
        last_high = max(float(c['mid']['h']) for c in last_5)
        last_low  = min(float(c['mid']['l']) for c in last_5)
        last_close = float(last_5[-1]['mid']['c'])
        if last_high > prev_high and (last_high - last_close) > atr * 1.5:
            sweep_without_retest = True
        if last_low < prev_low and (last_close - last_low) > atr * 1.5:
            sweep_without_retest = True
    volume_dissonance = False
    if len(recent) >= 5:
        volumes = [int(c.get('volume', 0)) for c in recent]
        avg_vol = sum(volumes[:-1]) / max(1, len(volumes) - 1)
        last_candle = recent[-1]
        last_vol = int(last_candle.get('volume', 0))
        last_range = float(last_candle['mid']['h']) - float(last_candle['mid']['l'])
        if last_range > atr * 1.5 and avg_vol > 0 and last_vol < avg_vol * 0.70:
            volume_dissonance = True
    if len(candles_h1) >= 4:
        velocity = abs(
            float(candles_h1[-1]['mid']['c']) - float(candles_h1[-4]['mid']['c'])
        ) / pip_value   # v2.6-fixed7: pip_value por par (era *10000)
    else:
        velocity = 0.0
    anomalies_active = []
    if max_streak >= 3: anomalies_active.append('long_wick_streak')
    if mechas_alternantes >= 2: anomalies_active.append('alternating_wicks')
    if gap_post_news: anomalies_active.append('gap_post_news')
    if sweep_without_retest: anomalies_active.append('sweep_without_retest')
    if volume_dissonance: anomalies_active.append('volume_dissonance')
    anomaly_count = len(anomalies_active)
    if anomaly_count >= 3: severity = 'high'
    elif anomaly_count == 2: severity = 'medium'
    elif anomaly_count == 1: severity = 'low'
    else: severity = 'none'
    return {
        'consecutive_long_wicks': max_streak, 'mechas_consecutivas': mechas_alternantes,
        'gap_post_news': gap_post_news, 'sweep_without_retest': sweep_without_retest,
        'volume_dissonance': volume_dissonance,
        'displacement_velocity_pips': round(velocity, 1),
        'anomaly_count': anomaly_count, 'anomalies_active': anomalies_active,
        'severity': severity,
    }
# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 9: SCORING ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def compute_score(htf_bias, htf_strength, sweep, inducement, disp_strength, bos_data, fvg_ob,
                  killzone, adr_pct, atr_pips, consol_ok, has_target):
    W = SCORE_WEIGHTS; score = 0.0; factors = {}
    action = 'SELL' if sweep.get('direction') == 'bearish' else 'BUY'
    aligned = (action == 'SELL' and htf_bias == 'bearish') or (action == 'BUY' and htf_bias == 'bullish')
    if htf_strength >= 0.7:   pts = W['htf_alignment'] * (1.0 if aligned else 0.2); tag = 'fuerte' if aligned else 'CONTRA HTF'
    elif htf_strength >= 0.4: pts = W['htf_alignment'] * (0.6 if aligned else 0.15); tag = 'moderado' if aligned else 'CONTRA HTF'
    else:                     pts = W['htf_alignment'] * (0.3 if aligned else 0.1); tag = 'debil'
    score += pts; factors['htf_alignment'] = {'pts': round(pts,1), 'max': W['htf_alignment'], 'tag': tag}
    sq = sweep.get('quality', 'low'); lt = sweep.get('level_type', '')
    sp = {'high': 1.0, 'medium': 0.65, 'low': 0.25}.get(sq, 0.25)
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
    min_req = MIN_SCORE + (10 if state.get('defensive_mode') else 0)
    executable = score >= min_req
    confidence = round(min(0.95, max(0.50, 0.55 + (score - 50) / 150)), 2)
    reasons = [f"{k}: {v['pts']:.0f}/{v['max']:.0f} ({v['tag']})" for k, v in factors.items()]
    return {'total': round(score,1), 'executable': executable, 'confidence': confidence,
            'factors': factors, 'reasons': reasons, 'action': action}

def compute_levels(sweep, fvg_ob, target_level, price, balance, risk_pct, atr):
    action = 'SELL' if sweep['direction'] == 'bearish' else 'BUY'
    buf = atr * 0.20
    if action == 'SELL':
        sl = sweep.get('sweep_high', sweep['level']) + buf
        sl_dist = abs(sl - price)
        tp1 = price - sl_dist * 1.5
        tp2 = target_level if target_level > 0 else price - sl_dist * 2.5
    else:
        sl = sweep.get('sweep_low', sweep['level']) - buf
        sl_dist = abs(price - sl)
        tp1 = price + sl_dist * 1.5
        tp2 = target_level if target_level > 0 else price + sl_dist * 2.5
    if sl_dist <= 0 or sl_dist > price * 0.012: return None
    risk_usd = balance * (risk_pct / 100)
    pos_size = max(1000, int(risk_usd / sl_dist))
    rr1 = round(abs(tp1 - price) / sl_dist, 2)
    rr2 = round(abs(tp2 - price) / sl_dist, 2)
    return {'action': action, 'sl': round(sl,5), 'tp1': round(tp1,5), 'tp2': round(tp2,5),
            'sl_dist': round(sl_dist,5), 'pos_size': pos_size, 'rr1': rr1, 'rr2': rr2,
            'entry_zone': fvg_ob.get('entry_zone', {'high': 0, 'low': 0})}

def run_ict_pipeline(h1, h4, d1, price, balance, risk_pct, hour=None, news_events=None, pip_value=0.0001):
    if len(h1) < 30:
        return {'sweep': {'detected': False},
                'score': {'total': 0, 'executable': False, 'confidence': 0, 'factors': {},
                          'reasons': ['Velas insuficientes'], 'action': None}, 'levels': None,
                'regime': {'type': 'unknown', 'volatility_z': 0.0, 'trending_score': 0.0,
                           'regime_quality': 'insufficient_data', 'momentum_consistency': 0.0},
                'anomalies': {'anomaly_count': 0, 'severity': 'none', 'anomalies_active': []}}
    atr = compute_atr(h1); atr_pips = atr / pip_value   # v2.6-fixed7: pip_value por par (era *10000)
    h = hour if hour is not None else now_et().hour
    kill = get_killzone(h)
    levels = identify_liquidity_levels(h1, d1[-5:] if d1 and len(d1) >= 5 else None)
    htf_bias, htf_str = detect_htf_bias(d1, h1)
    inducement = detect_inducement(h1)
    sweep = detect_sweep(h1, levels, atr)
    regime = detect_regime(h1, d1)
    anomalies = detect_anomalies(h1, news_events=news_events, candle_dt=now_et(), pip_value=pip_value)
    if not sweep.get('detected'):
        return {'sweep': sweep,
                'score': {'total': 0, 'executable': False, 'confidence': 0, 'factors': {},
                          'reasons': ['Sin sweep institucional detectado'], 'action': None},
                'levels': None, 'htf_bias': htf_bias, 'htf_strength': htf_str,
                'liq_levels': levels, 'atr': round(atr,5), 'atr_pips': round(atr_pips,1),
                'killzone': kill, 'adr_pct': 0.5,
                'inducement': {'found': False, 'quality': 'none'},
                'displacement': {'found': False, 'strength': 'none'},
                'structure': {'bos': False, 'bos_quality': 'none', 'bos_level': 0.0},
                'fvg_ob': {'ob': None, 'fvg': None, 'entry_zone': {'high': 0, 'low': 0}, 'valid': False},
                'liq_target': {'level': 0.0, 'type': 'NONE', 'rr': 0.0},
                'regime': regime, 'anomalies': anomalies}
    action = 'SELL' if sweep['direction'] == 'bearish' else 'BUY'
    disp_found, disp_str, _ = detect_displacement(h1, action, atr)
    bos_data = detect_bos(h1, action, atr)
    fvg_ob = detect_fvg_ob(h1, action, atr)
    tl, tt, tr = compute_liquidity_target(action, levels, price, atr)
    adr_pct = 0.5
    today = h1[-1].get('time', '')[:10]
    day_c = [c for c in h1[-24:] if c.get('time', '')[:10] == today]
    if len(day_c) >= 3:
        dh = max(float(c['mid']['h']) for c in day_c); dl = min(float(c['mid']['l']) for c in day_c)
        adr_pct = max(0, 1 - (dh - dl) / (atr * 8))
    rh = [float(c['mid']['h']) for c in h1[-8:]]; rl = [float(c['mid']['l']) for c in h1[-8:]]
    consol_ok = (max(rh) - min(rl)) >= atr * 0.8
    score = compute_score(htf_bias, htf_str, sweep, inducement, disp_str, bos_data, fvg_ob,
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
            'killzone': kill, 'adr_pct': round(adr_pct,2), 'consol_ok': consol_ok,
            'regime': regime, 'anomalies': anomalies}


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 10: COGNITIVE LAYER
# ═══════════════════════════════════════════════════════════════════════════════

class CognitiveValidation(BaseModel):
    model_config = ConfigDict(extra='forbid')
    veto: bool
    veto_reason: Optional[str] = Field(None, max_length=200)
    confidence_multiplier: float = Field(ge=0.5, le=1.0)
    narrative_quality: Literal['clean', 'acceptable', 'dirty']
    regime_assessment: str = Field(max_length=150)
    anomalies: List[str] = Field(default_factory=list, max_length=5)
    narrative: str = Field(max_length=500)

    @field_validator('veto_reason')
    @classmethod
    def reason_required_if_veto(cls, v, info):
        if info.data.get('veto') and not v:
            raise ValueError('veto=true requires veto_reason')
        return v

_cognitive_health = {'calls': [], 'failures': [], 'disabled_until': None}

def cognitive_record(success: bool):
    now_ts = time.time()
    _cognitive_health['calls'].append(now_ts)
    if not success: _cognitive_health['failures'].append(now_ts)
    cutoff = now_ts - 3600
    _cognitive_health['calls']    = [t for t in _cognitive_health['calls']    if t > cutoff]
    _cognitive_health['failures'] = [t for t in _cognitive_health['failures'] if t > cutoff]
    if len(_cognitive_health['calls']) >= 5:
        rate = len(_cognitive_health['failures']) / len(_cognitive_health['calls'])
        if rate > COGNITIVE_FAILURE_THRESHOLD:
            _cognitive_health['disabled_until'] = now_ts + 3600
            log.warning(f'[COGNITIVE] DISABLED MODE: tasa fallos {rate:.0%}')

def cognitive_is_disabled():
    if _cognitive_health['disabled_until'] is None: return False
    return time.time() < _cognitive_health['disabled_until']

COGNITIVE_PROMPT = """Eres una capa de validacion institucional para un sistema de trading EUR/USD.
Python ya tomo la decision tecnica (BUY o SELL) basada en su motor ICT/SMC.
TU TRABAJO NO ES decidir direccion. TU TRABAJO ES validar el contexto institucional.

Tu output DEBE ser un JSON con estos campos exactos:
{
  "veto": false,
  "veto_reason": null,
  "confidence_multiplier": 0.95,
  "narrative_quality": "clean",
  "regime_assessment": "expansion saludable post-Asia",
  "anomalies": [],
  "narrative": "Sweep limpio en PDH con mecha 68% seguido de displacement strong."
}

REGLAS:
- veto: true SOLO si detectas incoherencia institucional grave.
- confidence_multiplier: rango 0.5-1.0.
- narrative_quality: "clean" / "acceptable" / "dirty"
- narrative: max 500 caracteres.

DAYS OF CAUTION (Lunes/Viernes):
- Lunes: WR historico 20%, PnL -$1,585
- Viernes: WR historico 33%, PnL -$1,665
- En estos dias, SE EXTREMADAMENTE ESTRICTO.
- Si tienes CUALQUIER duda, VETA.

Responde SOLO el JSON. Sin texto antes ni despues."""

async def call_cognitive_layer(ict: dict, recent_history: list) -> Optional[CognitiveValidation]:
    if not ANTHROPIC_API_KEY: return None
    if cognitive_is_disabled(): return None
    score = ict.get('score', {}); sweep = ict.get('sweep', {})
    technical_action = score.get('action')
    if not technical_action or not sweep.get('detected') or not score.get('executable'):
        return None
    context_bundle = {
        'setup': {'instrument': 'EUR_USD', 'technical_action': technical_action,
                  'technical_confidence': score.get('confidence', 0),
                  'technical_score': score.get('total', 0),
                  'killzone': ict.get('killzone'),
                  'timestamp_et': now_et().isoformat(),
                  'day_of_week': now_et().strftime('%A'),
                  'is_caution_day': is_caution_day()},
        'features': {
            'sweep': {'level_type': sweep.get('level_type'), 'quality': sweep.get('quality'),
                      'wick_pct': sweep.get('wick_pct')},
            'displacement': {'strength': ict.get('displacement', {}).get('strength')},
            'bos':          {'quality': ict.get('structure', {}).get('bos_quality')},
            'htf_bias':     ict.get('htf_bias'), 'htf_strength': ict.get('htf_strength'),
            'inducement':   ict.get('inducement', {}),
            'fvg': {'valid': bool(ict.get('fvg_ob', {}).get('fvg')),
                    'quality': (ict.get('fvg_ob', {}).get('fvg') or {}).get('quality')},
            'atr_pips': ict.get('atr_pips'), 'adr_pct': ict.get('adr_pct'),
        },
        'regime': ict.get('regime', {}),
        'anomalies_detected': ict.get('anomalies', {}),
        'liq_target': ict.get('liq_target'),
        'context': {'last_5_decisions': (recent_history or [])[-5:],
                    'consecutive_losses': state.get('consecutive_losses', 0),
                    'defensive_mode': state.get('defensive_mode', False)},
        'day_statistics_warning': _get_caution_day_warning() if is_caution_day() else None}
    try:
        async with httpx.AsyncClient(timeout=COGNITIVE_TIMEOUT_SEC) as client:
            r = await client.post('https://api.anthropic.com/v1/messages',
                headers={'Content-Type': 'application/json',
                         'x-api-key': ANTHROPIC_API_KEY,
                         'anthropic-version': '2023-06-01'},
                json={'model': SONNET_MODEL, 'max_tokens': 600,
                      'system': COGNITIVE_PROMPT,
                      'messages': [{'role': 'user',
                                    'content': json.dumps(context_bundle, ensure_ascii=False)}]})
            if not r.is_success:
                log.warning(f'[COGNITIVE] HTTP {r.status_code}')
                cognitive_record(False); return None
            raw_text = r.json()['content'][0]['text']
    except Exception as e:
        log.warning(f'[COGNITIVE] Exception: {e}')
        cognitive_record(False); return None
    try:
        clean = raw_text.replace('```json', '').replace('```', '').strip()
        start = clean.find('{'); end = clean.rfind('}')
        if start == -1 or end == -1: raise ValueError('No JSON')
        parsed = json.loads(clean[start:end+1])
        parsed.pop('action', None)
        validation = CognitiveValidation(**parsed)
        cognitive_record(True)
        return validation
    except Exception as e:
        log.warning(f'[COGNITIVE] Parse failed: {e}')
        cognitive_record(False); return None


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 11: DECISION GATE
# ═══════════════════════════════════════════════════════════════════════════════

def decision_gate(ict: dict, cognitive: Optional[CognitiveValidation]) -> dict:
    score = ict.get('score', {}); sweep = ict.get('sweep', {}); levels = ict.get('levels')
    technical_action     = score.get('action')
    technical_confidence = score.get('confidence', 0)
    technical_score      = score.get('total', 0)

    if not score.get('executable') or technical_action not in ('BUY', 'SELL'):
        return {'action': 'HOLD', 'confidence': 0, 'score': technical_score,
                'source': 'hold_technical',
                'reason': score.get('reasons', ['HOLD'])[0] if score.get('reasons') else 'HOLD',
                'cognitive_veto': False, 'cognitive_multiplier': None,
                'narrative': '', 'anomalies': [],
                'narrative_quality': None, 'regime_assessment': None,
                'sl': 0, 'tp1': 0, 'tp2': 0, 'pos_size': 0, 'rr1': 0, 'rr2': 0,
                'entry_zone': {'high': 0, 'low': 0},
                'liq_target': ict.get('liq_target', {}),
                'killzone': ict.get('killzone'), 'htf_bias': ict.get('htf_bias')}

    if cognitive is None:
        if technical_score >= ELITE_SCORE_THRESHOLD:
            confidence_final = round(technical_confidence * ELITE_NO_COGNITIVE_PENALTY, 3)
            return {'action': technical_action, 'confidence': confidence_final,
                    'score': technical_score, 'source': 'cognitive_down_elite',
                    'reason': f'Elite setup (score {technical_score}) con cognitive down.',
                    'cognitive_veto': False, 'cognitive_multiplier': ELITE_NO_COGNITIVE_PENALTY,
                    'narrative': 'Cognitive layer no disponible - operando en modo elite',
                    'anomalies': [], 'narrative_quality': None, 'regime_assessment': None,
                    'sl': levels['sl'] if levels else 0,
                    'tp1': levels['tp1'] if levels else 0,
                    'tp2': levels['tp2'] if levels else 0,
                    'pos_size': levels['pos_size'] if levels else 0,
                    'rr1': levels['rr1'] if levels else 0,
                    'rr2': levels['rr2'] if levels else 0,
                    'entry_zone': levels['entry_zone'] if levels else {'high': 0, 'low': 0},
                    'liq_target': ict.get('liq_target', {}),
                    'killzone': ict.get('killzone'), 'htf_bias': ict.get('htf_bias')}
        else:
            return {'action': 'HOLD', 'confidence': 0, 'score': technical_score,
                    'source': 'cognitive_down_non_elite',
                    'reason': f'Cognitive down + score {technical_score} no elite',
                    'cognitive_veto': False, 'cognitive_multiplier': None,
                    'narrative': 'Cognitive layer no disponible y setup no es elite',
                    'anomalies': [], 'narrative_quality': None, 'regime_assessment': None,
                    'sl': 0, 'tp1': 0, 'tp2': 0, 'pos_size': 0, 'rr1': 0, 'rr2': 0,
                    'entry_zone': {'high': 0, 'low': 0},
                    'liq_target': ict.get('liq_target', {}),
                    'killzone': ict.get('killzone'), 'htf_bias': ict.get('htf_bias')}

    if cognitive.veto:
        return {'action': 'HOLD', 'confidence': 0, 'score': technical_score,
                'source': 'vetoed',
                'reason': f'Cognitive veto: {cognitive.veto_reason}',
                'cognitive_veto': True, 'cognitive_multiplier': cognitive.confidence_multiplier,
                'narrative': cognitive.narrative, 'anomalies': cognitive.anomalies,
                'narrative_quality': cognitive.narrative_quality,
                'regime_assessment': cognitive.regime_assessment,
                'sl': levels['sl'] if levels else 0,
                'tp1': levels['tp1'] if levels else 0,
                'tp2': levels['tp2'] if levels else 0,
                'pos_size': 0,
                'rr1': levels['rr1'] if levels else 0,
                'rr2': levels['rr2'] if levels else 0,
                'entry_zone': levels['entry_zone'] if levels else {'high': 0, 'low': 0},
                'liq_target': ict.get('liq_target', {}),
                'killzone': ict.get('killzone'), 'htf_bias': ict.get('htf_bias'),
                'technical_action_that_would_have_been': technical_action,
                'technical_confidence_pre_veto': technical_confidence}

    confidence_final = round(technical_confidence * cognitive.confidence_multiplier, 3)
    caution_filter_result = days_of_caution_filter(
        signal={
            'score': technical_score,
            'htf_strength': ict.get('htf_strength', 0),
            'action': technical_action
        },
        regime=ict.get('regime'),
        anomalies=ict.get('anomalies'),
        claude_response={
            'cognitive_veto': cognitive.veto,
            'multiplier': cognitive.confidence_multiplier,
            'narrative': cognitive.narrative
        }
    )

    if not caution_filter_result['allow']:
        log.warning(f"[CAUTION_DAY] Trade vetado: {caution_filter_result['veto_reason']}")
        return {'action': 'HOLD', 'confidence': 0, 'score': technical_score,
                'source': 'caution_day_veto',
                'reason': f"Caution Day VETO: {caution_filter_result['veto_reason']}",
                'cognitive_veto': False, 'cognitive_multiplier': cognitive.confidence_multiplier,
                'narrative': cognitive.narrative, 'anomalies': cognitive.anomalies,
                'narrative_quality': cognitive.narrative_quality,
                'regime_assessment': cognitive.regime_assessment,
                'sl': levels['sl'] if levels else 0,
                'tp1': levels['tp1'] if levels else 0,
                'tp2': levels['tp2'] if levels else 0,
                'pos_size': 0,
                'rr1': levels['rr1'] if levels else 0,
                'rr2': levels['rr2'] if levels else 0,
                'entry_zone': levels['entry_zone'] if levels else {'high': 0, 'low': 0},
                'liq_target': ict.get('liq_target', {}),
                'killzone': ict.get('killzone'), 'htf_bias': ict.get('htf_bias'),
                'caution_filter': caution_filter_result,
                'technical_action_that_would_have_been': technical_action}

    risk_mult = caution_filter_result['risk_multiplier']
    final_pos_size = levels['pos_size'] if levels else 0
    if risk_mult < 1.0 and final_pos_size > 0:
        final_pos_size = int(final_pos_size * risk_mult)
        log.info(f"[CAUTION_DAY] Risk reducido a {risk_mult*100:.0f}%")

    return {'action': technical_action, 'confidence': confidence_final,
            'score': technical_score,
            'source': 'validated' + ('_caution' if caution_filter_result['caution_mode'] else ''),
            'reason': cognitive.narrative,
            'cognitive_veto': False, 'cognitive_multiplier': cognitive.confidence_multiplier,
            'narrative': cognitive.narrative, 'anomalies': cognitive.anomalies,
            'narrative_quality': cognitive.narrative_quality,
            'regime_assessment': cognitive.regime_assessment,
            'sl': levels['sl'] if levels else 0,
            'tp1': levels['tp1'] if levels else 0,
            'tp2': levels['tp2'] if levels else 0,
            'pos_size': final_pos_size,
            'rr1': levels['rr1'] if levels else 0,
            'rr2': levels['rr2'] if levels else 0,
            'entry_zone': levels['entry_zone'] if levels else {'high': 0, 'low': 0},
            'liq_target': ict.get('liq_target', {}),
            'killzone': ict.get('killzone'), 'htf_bias': ict.get('htf_bias'),
            'caution_filter': caution_filter_result}
# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 12: ORQUESTADOR + MEMORIA
# ═══════════════════════════════════════════════════════════════════════════════

async def _send_critical_alert(alert_type: str, msg: str, details: dict):
    subj, html = build_critical_alert_email(alert_type, msg, details)
    await send_email(subj, html)

def update_memory(trade_result=None):
    global memory
    prev_defensive = state.get('defensive_mode', False)
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
            state['defensive_mode'] = True
            state['defensive_reason'] = f'{sl_count} SL en ultimos 10 trades'
            log.warning(f'[DEFENSE] Modo defensivo activado')
            if not prev_defensive:
                asyncio.create_task(_send_critical_alert(
                    'MODO DEFENSIVO ACTIVADO',
                    f'Sistema detecto {sl_count} SL en los ultimos 10 trades.',
                    {'SL recientes': sl_count, 'Total ultimos 10': len(recent10)}))
        elif sl_count <= 1 and state.get('defensive_mode'):
            state['defensive_mode'] = False; state['defensive_reason'] = ''
    memory['last_updated'] = now_utc().isoformat()
    storage_write_json('memory/edge_tracker.json', memory)
    storage_write_json('memory/session_stats.json', memory.get('session_stats', {}))


async def run_analysis_pair(pair, auto_execute=False, all_news=None):
    pair_cfg = get_pair_config(pair)
    display = pair_cfg['display']

    if not pair_cfg.get('enabled', True):
        log.info(f'=== SKIP {display} (disabled) ===')
        return None

    log.info(f'=== ANALISIS {display} ===')

    if all_news is None:
        all_news = HIGH_IMPACT_EVENTS

    et = now_et()
    news_blocked, news_reason = is_news_blocked(et, all_news)
    if news_blocked: log.info(f'[NEWS][{display}] Bloqueado: {news_reason}')

    h1 = h4 = d1 = []
    try:
        h1 = await get_candles('H1', 80, pair=pair)
        h4 = await get_candles('H4', 50, pair=pair)
        d1 = await get_candles('D', 10, pair=pair)
        log.info(f'[VELAS][{display}] H1:{len(h1)} H4:{len(h4)} D1:{len(d1)}')
    except Exception as e:
        log.error(f'[VELAS][{display}] {e}')
        return None

    price = 0.0
    try: price = await get_price(pair=pair)
    except Exception:
        if h1: price = float(h1[-1]['mid']['c'])

    risk_for_pair = pair_cfg['risk_pct']
    pip_value_pair = pair_cfg.get('pip_value', 0.0001)   # v2.6-fixed7

    ict = run_ict_pipeline(h1, h4, d1, price, state['balance'], risk_for_pair,
                            news_events=all_news, pip_value=pip_value_pair)
    ict['pair'] = pair

    log.info(f'[ICT][{display}] sweep:{ict["sweep"].get("detected")} score:{ict["score"]["total"]}/100')
    if ict.get('regime'):
        r = ict['regime']
        log.info(f'[REGIME][{display}] type={r.get("type")} vol_z={r.get("volatility_z")} '
                 f'trending={r.get("trending_score")}')

    cognitive = None
    if ict.get('score', {}).get('executable') and ict['sweep'].get('detected'):
        cognitive = await call_cognitive_layer(ict, state.get('history', [])[-10:])
        if cognitive:
            log.info(f'[COGNITIVE][{display}] veto={cognitive.veto} mult={cognitive.confidence_multiplier}')

    decision = decision_gate(ict, cognitive)
    decision['pair'] = pair
    log.info(f'[GATE][{display}] {decision["action"]} conf:{decision["confidence"]:.0%} source:{decision["source"]}')

    if decision.get('source') == 'vetoed':
        subj, html = build_cognitive_veto_email(decision)
        asyncio.create_task(send_email(subj, html))

    audit_record = {
        'pair': pair,
        'technical_action': ict.get('score', {}).get('action'),
        'technical_confidence': ict.get('score', {}).get('confidence', 0),
        'technical_score': ict.get('score', {}).get('total', 0),
        'technical_factors': ict.get('score', {}).get('factors', {}),
        'killzone': ict.get('killzone'), 'htf_bias': ict.get('htf_bias'),
        'news_blocked': news_blocked, 'news_reason': news_reason,
        'cognitive_called': cognitive is not None,
        'cognitive_veto': decision.get('cognitive_veto'),
        'cognitive_multiplier': decision.get('cognitive_multiplier'),
        'narrative': decision.get('narrative', ''),
        'narrative_quality': decision.get('narrative_quality'),
        'anomalies': decision.get('anomalies', []),
        'regime_assessment': decision.get('regime_assessment'),
        'final_action': decision['action'], 'final_confidence': decision['confidence'],
        'final_source': decision['source'], 'final_reason': decision['reason'],
        'price': price, 'sl': decision.get('sl', 0),
        'tp1': decision.get('tp1', 0), 'tp2': decision.get('tp2', 0),
    }
    audit_decision(audit_record)

    pair_state[pair]['last_analysis'] = {'ict': ict, 'price': price, 'ts': now_utc().isoformat()}
    pair_state[pair]['last_decision'] = decision

    if pair == 'EUR_USD':
        state['last_analysis'] = {'ict': ict, 'price': price}
        state['last_decision'] = decision
        state['last_update']   = now_utc().isoformat()

    if pair == 'EUR_USD':
        et_now = now_et()
        legacy_entry = {
            'timestamp': now_utc().isoformat(),
            'date': et_now.strftime('%Y-%m-%d'),
            'time': et_now.strftime('%H:%M ET'),
            'day_of_week': et_now.strftime('%A'),
            'month': et_now.strftime('%B %Y'),
            'pair': pair,
            'killzone': ict.get('killzone', '--'),
            'action': decision['action'], 'confidence': decision['confidence'],
            'score': decision['score'], 'source': decision.get('source', ''),
            'sweep_detected': ict['sweep'].get('detected', False),
            'sweep_level': ict['sweep'].get('level', 0),
            'sweep_type': ict['sweep'].get('level_type', ''),
            'sweep_quality': ict['sweep'].get('quality', ''),
            'htf_bias': ict.get('htf_bias', ''),
            'htf_strength': ict.get('htf_strength', 0),
            'bos_quality': ict.get('structure', {}).get('bos_quality', ''),
            'displacement': ict.get('displacement', {}).get('strength', ''),
            'inducement': ict.get('inducement', {}).get('quality', ''),
            'adr_pct': ict.get('adr_pct', 0), 'atr_pips': ict.get('atr_pips', 0),
            'sl': decision.get('sl', 0), 'tp1': decision.get('tp1', 0),
            'tp2': decision.get('tp2', 0),
            'rr1': decision.get('rr1', 0), 'rr2': decision.get('rr2', 0),
            'price': price, 'ceo_reason': decision.get('reason', ''),
            'ceo_macro': decision.get('regime_assessment', ''),
            'ceo_rec': ', '.join(decision.get('anomalies', [])) if decision.get('anomalies') else '',
            'cognitive_veto': decision.get('cognitive_veto', False),
            'cognitive_multiplier': decision.get('cognitive_multiplier'),
            'narrative_quality': decision.get('narrative_quality'),
            'outcome': '', 'result_usd': 0, 'trade_id': '',
            'news_blocked': news_blocked, 'news_reason': news_reason,
            'score_factors': ' | '.join(f"{k}:{v['pts']:.0f}" for k, v in ict.get('score', {}).get('factors', {}).items()),
            'defensive_mode': state.get('defensive_mode', False),
        }
        state['history'].append(legacy_entry)
        if len(state['history']) > 2000:
            state['history'] = state['history'][-2000:]
        storage_write_json('legacy/history.json', state['history'][-2000:])

    # v2.6.0-beta-fixed12: filtro de killzones permitidas por par (en vivo)
    # AUD/CAD solo operan en LONDON_OPEN (NY pierde dinero)
    pair_allowed_kz = pair_cfg.get('allowed_killzones', [])
    current_kz = ict.get('killzone', '')
    kz_blocked = bool(pair_allowed_kz and current_kz not in pair_allowed_kz)
    if kz_blocked:
        log.info(f'[KILLZONE][{display}] Bloqueado: {current_kz} no esta en {pair_allowed_kz}')

    if auto_execute and is_session() and not news_blocked and not kz_blocked:
        await execute_signal(decision, pair=pair)

    log.info(f'=== FIN ANALISIS {display} ===')
    return decision


async def run_analysis(auto_execute=False):
    try:
        acc = await get_account()
        state['balance'] = float(acc.get('balance', state['balance']))
    except Exception as e: log.error(f'[BALANCE] {e}')
    try: state['open_trades'] = await get_open_trades()
    except Exception: pass

    live_news = []
    try: live_news = await fetch_ff_calendar()
    except Exception: pass
    all_news = HIGH_IMPACT_EVENTS + live_news

    results = {}
    for pair in get_active_pairs():
        try:
            results[pair] = await run_analysis_pair(pair, auto_execute=auto_execute, all_news=all_news)
        except Exception as e:
            log.error(f'[ANALYSIS][{pair}] Error: {e}')
            results[pair] = None

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 13: EXECUTION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def update_risk(win, pnl):
    prev_paused = state.get('trading_paused', False)
    prev_consec = state.get('consecutive_losses', 0)
    if not win and pnl < 0:
        state['consecutive_losses'] += 1
        today = str(now_et().date())
        if state.get('daily_loss_date') != today:
            state['daily_loss_date'] = today; state['daily_loss_usd'] = 0.0
        state['daily_loss_usd'] += abs(pnl)
        bal = state.get('balance', 100000.0)
        if state['daily_loss_usd'] >= bal * (MAX_DAILY_LOSS / 100):
            state['trading_paused'] = True
            state['pause_reason'] = f'Limite perdida diaria ${state["daily_loss_usd"]:.0f}'
            if not prev_paused:
                asyncio.create_task(_send_critical_alert(
                    'TRADING PAUSADO',
                    f'Sistema alcanzo el limite de perdida diaria del {MAX_DAILY_LOSS}%.',
                    {'Perdida diaria': f'${state["daily_loss_usd"]:,.2f}',
                     'Balance': f'${bal:,.2f}',
                     'Perdida %': f'{(state["daily_loss_usd"]/bal*100):.2f}%'}))
        if state['consecutive_losses'] >= CONSEC_PAUSE:
            state['risk_pct_current'] = max(0.25, state['risk_pct_current'] / 2)
            if state['consecutive_losses'] != prev_consec:
                asyncio.create_task(_send_critical_alert(
                    'RIESGO REDUCIDO',
                    f'{state["consecutive_losses"]} perdidas consecutivas.',
                    {'Perdidas consecutivas': state['consecutive_losses'],
                     'Nuevo risk %': f'{state["risk_pct_current"]:.2f}%'}))
    else:
        state['consecutive_losses'] = 0
        if state['risk_pct_current'] < RISK_PCT: state['risk_pct_current'] = RISK_PCT


async def execute_signal(decision, pair=None):
    if state['trading_paused'] or decision['action'] == 'HOLD': return
    if decision['confidence'] < MIN_CONFIDENCE: return

    if pair is None:
        pair = decision.get('pair', 'EUR_USD')

    pair_cfg = get_pair_config(pair)
    display = pair_cfg['display']

    today_str = now_et().strftime('%Y-%m-%d')
    today_hour = now_et().hour

    p_state = pair_state.setdefault(pair, _init_pair_state())
    today_trades_pair = p_state.get('today_trades', [])
    today_date = p_state.get('today_trades_date')

    if today_date != today_str:
        today_trades_pair = []
        p_state['today_trades'] = today_trades_pair
        p_state['today_trades_date'] = today_str

    if len(today_trades_pair) >= MAX_TRADES_PER_DAY:
        log.info(f'[v2.6][{display}] Dia {today_str}: ya hay {len(today_trades_pair)} trades. Skip.')
        return

    if today_trades_pair:
        current_kz = decision.get('killzone', '')
        prev_kzs = [t.get('killzone', '') for t in today_trades_pair]
        if current_kz in prev_kzs:
            log.info(f'[v2.6][{display}] Killzone {current_kz} ya usada. Skip.')
            return
        last_trade = today_trades_pair[-1]
        if today_hour - last_trade.get('hour', 0) < MIN_HOURS_BETWEEN_TRADES:
            log.info(f'[v2.6][{display}] Solo {today_hour - last_trade.get("hour", 0)}h desde ultimo. Skip.')
            return

    sl = decision.get('sl', 0); tp = decision.get('tp1', 0)
    sz = max(1000, int(decision.get('pos_size', 1000)))
    if today_trades_pair:
        sz = int(sz * SECOND_TRADE_RISK_MULT)
        log.info(f'[v2.6][{display}] 2do trade - risk x{SECOND_TRADE_RISK_MULT} - sz={sz}')

    if sl <= 0 or tp <= 0: return
    try:
        result = await place_order(sz, sl, tp, decision['action'], pair=pair)
        fill = result.get('orderFillTransaction', {})
        trade_id = fill.get('tradeOpened', {}).get('tradeID', '')
        if trade_id:
            today_trades_pair.append({
                'trade_id': trade_id,
                'killzone': decision.get('killzone', ''),
                'hour': now_et().hour,
                'action': decision['action'],
                'pair': pair,
            })
            p_state['today_trades'] = today_trades_pair

            if pair == 'EUR_USD':
                state.setdefault('today_trades', {'date': today_str, 'trades': []})
                state['today_trades']['trades'] = today_trades_pair

            state['active_trades_meta'][trade_id] = {
                'pair': pair,
                'open_time': now_utc().isoformat(),
                'action': decision['action'],
                'entry_price': float(fill.get('price', 0)),
                'tp1': tp, 'tp2': decision.get('tp2', 0),
                'sl_original': sl, 'sl_current': sl,
                'partial_closed': False, 'sl_breakeven': False,
            }
            trade_record = {
                'trade_id': trade_id, 'date': now_et().strftime('%Y-%m-%d'),
                'time': now_et().strftime('%H:%M ET'),
                'day_of_week': now_et().strftime('%A'),
                'month': now_et().strftime('%B %Y'),
                'pair': pair,                          # v2.6.0-beta-fixed3
                'pair_id': pair,                       # v2.6.0-beta-fixed3
                'pair_display': pair_cfg.get('display', pair),
                'action': decision['action'],
                'entry_price': float(fill.get('price', 0)),
                'sl': sl, 'tp1': tp, 'tp2': decision.get('tp2', 0),
                'rr1': decision.get('rr1', 0), 'rr2': decision.get('rr2', 0),
                'pos_size': sz, 'score': decision.get('score', 0),
                'confidence': decision.get('confidence', 0),
                'killzone': decision.get('killzone', ''),
                'htf_bias': decision.get('htf_bias', ''),
                'cognitive_multiplier': decision.get('cognitive_multiplier'),
                'narrative': decision.get('narrative', ''),
                'narrative_quality': decision.get('narrative_quality'),
                'ceo_reason': decision.get('reason', ''),
                'outcome': 'OPEN', 'result_usd': 0,
                'close_time': '', 'gestion': '',
                'sl_moved_be': False, 'partial_closed': False, 'ceo_obs': '',
            }
            state['live_trades'].append(trade_record)
            storage_append_jsonl('trades/trade_journal.jsonl', trade_record)
            storage_write_json('legacy/live_trades.json', state['live_trades'][-500:])
            subj, html = build_trade_open_email(trade_record)
            asyncio.create_task(send_email(subj, html))
        log.info(f'[EXEC] {decision["action"]} id:{trade_id}')
    except Exception as e:
        log.error(f'[EXEC] {e}')


_prev_trades = {}

async def monitor_trades():
    global _prev_trades
    et = now_et()
    fuera  = et.hour >= SESSION_END_ET or et.hour < SESSION_START_ET
    finde  = et.weekday() in (5, 6)
    vierne = et.weekday() == 4 and et.hour >= FRIDAY_CLOSE_ET
    try:
        trades = await get_open_trades()
        state['open_trades'] = trades
    except Exception: return
    cur = {t['id']: t for t in trades}

    for tid, trade in _prev_trades.items():
        if tid not in cur:
            pnl = float(trade.get('unrealizedPL', 0))
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
            if par:   gestion.append('Parcial 50% cerrado en TP1')
            if sl_be: gestion.append('SL movido a Break-Even tras BOS')
            if outcome == 'SL':   gestion.append('Stop Loss hit')
            elif outcome == 'BE': gestion.append('Cerrado en BE - capital protegido')
            elif outcome in ('TP', 'TP2'): gestion.append(f'{outcome} alcanzado en target')
            ceo_obs = (f'SL hit. P&L: ${pnl:.0f}.' if outcome == 'SL' else
                       f'TP2 alcanzado. P&L: ${pnl:.0f}.' if outcome == 'TP2' else
                       f'BE. Capital protegido. P&L: ${pnl:.0f}.' if outcome == 'BE' else
                       f'TP alcanzado. P&L: ${pnl:.0f}.')
            trade_record = None
            for lt in reversed(state['live_trades']):
                if lt.get('trade_id') == tid:
                    lt.update({'outcome': outcome, 'result_usd': round(pnl, 2),
                               'close_time': now_et().strftime('%H:%M ET'),
                               'gestion': ' | '.join(gestion), 'sl_moved_be': sl_be,
                               'partial_closed': par, 'ceo_obs': ceo_obs})
                    trade_record = lt; break
            for h in reversed(state['history']):
                if h.get('trade_id') == tid:
                    h['outcome'] = outcome; h['result_usd'] = round(pnl, 2); break
            update_memory({'outcome': outcome, 'result': round(pnl, 2),
                          'killzone': meta.get('action', ''), 'sweep_quality': ''})
            adjust_weights(bt_state.get('log', []) + [{'outcome': outcome, 'failure_factors': []}])
            storage_write_json('legacy/live_trades.json', state['live_trades'][-500:])
            storage_write_json('legacy/history.json', state['history'][-2000:])
            storage_append_jsonl('trades/trade_journal.jsonl', {
                'trade_id': tid, 'closed_at': now_utc().isoformat(),
                'outcome': outcome, 'pnl_usd': round(pnl, 2),
                'partial_closed': par, 'sl_moved_be': sl_be,
                'gestion': ' | '.join(gestion)})
            log.info(f'[MONITOR] {tid} cerrado: {outcome} P&L:${pnl:.2f}')
            if trade_record:
                subj, html = build_trade_close_email(trade_record, outcome, pnl, gestion)
                asyncio.create_task(send_email(subj, html))
    _prev_trades = cur

    if (fuera or finde or vierne) and trades:
        for t in trades:
            try:
                await close_trade(t['id'])
                log.info(f'[MONITOR] Cerrado fuera sesion: {t["id"]}')
            except Exception as e: log.error(f'[MONITOR] {e}')
        return

    for trade in trades:
        tid = str(trade['id']); units = int(trade.get('currentUnits', 0))
        action = 'BUY' if units > 0 else 'SELL'
        meta = state['active_trades_meta'].get(tid, {})
        tp1 = meta.get('tp1', 0)
        entry = meta.get('entry_price', float(trade.get('price', 0)))
        if meta.get('partial_closed') and not meta.get('sl_breakeven'):
            try:
                h1_ = await get_candles('H1', 20); atr_ = compute_atr(h1_)
                b, bq, _ = detect_bos(h1_, action, atr_)
                if b and bq in ('strong', 'medium'):
                    await modify_sl(tid, entry)
                    meta['sl_breakeven'] = True
                    state['active_trades_meta'][tid] = meta
                    log.info(f'[MONITOR] BE activado {tid}')
            except Exception as e: log.error(f'[MONITOR] BE error: {e}')
        if tp1 and not meta.get('partial_closed'):
            try:
                cp = await get_price()
                hit = (action == 'BUY' and cp >= tp1) or (action == 'SELL' and cp <= tp1)
                if hit:
                    partial = abs(units) // 2
                    if partial >= 1:
                        await close_trade(tid, partial=partial)
                        meta['partial_closed'] = True
                        state['active_trades_meta'][tid] = meta
                        log.info(f'[MONITOR] Parcial 50% {tid}')
            except Exception as e: log.error(f'[MONITOR] Parcial error: {e}')
# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 14: BACKTESTING
# ═══════════════════════════════════════════════════════════════════════════════

def generate_post_mortem_local(td):
    outcome = td.get('outcome', ''); action = td.get('action', ''); score = td.get('score', 0)
    pnl = td.get('result', 0); disp = td.get('displacement_strength', '')
    ind = td.get('inducement_quality', ''); bos = td.get('bos_quality', '')
    htf = td.get('htf_bias', ''); adr = td.get('adr_pct', 0); news = td.get('news_blocked', False)
    analysis = ''; rec = ''; fail_factors = []
    if outcome in ('TP', 'TP2'):
        analysis = f'Trade ganador. {action} score {score}/100.'
        rec = 'Setup valido.'
    elif outcome == 'BE':
        analysis = f'BE. {action} score {score}/100.'; rec = 'Revisar TP2.'
    elif outcome == 'SL':
        analysis = f'SL hit. {action} score {score}/100. P&L: ${pnl:.0f}. '
        if news: analysis += 'Afectado por noticia.'; fail_factors.append('news_filter')
        elif disp in ('weak', 'none'): fail_factors.append('displacement')
        elif ind in ('weak', 'none'): fail_factors.append('inducement')
        elif bos == 'weak': fail_factors.append('bos_quality')
        elif adr < ADR_MIN: fail_factors.append('adr_remaining')
        else: fail_factors.append('market_noise')
        rec = 'Loss aceptable.'
    return analysis, rec, fail_factors


def simulate_trade(candles, entry_idx, action, sl, tp1, tp2, balance, atr, risk_multiplier=1.0,
                   pip_value=0.0001, spread_pips=1.5, slippage_pips=0.3):
    entry_p = float(candles[entry_idx]['mid']['c'])
    # v2.6-fixed7: spread/slip derivados de pips * pip_value (era hardcoded 0.00015/0.00003)
    # EUR_USD: 1.5*0.0001=0.00015, 0.3*0.0001=0.00003 (identico al original)
    spread = spread_pips * pip_value; slip = slippage_pips * pip_value
    fill = entry_p + (spread + slip) if action == 'BUY' else entry_p - (spread + slip)
    sl_dist = abs(fill - sl)
    if sl_dist <= 0: return None
    effective_risk_pct = RISK_PCT * risk_multiplier
    risk_usd = balance * (effective_risk_pct / 100)
    units = max(1000, int(risk_usd / sl_dist))
    sl_cur = sl; units_cur = units; sl_be = False; par = False; pnl_par = 0.0
    tp1_vela = None; be_vela = None; gestion = []; MAX_V = 25
    for v, fc in enumerate(candles[entry_idx+1:entry_idx+MAX_V+1], 1):
        fh = float(fc['mid']['h']); fl = float(fc['mid']['l'])
        if not par:
            if (action == 'BUY' and fh >= tp1) or (action == 'SELL' and fl <= tp1):
                tp1_vela = v; half = units_cur // 2; pnl_par = half * abs(tp1 - fill)
                units_cur -= half; par = True
                gestion.append(f'V{v}: Parcial 50% @ {tp1:.5f}')
        if par and not sl_be and tp1_vela and v >= tp1_vela + 2:
            wi = candles[entry_idx+v-2:entry_idx+v+1]
            if len(wi) >= 3:
                hs = [float(c['mid']['h']) for c in wi]; ls = [float(c['mid']['l']) for c in wi]
                bos_ok = ((action == 'BUY' and hs[-1] > hs[0] + atr * 0.3) or
                          (action == 'SELL' and ls[-1] < ls[0] - atr * 0.3))
                if bos_ok:
                    sl_cur = fill; sl_be = True; be_vela = v
                    gestion.append(f'V{v}: SL->BE')
        sl_hit = (action == 'BUY' and fl <= sl_cur) or (action == 'SELL' and fh >= sl_cur)
        if sl_hit:
            pnl_rest = 0.0 if sl_be else -(units_cur * sl_dist)
            outcome = 'BE' if sl_be else 'SL'
            gestion.append(f'V{v}: SL hit')
            total = pnl_par + pnl_rest
            return _sim_r(total, outcome, units, fill, sl_dist, tp2, sl_be, be_vela, par, pnl_par, tp1_vela, gestion, v)
        if (action == 'BUY' and fh >= tp2) or (action == 'SELL' and fl <= tp2):
            pnl_rest = units_cur * abs(tp2 - fill)
            total = pnl_par + pnl_rest
            gestion.append(f'V{v}: TP2')
            return _sim_r(total, 'TP2', units, fill, sl_dist, tp2, sl_be, be_vela, par, pnl_par, tp1_vela, gestion, v)
    last_p = float(candles[min(entry_idx+MAX_V, len(candles)-1)]['mid']['c'])
    pnl_rest = units_cur * (last_p - fill) if action == 'BUY' else units_cur * (fill - last_p)
    total = pnl_par + pnl_rest
    outcome = 'TP' if ((action == 'BUY' and last_p > fill) or (action == 'SELL' and last_p < fill)) else 'TIMEOUT'
    return _sim_r(total, outcome, units, fill, sl_dist, tp2, sl_be, be_vela, par, pnl_par, tp1_vela, gestion, MAX_V)


def _sim_r(total, outcome, units, fill, sl_dist, tp2, sl_be, be_vela, par, pnl_par, tp1_vela, gestion, dur):
    return {'result': round(total,2), 'outcome': outcome, 'units': units, 'fill': round(fill,5),
            'rr_real': round(abs(tp2-fill)/sl_dist,2) if sl_dist > 0 else 0,
            'sl_moved_be': sl_be, 'be_vela': be_vela, 'be_reason': 'BOS post-TP1' if sl_be else '',
            'partial_closed': par, 'pnl_parcial': round(pnl_par,2), 'tp1_vela': tp1_vela,
            'gestion': ' | '.join(gestion) if gestion else 'Sin eventos', 'velas_duration': dur}


# ═══════════════════════════════════════════════════════════════════════════════
# 🟢 run_backtesting_pair — REESCRITA en v2.6.0-beta-fixed
# ═══════════════════════════════════════════════════════════════════════════════
# Filosofia: reutilizar run_ict_pipeline() (que ya funciona en vivo)
# en vez de duplicar la logica de analisis con funciones inexistentes.
#
# Beneficios:
# - Backtest usa EXACTAMENTE la misma logica que el trading en vivo
# - Imposible que el backtest diverja del sistema real
# - Solo usa funciones que SI existen en main.py
# - Mantiene TODOS los filtros (caution days, killzones, 2 trades/dia)
# ═══════════════════════════════════════════════════════════════════════════════

async def run_backtesting_pair(pair):
    """
    v2.6.0-beta-fixed: Backtest para un par especifico.
    Reutiliza run_ict_pipeline() para no duplicar logica de analisis.
    """
    pair_cfg = get_pair_config(pair)
    display = pair_cfg['display']

    log.info(f'[BT][{display}] BACKTESTING INICIANDO (usando run_ict_pipeline)')
    all_trades = []
    balance = state.get('balance', 110000.0)
    ET = ZoneInfo('America/New_York')

    live_news = []
    try:
        live_news = await fetch_ff_calendar()
    except Exception:
        pass
    all_news = HIGH_IMPACT_EVENTS + live_news

    try:
        # CARGAR VELAS HISTORICAS DEL PAR
        h1 = await get_candles_to('H1', 500, pair=pair)
        for _ in range(1, 17):
            if len(h1) >= 8500:
                break
            oldest = h1[0].get('time', '')
            if not oldest:
                break
            batch = await get_candles_to('H1', 500, to_dt=oldest, pair=pair)
            if not batch or len(batch) < 2:
                break
            h1 = batch[:-1] + h1
            await asyncio.sleep(0.3)
        log.info(f'[BT][{display}] {len(h1)} velas H1 totales')

        # D1 y H4 para contexto HTF
        d1_all = []
        try:
            d1_all = await get_candles_to('D', 300, pair=pair)
        except Exception:
            pass

        h4_all = []
        try:
            h4_all = await get_candles_to('H4', 500, pair=pair)
        except Exception:
            pass

        log.info(f'[BT][{display}] H4:{len(h4_all)} D1:{len(d1_all)}')

        cnt = defaultdict(int)
        day_trades = {}
        last_idx = -999

        min_score_pair = pair_cfg['min_score']
        min_score_wed_pair = pair_cfg['min_score_wed']
        risk_pct_pair = pair_cfg['risk_pct']

        for i in range(50, len(h1) - 26):
            c_time = h1[i].get('time', '')

            try:
                c_price = float(h1[i]['mid']['c'])
            except (KeyError, ValueError, TypeError):
                continue

            try:
                cdt = datetime.fromisoformat(c_time.replace('Z', '+00:00')).astimezone(ET)
                c_h = cdt.hour
                c_dow = cdt.weekday()
                c_day = cdt.strftime('%Y-%m-%d')
            except Exception:
                cnt['parse_error'] += 1
                continue

            if c_dow in (5, 6):
                cnt['weekend'] += 1
                continue

            if not (SESSION_START_ET <= c_h < SESSION_END_ET):
                cnt['sesion'] += 1
                continue

            kill = get_killzone(c_h)
            if not kill:
                cnt['sesion'] += 1
                continue

            nb, _ = is_news_blocked(cdt, all_news)
            if nb:
                cnt['news'] += 1
                continue

            today_trades = day_trades.get(c_day, [])
            if len(today_trades) >= MAX_TRADES_PER_DAY:
                cnt['cooldown'] += 1
                continue
            if today_trades:
                prev_kzs = [t['killzone'] for t in today_trades]
                if kill in prev_kzs:
                    cnt['cooldown'] += 1
                    continue
                last_trade = today_trades[-1]
                if c_h - last_trade['hour'] < MIN_HOURS_BETWEEN_TRADES:
                    cnt['cooldown'] += 1
                    continue

            if i - last_idx < 4:   # v2.6.0-beta-fixed12: 5->4 velas (ajuste fino)
                cnt['cooldown'] += 1
                continue

            # CONSTRUIR VENTANAS DE CONTEXTO
            h1_w = h1[max(0, i - 50):i + 1]

            target_t = c_time
            h4_w = [c for c in h4_all if c.get('time', '') <= target_t][-30:]

            target_d = c_time[:10]
            d1_w = [c for c in d1_all if c.get('time', '')[:10] < target_d][-10:]

            # AQUI ESTA LA MAGIA: usamos el MISMO pipeline que el sistema en vivo
            ict = run_ict_pipeline(
                h1_w, h4_w, d1_w, c_price, balance, risk_pct_pair,
                hour=c_h, news_events=all_news,
                pip_value=pair_cfg.get('pip_value', 0.0001)   # v2.6-fixed7
            )

            sweep = ict.get('sweep', {})
            if not sweep.get('detected'):
                cnt['no_sweep'] += 1
                continue

            score = ict.get('score', {})
            action = score.get('action')
            if not action:
                cnt['no_action'] += 1
                continue

            # FILTROS POR PAR (ATR/ADR segun config)
            atr_p = ict.get('atr_pips', 0)
            adr_pct = ict.get('adr_pct', 0)
            if atr_p < pair_cfg['atr_min_pips'] or atr_p > pair_cfg['atr_max_pips']:
                cnt['atr'] += 1
                continue
            if adr_pct < pair_cfg['adr_min']:
                cnt['adr'] += 1
                continue

            # SCORE MINIMO (config por par + defensive bonus)
            min_sc = min_score_wed_pair if c_dow in (2, 3) else min_score_pair
            if state.get('defensive_mode'):
                min_sc += 10
            if score.get('total', 0) < min_sc:
                cnt['score'] += 1
                continue

            # ═══ FILTROS ESPECIFICOS DEL PAR (v2.6.0-beta-fixed3) ═══
            # Basados en backtest historico GBP/USD (24 trades, +Tuesday era WR 25%)
            pair_extra_caution_days = pair_cfg.get('extra_caution_days', [])
            pair_blocked_regimes = pair_cfg.get('block_regimes_always', [])

            # Bloquear regimes especificos siempre (no solo en caution days)
            if pair_blocked_regimes:
                regime_t = ict.get('regime', {}).get('type', '')
                if regime_t in pair_blocked_regimes:
                    cnt['pair_blocked_regime'] += 1
                    continue

            # v2.6.0-beta-fixed12: solo operar en killzones permitidas (si se define)
            # AUD/CAD solo en LONDON_OPEN (NY pierde dinero historicamente)
            pair_allowed_kz = pair_cfg.get('allowed_killzones', [])
            if pair_allowed_kz and kill not in pair_allowed_kz:
                cnt['pair_blocked_killzone'] += 1
                continue

            # Days of caution adicionales por par
            day_name_curr = cdt.strftime('%A')
            is_pair_caution = day_name_curr in pair_extra_caution_days

            # DAYS OF CAUTION FILTER (lunes/viernes globales O caution day del par)
            risk_mult = 1.0
            is_caution = c_dow in (0, 4) or is_pair_caution
            if is_caution:
                if score.get('total', 0) < CAUTION_MIN_SCORE:
                    cnt['caution_score'] += 1
                    continue
                if ict.get('htf_strength', 0) < CAUTION_MIN_HTF_STRENGTH:
                    cnt['caution_htf'] += 1
                    continue
                if sweep.get('quality') == 'low':
                    cnt['caution_sweep'] += 1
                    continue
                if ict.get('displacement', {}).get('strength') in ('none', 'weak'):
                    cnt['caution_disp'] += 1
                    continue
                if ict.get('inducement', {}).get('quality') in ('none', 'weak'):
                    cnt['caution_ind'] += 1
                    continue
                if ict.get('regime', {}).get('type') in CAUTION_BLOCKED_REGIMES:
                    cnt['caution_regime'] += 1
                    continue
                if ict.get('anomalies', {}).get('severity') in CAUTION_BLOCKED_ANOMALIES:
                    cnt['caution_anomaly'] += 1
                    continue
                risk_mult = CAUTION_RISK_MULTIPLIER

            # 2DO TRADE DEL DIA: REDUCE RISK
            is_second_trade = len(today_trades) >= 1
            if is_second_trade:
                risk_mult *= SECOND_TRADE_RISK_MULT

            # NIVELES
            levels = ict.get('levels')
            if not levels:
                cnt['no_levels'] += 1
                continue

            sl = levels.get('sl', 0)
            tp1 = levels.get('tp1', 0)
            tp2 = levels.get('tp2', 0)
            if sl <= 0 or tp1 <= 0 or tp2 <= 0:
                cnt['no_levels'] += 1
                continue

            # SIMULAR TRADE
            atr_for_sim = ict.get('atr', 0.0010)
            sim = simulate_trade(
                h1, i, action, sl, tp1, tp2, balance, atr_for_sim,
                risk_multiplier=risk_mult,
                pip_value=pair_cfg.get('pip_value', 0.0001),       # v2.6-fixed7
                spread_pips=pair_cfg.get('spread_pips', 1.5),
                slippage_pips=pair_cfg.get('slippage_pips', 0.3)
            )
            if not sim:
                continue

            day_trades.setdefault(c_day, []).append({
                'idx': i, 'killzone': kill, 'hour': c_h,
                'is_second': is_second_trade
            })
            last_idx = i

            all_trades.append({
                'pair': display,
                'pair_id': pair,
                'date': c_day,
                'session': f'{c_h:02d}:00 ET',
                'killzone': kill,
                'action': action,
                'entry_price': round(c_price, 5),
                'fill_price': round(sim['fill'], 5),
                'sl': round(sl, 5),
                'tp1': round(tp1, 5),
                'tp2': round(tp2, 5),
                'rr_tp1': round(levels.get('rr1', 0), 2),
                'rr_tp2_real': round(sim['rr_real'], 2),
                'outcome': sim['outcome'],
                'result': sim['result'],
                'confidence': score.get('confidence', 0),
                'score': round(score.get('total', 0), 1),
                'sweep_level': round(sweep.get('level', 0), 5),
                'sweep_type': sweep.get('level_type', ''),
                'sweep_quality': sweep.get('quality', ''),
                'wick_pct': round(sweep.get('wick_pct', 0), 2),
                'htf_bias': ict.get('htf_bias', ''),
                'htf_strength': round(ict.get('htf_strength', 0), 2),
                'bos_quality': ict.get('structure', {}).get('bos_quality', ''),
                'displacement_strength': ict.get('displacement', {}).get('strength', ''),
                'inducement_quality': ict.get('inducement', {}).get('quality', ''),
                'liq_obj_level': ict.get('liq_target', {}).get('level', 0),
                'liq_obj_type': ict.get('liq_target', {}).get('type', ''),
                'adr_pct': round(adr_pct, 2),
                'atr_pips': round(atr_p, 1),
                'velas_duration': sim['velas_duration'],
                'sl_moved_be': sim['sl_moved_be'],
                'be_vela': sim.get('be_vela'),
                'be_reason': sim.get('be_reason', ''),
                'partial_closed': sim['partial_closed'],
                'pnl_parcial': sim['pnl_parcial'],
                'tp1_vela': sim.get('tp1_vela'),
                'gestion': sim['gestion'],
                'news_blocked': nb,
                'score_factors': ' | '.join(
                    f"{k}:{v['pts']:.0f}"
                    for k, v in score.get('factors', {}).items()
                ),
                'confirmaciones': (
                    f"Sweep {sweep.get('quality', '')} | "
                    f"HTF {ict.get('htf_bias', '')} | "
                    f"BOS {ict.get('structure', {}).get('bos_quality', '')}"
                ),
                'regime_type': ict.get('regime', {}).get('type', ''),
                'anomaly_severity': ict.get('anomalies', {}).get('severity', ''),
                'caution_mode': is_caution,
                'risk_multiplier': risk_mult,
                'day_of_week': cdt.strftime('%A'),
                'is_second_trade': is_second_trade,
                'killzone_type': kill,
            })

        log.info(f'[BT][{display}] {len(all_trades)} trades generados')
        log.info(
            f'[BT][{display}][FILTROS] sesion={cnt.get("sesion",0)} '
            f'news={cnt.get("news",0)} no_sweep={cnt.get("no_sweep",0)} '
            f'score={cnt.get("score",0)} atr={cnt.get("atr",0)} '
            f'adr={cnt.get("adr",0)} cooldown={cnt.get("cooldown",0)}'
        )
        log.info(
            f'[BT][{display}][CAUTION] score={cnt.get("caution_score",0)} '
            f'htf={cnt.get("caution_htf",0)} sweep={cnt.get("caution_sweep",0)} '
            f'disp={cnt.get("caution_disp",0)} ind={cnt.get("caution_ind",0)} '
            f'regime={cnt.get("caution_regime",0)} '
            f'anomaly={cnt.get("caution_anomaly",0)}'
        )

    except Exception as e:
        log.error(f'[BT][{display}] Error: {e}', exc_info=True)

    pair_summary = {'pair': display, 'pair_id': pair, 'total_trades': 0}
    if all_trades:
        # v2.6.0-beta-fixed3: BUG FIX - clasificacion correcta por outcome
        # Antes: wins=result>0, BEs contados por outcome → algunos double-counted
        # Ahora: clasificacion mutuamente exclusiva por outcome real
        wins_pure = [t for t in all_trades if t['outcome'] in ('TP', 'TP2') and t['result'] > 0]
        losses = [t for t in all_trades if t['outcome'] == 'SL' and t['result'] < 0]
        bes = [t for t in all_trades if t['outcome'] == 'BE']
        timeouts_pos = [t for t in all_trades if t['outcome'] == 'TIMEOUT' and t['result'] > 0]
        timeouts_neg = [t for t in all_trades if t['outcome'] == 'TIMEOUT' and t['result'] <= 0]
        # WR considera "ganadores" a TP/TP2 + BEs con parcial cobrado (siguen sumando $)
        bes_positive = [t for t in bes if t['result'] > 0]
        bes_zero = [t for t in bes if t['result'] <= 0]
        # Wins efectivos para WR: cualquier trade con result > 0
        total_winners = len(wins_pure) + len(bes_positive) + len(timeouts_pos)
        total_losers = len(losses) + len(timeouts_neg)
        pnl = sum(t['result'] for t in all_trades)
        wr = total_winners / len(all_trades) if all_trades else 0
        gw = sum(t['result'] for t in all_trades if t['result'] > 0)
        gl = abs(sum(t['result'] for t in all_trades if t['result'] < 0))
        pf = round(gw / gl, 2) if gl > 0 else 0

        equity = balance
        peak = equity
        max_dd = 0.0
        for t in sorted(all_trades, key=lambda x: x['date']):
            equity += t['result']
            if equity > peak:
                peak = equity
            dd = (peak - equity) / peak if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd

        pair_summary = {
            'pair': display,
            'pair_id': pair,
            'total_trades': len(all_trades),
            'wins': total_winners,
            'losses': total_losers,
            'breakeven': len(bes_zero),
            'win_rate': round(wr, 4),
            'total_pnl': round(pnl, 2),
            'profit_factor': pf,
            'max_drawdown': round(max_dd * 100, 2),
            'trades_per_week': round(len(all_trades) / 52, 1),
            'caution_day_trades': len([t for t in all_trades if t.get('caution_mode')]),
            'caution_day_vetos': sum([
                cnt.get('caution_score', 0),
                cnt.get('caution_htf', 0),
                cnt.get('caution_sweep', 0),
                cnt.get('caution_disp', 0),
                cnt.get('caution_ind', 0),
                cnt.get('caution_regime', 0),
                cnt.get('caution_anomaly', 0),
            ]),
            # v2.6.0-beta-fixed3: detalle ampliado de outcomes
            'breakdown': {
                'tp_pure':       len(wins_pure),
                'sl_pure':       len(losses),
                'be_positive':   len(bes_positive),
                'be_zero':       len(bes_zero),
                'timeout_pos':   len(timeouts_pos),
                'timeout_neg':   len(timeouts_neg),
            },
            'pair_specific_vetos': cnt.get('pair_blocked_regime', 0) + cnt.get('pair_blocked_killzone', 0),
        }

    return {'trades': all_trades, 'summary': pair_summary}


async def run_backtesting(pair=None):
    """
    v2.6.0-beta-fixed: Backtest multi-par.
    - pair=None: backtest de TODOS los pares activos
    - pair='EUR_USD' o 'GBP_USD': backtest individual
    """
    if bt_state['running']: return
    bt_state['running'] = True

    try:
        if pair:
            log.info(f'[BT] BACKTESTING individual: {pair}')
            result = await run_backtesting_pair(pair)
            bt_state['trades'] = result['trades']
            bt_state['summary'] = result['summary']
            bt_state['summary']['pairs_backtested'] = [pair]
        else:
            log.info('[BT] BACKTESTING multi-par - todos los pares activos')
            active = get_active_pairs()
            log.info(f'[BT] Pares a procesar: {active}')

            all_trades = []
            pair_summaries = {}

            for p in active:
                result = await run_backtesting_pair(p)
                all_trades.extend(result['trades'])
                pair_summaries[p] = result['summary']

            bt_state['trades'] = all_trades

            if all_trades:
                wins = [t for t in all_trades if t['result'] > 0]
                bes = [t for t in all_trades if t['outcome'] == 'BE']
                pnl = sum(t['result'] for t in all_trades)
                wr = len(wins) / len(all_trades)
                gw = sum(t['result'] for t in all_trades if t['result'] > 0)
                gl = abs(sum(t['result'] for t in all_trades if t['result'] < 0))
                pf = round(gw / gl, 2) if gl > 0 else 0

                balance = state.get('balance', 110000.0)
                equity = balance; peak = equity; max_dd = 0.0
                for t in sorted(all_trades, key=lambda x: x['date']):
                    equity += t['result']
                    if equity > peak: peak = equity
                    dd = (peak - equity) / peak if peak > 0 else 0
                    if dd > max_dd: max_dd = dd

                bt_state['summary'] = {
                    'multi_pair': True,
                    'pairs_backtested': active,
                    'total_trades': len(all_trades), 'wins': len(wins),
                    'losses': len(all_trades) - len(wins) - len(bes),
                    'breakeven': len(bes),
                    'win_rate': round(wr, 4), 'total_pnl': round(pnl, 2),
                    'profit_factor': pf, 'max_drawdown': round(max_dd*100, 2),
                    'trades_per_week': round(len(all_trades)/52, 1),
                    'pair_summaries': pair_summaries,
                }
                log.info(f'[BT] AGREGADO: {len(all_trades)} trades, PnL ${pnl:,.2f}, WR {wr*100:.1f}%, PF {pf}')
                for p, summ in pair_summaries.items():
                    log.info(f'[BT][{p}] Trades: {summ.get("total_trades",0)}, '
                             f'PnL: ${summ.get("total_pnl",0):,.2f}, '
                             f'WR: {summ.get("win_rate",0)*100:.1f}%, '
                             f'PF: {summ.get("profit_factor",0)}')
            else:
                bt_state['summary'] = {
                    'multi_pair': True,
                    'pairs_backtested': active,
                    'total_trades': 0,
                    'pair_summaries': pair_summaries,
                }

        bt_state['last_run'] = now_utc().isoformat()
        storage_write_json('backtests/last_run.json', {
            'summary': bt_state['summary'],
            'trades': bt_state['trades'][:200],
        })
    except Exception as e:
        log.error(f'[BT] Error: {e}', exc_info=True)
    finally:
        bt_state['running'] = False
        log.info('[BT] FINALIZADO')
# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 15: SCHEDULED REPORTS + HEALTH MONITORING + CLAUDE CONVERSATION
# ═══════════════════════════════════════════════════════════════════════════════

_last_alert_sent = {}


def _is_market_active():
    et = now_et()
    if et.weekday() == 5: return False
    if et.weekday() == 6 and et.hour < 17: return False
    if et.weekday() == 4 and et.hour >= FRIDAY_CLOSE_ET: return False
    return True


async def scheduled_pre_london_report():
    if not _is_market_active(): return
    try:
        await run_analysis(auto_execute=False)
        subj, html = build_pre_london_report()
        await send_email(subj, html)
        log.info('[REPORT] Pre-London 7AM enviado')
    except Exception as e:
        log.error(f'[REPORT] Pre-London error: {e}')


async def scheduled_ny_open_report():
    if not _is_market_active(): return
    try:
        await run_analysis(auto_execute=AUTO_EXECUTE)
        subj, html = build_ny_open_report()
        await send_email(subj, html)
        log.info('[REPORT] NY Open 9AM enviado')
    except Exception as e:
        log.error(f'[REPORT] NY Open error: {e}')


async def healthcheck_monitor():
    issues = []
    try: await get_account()
    except Exception as e: issues.append(f'OANDA no responde: {str(e)[:80]}')
    if cognitive_is_disabled():
        if _last_alert_sent.get('cognitive_down', 0) < time.time() - 7200:
            issues.append('Cognitive layer disabled por failure rate')
            _last_alert_sent['cognitive_down'] = time.time()
    if state.get('trading_paused'):
        if _last_alert_sent.get('trading_paused', 0) < time.time() - 7200:
            issues.append(f'Trading pausado: {state.get("pause_reason", "sin razon")}')
            _last_alert_sent['trading_paused'] = time.time()
    if issues:
        await _send_critical_alert(
            'ALERTAS DEL SISTEMA',
            'Se detectaron las siguientes anomalias:',
            {f'Issue {i+1}': issue for i, issue in enumerate(issues)})


async def drawdown_monitor():
    try:
        recent = state.get('history', [])[-50:]
        if len(recent) < 10: return
        pnl_total = sum(t.get('result_usd', 0) for t in recent if t.get('result_usd'))
        balance_initial = state.get('balance', 100000) - pnl_total
        if balance_initial <= 0: return
        dd_pct = (pnl_total / balance_initial) * 100 if pnl_total < 0 else 0
        if dd_pct <= -3.0:
            if _last_alert_sent.get('dd_3pct', 0) < time.time() - 86400:
                await _send_critical_alert(
                    'DRAWDOWN ALERT',
                    f'Drawdown del {abs(dd_pct):.2f}% detectado en ultimos 50 trades.',
                    {'Drawdown %': f'{dd_pct:.2f}%',
                     'P&L acumulado': f'${pnl_total:,.2f}',
                     'Trades en ventana': len(recent)})
                _last_alert_sent['dd_3pct'] = time.time()
    except Exception as e:
        log.error(f'[DD-MONITOR] {e}')


def compute_weekly_stats():
    now = now_et()
    one_week_ago = now - timedelta(days=7)
    history = state.get('history', [])
    week_trades = [
        t for t in history
        if t.get('outcome') and t.get('outcome') != ''
        and datetime.fromisoformat(t['timestamp'].replace('Z', '+00:00')) >= one_week_ago.astimezone(timezone.utc)
    ]
    if not week_trades: return None
    wins = [t for t in week_trades if t.get('result_usd', 0) > 0]
    losses = [t for t in week_trades if t.get('result_usd', 0) < 0]
    bes = [t for t in week_trades if t.get('outcome') == 'BE']
    total_pnl = sum(t.get('result_usd', 0) for t in week_trades)
    wr = len(wins) / len(week_trades) if week_trades else 0
    gw = sum(t.get('result_usd', 0) for t in wins)
    gl = abs(sum(t.get('result_usd', 0) for t in losses))
    pf = round(gw / gl, 2) if gl > 0 else 0
    by_day = defaultdict(list)
    for t in week_trades:
        by_day[t.get('day_of_week', 'Unknown')].append(t)
    by_killzone = defaultdict(list)
    for t in week_trades:
        by_killzone[t.get('killzone', 'Unknown')].append(t)
    by_outcome = defaultdict(int)
    for t in week_trades: by_outcome[t.get('outcome', 'Unknown')] += 1
    return {
        'period_start': one_week_ago.strftime('%Y-%m-%d'),
        'period_end': now.strftime('%Y-%m-%d'),
        'total_trades': len(week_trades),
        'wins': len(wins), 'losses': len(losses), 'breakeven': len(bes),
        'win_rate': round(wr, 4), 'total_pnl': round(total_pnl, 2),
        'profit_factor': pf,
        'avg_win': round(gw / max(1, len(wins)), 2),
        'avg_loss': round(gl / max(1, len(losses)), 2),
        'by_day': {day: {
            'trades': len(trades),
            'pnl': sum(t.get('result_usd', 0) for t in trades),
            'wr': round(sum(1 for t in trades if t.get('result_usd', 0) > 0) / len(trades), 4) if trades else 0
        } for day, trades in by_day.items()},
        'by_killzone': {kz: {
            'trades': len(trades),
            'pnl': sum(t.get('result_usd', 0) for t in trades),
            'wr': round(sum(1 for t in trades if t.get('result_usd', 0) > 0) / len(trades), 4) if trades else 0
        } for kz, trades in by_killzone.items()},
        'outcomes': dict(by_outcome),
        'cognitive_health': {
            'disabled': cognitive_is_disabled(),
            'recent_calls': len(_cognitive_health['calls']),
            'recent_failures': len(_cognitive_health['failures']),
        },
        'system_state': {
            'defensive_mode': state.get('defensive_mode', False),
            'consecutive_losses': state.get('consecutive_losses', 0),
            'risk_pct_current': state.get('risk_pct_current', 1.0),
            'edge_score': memory.get('edge_score', 100),
        }
    }


def build_weekly_stats_report():
    stats = compute_weekly_stats()
    if not stats:
        return 'TPDCM-IA · Sin actividad esta semana', _email_wrapper(
            'Reporte Semanal',
            '<div class="card"><div class="value">No hubo trades cerrados esta semana.</div></div>'
        )
    pnl_color = 'green' if stats['total_pnl'] > 0 else 'red' if stats['total_pnl'] < 0 else 'gold'
    body = f"""<div class="card"><div class="label">Resumen Semanal</div>
<div class="row"><span class="k">Periodo</span><span class="v">{stats['period_start']} a {stats['period_end']}</span></div>
<div class="row"><span class="k">Total trades</span><span class="v">{stats['total_trades']}</span></div>
<div class="row"><span class="k">Wins</span><span class="v green">{stats['wins']}</span></div>
<div class="row"><span class="k">Losses</span><span class="v red">{stats['losses']}</span></div>
<div class="row"><span class="k">Break-even</span><span class="v gold">{stats['breakeven']}</span></div>
<div class="row"><span class="k">Win Rate</span><span class="v">{stats['win_rate']*100:.1f}%</span></div>
<div class="row"><span class="k">Profit Factor</span><span class="v gold">{stats['profit_factor']}</span></div>
<div class="row"><span class="k">P&L Total</span><span class="v {pnl_color}"><strong>${stats['total_pnl']:+,.2f}</strong></span></div>
</div>"""
    if stats['by_day']:
        body += '<div class="card"><div class="label">Por Dia</div>'
        for day, data in stats['by_day'].items():
            color = 'green' if data['pnl'] > 0 else 'red' if data['pnl'] < 0 else 'gold'
            body += f'<div class="row"><span class="k">{day}</span><span class="v">{data["trades"]} trades · WR {data["wr"]*100:.0f}% · <span class="{color}">${data["pnl"]:+,.0f}</span></span></div>'
        body += '</div>'
    if stats['by_killzone']:
        body += '<div class="card"><div class="label">Por Killzone</div>'
        for kz, data in stats['by_killzone'].items():
            color = 'green' if data['pnl'] > 0 else 'red' if data['pnl'] < 0 else 'gold'
            body += f'<div class="row"><span class="k">{kz}</span><span class="v">{data["trades"]} trades · WR {data["wr"]*100:.0f}% · <span class="{color}">${data["pnl"]:+,.0f}</span></span></div>'
        body += '</div>'
    body += f"""<div class="card"><div class="label">Estado Sistema</div>
<div class="row"><span class="k">Edge Score</span><span class="v green">{stats['system_state']['edge_score']:.0f}/100</span></div>
<div class="row"><span class="k">Risk actual</span><span class="v">{stats['system_state']['risk_pct_current']:.2f}%</span></div>
<div class="row"><span class="k">Defensivo</span><span class="v {'red' if stats['system_state']['defensive_mode'] else 'green'}">{'ACTIVO' if stats['system_state']['defensive_mode'] else 'NO'}</span></div>
<div class="row"><span class="k">Perdidas consec</span><span class="v">{stats['system_state']['consecutive_losses']}</span></div>
<div class="row"><span class="k">Cognitive</span><span class="v {'red' if stats['cognitive_health']['disabled'] else 'green'}">{'DEGRADED' if stats['cognitive_health']['disabled'] else 'OK'}</span></div>
</div>"""
    subject = f'TPDCM-IA · Reporte Semanal · ${stats["total_pnl"]:+,.0f} · WR {stats["win_rate"]*100:.0f}%'
    return subject, _email_wrapper('Reporte Semanal', body)


async def scheduled_weekly_stats_report():
    try:
        subj, html = build_weekly_stats_report()
        await send_email(subj, html)
        log.info('[REPORT] Weekly stats enviado')
    except Exception as e:
        log.error(f'[REPORT] Weekly stats error: {e}')


# ═══════════════════════════════════════════════════════════════════════════════
# CLAUDE CONVERSATION (chat con Claude desde dashboard)
# ═══════════════════════════════════════════════════════════════════════════════

CHAT_SYSTEM_PROMPT = """Eres TPDCM-IA, un sistema de trading institucional EUR/USD + GBP/USD.

Tu rol cuando hablas con el operador (la duena del sistema):
- Eres su companera de analisis de mercado, no un asistente generico.
- Hablas en espanol, tono profesional pero cercano.
- Usas terminologia institucional (ICT/SMC: sweep, BOS, displacement, FVG, OB, killzone, HTF bias).
- Eres honesto: si algo no se sabe, lo dices.
- No haces promesas de rentabilidad. Hablas de probabilidades y edge.

Cuando te pregunten por el sistema:
- Tienes acceso al ultimo analisis (state['last_analysis']) y ultima decision (state['last_decision']).
- Puedes interpretar los datos tecnicos y explicarlos en lenguaje claro.
- Puedes sugerir ajustes pero NO los aplicas sin aprobacion explicita.

Cuando te pidan que "decidas" algo:
- NUNCA cambies parametros del sistema en vivo.
- Solo das tu analisis/recomendacion. El operador decide.

Limites importantes:
- 10% mensual sostenido NO es realista. 1-3% mensual con bajo DD es excelente.
- Siempre menciona el drawdown junto al return.
- Si el sistema esta en modo defensivo, explica por que.
"""


async def claude_conversation(message: str, history: list = None) -> dict:
    if not ANTHROPIC_API_KEY:
        return {'response': 'Claude API key no configurada.', 'error': True}

    history = history or []
    last_ict = state.get('last_analysis', {}).get('ict', {})
    last_dec = state.get('last_decision', {}) or {}

    context_snapshot = {
        'timestamp_et': now_et().isoformat(),
        'day_of_week': now_et().strftime('%A'),
        'is_caution_day': is_caution_day(),
        'is_session': is_session(),
        'balance': state.get('balance', 0),
        'risk_pct_current': state.get('risk_pct_current', RISK_PCT),
        'defensive_mode': state.get('defensive_mode', False),
        'defensive_reason': state.get('defensive_reason', ''),
        'consecutive_losses': state.get('consecutive_losses', 0),
        'trading_paused': state.get('trading_paused', False),
        'pause_reason': state.get('pause_reason', ''),
        'edge_score': memory.get('edge_score', 100),
        'cognitive_disabled': cognitive_is_disabled(),
        'active_pairs': get_active_pairs(),
        'last_analysis': {
            'price': state.get('last_analysis', {}).get('price', 0),
            'sweep_detected': last_ict.get('sweep', {}).get('detected', False),
            'sweep_quality': last_ict.get('sweep', {}).get('quality', ''),
            'score': last_ict.get('score', {}).get('total', 0),
            'htf_bias': last_ict.get('htf_bias', ''),
            'htf_strength': last_ict.get('htf_strength', 0),
            'regime': last_ict.get('regime', {}).get('type', ''),
            'anomalies': last_ict.get('anomalies', {}).get('severity', ''),
            'atr_pips': last_ict.get('atr_pips', 0),
            'adr_pct': last_ict.get('adr_pct', 0),
        },
        'last_decision': {
            'action': last_dec.get('action', ''),
            'confidence': last_dec.get('confidence', 0),
            'source': last_dec.get('source', ''),
            'reason': last_dec.get('reason', ''),
        },
        'recent_trades_outcomes': [
            t.get('outcome', '') for t in state.get('history', [])[-5:]
            if t.get('outcome')
        ],
    }

    user_content = f"""CONTEXTO ACTUAL DEL SISTEMA (snapshot):
{json.dumps(context_snapshot, ensure_ascii=False, indent=2)}

PREGUNTA / MENSAJE DEL OPERADOR:
{message}"""

    messages = []
    for h in history[-8:]:
        role = h.get('role', '')
        content = h.get('content', '')
        if role in ('user', 'assistant') and content:
            messages.append({'role': role, 'content': content})
    messages.append({'role': 'user', 'content': user_content})

    try:
        async with httpx.AsyncClient(timeout=45) as client:
            r = await client.post('https://api.anthropic.com/v1/messages',
                headers={'Content-Type': 'application/json',
                         'x-api-key': ANTHROPIC_API_KEY,
                         'anthropic-version': '2023-06-01'},
                json={'model': SONNET_MODEL, 'max_tokens': 1500,
                      'system': CHAT_SYSTEM_PROMPT,
                      'messages': messages})
        if not r.is_success:
            return {'response': f'Error API: HTTP {r.status_code}', 'error': True}
        response_text = r.json()['content'][0]['text']
        record = {
            'timestamp': now_utc().isoformat(),
            'user_message': message,
            'assistant_response': response_text,
            'context_snapshot': context_snapshot,
        }
        storage_append_jsonl('claude_conversations/chat.jsonl', record)
        return {'response': response_text, 'error': False,
                'context_used': context_snapshot}
    except Exception as e:
        return {'response': f'Excepcion: {str(e)[:200]}', 'error': True}
# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 16: ENDPOINTS API
# ═══════════════════════════════════════════════════════════════════════════════

class ChatMessageIn(BaseModel):
    message: str = Field(min_length=1, max_length=2000)
    history: List[dict] = Field(default_factory=list, max_length=20)


@app.get('/', response_class=HTMLResponse)
async def serve_dashboard():
    # v2.6.0-beta-fixed4: sirve el dashboard HTML directamente desde Railway.
    # Si el archivo no existe, devuelve un mensaje informativo.
    index_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'index.html')
    try:
        with open(index_path, 'r', encoding='utf-8') as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        fallback = """<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>TPDCM-IA</title>
<style>body{background:#070a0d;color:#c4d8cc;font-family:monospace;padding:40px;line-height:1.6}
h1{color:#00e87a}a{color:#4a9eff}</style></head>
<body><h1>TPDCM-IA Backend Running</h1>
<p>El backend funciona, pero el dashboard (index.html) no se encontro en el repo.</p>
<p>Endpoints disponibles:</p>
<ul>
<li><a href="/api">/api</a> - Estado del sistema (JSON)</li>
<li><a href="/health">/health</a> - Health check</li>
<li><a href="/backtesting">/backtesting</a> - Resultados ultimo backtest</li>
<li><a href="/dashboard">/dashboard</a> - Dashboard JSON</li>
<li><a href="/pairs">/pairs</a> - Estado de pares</li>
</ul></body></html>"""
        return HTMLResponse(content=fallback)


@app.get('/api')
@app.get('/status')
async def api_status():
    # v2.6.0-beta-fixed4: el JSON de estado se mueve aqui (antes estaba en /)
    return {
        'ok': True, 'service': 'TPDCM-IA', 'version': '2.6.0-beta-fixed12',
        'now_et': now_et().isoformat(),
        'session_active': is_session(),
        'auto_execute': AUTO_EXECUTE,
        'active_pairs': get_active_pairs(),
        'dashboard_url': '/',
    }


@app.get('/health')
async def health():
    return {
        'ok': True,
        'oanda_configured': bool(OANDA_TOKEN and OANDA_ACCOUNT),
        'anthropic_configured': bool(ANTHROPIC_API_KEY),
        'resend_configured': bool(RESEND_API_KEY),
        'cognitive_disabled': cognitive_is_disabled(),
        'trading_paused': state.get('trading_paused', False),
        'defensive_mode': state.get('defensive_mode', False),
        'balance': state.get('balance', 0),
        'edge_score': memory.get('edge_score', 100),
        'active_pairs': get_active_pairs(),
        'auto_execute': AUTO_EXECUTE,          # v2.7: para badge MANUAL/AUTO del dashboard
        'session_active': is_session(),        # v2.7: para badge ONLINE/OFFLINE
        'server': 'online',
    }


@app.get('/dashboard')
async def dashboard(pair: Optional[str] = None):
    # v2.6.0-beta-fixed3: si se pasa pair, devuelve datos de ese par especifico
    # usando pair_state[pair]. Si no, mantiene comportamiento legacy (EUR/USD).
    if pair and pair in PAIRS:
        p_st = pair_state.get(pair, {})
        last_an = p_st.get('last_analysis')
        last_dc = p_st.get('last_decision')
    else:
        last_an = state.get('last_analysis')
        last_dc = state.get('last_decision')

    return {
        'state': {
            'balance': state.get('balance', 0),
            'risk_pct_current': state.get('risk_pct_current', 1.0),
            'defensive_mode': state.get('defensive_mode', False),
            'defensive_reason': state.get('defensive_reason', ''),
            'trading_paused': state.get('trading_paused', False),
            'pause_reason': state.get('pause_reason', ''),
            'consecutive_losses': state.get('consecutive_losses', 0),
            'daily_loss_usd': state.get('daily_loss_usd', 0),
            'last_update': state.get('last_update'),
            'session_active': is_session(),
            'is_caution_day': is_caution_day(),
            'edge_score': memory.get('edge_score', 100),
            'active_pairs': get_active_pairs(),
            'current_pair': pair if pair else 'EUR_USD',
        },
        'last_analysis': last_an,
        'last_decision': last_dc,
        'open_trades_count': len(state.get('open_trades', [])),
        'cognitive_disabled': cognitive_is_disabled(),
    }


@app.get('/prices')
async def prices_endpoint(pair: Optional[str] = None):
    try:
        p = await get_price(pair=pair)
        return {'ok': True, 'pair': pair or PAIR, 'price': round(p, 5),
                'timestamp': now_utc().isoformat()}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


@app.get('/candles')
async def candles_endpoint(granularity: str = 'H1', count: int = 100,
                            pair: Optional[str] = None):
    try:
        c = await get_candles(granularity, count, pair=pair)
        return {'ok': True, 'pair': pair or PAIR, 'granularity': granularity,
                'count': len(c), 'candles': c}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


@app.api_route('/trigger-analysis', methods=['GET', 'POST'])
async def trigger_analysis_endpoint():
    try:
        results = await run_analysis(auto_execute=AUTO_EXECUTE)
        return {'ok': True, 'results': {
            pair: {'action': r['action'] if r else None,
                   'confidence': r['confidence'] if r else 0,
                   'score': r['score'] if r else 0}
            for pair, r in (results or {}).items()
        }}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


@app.api_route('/all-setups', methods=['GET', 'POST'])
async def all_setups_endpoint(run: bool = False, cognitive: bool = True):
    """v2.7: devuelve las posibles entradas de TODOS los pares activos.
    Si run=true, ejecuta el analisis primero (bajo demanda).
    Si cognitive=true, llama a Claude SOLO para pares con score "cerca" del minimo.
    Siempre devuelve niveles teoricos + motivo del HOLD aunque no haya senal."""
    try:
        if run:
            await run_analysis(auto_execute=False)

        # Umbral para considerar un score "cerca" del minimo (llamar a Claude)
        COGNITIVE_GAP = 10  # si score >= (min_score - 10), Claude opina

        setups = []
        for pair in get_active_pairs():
            cfg = PAIR_CONFIG.get(pair, {})
            p_st = pair_state.get(pair, {})
            last_an = p_st.get('last_analysis') or {}
            last_dc = p_st.get('last_decision') or {}
            ict = last_an.get('ict') or {}
            sweep = ict.get('sweep', {})
            score = ict.get('score', {})
            levels = ict.get('levels') or {}

            score_total = score.get('total', 0)
            min_score = cfg.get('min_score', 58)
            action = last_dc.get('action', 'WAIT')
            tech_action = score.get('action')  # accion tecnica (aunque no ejecutable)
            kz = ict.get('killzone', '')
            allowed_kz = cfg.get('allowed_killzones', [])
            kz_ok = (not allowed_kz) or (kz in allowed_kz)
            has_sweep = sweep.get('detected', False)

            # Clasificar estado
            if not last_an:
                estado = 'SIN_DATOS'
            elif action in ('BUY', 'SELL'):
                estado = 'SENAL'
            elif not has_sweep:
                estado = 'SIN_SETUP'
            elif not kz_ok:
                estado = 'FUERA_KILLZONE'
            else:
                estado = 'ESPERAR'

            # v2.7: MOTIVO detallado del HOLD (por que no se toma)
            motivos = []
            if not has_sweep:
                motivos.append('Falta sweep institucional')
            if not kz_ok and allowed_kz:
                motivos.append(f'Fuera de killzone (solo {", ".join(allowed_kz)})')
            if has_sweep and score_total < min_score:
                motivos.append(f'Score {score_total:.0f} < minimo {min_score}')
            if has_sweep and not score.get('executable'):
                motivos.append('Setup no ejecutable (calidad insuficiente)')
            motivo_str = ' · '.join(motivos) if motivos else (last_dc.get('reason') or '--')

            # v2.7: niveles TEORICOS (de la posible entrada, aunque sea HOLD)
            # Usa los del decision si existe, sino los calculados en el pipeline
            sl_t = last_dc.get('sl', 0) or levels.get('sl', 0)
            tp1_t = last_dc.get('tp1', 0) or levels.get('tp1', 0)
            tp2_t = last_dc.get('tp2', 0) or levels.get('tp2', 0)
            rr1_t = last_dc.get('rr1', 0) or levels.get('rr_tp1', 0)
            rr2_t = last_dc.get('rr2', 0) or levels.get('rr_tp2', 0)

            # v2.7: sesgo direccional (barra bull/bear) desde indicadores actuales
            bias = compute_directional_bias(ict) if last_an else {
                'bull_pct': 50, 'bear_pct': 50, 'label': 'neutral',
                'strength_label': 'debil', 'regime_type': 'unknown'}

            # v2.7: narrativa de Claude SOLO si score esta "cerca" (ahorra API)
            narrativa = ''
            claude_called = False
            score_cerca = has_sweep and score_total >= (min_score - COGNITIVE_GAP)
            if cognitive and score_cerca and tech_action and not cognitive_is_disabled():
                try:
                    cog = await call_cognitive_layer(ict, state.get('history', [])[-10:])
                    if cog:
                        claude_called = True
                        narrativa = getattr(cog, 'narrative', '') or getattr(cog, 'reasoning', '') or ''
                except Exception as ce:
                    log.warning(f'[ALL-SETUPS] cognitive {pair}: {ce}')

            setups.append({
                'pair': pair,
                'display': cfg.get('display', pair),
                'estado': estado,
                'action': action,
                'tech_action': tech_action,        # lo que diria tecnicamente
                'confidence': last_dc.get('confidence', 0),
                'score': score_total,
                'min_score': min_score,
                'score_gap': round(min_score - score_total, 1) if score_total else None,
                'killzone': kz,
                'killzone_ok': kz_ok,
                'allowed_killzones': allowed_kz,
                'price': last_an.get('price', 0),
                'sweep_detected': has_sweep,
                'sweep_quality': sweep.get('quality', ''),
                'htf_bias': ict.get('htf_bias', ''),
                'htf_strength': ict.get('htf_strength', 0),
                'atr_pips': ict.get('atr_pips', 0),
                'adr_pct': ict.get('adr_pct', 0),
                # v2.7: sesgo direccional (barra visual bull/bear)
                'bias_bull': bias['bull_pct'],
                'bias_bear': bias['bear_pct'],
                'bias_label': bias['label'],
                'bias_strength': bias['strength_label'],
                'regime_type': bias['regime_type'],
                # niveles teoricos (posible entrada)
                'sl': sl_t, 'tp1': tp1_t, 'tp2': tp2_t, 'rr1': rr1_t, 'rr2': rr2_t,
                'motivo': motivo_str,              # por que HOLD
                'narrativa': narrativa,            # opinion de Claude (si score cerca)
                'claude_called': claude_called,
                'source': last_dc.get('source', ''),
                'ts': last_an.get('ts', ''),
                'session': 'London only' if cfg.get('allowed_killzones') else 'Todas',
            })

        return {
            'ok': True,
            'session_active': is_session(),
            'now_et': now_et().isoformat(),
            'cognitive_enabled': cognitive,
            'count': len(setups),
            'setups': setups,
        }
    except Exception as e:
        return {'ok': False, 'error': str(e)}


@app.api_route('/run-backtesting', methods=['GET', 'POST'])
async def run_backtesting_endpoint(pair: Optional[str] = None):
    if bt_state['running']:
        return {'ok': False, 'error': 'Backtest ya en curso'}
    asyncio.create_task(run_backtesting(pair=pair))
    return {'ok': True, 'pair': pair or 'all',
            'message': 'Backtest iniciado. Consultar /backtesting.'}


@app.get('/backtesting')
async def backtesting_endpoint():
    return {
        'running': bt_state['running'],
        'last_run': bt_state['last_run'],
        'summary': bt_state.get('summary'),
        'trades_count': len(bt_state.get('trades', [])),
        'trades': bt_state.get('trades', [])[-200:],
    }


@app.get('/signal-history')
async def signal_history_endpoint(limit: int = 100, pair: Optional[str] = None):
    history = state.get('history', [])
    if pair:
        history = [h for h in history if h.get('pair') == pair]
    return {'count': len(history), 'history': history[-limit:]}


@app.get('/live-trades')
async def live_trades_endpoint(limit: int = 100, pair: Optional[str] = None):
    # v2.6.0-beta-fixed3: filtro opcional por pair
    live = state.get('live_trades', [])
    if pair:
        live = [t for t in live
                if t.get('pair') == pair or t.get('pair_id') == pair]
    return {'count': len(live), 'trades': live[-limit:], 'pair_filter': pair}


@app.get('/audit/decisions')
async def audit_decisions_endpoint(month: Optional[str] = None, limit: int = 100):
    et = now_et()
    month_str = month or et.strftime('%Y-%m')
    records = storage_read_jsonl(f'audit/decisions_{month_str}.jsonl', limit=limit)
    return {'month': month_str, 'count': len(records), 'records': records}


@app.get('/audit/cognitive-health')
async def cognitive_health_endpoint():
    return {
        'disabled': cognitive_is_disabled(),
        'disabled_until': _cognitive_health['disabled_until'],
        'recent_calls': len(_cognitive_health['calls']),
        'recent_failures': len(_cognitive_health['failures']),
        'failure_rate': (
            len(_cognitive_health['failures']) / max(1, len(_cognitive_health['calls']))
        ),
        'threshold': COGNITIVE_FAILURE_THRESHOLD,
    }


@app.get('/pairs')
async def pairs_endpoint():
    pairs_info = {}
    for p in PAIRS:
        cfg = PAIR_CONFIG.get(p, {})
        p_st = pair_state.get(p, {})
        pairs_info[p] = {
            'display': cfg.get('display'),
            'enabled': cfg.get('enabled', False),
            'tier': cfg.get('tier', ''),
            'config': {
                'min_score': cfg.get('min_score'),
                'min_score_wed': cfg.get('min_score_wed'),
                'risk_pct': cfg.get('risk_pct'),
                'atr_min_pips': cfg.get('atr_min_pips'),
                'atr_max_pips': cfg.get('atr_max_pips'),
                'adr_min': cfg.get('adr_min'),
            },
            'state': {
                'today_trades_count': len(p_st.get('today_trades', [])),
                'last_analysis_time': (p_st.get('last_analysis') or {}).get('ts'),
                'last_action': (p_st.get('last_decision') or {}).get('action'),
                'last_score': (p_st.get('last_decision') or {}).get('score'),
                'last_source': (p_st.get('last_decision') or {}).get('source'),
            }
        }
    return {'pairs': pairs_info, 'active_pairs': get_active_pairs(),
            'max_trades_per_day_per_pair': MAX_TRADES_PER_DAY}


@app.get('/regime')
async def regime_endpoint(pair: Optional[str] = None):
    # v2.6.0-beta-fixed3: si se pasa pair, usa pair_state[pair]
    if pair and pair in PAIRS:
        p_st = pair_state.get(pair, {})
        last_an = p_st.get('last_analysis', {}) or {}
        last = last_an.get('ict', {})
        price = last_an.get('price', 0)
        ts = last_an.get('ts')
    else:
        last_an = state.get('last_analysis', {}) or {}
        last = last_an.get('ict', {})
        price = last_an.get('price', 0)
        ts = state.get('last_update')
    return {
        'pair': pair or 'EUR_USD',
        'regime': last.get('regime', {}),
        'anomalies': last.get('anomalies', {}),
        'price': price,
        'timestamp': ts,
    }


@app.api_route('/notify/test', methods=['GET', 'POST'])
async def notify_test_endpoint():
    body = '<div class="card"><div class="value">Email de prueba. TPDCM-IA operativo.</div></div>'
    sent = await send_email('TPDCM-IA · Test Notification', _email_wrapper('Test', body))
    return {'ok': sent, 'enabled': NOTIFICATIONS_ENABLED,
            'configured': bool(RESEND_API_KEY)}


@app.api_route('/notify/pre-london-report', methods=['GET', 'POST'])
async def notify_pre_london_endpoint():
    subj, html = build_pre_london_report()
    sent = await send_email(subj, html)
    return {'ok': sent, 'subject': subj}


@app.api_route('/notify/ny-open-report', methods=['GET', 'POST'])
async def notify_ny_open_endpoint():
    subj, html = build_ny_open_report()
    sent = await send_email(subj, html)
    return {'ok': sent, 'subject': subj}


@app.get('/notify/history')
async def notify_history_endpoint(limit: int = 50):
    history = storage_read_jsonl('notifications/sent.jsonl', limit=limit)
    return {'count': len(history), 'notifications': history}


@app.post('/claude-conversation')
async def claude_conversation_endpoint(payload: ChatMessageIn):
    result = await claude_conversation(payload.message, payload.history)
    return result


@app.get('/claude-analysis-history')
async def claude_analysis_history_endpoint(limit: int = 50):
    history = state.get('history', [])
    with_narrative = [
        h for h in history
        if h.get('ceo_reason') or h.get('narrative_quality')
    ]
    return {'count': len(with_narrative), 'history': with_narrative[-limit:]}


@app.get('/claude-conversation-history')
async def claude_conversation_history_endpoint(limit: int = 30):
    history = storage_read_jsonl('claude_conversations/chat.jsonl', limit=limit)
    return {'count': len(history), 'conversations': history}


@app.get('/weekly-stats')
async def weekly_stats_endpoint():
    stats = compute_weekly_stats()
    return stats or {'message': 'Sin actividad esta semana'}


@app.api_route('/notify/weekly-stats-now', methods=['GET', 'POST'])
async def notify_weekly_stats_endpoint():
    subj, html = build_weekly_stats_report()
    sent = await send_email(subj, html)
    return {'ok': sent, 'subject': subj}


@app.get('/caution-days-stats')
async def caution_days_stats_endpoint():
    history = state.get('history', [])
    caution_trades = [
        t for t in history
        if t.get('day_of_week') in CAUTION_DAYS and t.get('outcome')
    ]
    by_day = {}
    for day in CAUTION_DAYS:
        day_trades = [t for t in caution_trades if t.get('day_of_week') == day]
        if not day_trades:
            by_day[day] = {'trades': 0}
            continue
        wins = [t for t in day_trades if t.get('result_usd', 0) > 0]
        total_pnl = sum(t.get('result_usd', 0) for t in day_trades)
        gw = sum(t.get('result_usd', 0) for t in wins)
        gl = abs(sum(t.get('result_usd', 0) for t in day_trades
                     if t.get('result_usd', 0) < 0))
        by_day[day] = {
            'trades': len(day_trades),
            'wins': len(wins),
            'wr': round(len(wins) / len(day_trades), 4),
            'pnl': round(total_pnl, 2),
            'profit_factor': round(gw / gl, 2) if gl > 0 else 0,
        }
    return {
        'caution_days': CAUTION_DAYS,
        'config': {
            'min_score': CAUTION_MIN_SCORE,
            'min_htf_strength': CAUTION_MIN_HTF_STRENGTH,
            'min_claude_multiplier': CAUTION_MIN_CLAUDE_MULT,
            'risk_multiplier': CAUTION_RISK_MULTIPLIER,
            'blocked_regimes': CAUTION_BLOCKED_REGIMES,
            'blocked_anomalies': CAUTION_BLOCKED_ANOMALIES,
        },
        'historical': by_day,
        'today_is_caution': is_caution_day(),
        'today': now_et().strftime('%A'),
    }


@app.api_route('/healthcheck-monitor', methods=['GET', 'POST'])
async def healthcheck_monitor_endpoint():
    await healthcheck_monitor()
    return {'ok': True, 'message': 'Healthcheck ejecutado'}


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 17: STARTUP
# ═══════════════════════════════════════════════════════════════════════════════

@app.on_event('startup')
async def startup_event():
    _ensure_data_dirs()
    log.info('=' * 60)
    log.info(f'TPDCM-IA v2.6.0-beta-fixed STARTUP')
    log.info(f'OANDA: {OANDA_ENV} | Auto-Execute: {AUTO_EXECUTE}')
    log.info(f'Active pairs: {get_active_pairs()}')
    log.info(f'Risk default: {RISK_PCT}% | Min confidence: {MIN_CONFIDENCE}')
    log.info('=' * 60)

    try:
        acc = await get_account()
        state['balance'] = float(acc.get('balance', 100000))
        log.info(f'[STARTUP] Balance OANDA: ${state["balance"]:,.2f}')
    except Exception as e:
        log.warning(f'[STARTUP] No se pudo cargar balance: {e}')
        state['balance'] = 100000.0

    scheduler = AsyncIOScheduler(timezone=ZoneInfo('America/New_York'))

    scheduler.add_job(
        run_analysis, CronTrigger(minute='1', hour='3-12'),
        kwargs={'auto_execute': AUTO_EXECUTE},
        id='analysis_hourly', max_instances=1, coalesce=True
    )

    scheduler.add_job(
        monitor_trades, CronTrigger(minute='*/5'),
        id='monitor_5min', max_instances=1, coalesce=True
    )

    scheduler.add_job(
        run_backtesting, CronTrigger(hour='3', minute='30'),
        id='bt_daily', max_instances=1, coalesce=True
    )

    scheduler.add_job(
        scheduled_pre_london_report, CronTrigger(hour='7', minute='0',
                                                  day_of_week='mon-fri'),
        id='report_pre_london', max_instances=1, coalesce=True
    )

    scheduler.add_job(
        scheduled_ny_open_report, CronTrigger(hour='9', minute='0',
                                               day_of_week='mon-fri'),
        id='report_ny_open', max_instances=1, coalesce=True
    )

    scheduler.add_job(
        healthcheck_monitor, CronTrigger(minute='*/30'),
        id='healthcheck', max_instances=1, coalesce=True
    )

    scheduler.add_job(
        drawdown_monitor, CronTrigger(hour='12', minute='0'),
        id='dd_monitor', max_instances=1, coalesce=True
    )

    scheduler.add_job(
        scheduled_weekly_stats_report,
        CronTrigger(day_of_week='sun', hour='18', minute='0'),
        id='weekly_stats', max_instances=1, coalesce=True
    )

    scheduler.start()
    log.info('[STARTUP] Scheduler iniciado con 8 jobs')


@app.on_event('shutdown')
async def shutdown_event():
    log.info('[SHUTDOWN] Guardando estado final...')
    storage_write_json('legacy/history.json', state.get('history', [])[-2000:])
    storage_write_json('legacy/live_trades.json', state.get('live_trades', [])[-500:])
    storage_write_json('memory/edge_tracker.json', memory)
    storage_write_json('state/pair_state.json', pair_state)
    log.info('[SHUTDOWN] OK')
