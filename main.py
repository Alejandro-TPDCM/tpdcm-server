"""
═══════════════════════════════════════════════════════════════════════════════
TPDCM-IA v2.4 — Trading Platform Deep Claude Machine Intelligence
EUR/USD Institucional · Prop Firm System
═══════════════════════════════════════════════════════════════════════════════

CAMBIOS v2.3 -> v2.4 (DAYS OF CAUTION ENGINE):
  + Days of Caution Engine basado en análisis cuantitativo
  + Lunes/Viernes: filtros estrictos + risk 50% + Claude validation
  + Score mínimo elevado a 70 en días de caution
  + HTF strength mínimo 0.50 requerido
  + Régimen choppy/compression: VETO automático en L/V
  + Anomalías medium/high: VETO automático en L/V
  + Claude debe aprobar con multiplier >= 0.85 en L/V
  + Endpoint /caution-days-stats para monitoreo
  + Prompt de Claude actualizado con contexto estadístico

CAMBIOS v2.2 -> v2.3 (MEJORAS ROBUSTEZ):
  + Healthcheck interno: monitoreo de scheduler
  + Alerta automatica si scheduler no ejecuta analisis en 2h
  + Alerta de drawdown critico (>5% en 7 dias)
  + Reporte semanal estadistico automatico (domingos 18:00 ET)
  + Endpoint /claude-conversation: chat libre con Claude sobre mercado
  + Endpoint /claude-analysis-history: historial de narrativas
  + Endpoint /healthcheck-monitor: monitoreo activo
  + Endpoint /weekly-stats: stats semanales sin Claude

CAMBIOS v2.1 -> v2.2 (FASE 2):
  + Regime Detector + Anomaly Features
  + Contexto enriquecido a Claude
  + Reportes 7AM/9AM con seccion Regimen
  + Endpoint /regime
  + /dashboard expone regime + anomalies

CAMBIOS v2.0 -> v2.1:
  + Notification Layer (Resend API)
  + Reportes 7AM y 9AM ET
  + Notificacion trade open/close + veto + alertas criticas

CAMBIOS v1.x -> v2.0:
  + Decision Gate refactor (Python autoridad final)
  + Cognitive Layer estricto (Claude solo valida)
  + Persistencia /data volume + audit trail completo
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

# Notification Layer (NUEVO v2.1)
RESEND_API_KEY        = os.environ.get('RESEND_API_KEY', '')
NOTIFY_EMAIL_TO       = os.environ.get('NOTIFY_EMAIL_TO', 'tpdcmia@gmail.com')
NOTIFY_EMAIL_FROM     = os.environ.get('NOTIFY_EMAIL_FROM', 'TPDCM-IA <onboarding@resend.dev>')
NOTIFICATIONS_ENABLED = os.environ.get('NOTIFICATIONS_ENABLED', 'true').lower() == 'true'

PAIR             = 'EUR_USD'
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

# ═══ DAYS OF CAUTION ENGINE (NUEVO v2.4) ═══
# Configuración basada en análisis estadístico de 39 trades reales:
# - Lunes:   WR 20%, PnL -$1,585
# - Viernes: WR 33%, PnL -$1,665
# - Martes:  WR 80%, PnL +$14,316
# - Miércoles: WR 80%, PnL +$5,618
CAUTION_DAYS              = ['Monday', 'Friday']  # Días estadísticamente débiles
CAUTION_RISK_MULTIPLIER   = 0.5    # Reducir risk al 50% en días de caution
CAUTION_MIN_SCORE         = 70     # Score mínimo elevado en L/V
CAUTION_MIN_CLAUDE_MULT   = 0.85   # Multiplier mínimo de Claude
CAUTION_MIN_HTF_STRENGTH  = 0.50   # HTF debe ser fuerte
CAUTION_BLOCKED_REGIMES   = ['choppy', 'compression']  # Régimenes que vetan
CAUTION_BLOCKED_ANOMALIES = ['medium', 'high']  # Severidades que vetan

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
<div class="header"><div class="logo">TPDCM-IA<span>EUR/USD INSTITUCIONAL · v2.2</span></div></div>
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
    htf_bias = ict.get('htf_bias', 'neutral')
    htf_color = 'green' if htf_bias == 'bullish' else 'red' if htf_bias == 'bearish' else 'gold'
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
<div class="row"><span class="k">HTF Bias</span><span class="v {htf_color}">{htf_bias.upper()} ({ict.get('htf_strength', 0)*100:.0f}%)</span></div>
<div class="row"><span class="k">ATR H1</span><span class="v">{ict.get('atr_pips', 0):.1f} pips</span></div>
<div class="row"><span class="k">ADR restante</span><span class="v">{ict.get('adr_pct', 0)*100:.0f}%</span></div></div>
<div class="card"><div class="label">📊 Regimen de Mercado (Fase 2)</div>
<div class="row"><span class="k">Tipo de regimen</span><span class="v gold">{ict.get('regime', {}).get('type', 'unknown').upper()}</span></div>
<div class="row"><span class="k">Calidad del regimen</span><span class="v">{ict.get('regime', {}).get('regime_quality', 'unknown').upper()}</span></div>
<div class="row"><span class="k">Volatilidad (Z-score)</span><span class="v">{ict.get('regime', {}).get('volatility_z', 0):+.2f}</span></div>
<div class="row"><span class="k">Direccionalidad</span><span class="v">{ict.get('regime', {}).get('trending_score', 0)*100:.0f}%</span></div>
<div class="row"><span class="k">Anomalias detectadas</span><span class="v {'red' if ict.get('anomalies', {}).get('severity') == 'high' else 'gold' if ict.get('anomalies', {}).get('severity') == 'medium' else 'green'}">{ict.get('anomalies', {}).get('severity', 'none').upper()}</span></div>
</div>
<div class="card"><div class="label">Niveles Institucionales del Día</div>
<div class="row"><span class="k">PDH (Previous Day High)</span><span class="v gold">{liq.get('pdh', 0):.5f}</span></div>
<div class="row"><span class="k">PDL (Previous Day Low)</span><span class="v gold">{liq.get('pdl', 0):.5f}</span></div>
<div class="row"><span class="k">Weekly High</span><span class="v blue">{liq.get('weekly_high', 0):.5f}</span></div>
<div class="row"><span class="k">Weekly Low</span><span class="v blue">{liq.get('weekly_low', 0):.5f}</span></div></div>
{news_html}
<div class="card"><div class="label">Salud del Sistema</div>
<div class="row"><span class="k">Edge Score</span><span class="v green">{memory.get('edge_score', 100):.0f}/100</span></div>
<div class="row"><span class="k">Modo defensivo</span><span class="v {'red' if defensive else 'green'}">{ 'ACTIVO' if defensive else 'NO' }</span></div>
<div class="row"><span class="k">Pérdidas consecutivas</span><span class="v">{state.get('consecutive_losses', 0)}</span></div>
<div class="row"><span class="k">Riesgo actual / trade</span><span class="v">{state.get('risk_pct_current', 1.0):.2f}%</span></div>
<div class="row"><span class="k">Cognitive layer</span><span class="v {'red' if cognitive_is_disabled() else 'green'}">{'DEGRADED' if cognitive_is_disabled() else 'HABILITADO'}</span></div>
<div class="row"><span class="k">Auto Execute</span><span class="v {'green' if AUTO_EXECUTE else 'gold'}">{'ACTIVO' if AUTO_EXECUTE else 'SOLO SEÑAL'}</span></div></div>
<div class="card"><div class="label">Plan del Día</div>
<div class="value" style="font-size:11px">
🕒 <strong>3:00-5:00 AM ET</strong> · Killzone London<br>
🕗 <strong>8:00-11:00 AM ET</strong> · Killzone NY Open (auto-execute)<br>
🕓 <strong>11:00-12:00 AM ET</strong> · Killzone NY Late (selectivo)<br>
🛑 <strong>Friday 14:00 ET</strong> · Cierre forzado fin de semana</div></div>"""
    subject = f'TPDCM-IA · Briefing 7AM · EUR/USD · {htf_bias.upper()}'
    return subject, _email_wrapper('🌅 Pre-Londres Briefing', body)


def build_ny_open_report():
    ict = state.get('last_analysis', {}).get('ict', {})
    dec = state.get('last_decision', {}) or {}
    sweep = ict.get('sweep', {})
    action = dec.get('action', 'HOLD')
    action_color = 'green' if action == 'BUY' else 'red' if action == 'SELL' else 'gold'
    source = dec.get('source', '')
    source_explain = {
        'hold_technical': 'Python decidió HOLD (no hay setup técnico válido)',
        'validated':      'Setup validado por Claude · Listo para ejecutar',
        'vetoed':         'Setup técnico válido pero VETADO por Claude',
        'cognitive_down_elite':     'Claude no disponible · Setup elite operando con penalty',
        'cognitive_down_non_elite': 'Claude no disponible · Score no elite · HOLD',
    }.get(source, source)
    confidence_pct = dec.get('confidence', 0) * 100
    conf_color = 'green' if confidence_pct >= 70 else 'gold' if confidence_pct >= 50 else 'red'
    setup_html = ""
    if sweep.get('detected'):
        setup_html = f"""<div class="card"><div class="label">Setup Técnico Detectado</div>
<div class="row"><span class="k">Sweep</span><span class="v">{sweep.get('quality','').upper()} @ {sweep.get('level_type','')}</span></div>
<div class="row"><span class="k">Nivel sweep</span><span class="v gold">{sweep.get('level',0):.5f}</span></div>
<div class="row"><span class="k">Mecha %</span><span class="v">{sweep.get('wick_pct',0)*100:.0f}%</span></div>
<div class="row"><span class="k">BOS</span><span class="v">{ict.get('structure',{}).get('bos_quality','--').upper()}</span></div>
<div class="row"><span class="k">Displacement</span><span class="v">{ict.get('displacement',{}).get('strength','--').upper()}</span></div>
<div class="row"><span class="k">Inducement</span><span class="v">{ict.get('inducement',{}).get('quality','--').upper()}</span></div></div>"""
    cognitive_html = ""
    if dec.get('narrative'):
        veto = dec.get('cognitive_veto', False)
        mult = dec.get('cognitive_multiplier')
        nq = dec.get('narrative_quality', '--')
        cognitive_html = f"""<div class="card"><div class="label">Validación Cognitiva (Claude)</div>
<div class="row"><span class="k">Veto</span><span class="v {'red' if veto else 'green'}">{ 'SI' if veto else 'NO' }</span></div>
<div class="row"><span class="k">Confidence multiplier</span><span class="v">{mult if mult else '--'}</span></div>
<div class="row"><span class="k">Calidad narrativa</span><span class="v {'green' if nq=='clean' else 'gold' if nq=='acceptable' else 'red' if nq=='dirty' else ''}">{nq.upper() if nq else '--'}</span></div>
<div class="row"><span class="k">Régimen</span><span class="v">{dec.get('regime_assessment','--')}</span></div>
<div style="margin-top:10px;padding:10px;background:rgba(0,0,0,0.3);border-radius:4px;font-size:11px;color:#c4d8cc;font-style:italic">"{dec.get('narrative','--')}"</div></div>"""
    levels_html = ""
    if dec.get('sl', 0) > 0 and action != 'HOLD':
        levels_html = f"""<div class="card"><div class="label">Niveles Operativos Calculados</div>
<div class="row"><span class="k">Entry</span><span class="v blue">{state.get('last_analysis',{}).get('price',0):.5f}</span></div>
<div class="row"><span class="k">Stop Loss</span><span class="v red">{dec.get('sl',0):.5f}</span></div>
<div class="row"><span class="k">TP1 (50% close)</span><span class="v green">{dec.get('tp1',0):.5f}</span></div>
<div class="row"><span class="k">TP2 (target liquidez)</span><span class="v green">{dec.get('tp2',0):.5f}</span></div>
<div class="row"><span class="k">RR TP1 / TP2</span><span class="v">{dec.get('rr1',0)} / {dec.get('rr2',0)}</span></div>
<div class="row"><span class="k">Position size</span><span class="v">{dec.get('pos_size',0):,} units</span></div></div>"""
    body = f"""<div class="card"><div class="label">Decisión Final</div>
<div style="display:flex;align-items:center;gap:16px;margin-top:4px">
<span class="big {action_color}">{action}</span>
<span class="value">Confianza: <strong class="{conf_color}">{confidence_pct:.0f}%</strong></span>
<span class="value">Score: <strong class="gold">{dec.get('score',0)}/100</strong></span></div>
<div style="margin-top:10px;color:#5a7a68;font-size:11px">{source_explain}</div></div>
<div class="card"><div class="label">📊 Regimen de Mercado (Fase 2)</div>
<div class="row"><span class="k">Tipo de regimen</span><span class="v gold">{ict.get('regime', {}).get('type', 'unknown').upper()}</span></div>
<div class="row"><span class="k">Calidad del regimen</span><span class="v">{ict.get('regime', {}).get('regime_quality', 'unknown').upper()}</span></div>
<div class="row"><span class="k">Volatilidad (Z-score)</span><span class="v">{ict.get('regime', {}).get('volatility_z', 0):+.2f}</span></div>
<div class="row"><span class="k">Direccionalidad</span><span class="v">{ict.get('regime', {}).get('trending_score', 0)*100:.0f}%</span></div>
<div class="row"><span class="k">Momentum consistency</span><span class="v">{ict.get('regime', {}).get('momentum_consistency', 0)*100:.0f}%</span></div>
<div class="row"><span class="k">Anomalias detectadas</span><span class="v {'red' if ict.get('anomalies', {}).get('severity') == 'high' else 'gold' if ict.get('anomalies', {}).get('severity') == 'medium' else 'green'}">{ict.get('anomalies', {}).get('severity', 'none').upper()} ({ict.get('anomalies', {}).get('anomaly_count', 0)})</span></div>
</div>
{setup_html}{cognitive_html}{levels_html}
<div class="card"><div class="label">Próximos Análisis Programados</div>
<div class="value" style="font-size:11px;color:#5a7a68">
🕙 <strong>10:00 AM ET</strong> · Análisis NY medio (auto-execute habilitado)<br>
🕛 <strong>Cada hora</strong> · Análisis continuo durante sesión<br>
🛑 <strong>12:00 PM ET</strong> · Fin de sesión, cierre de trades abiertos</div></div>"""
    subject = f'TPDCM-IA · NY 9AM · {action} ({confidence_pct:.0f}%) · Score {dec.get("score",0)}/100'
    return subject, _email_wrapper('🌇 NY Open Analysis', body)


def build_trade_open_email(trade_record: dict):
    action = trade_record.get('action', '')
    action_color = 'green' if action == 'BUY' else 'red'
    body = f"""<div class="card"><div class="label">Orden Ejecutada en OANDA Demo</div>
<div style="display:flex;align-items:center;gap:12px;margin:10px 0">
<span class="big {action_color}">{action}</span>
<span class="value">@ <strong>{trade_record.get('entry_price', 0):.5f}</strong></span>
<span class="badge badge-{('green' if action == 'BUY' else 'red')}">EUR/USD</span></div>
<div style="font-size:11px;color:#5a7a68">Trade ID: {trade_record.get('trade_id', '--')}</div></div>
<div class="card"><div class="label">Niveles del Trade</div>
<div class="row"><span class="k">Entry</span><span class="v blue">{trade_record.get('entry_price',0):.5f}</span></div>
<div class="row"><span class="k">Stop Loss</span><span class="v red">{trade_record.get('sl',0):.5f}</span></div>
<div class="row"><span class="k">Take Profit 1 (50% close)</span><span class="v green">{trade_record.get('tp1',0):.5f}</span></div>
<div class="row"><span class="k">Take Profit 2 (objetivo)</span><span class="v green">{trade_record.get('tp2',0):.5f}</span></div>
<div class="row"><span class="k">RR TP1 / TP2</span><span class="v">{trade_record.get('rr1',0)} / {trade_record.get('rr2',0)}</span></div>
<div class="row"><span class="k">Position size</span><span class="v">{trade_record.get('pos_size',0):,} units</span></div></div>
<div class="card"><div class="label">Contexto Institucional</div>
<div class="row"><span class="k">Killzone</span><span class="v gold">{trade_record.get('killzone','--')}</span></div>
<div class="row"><span class="k">HTF Bias</span><span class="v">{trade_record.get('htf_bias','--').upper()}</span></div>
<div class="row"><span class="k">Score ICT</span><span class="v gold">{trade_record.get('score',0)}/100</span></div>
<div class="row"><span class="k">Confianza final</span><span class="v green">{trade_record.get('confidence',0)*100:.0f}%</span></div></div>
<div class="card"><div class="label">Razón del Decision Gate</div>
<div style="font-style:italic;font-size:11px;line-height:1.6;color:#c4d8cc;padding:10px;background:rgba(0,0,0,0.3);border-radius:4px">
"{trade_record.get('narrative') or trade_record.get('ceo_reason','--')}"</div></div>
<div class="card"><div class="label">Gestión Programada</div>
<div class="value" style="font-size:11px;color:#5a7a68">
✓ Cierre 50% al alcanzar TP1<br>
✓ SL movido a Break-Even tras BOS post-TP1<br>
✓ Cierre forzado fuera de sesión / Friday 14:00 ET<br>
✓ Monitor cada 5 minutos</div></div>"""
    emoji = '🟢' if action == 'BUY' else '🔴'
    subject = f'{emoji} TPDCM-IA · TRADE OPEN · {action} EUR/USD @ {trade_record.get("entry_price",0):.5f}'
    return subject, _email_wrapper(f'{emoji} Trade Abierto', body)


def build_trade_close_email(trade: dict, outcome: str, pnl: float, gestion: list):
    pnl_color = 'green' if pnl > 0 else 'red' if pnl < 0 else 'gold'
    outcome_color = {'TP':'green','TP2':'green','SL':'red','BE':'gold','TIMEOUT':'gold'}.get(outcome,'gold')
    outcome_emoji = {'TP':'✅','TP2':'🎯','SL':'❌','BE':'⚖️','TIMEOUT':'⏱️'}.get(outcome,'•')
    outcome_label = {
        'TP':'Take Profit 1 alcanzado','TP2':'Take Profit 2 (objetivo final) alcanzado',
        'SL':'Stop Loss hit','BE':'Cerrado en Break-Even','TIMEOUT':'Timeout (25 velas)',
    }.get(outcome, outcome)
    gestion_html = ""
    if gestion:
        items = "".join(f'<div style="padding:4px 0;font-size:11px">✓ {g}</div>' for g in gestion)
        gestion_html = f'<div class="card"><div class="label">Gestión Aplicada</div>{items}</div>'
    body = f"""<div class="card"><div class="label">Resultado del Trade</div>
<div style="display:flex;align-items:center;gap:14px;margin:10px 0">
<span style="font-size:32px">{outcome_emoji}</span>
<div><div class="big {outcome_color}">{outcome}</div>
<div style="font-size:11px;color:#5a7a68;margin-top:2px">{outcome_label}</div></div>
<div style="margin-left:auto;text-align:right">
<div class="big {pnl_color}">{('+' if pnl > 0 else '')}${pnl:,.2f}</div>
<div style="font-size:10px;color:#5a7a68">P&L Final</div></div></div></div>
<div class="card"><div class="label">Detalles del Trade</div>
<div class="row"><span class="k">Trade ID</span><span class="v" style="font-size:10px">{trade.get('trade_id','--')}</span></div>
<div class="row"><span class="k">Tipo</span><span class="v {'green' if trade.get('action')=='BUY' else 'red'}">{trade.get('action','--')}</span></div>
<div class="row"><span class="k">Entry</span><span class="v">{trade.get('entry_price',0):.5f}</span></div>
<div class="row"><span class="k">SL programado</span><span class="v red">{trade.get('sl',0):.5f}</span></div>
<div class="row"><span class="k">TP1 / TP2</span><span class="v">{trade.get('tp1',0):.5f} / {trade.get('tp2',0):.5f}</span></div>
<div class="row"><span class="k">Hora apertura</span><span class="v">{trade.get('time','--')}</span></div>
<div class="row"><span class="k">Hora cierre</span><span class="v">{datetime.now(ZoneInfo('America/New_York')).strftime('%H:%M ET')}</span></div>
<div class="row"><span class="k">Killzone</span><span class="v gold">{trade.get('killzone','--')}</span></div></div>
{gestion_html}
<div class="card"><div class="label">Estado del Sistema Post-Trade</div>
<div class="row"><span class="k">Balance actualizado</span><span class="v green">${state.get('balance', 0):,.2f}</span></div>
<div class="row"><span class="k">Edge score</span><span class="v">{memory.get('edge_score', 100):.0f}/100</span></div>
<div class="row"><span class="k">Pérdidas consecutivas</span><span class="v {'red' if state.get('consecutive_losses', 0) >= 2 else ''}">{state.get('consecutive_losses', 0)}</span></div>
<div class="row"><span class="k">Modo defensivo</span><span class="v {'red' if state.get('defensive_mode') else 'green'}">{ 'ACTIVO' if state.get('defensive_mode') else 'NO' }</span></div></div>"""
    subject = f'{outcome_emoji} TPDCM-IA · {outcome} · {("+" if pnl > 0 else "")}${pnl:,.0f} · EUR/USD'
    return subject, _email_wrapper(f'{outcome_emoji} Trade Cerrado · {outcome}', body)


def build_cognitive_veto_email(decision: dict):
    technical_action = decision.get('technical_action_that_would_have_been', '--')
    technical_conf = decision.get('technical_confidence_pre_veto', 0)
    anomalies = decision.get('anomalies', [])
    anomalies_html = ""
    if anomalies:
        items = "".join(f'<div style="padding:4px 0;font-size:11px;color:#f0c040">⚠️ {a}</div>' for a in anomalies)
        anomalies_html = f'<div class="card"><div class="label">Anomalías Detectadas</div>{items}</div>'
    body = f"""<div class="card"><div class="label">Setup Vetado</div>
<div style="margin:10px 0"><span class="big gold">🟡 VETO COGNITIVO</span></div>
<div style="font-size:11px;color:#5a7a68">Python detectó setup técnico válido, pero Claude lo rechazó por incoherencia institucional.<br>Esta es la <strong>capa de protección cognitiva</strong> funcionando correctamente.</div></div>
<div class="card"><div class="label">Setup Técnico que se Iba a Tomar</div>
<div class="row"><span class="k">Acción</span><span class="v {'green' if technical_action == 'BUY' else 'red'}">{technical_action}</span></div>
<div class="row"><span class="k">Confianza técnica</span><span class="v">{technical_conf*100:.0f}%</span></div>
<div class="row"><span class="k">Score ICT</span><span class="v gold">{decision.get('score',0)}/100</span></div>
<div class="row"><span class="k">Killzone</span><span class="v">{decision.get('killzone','--')}</span></div></div>
<div class="card"><div class="label">Razón del Veto (Claude)</div>
<div style="font-style:italic;font-size:12px;line-height:1.7;padding:14px;background:rgba(240,192,64,0.08);border-left:3px solid #f0c040;border-radius:4px;color:#c4d8cc">
"{decision.get('reason','--')}"</div></div>
<div class="card"><div class="label">Análisis Contextual</div>
<div class="row"><span class="k">Calidad narrativa</span><span class="v red">{decision.get('narrative_quality','--').upper() if decision.get('narrative_quality') else '--'}</span></div>
<div class="row"><span class="k">Régimen detectado</span><span class="v">{decision.get('regime_assessment','--')}</span></div>
<div class="row"><span class="k">Multiplier aplicado</span><span class="v">{decision.get('cognitive_multiplier','--')}</span></div></div>
{anomalies_html}"""
    subject = f'🟡 TPDCM-IA · VETO COGNITIVO · {technical_action} rechazado'
    return subject, _email_wrapper('🟡 Cognitive Veto', body)


def build_critical_alert_email(alert_type: str, message: str, details: dict):
    body = f"""<div class="card" style="border-color:rgba(255,51,85,0.3)">
<div class="label" style="color:#ff3355">⚠️ Alerta Crítica</div>
<div style="margin:10px 0"><span class="big red">{alert_type}</span></div>
<div style="font-size:12px;color:#c4d8cc;line-height:1.6;padding:10px;background:rgba(255,51,85,0.05);border-radius:4px;border-left:3px solid #ff3355">{message}</div></div>
<div class="card"><div class="label">Detalles</div>
{"".join(f'<div class="row"><span class="k">{k}</span><span class="v">{v}</span></div>' for k, v in details.items())}</div>
<div class="card"><div class="label">Estado del Sistema</div>
<div class="row"><span class="k">Trading pausado</span><span class="v {'red' if state.get('trading_paused') else 'green'}">{ 'SI' if state.get('trading_paused') else 'NO' }</span></div>
<div class="row"><span class="k">Pérdidas consecutivas</span><span class="v">{state.get('consecutive_losses', 0)}</span></div>
<div class="row"><span class="k">Pérdida diaria USD</span><span class="v red">${state.get('daily_loss_usd', 0):,.2f}</span></div>
<div class="row"><span class="k">Balance actual</span><span class="v">${state.get('balance', 0):,.2f}</span></div></div>"""
    subject = f'⚠️ TPDCM-IA · CRÍTICO · {alert_type}'
    return subject, _email_wrapper(f'⚠️ {alert_type}', body)


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 4: APP FASTAPI + ESTADO GLOBAL
# ═══════════════════════════════════════════════════════════════════════════════

app = FastAPI(title='TPDCM-IA', version='2.4.0')
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
    if 3 <= hour < 5:  return 'LONDON_OPEN'
    if 8 <= hour < 11: return 'NY_OPEN'
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
# DAYS OF CAUTION ENGINE (NUEVO v2.4)
# ═══════════════════════════════════════════════════════════════════════════════
# Sistema de filtrado inteligente para días estadísticamente débiles (Lunes/Viernes).
# Basado en análisis cuantitativo de 39 trades reales del backtest.
#
# En días de caution:
#  - Risk se reduce al 50%
#  - Score mínimo elevado a 70
#  - Régimen choppy/compression: VETO automático
#  - Anomalías medium/high: VETO automático
#  - Claude debe aprobar con multiplier >= 0.85
#  - HTF strength debe ser >= 0.50
# ═══════════════════════════════════════════════════════════════════════════════

def is_caution_day(dt=None):
    """Verifica si el día actual es un Day of Caution (lunes o viernes)."""
    if dt is None:
        dt = now_et()
    day_name = dt.strftime('%A')
    return day_name in CAUTION_DAYS


def _get_caution_day_warning():
    """Genera advertencia estadística para Claude sobre días débiles."""
    day = now_et().strftime('%A')
    if day == 'Monday':
        return {
            'day': 'Monday',
            'historical_wr': '20%',
            'historical_pnl': '-$1,585 over 10 trades',
            'recommendation': 'BE EXTREMELY STRICT. Monday has historically been the worst trading day.',
            'requirements': [
                'Setup must be ELITE quality, not just good',
                'Regime must be TRENDING or EXPANSION (not choppy)',
                'HTF bias must be strong (>0.5)',
                'No anomalies should be present',
                'If ANY doubt exists, VETO the trade',
                'Even score 75+ trades have failed on Mondays in backtest'
            ]
        }
    elif day == 'Friday':
        return {
            'day': 'Friday',
            'historical_wr': '33%',
            'historical_pnl': '-$1,665 over 12 trades',
            'recommendation': 'BE EXTREMELY STRICT. Friday afternoons especially weak.',
            'requirements': [
                'Avoid trades close to weekend (after 12:00 ET)',
                'Setup must be ELITE quality',
                'Strong HTF alignment required',
                'No medium/high anomalies',
                'If ANY doubt exists, VETO the trade'
            ]
        }
    return None


def days_of_caution_filter(signal, regime, anomalies, claude_response, dt=None):
    """
    Filtro institucional para días de caution.
    
    Args:
        signal: dict con score, htf_strength, action, etc.
        regime: dict con type, quality
        anomalies: dict con severity, active, count
        claude_response: dict con cognitive_veto, multiplier, narrative
        dt: datetime (opcional, default: now_et())
    
    Returns:
        dict con:
            - allow: bool (permitir trade o no)
            - veto_reason: str (si allow=False)
            - risk_multiplier: float (0.5 si caution, 1.0 si normal)
            - caution_mode: bool
            - filters_passed: list de filtros que pasó
            - detail: str con explicación
    """
    if dt is None:
        dt = now_et()
    
    day_name = dt.strftime('%A')
    
    # Si NO es día de caution, paso normal
    if day_name not in CAUTION_DAYS:
        return {
            'allow': True,
            'risk_multiplier': 1.0,
            'caution_mode': False,
            'day': day_name,
            'detail': 'Día normal - sin filtros adicionales'
        }
    
    # Es día de caution: aplicar filtros estrictos
    filters_passed = []
    
    # FILTRO 1: Régimen debe ser favorable
    regime_type = (regime or {}).get('type', 'unknown')
    if regime_type in CAUTION_BLOCKED_REGIMES:
        return {
            'allow': False,
            'veto_reason': 'caution_day_unfavorable_regime',
            'risk_multiplier': 0,
            'caution_mode': True,
            'day': day_name,
            'detail': f'{day_name} + régimen {regime_type} = riesgo elevado',
            'filters_passed': filters_passed
        }
    filters_passed.append('regime_ok')
    
    # FILTRO 2: Sin anomalías significativas
    anomaly_severity = (anomalies or {}).get('severity', 'none')
    if anomaly_severity in CAUTION_BLOCKED_ANOMALIES:
        active_anomalies = (anomalies or {}).get('active', [])
        return {
            'allow': False,
            'veto_reason': 'caution_day_anomaly_detected',
            'risk_multiplier': 0,
            'caution_mode': True,
            'day': day_name,
            'detail': f'{day_name} con anomalía {anomaly_severity}: {active_anomalies}',
            'filters_passed': filters_passed
        }
    filters_passed.append('no_anomalies')
    
    # FILTRO 3: Score técnico mínimo elevado
    score = signal.get('score', 0)
    if score < CAUTION_MIN_SCORE:
        return {
            'allow': False,
            'veto_reason': 'caution_day_score_too_low',
            'risk_multiplier': 0,
            'caution_mode': True,
            'day': day_name,
            'detail': f'Score {score:.1f} < {CAUTION_MIN_SCORE} mínimo en día de caution',
            'filters_passed': filters_passed
        }
    filters_passed.append('score_elite')
    
    # FILTRO 4: HTF strength suficiente
    htf_strength = signal.get('htf_strength', 0)
    if htf_strength < CAUTION_MIN_HTF_STRENGTH:
        return {
            'allow': False,
            'veto_reason': 'caution_day_weak_htf',
            'risk_multiplier': 0,
            'caution_mode': True,
            'day': day_name,
            'detail': f'HTF strength {htf_strength:.2f} < {CAUTION_MIN_HTF_STRENGTH} mínimo',
            'filters_passed': filters_passed
        }
    filters_passed.append('htf_strong')
    
    # FILTRO 5: Claude debe haber validado (no vetado)
    if claude_response and claude_response.get('cognitive_veto'):
        return {
            'allow': False,
            'veto_reason': 'caution_day_claude_veto',
            'risk_multiplier': 0,
            'caution_mode': True,
            'day': day_name,
            'detail': f'Claude vetó en día de caution: {claude_response.get("narrative", "")[:100]}',
            'filters_passed': filters_passed
        }
    filters_passed.append('claude_ok')
    
    # FILTRO 6: Claude multiplier suficientemente alto
    claude_mult = (claude_response or {}).get('multiplier', 1.0)
    if claude_mult < CAUTION_MIN_CLAUDE_MULT:
        return {
            'allow': False,
            'veto_reason': 'caution_day_low_claude_confidence',
            'risk_multiplier': 0,
            'caution_mode': True,
            'day': day_name,
            'detail': f'Multiplier Claude {claude_mult:.2f} < {CAUTION_MIN_CLAUDE_MULT} requerido',
            'filters_passed': filters_passed
        }
    filters_passed.append('claude_confident')
    
    # ✅ TODOS LOS FILTROS PASARON: permitir con risk reducido
    return {
        'allow': True,
        'risk_multiplier': CAUTION_RISK_MULTIPLIER,
        'caution_mode': True,
        'day': day_name,
        'detail': (f'CAUTION DAY ELITE PASS: score {score:.1f}, '
                   f'HTF {htf_strength:.2f}, Claude mult {claude_mult:.2f}'),
        'filters_passed': filters_passed,
        'reason': 'elite_setup_caution_day'
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

async def get_candles(granularity='H1', count=100):
    data = await oanda_get(f'/v3/instruments/{PAIR}/candles?granularity={granularity}&count={count}&price=M')
    return data.get('candles', [])

async def get_candles_to(granularity='H1', count=500, to_dt=None):
    path = f'/v3/instruments/{PAIR}/candles?granularity={granularity}&count={count}&price=M'
    if to_dt: path += f'&to={to_dt.split(".")[0]}Z'
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
# SECCION 8b: REGIME DETECTOR (Fase 2 - NUEVO v2.2)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Clasifica automaticamente el regimen del mercado en cada vela:
#   - trending      : mercado con direccion clara y momentum sostenido
#   - ranging       : lateral, sin direccion definida, oscilando
#   - expansion     : volatilidad alta saludable (post-Asia, NY Open)
#   - compression   : volatilidad baja, antes de movimiento grande
#   - choppy        : erratico, peligroso, evitar operar
#
# ═══════════════════════════════════════════════════════════════════════════════

def detect_regime(candles_h1, candles_d1=None):
    """
    Clasifica el regimen actual del mercado.
    Retorna dict con type, volatility_z, trending_score, regime_quality.
    """
    if len(candles_h1) < 30:
        return {
            'type': 'unknown',
            'volatility_z': 0.0,
            'trending_score': 0.0,
            'regime_quality': 'insufficient_data',
            'momentum_consistency': 0.0,
        }

    # Ventanas de analisis
    window_recent = candles_h1[-12:]   # ultimas 12h
    window_long   = candles_h1[-50:]   # ultimas 50h (~2 dias)

    # === Volatility Z-score ===
    # Compara ATR reciente vs ATR historico
    atr_recent = compute_atr(window_recent, period=12)
    atr_long   = compute_atr(window_long, period=50)
    if atr_long > 0:
        volatility_z = (atr_recent - atr_long) / atr_long
    else:
        volatility_z = 0.0
    volatility_z = max(-3.0, min(3.0, volatility_z * 3))  # clamp a [-3, 3]

    # === Trending Score ===
    # Mide si las velas tienen direccion consistente
    closes = [float(c['mid']['c']) for c in window_recent]
    opens  = [float(c['mid']['o']) for c in window_recent]
    bullish_candles = sum(1 for c, o in zip(closes, opens) if c > o)
    bearish_candles = sum(1 for c, o in zip(closes, opens) if c < o)
    total = len(closes)
    # Trending si la mayoria va en la misma direccion
    directional_bias = max(bullish_candles, bearish_candles) / total if total > 0 else 0.5
    # Penaliza si las velas cambian mucho de direccion (chop)
    direction_changes = sum(
        1 for i in range(1, len(closes))
        if (closes[i] - opens[i]) * (closes[i-1] - opens[i-1]) < 0
    )
    chop_penalty = direction_changes / max(1, total - 1)
    trending_score = round(max(0.0, min(1.0, directional_bias - chop_penalty * 0.5)), 2)

    # === Momentum Consistency ===
    # Cuanto avanza el precio neto vs el rango total recorrido
    high_recent = max(float(c['mid']['h']) for c in window_recent)
    low_recent  = min(float(c['mid']['l']) for c in window_recent)
    net_move    = abs(closes[-1] - closes[0])
    total_range = high_recent - low_recent
    momentum_consistency = round(net_move / total_range, 2) if total_range > 0 else 0.0

    # === Regime Type Classification ===
    # Reglas deterministas:
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

    # === Regime Quality ===
    if chop_penalty < 0.20 and momentum_consistency >= 0.35:
        regime_quality = 'clean'
    elif chop_penalty < 0.40:
        regime_quality = 'noisy'
    else:
        regime_quality = 'choppy'

    return {
        'type': regime_type,
        'volatility_z': round(volatility_z, 2),
        'trending_score': trending_score,
        'momentum_consistency': momentum_consistency,
        'regime_quality': regime_quality,
        'chop_penalty': round(chop_penalty, 2),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 8c: ANOMALY FEATURES (Fase 2 - NUEVO v2.2)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Python detecta caracteristicas anomalas. Claude las interpreta.
#
# ═══════════════════════════════════════════════════════════════════════════════

def detect_anomalies(candles_h1, news_events=None, candle_dt=None):
    """
    Detecta anomalias cuantitativas en las ultimas velas.
    Retorna dict con multiples senales binarias o numericas.
    """
    if len(candles_h1) < 10:
        return {
            'consecutive_long_wicks': 0,
            'mechas_consecutivas': 0,
            'gap_post_news': False,
            'sweep_without_retest': False,
            'volume_dissonance': False,
            'displacement_velocity_pips': 0.0,
            'anomaly_count': 0,
            'severity': 'none',
        }

    recent = candles_h1[-8:]
    atr = compute_atr(candles_h1)

    # === Mechas largas consecutivas ===
    # Vela tiene mecha "larga" si la mecha > 60% del rango total
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

    # === Mechas consecutivas (con direcciones opuestas - indecision) ===
    mechas_alternantes = 0
    for i in range(1, len(recent)):
        c_prev = recent[i-1]; c_curr = recent[i]
        h_p, l_p = float(c_prev['mid']['h']), float(c_prev['mid']['l'])
        o_p, cc_p = float(c_prev['mid']['o']), float(c_prev['mid']['c'])
        h_c, l_c = float(c_curr['mid']['h']), float(c_curr['mid']['l'])
        o_c, cc_c = float(c_curr['mid']['o']), float(c_curr['mid']['c'])

        upper_p = h_p - max(o_p, cc_p); upper_c = h_c - max(o_c, cc_c)
        lower_p = min(o_p, cc_p) - l_p; lower_c = min(o_c, cc_c) - l_c

        # Vela previa con mecha arriba seguida de vela actual con mecha abajo (o viceversa)
        if upper_p > atr * 0.3 and lower_c > atr * 0.3:
            mechas_alternantes += 1
        elif lower_p > atr * 0.3 and upper_c > atr * 0.3:
            mechas_alternantes += 1

    # === Gap post-news ===
    # Detecta si hubo gap significativo recientemente (>0.5 * ATR)
    gap_post_news = False
    if news_events and candle_dt:
        for i in range(1, len(recent)):
            prev_close = float(recent[i-1]['mid']['c'])
            curr_open = float(recent[i]['mid']['o'])
            gap = abs(curr_open - prev_close)
            if gap > atr * 0.5:
                # Verificar si hubo noticia high-impact en las ultimas 2h
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

    # === Sweep sin retest ===
    # Si hubo sweep en ultimas 5 velas pero el precio no volvio cerca del nivel
    sweep_without_retest = False
    if len(candles_h1) >= 10:
        last_5 = candles_h1[-5:]
        prev_5 = candles_h1[-10:-5]
        prev_high = max(float(c['mid']['h']) for c in prev_5)
        prev_low  = min(float(c['mid']['l']) for c in prev_5)
        last_high = max(float(c['mid']['h']) for c in last_5)
        last_low  = min(float(c['mid']['l']) for c in last_5)
        last_close = float(last_5[-1]['mid']['c'])

        # Sweep bearish: precio rompio el high previo pero ahora esta lejos abajo
        if last_high > prev_high and (last_high - last_close) > atr * 1.5:
            sweep_without_retest = True
        # Sweep bullish: precio rompio el low previo pero ahora esta lejos arriba
        if last_low < prev_low and (last_close - last_low) > atr * 1.5:
            sweep_without_retest = True

    # === Volume Dissonance ===
    # Vela grande con volumen bajo (sospechoso)
    volume_dissonance = False
    if len(recent) >= 5:
        volumes = [int(c.get('volume', 0)) for c in recent]
        avg_vol = sum(volumes[:-1]) / max(1, len(volumes) - 1)
        last_candle = recent[-1]
        last_vol = int(last_candle.get('volume', 0))
        last_range = float(last_candle['mid']['h']) - float(last_candle['mid']['l'])
        # Si la ultima vela es grande (>1.5 ATR) pero el volumen es <70% del promedio
        if last_range > atr * 1.5 and avg_vol > 0 and last_vol < avg_vol * 0.70:
            volume_dissonance = True

    # === Displacement Velocity ===
    # Pips netos movidos en las ultimas 3 velas
    if len(candles_h1) >= 4:
        velocity = abs(
            float(candles_h1[-1]['mid']['c']) - float(candles_h1[-4]['mid']['c'])
        ) * 10000
    else:
        velocity = 0.0

    # === Severity ===
    anomalies_active = []
    if max_streak >= 3: anomalies_active.append('long_wick_streak')
    if mechas_alternantes >= 2: anomalies_active.append('alternating_wicks')
    if gap_post_news: anomalies_active.append('gap_post_news')
    if sweep_without_retest: anomalies_active.append('sweep_without_retest')
    if volume_dissonance: anomalies_active.append('volume_dissonance')

    anomaly_count = len(anomalies_active)
    if anomaly_count >= 3:
        severity = 'high'
    elif anomaly_count == 2:
        severity = 'medium'
    elif anomaly_count == 1:
        severity = 'low'
    else:
        severity = 'none'

    return {
        'consecutive_long_wicks': max_streak,
        'mechas_consecutivas': mechas_alternantes,
        'gap_post_news': gap_post_news,
        'sweep_without_retest': sweep_without_retest,
        'volume_dissonance': volume_dissonance,
        'displacement_velocity_pips': round(velocity, 1),
        'anomaly_count': anomaly_count,
        'anomalies_active': anomalies_active,
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

def run_ict_pipeline(h1, h4, d1, price, balance, risk_pct, hour=None, news_events=None):
    if len(h1) < 30:
        return {'sweep': {'detected': False},
                'score': {'total': 0, 'executable': False, 'confidence': 0, 'factors': {},
                          'reasons': ['Velas insuficientes'], 'action': None}, 'levels': None,
                'regime': {'type': 'unknown', 'volatility_z': 0.0, 'trending_score': 0.0,
                           'regime_quality': 'insufficient_data', 'momentum_consistency': 0.0},
                'anomalies': {'anomaly_count': 0, 'severity': 'none', 'anomalies_active': []}}
    atr = compute_atr(h1); atr_pips = atr * 10000
    h = hour if hour is not None else now_et().hour
    kill = get_killzone(h)
    levels = identify_liquidity_levels(h1, d1[-5:] if d1 and len(d1) >= 5 else None)
    htf_bias, htf_str = detect_htf_bias(d1, h1)
    inducement = detect_inducement(h1)
    sweep = detect_sweep(h1, levels, atr)

    # FASE 2: Regime + Anomalies (siempre se calculan)
    regime = detect_regime(h1, d1)
    anomalies = detect_anomalies(h1, news_events=news_events, candle_dt=now_et())

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

Python ya tomo la decision tecnica (BUY o SELL) basada en su motor ICT/SMC determinístico.
TU TRABAJO NO ES decidir direccion. TU TRABAJO ES validar el contexto institucional.

Python ahora te proporciona DOS capas adicionales de contexto cuantitativo:
1. REGIME: regimen del mercado (trending/ranging/expansion/compression/choppy) + quality
2. ANOMALIES_DETECTED: senales anomalas cuantificadas (mechas multiples, gaps, sweeps sin retest, etc.)

Tu trabajo es INTERPRETAR estas senales junto con el setup tecnico.

Tu output DEBE ser un JSON con estos campos exactos:

{
  "veto": false,
  "veto_reason": null,
  "confidence_multiplier": 0.95,
  "narrative_quality": "clean",
  "regime_assessment": "expansion saludable post-Asia",
  "anomalies": [],
  "narrative": "Sweep limpio en PDH con mecha 68% seguido de displacement strong. Regime expansion + 0 anomalias = setup A+."
}

REGLAS ESTRICTAS:
- Tu NO puedes incluir el campo "action".
- "veto": true SOLO si detectas incoherencia institucional grave.
- "confidence_multiplier": rango 0.5-1.0.
- "narrative_quality": "clean" / "acceptable" / "dirty"
- "anomalies": lista corta de strings interpretadas (no copies anomalies_detected, interpretalas).
- "narrative": max 500 caracteres. Menciona regime y anomalias si son relevantes.

INTERPRETACION DE REGIME:
- expansion + trending_score alto = entorno ideal para tu setup tecnico
- compression = peligroso para setups de continuacion, mejor reversal
- choppy + regime_quality 'choppy' = considera vetar
- ranging con setup direccional = reducir confidence_multiplier

INTERPRETACION DE ANOMALIES_DETECTED:
- severity 'high' (3+ anomalias) = considera vetar si tambien hay contexto debil
- gap_post_news=true = vetar siempre
- sweep_without_retest=true en setup de continuacion = warning, reduce multiplier
- mechas_consecutivas >= 2 = indecision, reduce multiplier
- volume_dissonance=true = warning de delivery debil

CRITERIOS PARA VETAR:
- Delivery institucional incoherente
- Post-news chaos (gap_post_news=true)
- Regime mismatch claro
- Stale liquidity
- Severity 'high' + setup tecnico borderline (score 58-65)

DAYS OF CAUTION (NUEVO v2.4):
Si is_caution_day=true (Lunes/Viernes), el campo day_statistics_warning te dará el contexto histórico.
ESTOS DÍAS SON ESTADÍSTICAMENTE DÉBILES:
- Lunes: WR histórico 20%, PnL -$1,585 en 10 trades
- Viernes: WR histórico 33%, PnL -$1,665 en 12 trades
- Incluso trades con score 75+ HAN FALLADO en lunes en el histórico

EN DÍAS DE CAUTION, SÉ EXTREMADAMENTE ESTRICTO:
- La carga de prueba está en APROBAR, no en vetar
- Si tienes CUALQUIER duda, VETA
- Solo aprobar setups ELITE (no buenos, ELITE)
- Confidence_multiplier debe ser >= 0.85 para que el trade pase (el sistema vetará si es menor)
- Si el régimen no es claramente trending o expansion, VETA
- Si hay cualquier anomalía medium o high, VETA

NO vetes por feeling. Solo veta con razon concreta apoyada en datos cuantitativos.
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
        # FASE 2: Regime + Anomalies enriquecen el contexto cognitivo
        'regime': ict.get('regime', {}),
        'anomalies_detected': ict.get('anomalies', {}),
        'liq_target': ict.get('liq_target'),
        'context': {'last_5_decisions': (recent_history or [])[-5:],
                    'consecutive_losses': state.get('consecutive_losses', 0),
                    'defensive_mode': state.get('defensive_mode', False)},
        # NUEVO v2.4: Estadísticas históricas del día (Days of Caution)
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
    
    # ═══ DAYS OF CAUTION FILTER (NUEVO v2.4) ═══
    # Si es lunes/viernes, aplicar filtros adicionales antes de aprobar
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
    
    # Si el caution filter rechaza el trade, VETO
    if not caution_filter_result['allow']:
        log.warning(f"[CAUTION_DAY] Trade vetado: {caution_filter_result['veto_reason']} - {caution_filter_result['detail']}")
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
    
    # Aplicar risk multiplier del caution filter (0.5 si caution day, 1.0 si normal)
    risk_mult = caution_filter_result['risk_multiplier']
    final_pos_size = levels['pos_size'] if levels else 0
    if risk_mult < 1.0 and final_pos_size > 0:
        final_pos_size = int(final_pos_size * risk_mult)
        log.info(f"[CAUTION_DAY] Risk reducido a {risk_mult*100:.0f}%: pos_size {levels['pos_size']} -> {final_pos_size}")
    
    return {'action': technical_action, 'confidence': confidence_final,
            'score': technical_score, 'source': 'validated' + ('_caution' if caution_filter_result['caution_mode'] else ''),
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
                    f'Sistema detectó {sl_count} SL en los últimos 10 trades. Score mínimo aumentado +10 puntos.',
                    {'SL recientes': sl_count, 'Total últimos 10': len(recent10)}))
        elif sl_count <= 1 and state.get('defensive_mode'):
            state['defensive_mode'] = False; state['defensive_reason'] = ''
    memory['last_updated'] = now_utc().isoformat()
    storage_write_json('memory/edge_tracker.json', memory)
    storage_write_json('memory/session_stats.json', memory.get('session_stats', {}))


async def run_analysis(auto_execute=False):
    log.info('=== ANALISIS EUR/USD ===')
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
    et = now_et()
    news_blocked, news_reason = is_news_blocked(et, all_news)
    if news_blocked: log.info(f'[NEWS] Bloqueado: {news_reason}')

    h1 = h4 = d1 = []
    try:
        h1 = await get_candles('H1', 80)
        h4 = await get_candles('H4', 50)
        d1 = await get_candles('D', 10)
        log.info(f'[VELAS] H1:{len(h1)} H4:{len(h4)} D1:{len(d1)}')
    except Exception as e: log.error(f'[VELAS] {e}')

    price = 0.0
    try: price = await get_price()
    except Exception:
        if h1: price = float(h1[-1]['mid']['c'])

    ict = run_ict_pipeline(h1, h4, d1, price, state['balance'], state['risk_pct_current'],
                            news_events=all_news)
    log.info(f'[ICT] sweep:{ict["sweep"].get("detected")} score:{ict["score"]["total"]}/100')
    if ict.get('regime'):
        r = ict['regime']
        log.info(f'[REGIME] type={r.get("type")} vol_z={r.get("volatility_z")} '
                 f'trending={r.get("trending_score")} quality={r.get("regime_quality")}')
    if ict.get('anomalies', {}).get('anomaly_count', 0) > 0:
        a = ict['anomalies']
        log.info(f'[ANOMALY] count={a.get("anomaly_count")} severity={a.get("severity")} '
                 f'active={a.get("anomalies_active", [])}')

    cognitive = None
    if ict.get('score', {}).get('executable') and ict['sweep'].get('detected'):
        cognitive = await call_cognitive_layer(ict, state.get('history', [])[-10:])
        if cognitive:
            log.info(f'[COGNITIVE] veto={cognitive.veto} mult={cognitive.confidence_multiplier}')

    decision = decision_gate(ict, cognitive)
    log.info(f'[GATE] {decision["action"]} conf:{decision["confidence"]:.0%} source:{decision["source"]}')

    # NOTIFICACION: si Claude veto, mandar email
    if decision.get('source') == 'vetoed':
        subj, html = build_cognitive_veto_email(decision)
        asyncio.create_task(send_email(subj, html))

    audit_record = {
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

    state['last_analysis'] = {'ict': ict, 'price': price}
    state['last_decision'] = decision
    state['last_update']   = now_utc().isoformat()

    et_now = now_et()
    legacy_entry = {
        'timestamp': now_utc().isoformat(),
        'date': et_now.strftime('%Y-%m-%d'),
        'time': et_now.strftime('%H:%M ET'),
        'day_of_week': et_now.strftime('%A'),
        'month': et_now.strftime('%B %Y'),
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

    if auto_execute and is_session() and not news_blocked:
        await execute_signal(decision)
    log.info('=== FIN ANALISIS ===')


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
                    'TRADING PAUSADO · DAILY LOSS LIMIT',
                    f'Sistema alcanzó el límite de pérdida diaria del {MAX_DAILY_LOSS}% del balance. Trading pausado hasta mañana 3:00 AM ET.',
                    {'Pérdida diaria': f'${state["daily_loss_usd"]:,.2f}',
                     'Balance': f'${bal:,.2f}',
                     'Pérdida %': f'{(state["daily_loss_usd"]/bal*100):.2f}%'}))
        if state['consecutive_losses'] >= CONSEC_PAUSE:
            state['risk_pct_current'] = max(0.25, state['risk_pct_current'] / 2)
            if state['consecutive_losses'] != prev_consec:
                asyncio.create_task(_send_critical_alert(
                    'RIESGO REDUCIDO · LOSSES CONSECUTIVOS',
                    f'{state["consecutive_losses"]} pérdidas consecutivas. Riesgo por trade reducido automáticamente.',
                    {'Pérdidas consecutivas': state['consecutive_losses'],
                     'Nuevo risk %': f'{state["risk_pct_current"]:.2f}%'}))
    else:
        state['consecutive_losses'] = 0
        if state['risk_pct_current'] < RISK_PCT: state['risk_pct_current'] = RISK_PCT

async def execute_signal(decision):
    if state['trading_paused'] or decision['action'] == 'HOLD': return
    if decision['confidence'] < MIN_CONFIDENCE: return
    sl = decision.get('sl', 0); tp = decision.get('tp1', 0)
    sz = max(1000, int(decision.get('pos_size', 1000)))
    if sl <= 0 or tp <= 0: return
    try:
        result = await place_order(sz, sl, tp, decision['action'])
        fill = result.get('orderFillTransaction', {})
        trade_id = fill.get('tradeOpened', {}).get('tradeID', '')
        if trade_id:
            state['active_trades_meta'][trade_id] = {
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
            # NOTIFICACION: trade abierto
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
            # NOTIFICACION: trade cerrado
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

def simulate_trade(candles, entry_idx, action, sl, tp1, tp2, balance, atr):
    entry_p = float(candles[entry_idx]['mid']['c'])
    spread = 0.00015; slip = 0.00003
    fill = entry_p + (spread + slip) if action == 'BUY' else entry_p - (spread + slip)
    sl_dist = abs(fill - sl)
    if sl_dist <= 0: return None
    risk_usd = balance * (RISK_PCT / 100)
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

async def run_backtesting():
    if bt_state['running']: return
    bt_state['running'] = True
    log.info('[BT] BACKTESTING EUR/USD INICIANDO')
    all_trades = []; trade_log = []; balance = state.get('balance', 110000.0)
    ET = ZoneInfo('America/New_York')
    live_news = []
    try: live_news = await fetch_ff_calendar()
    except Exception: pass
    all_news = HIGH_IMPACT_EVENTS + live_news
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
        log.info(f'[BT] {len(h1)} velas H1 totales')
        d1_all = []
        try: d1_all = await get_candles_to('D', 300)
        except Exception: pass
        cnt = defaultdict(int); last_day = ''; last_idx = -999
        d1_by_date = {c.get('time','')[:10]: i for i, c in enumerate(d1_all)}
        for i in range(30, len(h1) - 26):
            c_time = h1[i].get('time', ''); c_price = float(h1[i]['mid']['c'])
            try:
                cdt = datetime.fromisoformat(c_time.replace('Z', '+00:00')).astimezone(ET)
                c_h = cdt.hour; c_dow = cdt.weekday(); c_day = cdt.strftime('%Y-%m-%d')
                if c_dow in (5, 6): cnt['weekend'] += 1; continue
                if not (SESSION_START_ET <= c_h < SESSION_END_ET): cnt['sesion'] += 1; continue
            except Exception:
                c_day = c_time[:10]; c_h = 8; cdt = None
            kill = get_killzone(c_h)
            if not kill: cnt['sesion'] += 1; continue
            if cdt:
                nb, _ = is_news_blocked(cdt, all_news)
                if nb: cnt['news'] += 1; continue
            else: nb = False
            if c_day == last_day: cnt['cooldown'] += 1; continue
            if i - last_idx < 5: cnt['cooldown'] += 1; continue
            h1_w = h1[max(0, i-60):i+1]
            atr = compute_atr(h1_w); atr_p = atr * 10000
            if atr_p < ATR_MIN_PIPS or atr_p > ATR_MAX_PIPS: cnt['vol'] += 1; continue
            d1_idx = d1_by_date.get(c_day, -1)
            d1_w = d1_all[:d1_idx+1] if d1_idx >= 0 else []
            liq = identify_liquidity_levels(h1_w, d1_w[-5:] if len(d1_w) >= 5 else None)
            htf_b, htf_s = detect_htf_bias(d1_w[-5:] if len(d1_w) >= 5 else None, h1_w)
            ind = detect_inducement(h1_w); sweep = detect_sweep(h1_w, liq, atr)
            if not sweep.get('detected'): cnt['no_sweep'] += 1; continue
            action = 'SELL' if sweep['direction'] == 'bearish' else 'BUY'
            df, ds, _ = detect_displacement(h1_w, action, atr)
            bos_data = detect_bos(h1_w, action, atr)
            fvg_ob = detect_fvg_ob(h1_w, action, atr)
            tl, tt, tr = compute_liquidity_target(action, liq, c_price, atr)
            adr_pct = 0.5
            day_c = [c for c in h1_w[-24:] if c.get('time','')[:10] == c_day]
            if len(day_c) >= 3:
                dh = max(float(c['mid']['h']) for c in day_c); dl = min(float(c['mid']['l']) for c in day_c)
                adr_pct = max(0, 1 - (dh - dl) / (atr * 8))
            if adr_pct < ADR_MIN: cnt['adr'] += 1; continue
            rh = [float(c['mid']['h']) for c in h1_w[-8:]]; rl = [float(c['mid']['l']) for c in h1_w[-8:]]
            consol_ok = (max(rh) - min(rl)) >= atr * 0.8
            c_dow_n = cdt.weekday() if cdt else 2
            min_sc = MIN_SCORE_WEDTHU if c_dow_n in (2, 3) else MIN_SCORE
            min_sc += 10 if state.get('defensive_mode') else 0
            score = compute_score(htf_b, htf_s, sweep, ind, ds, bos_data, fvg_ob,
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
                  'htf_bias': htf_b, 'adr_pct': adr_pct, 'news_blocked': nb,
                  'sweep_quality': sweep.get('quality','')}
            analysis, rec, fail_factors = generate_post_mortem_local(td)
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
                'sweep_level_type': sweep.get('level_type',''),
                'sweep_quality': sweep.get('quality',''),
                'wick_pct': sweep.get('wick_pct',0),
                'htf_bias': htf_b, 'htf_strength': round(htf_s,2),
                'bos_quality': bos_data[1], 'displacement_strength': ds,
                'inducement_quality': ind[1],
                'liq_obj_level': round(tl,5) if tl else 0, 'liq_obj_type': tt,
                'adr_pct': round(adr_pct,2), 'atr_pips': round(atr_p,1),
                'velas_duration': sim['velas_duration'],
                'sl_moved_be': sim['sl_moved_be'], 'be_vela': sim['be_vela'],
                'be_reason': sim.get('be_reason',''),
                'partial_closed': sim['partial_closed'],
                'pnl_parcial': sim['pnl_parcial'],
                'tp1_vela': sim['tp1_vela'], 'gestion': sim['gestion'],
                'news_blocked': nb,
                'score_factors': ' | '.join(f"{k}:{v['pts']:.0f}" for k, v in score['factors'].items()),
                'confirmaciones': f"Sweep {sweep.get('quality','')} | HTF {htf_b} | BOS {bos_data[1]}",
                'ceo_analysis': analysis, 'ceo_recommendation': rec,
                'failure_factors': ', '.join(fail_factors) if fail_factors else 'N/A'})
        log.info(f'[BT] {len(all_trades)} trades')
    except Exception as e:
        log.error(f'[BT] Error: {e}', exc_info=True)
    if all_trades:
        wins = [t for t in all_trades if t['result'] > 0]
        bes = [t for t in all_trades if t['outcome'] == 'BE']
        pnl = sum(t['result'] for t in all_trades); wr = len(wins) / len(all_trades)
        gw = sum(t['result'] for t in all_trades if t['result'] > 0)
        gl = abs(sum(t['result'] for t in all_trades if t['result'] < 0))
        pf = round(gw / gl, 2) if gl > 0 else 0
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
            'max_drawdown': round(max_dd*100,2),
            'trades_per_week': round(len(all_trades)/52,1)}
        all_trades.sort(key=lambda x: x['date'], reverse=True)
        bt_state['trades'] = all_trades[:300]
        bt_state['last_run'] = now_utc().isoformat()
        bt_state['log'] = trade_log
        storage_write_json('backtest/latest_run.json',
                           {'summary': bt_state['summary'], 'trades': bt_state['trades'],
                            'last_run': bt_state['last_run']})
        log.info(f'[BT] COMPLETADO: {len(all_trades)} trades WR:{wr:.1%}')
    bt_state['running'] = False


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 15: SCHEDULED REPORTS (NUEVO v2.1)
# ═══════════════════════════════════════════════════════════════════════════════

async def scheduled_pre_london_report():
    log.info('[NOTIFY] Generando reporte pre-Londres 7AM')
    try:
        try:
            acc = await get_account()
            state['balance'] = float(acc.get('balance', state['balance']))
        except Exception: pass
        if not state.get('last_analysis'):
            await run_analysis(auto_execute=False)
        subj, html = build_pre_london_report()
        ok = await send_email(subj, html)
        log.info(f'[NOTIFY] Pre-Londres: {"OK" if ok else "FAIL"}')
    except Exception as e:
        log.error(f'[NOTIFY] Pre-Londres error: {e}', exc_info=True)

async def scheduled_ny_open_report():
    log.info('[NOTIFY] Generando reporte NY Open 9AM')
    try:
        await run_analysis(auto_execute=False)
        subj, html = build_ny_open_report()
        ok = await send_email(subj, html)
        log.info(f'[NOTIFY] NY Open: {"OK" if ok else "FAIL"}')
    except Exception as e:
        log.error(f'[NOTIFY] NY Open error: {e}', exc_info=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 15b: HEALTH MONITORING + ALERTAS AUTOMATICAS (NUEVO v2.3)
# ═══════════════════════════════════════════════════════════════════════════════

_last_alert_sent = {}  # cache para no spamear alertas

async def healthcheck_monitor():
    """
    Verifica si el sistema ha ejecutado analisis recientemente.
    Si lleva mas de 2 horas sin analisis en horario de mercado -> alerta critica.
    """
    try:
        now = now_et()
        # Solo aplica en horario de mercado (lunes-viernes, 3 AM - 5 PM ET)
        if now.weekday() >= 5:  # Sabado o Domingo
            return
        if now.hour < 3 or now.hour >= 17:  # Fuera de horario activo
            return

        last_update = state.get('last_update')
        if not last_update:
            return

        try:
            last_dt = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            now_utc = datetime.now(timezone.utc)
            hours_since = (now_utc - last_dt).total_seconds() / 3600
        except Exception as e:
            log.warning(f'[HEALTHCHECK] Error parsing date: {e}')
            return

        if hours_since >= 2.0:
            alert_key = 'no_analysis_2h'
            last_sent = _last_alert_sent.get(alert_key, 0)
            if time.time() - last_sent > 3600:  # No repetir mas de 1 vez/hora
                log.warning(f'[HEALTHCHECK] Sin analisis hace {hours_since:.1f}h')
                await _send_critical_alert(
                    'Scheduler Inactivo',
                    f'El sistema lleva {hours_since:.1f} horas sin ejecutar analisis en horario de mercado.',
                    {'hours_since_last': round(hours_since, 1),
                     'last_update': last_update,
                     'recommendation': 'Revisar logs de Railway para identificar el problema'}
                )
                _last_alert_sent[alert_key] = time.time()
    except Exception as e:
        log.error(f'[HEALTHCHECK] {e}')


async def drawdown_monitor():
    """
    Verifica si el drawdown semanal supera el 5%.
    Si lo supera -> alerta critica.
    """
    try:
        # Obtener trades de los ultimos 7 dias
        if not Path(f'{DATA_PATH}/trades.jsonl').exists():
            return
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        weekly_pnl = 0.0
        weekly_trades = 0
        with open(f'{DATA_PATH}/trades.jsonl') as f:
            for line in f:
                try:
                    t = json.loads(line)
                    closed = t.get('closed_at') or t.get('opened_at')
                    if not closed:
                        continue
                    closed_dt = datetime.fromisoformat(closed.replace('Z', '+00:00'))
                    if closed_dt.tzinfo is None:
                        closed_dt = closed_dt.replace(tzinfo=timezone.utc)
                    if closed_dt >= cutoff:
                        weekly_pnl += float(t.get('result_usd', 0))
                        weekly_trades += 1
                except Exception:
                    continue

        if weekly_trades == 0:
            return

        # Calcular drawdown semanal
        initial_balance = state.get('balance', 100000) - weekly_pnl  # balance al inicio de semana
        if initial_balance <= 0:
            return
        weekly_dd_pct = (weekly_pnl / initial_balance) * 100

        if weekly_dd_pct <= -5.0:
            alert_key = 'drawdown_5pct'
            last_sent = _last_alert_sent.get(alert_key, 0)
            if time.time() - last_sent > 21600:  # No repetir mas de 1 vez/6h
                log.warning(f'[DD-MONITOR] DD semanal: {weekly_dd_pct:.2f}%')
                await _send_critical_alert(
                    'Drawdown Critico Semanal',
                    f'Drawdown de {abs(weekly_dd_pct):.2f}% en los ultimos 7 dias ({weekly_trades} trades).',
                    {'weekly_pnl_usd': round(weekly_pnl, 2),
                     'weekly_drawdown_pct': round(weekly_dd_pct, 2),
                     'trades_last_7d': weekly_trades,
                     'current_balance': round(state.get('balance', 0), 2),
                     'recommendation': 'Considerar pausar trading manualmente y revisar estadisticas'}
                )
                _last_alert_sent[alert_key] = time.time()
    except Exception as e:
        log.error(f'[DD-MONITOR] {e}')


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 15c: REPORTE SEMANAL ESTADISTICO (NUEVO v2.3)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_weekly_stats():
    """Calcula estadisticas de la ultima semana."""
    stats = {
        'total_trades': 0,
        'wins': 0,
        'losses': 0,
        'be': 0,
        'pnl_total': 0.0,
        'win_rate': 0.0,
        'best_trade': None,
        'worst_trade': None,
        'best_trade_pnl': 0.0,
        'worst_trade_pnl': 0.0,
        'by_killzone': {},
        'by_day': {},
        'avg_score': 0.0,
        'avg_confidence': 0.0,
        'cognitive_vetos': 0,
        'total_decisions': 0,
        'cognitive_validations': 0,
    }
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    scores = []
    confidences = []

    # Trades cerrados
    if Path(f'{DATA_PATH}/trades.jsonl').exists():
        with open(f'{DATA_PATH}/trades.jsonl') as f:
            for line in f:
                try:
                    t = json.loads(line)
                    closed = t.get('closed_at') or t.get('opened_at')
                    if not closed:
                        continue
                    closed_dt = datetime.fromisoformat(closed.replace('Z', '+00:00'))
                    if closed_dt.tzinfo is None:
                        closed_dt = closed_dt.replace(tzinfo=timezone.utc)
                    if closed_dt < cutoff:
                        continue
                    stats['total_trades'] += 1
                    pnl = float(t.get('result_usd', 0))
                    stats['pnl_total'] += pnl
                    outcome = t.get('outcome', '')
                    if outcome in ('TP', 'TP2'):
                        stats['wins'] += 1
                    elif outcome == 'SL':
                        stats['losses'] += 1
                    elif outcome == 'BE':
                        stats['be'] += 1
                    if pnl > stats['best_trade_pnl']:
                        stats['best_trade_pnl'] = pnl
                        stats['best_trade'] = t
                    if pnl < stats['worst_trade_pnl']:
                        stats['worst_trade_pnl'] = pnl
                        stats['worst_trade'] = t
                    kz = t.get('killzone', 'unknown')
                    if kz not in stats['by_killzone']:
                        stats['by_killzone'][kz] = {'trades': 0, 'wins': 0, 'pnl': 0}
                    stats['by_killzone'][kz]['trades'] += 1
                    stats['by_killzone'][kz]['pnl'] += pnl
                    if outcome in ('TP', 'TP2'):
                        stats['by_killzone'][kz]['wins'] += 1
                    day = closed[:10]
                    if day not in stats['by_day']:
                        stats['by_day'][day] = 0
                    stats['by_day'][day] += pnl
                except Exception:
                    continue

    if stats['total_trades'] > 0:
        stats['win_rate'] = stats['wins'] / stats['total_trades']

    # Decisiones de auditoria
    month_key = datetime.now(timezone.utc).strftime('%Y-%m')
    audit_file = f'{DATA_PATH}/audit/decisions_{month_key}.jsonl'
    if Path(audit_file).exists():
        with open(audit_file) as f:
            for line in f:
                try:
                    d = json.loads(line)
                    ts = d.get('timestamp_utc', '')
                    if not ts:
                        continue
                    d_dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    if d_dt.tzinfo is None:
                        d_dt = d_dt.replace(tzinfo=timezone.utc)
                    if d_dt < cutoff:
                        continue
                    stats['total_decisions'] += 1
                    if d.get('cognitive_called'):
                        stats['cognitive_validations'] += 1
                    if d.get('cognitive_veto'):
                        stats['cognitive_vetos'] += 1
                    s = d.get('technical_score', 0)
                    c = d.get('technical_confidence', 0)
                    if s > 0:
                        scores.append(s)
                    if c > 0:
                        confidences.append(c)
                except Exception:
                    continue

    if scores:
        stats['avg_score'] = sum(scores) / len(scores)
    if confidences:
        stats['avg_confidence'] = sum(confidences) / len(confidences)

    return stats


def build_weekly_stats_report():
    """Genera HTML del reporte semanal sin Claude."""
    stats = compute_weekly_stats()

    wr = stats['win_rate'] * 100
    wr_color = 'green' if wr >= 55 else 'gold' if wr >= 45 else 'red'
    pnl_color = 'green' if stats['pnl_total'] >= 0 else 'red'

    best_html = ''
    if stats['best_trade']:
        bt = stats['best_trade']
        best_html = f"""<div class="card"><div class="label">🏆 Mejor Trade de la Semana</div>
<div class="row"><span class="k">Fecha / Hora</span><span class="v">{bt.get('opened_at', '')[:16]}</span></div>
<div class="row"><span class="k">Accion</span><span class="v gold">{bt.get('action', '')}</span></div>
<div class="row"><span class="k">Outcome</span><span class="v green">{bt.get('outcome', '')}</span></div>
<div class="row"><span class="k">P&L</span><span class="v green">+${stats['best_trade_pnl']:,.2f}</span></div>
<div class="row"><span class="k">Killzone</span><span class="v">{bt.get('killzone', '')}</span></div></div>"""

    worst_html = ''
    if stats['worst_trade'] and stats['worst_trade_pnl'] < 0:
        wt = stats['worst_trade']
        worst_html = f"""<div class="card"><div class="label">📉 Peor Trade de la Semana</div>
<div class="row"><span class="k">Fecha / Hora</span><span class="v">{wt.get('opened_at', '')[:16]}</span></div>
<div class="row"><span class="k">Accion</span><span class="v gold">{wt.get('action', '')}</span></div>
<div class="row"><span class="k">Outcome</span><span class="v red">{wt.get('outcome', '')}</span></div>
<div class="row"><span class="k">P&L</span><span class="v red">${stats['worst_trade_pnl']:,.2f}</span></div>
<div class="row"><span class="k">Killzone</span><span class="v">{wt.get('killzone', '')}</span></div></div>"""

    kz_html = '<div class="card"><div class="label">📊 Estadisticas por Killzone</div>'
    for kz, s in stats['by_killzone'].items():
        kz_wr = (s['wins'] / s['trades'] * 100) if s['trades'] > 0 else 0
        kz_html += f'<div class="row"><span class="k">{kz}</span><span class="v">{s["trades"]} trades · WR {kz_wr:.0f}% · ${s["pnl"]:+,.2f}</span></div>'
    if not stats['by_killzone']:
        kz_html += '<div style="color:#5a7a68; font-style:italic">Sin trades esta semana</div>'
    kz_html += '</div>'

    body = f"""
<div class="card"><div class="label">📅 Semana del {(datetime.now(timezone.utc) - timedelta(days=7)).strftime('%d %b')} al {datetime.now(timezone.utc).strftime('%d %b %Y')}</div>
<div style="display:flex; gap:20px; margin-top:8px; flex-wrap:wrap">
<div><div style="font-size:9px;color:#5a7a68;letter-spacing:0.1em">TRADES TOTALES</div><div style="font-family:monospace;font-size:24px;font-weight:700">{stats['total_trades']}</div></div>
<div><div style="font-size:9px;color:#5a7a68;letter-spacing:0.1em">WIN RATE</div><div style="font-family:monospace;font-size:24px;font-weight:700" class="{wr_color}">{wr:.0f}%</div></div>
<div><div style="font-size:9px;color:#5a7a68;letter-spacing:0.1em">P&L SEMANAL</div><div style="font-family:monospace;font-size:24px;font-weight:700" class="{pnl_color}">${stats['pnl_total']:+,.2f}</div></div>
</div>
<div class="row" style="margin-top:14px"><span class="k">Wins / Losses / BE</span><span class="v">{stats['wins']} / {stats['losses']} / {stats['be']}</span></div>
<div class="row"><span class="k">Total decisiones tomadas</span><span class="v">{stats['total_decisions']}</span></div>
<div class="row"><span class="k">Validaciones cognitivas</span><span class="v">{stats['cognitive_validations']}</span></div>
<div class="row"><span class="k">Vetos cognitivos</span><span class="v">{stats['cognitive_vetos']}</span></div>
<div class="row"><span class="k">Score promedio</span><span class="v">{stats['avg_score']:.1f}/100</span></div>
<div class="row"><span class="k">Confianza promedio</span><span class="v">{stats['avg_confidence']*100:.0f}%</span></div>
</div>
{best_html}{worst_html}{kz_html}
<div class="card"><div class="label">📋 Notas</div>
<div style="font-size:11px;color:#c4d8cc;line-height:1.6">
• Este es el reporte automatico determinista. NO usa Claude para analisis cualitativo.<br>
• El Weekly Cognitive Review con Claude Opus se activara cuando acumulen 20-30 trades reales.<br>
• Recordatorio: el sistema esta en demo OANDA con balance ~${state.get('balance', 0):,.2f}.</div></div>"""

    subject = f'TPDCM-IA · Reporte Semanal · {stats["total_trades"]} trades · WR {wr:.0f}% · ${stats["pnl_total"]:+,.2f}'
    return subject, _email_wrapper('📊 Reporte Semanal', body)


async def scheduled_weekly_stats_report():
    """Job: Domingos 18:00 ET, envia reporte semanal."""
    try:
        subject, html = build_weekly_stats_report()
        await send_email(subject, html, category='weekly_stats_report')
        log.info('[WEEKLY-STATS] Reporte semanal enviado')
    except Exception as e:
        log.error(f'[WEEKLY-STATS] Error: {e}')


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 15d: CONVERSACION LIBRE CON CLAUDE (NUEVO v2.3)
# ═══════════════════════════════════════════════════════════════════════════════

CHAT_SYSTEM_PROMPT = """Eres el analista institucional senior del sistema TPDCM-IA.
El usuario es una trader profesional que opera EUR/USD con metodologia ICT/SMC.
Tienes acceso al estado actual completo del mercado y del sistema.

Responde de forma CONCISA, PROFESIONAL y ESTRUCTURADA:
- Maximo 250 palabras por respuesta
- Usa lenguaje claro pero tecnico (asume que la usuaria conoce ICT)
- Si no tienes datos suficientes, di "Sin datos suficientes para responder eso"
- Si te preguntan por predicciones especificas, recuerda que NO predices precios futuros,
  solo interpretas la informacion actual y los regimenes detectados
- Usa los datos reales del contexto que recibes
- Si te preguntan algo fuera del scope del sistema (politica, etc.), redirigir amablemente al mercado

Tu personalidad: profesional, directa, util, sin exceso de cumplidos.
"""


async def claude_conversation(user_message: str, conversation_history: list = None) -> dict:
    """
    Permite al usuario conversar libremente con Claude sobre el mercado.
    Claude tiene contexto del estado actual del sistema.
    """
    if not ANTHROPIC_API_KEY:
        return {'error': 'API key no configurada', 'response': None}

    # Construir contexto actual del sistema
    ict = state.get('last_analysis', {}).get('ict', {})
    dec = state.get('last_decision', {}) or {}
    regime = ict.get('regime', {})
    anomalies = ict.get('anomalies', {})

    context = f"""ESTADO ACTUAL DEL SISTEMA TPDCM-IA:

Precio EUR/USD: {state.get('last_analysis', {}).get('price', 'N/A')}
Balance: ${state.get('balance', 0):,.2f}
Edge Score: {memory.get('edge_score', 100):.0f}/100
Defensive Mode: {memory.get('defensive_mode', False)}

ULTIMA DECISION:
- Accion: {dec.get('action', 'HOLD')}
- Score tecnico: {dec.get('score', 0)}/100
- Confianza: {dec.get('confidence', 0)*100:.0f}%
- Source: {dec.get('source', 'N/A')}
- Cognitive veto: {dec.get('cognitive_veto', False)}
- Narrativa Claude: {dec.get('narrative', 'N/A')}

REGIMEN ACTUAL:
- Tipo: {regime.get('type', 'unknown')}
- Calidad: {regime.get('regime_quality', 'unknown')}
- Volatility Z: {regime.get('volatility_z', 0):+.2f}
- Trending score: {regime.get('trending_score', 0)*100:.0f}%
- Momentum: {regime.get('momentum_consistency', 0)*100:.0f}%
- Chop penalty: {regime.get('chop_penalty', 0):.2f}

ANOMALIAS DETECTADAS:
- Severidad: {anomalies.get('severity', 'none')}
- Count: {anomalies.get('anomaly_count', 0)}
- Activas: {anomalies.get('anomalies_active', [])}

CONTEXTO ICT:
- HTF Bias: {ict.get('htf_bias', 'N/A')} (strength {ict.get('htf_strength', 0)*100:.0f}%)
- Killzone: {ict.get('killzone', 'fuera de sesion')}
- ATR H1: {ict.get('atr_pips', 0):.1f} pips
- ADR restante: {ict.get('adr_pct', 0)*100:.0f}%
- Sweep detectado: {ict.get('sweep', {}).get('detected', False)}

PERDIDAS CONSECUTIVAS: {state.get('consecutive_losses', 0)}
RISK ACTUAL: {state.get('risk_pct_current', 1.0):.2f}%
"""

    # Construir mensajes
    messages = []
    if conversation_history:
        # Limitar a ultimos 6 mensajes para no exceder contexto
        for h in conversation_history[-6:]:
            messages.append({'role': h.get('role', 'user'), 'content': h.get('content', '')})

    messages.append({
        'role': 'user',
        'content': f'{context}\n\nPregunta de la usuaria: {user_message}'
    })

    try:
        async with httpx.AsyncClient(timeout=30.0) as cli:
            r = await cli.post(
                'https://api.anthropic.com/v1/messages',
                headers={'x-api-key': ANTHROPIC_API_KEY,
                         'content-type': 'application/json',
                         'anthropic-version': '2023-06-01'},
                json={'model': 'claude-sonnet-4-6',
                      'max_tokens': 800,
                      'system': CHAT_SYSTEM_PROMPT,
                      'messages': messages}
            )

        if r.status_code != 200:
            log.warning(f'[CHAT] HTTP {r.status_code}: {r.text[:200]}')
            return {'error': f'API error {r.status_code}', 'response': None}

        data = r.json()
        response_text = data.get('content', [{}])[0].get('text', '')

        # Guardar conversacion en historial
        try:
            storage_append_jsonl('claude_conversations/history.jsonl', {
                'ts': datetime.now(timezone.utc).isoformat(),
                'user_message': user_message,
                'response': response_text,
                'context_snapshot': {
                    'price': state.get('last_analysis', {}).get('price'),
                    'regime': regime.get('type'),
                    'last_action': dec.get('action'),
                    'edge_score': memory.get('edge_score', 100),
                }
            })
        except Exception as e:
            log.warning(f'[CHAT] No se pudo guardar historial: {e}')

        return {
            'response': response_text,
            'tokens_used': data.get('usage', {}).get('output_tokens', 0),
            'context_used': {
                'price': state.get('last_analysis', {}).get('price'),
                'regime_type': regime.get('type'),
                'last_action': dec.get('action'),
            },
            'error': None
        }
    except httpx.TimeoutException:
        log.error('[CHAT] Timeout')
        return {'error': 'Timeout - Claude tardo mas de 30s', 'response': None}
    except Exception as e:
        log.error(f'[CHAT] Exception: {e}')
        return {'error': str(e), 'response': None}


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 16: API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.get('/')
@app.head('/')
async def root():
    """Endpoint raiz simple para monitoreo externo (UptimeRobot, etc).
    Responde tanto a GET como HEAD requests."""
    return {'service': 'TPDCM-IA', 'version': '2.4', 'status': 'alive'}


@app.get('/health')
async def health():
    ict = state.get('last_analysis', {}).get('ict', {})
    return {'status': 'ok', 'version': '2.4', 'pair': 'EUR/USD',
            'trading_paused': state['trading_paused'],
            'pause_reason': state['pause_reason'],
            'consecutive_losses': state['consecutive_losses'],
            'daily_loss_usd': state['daily_loss_usd'],
            'risk_pct_current': state['risk_pct_current'],
            'balance': state['balance'],
            'defensive_mode': state.get('defensive_mode', False),
            'defensive_reason': state.get('defensive_reason', ''),
            'edge_score': memory.get('edge_score', 100.0),
            'min_score': MIN_SCORE, 'score_weights': SCORE_WEIGHTS,
            'cognitive_disabled': cognitive_is_disabled(),
            'data_path': DATA_PATH, 'data_path_exists': Path(DATA_PATH).exists(),
            'notifications': {'enabled': NOTIFICATIONS_ENABLED,
                              'configured': bool(RESEND_API_KEY),
                              'email_to': NOTIFY_EMAIL_TO,
                              'email_from': NOTIFY_EMAIL_FROM},
            'auto_execute': AUTO_EXECUTE,
            # FASE 2: Regime + Anomaly summary
            'regime': {
                'type': ict.get('regime', {}).get('type', 'unknown'),
                'volatility_z': ict.get('regime', {}).get('volatility_z', 0),
                'trending_score': ict.get('regime', {}).get('trending_score', 0),
                'quality': ict.get('regime', {}).get('regime_quality', 'unknown'),
            },
            'anomalies': {
                'count': ict.get('anomalies', {}).get('anomaly_count', 0),
                'severity': ict.get('anomalies', {}).get('severity', 'none'),
                'active': ict.get('anomalies', {}).get('anomalies_active', []),
            }}

@app.get('/dashboard')
async def dashboard():
    ict = state.get('last_analysis', {}).get('ict', {})
    dec = state.get('last_decision', {}) or {}
    score = ict.get('score', {}); sweep = ict.get('sweep', {})
    struct = ict.get('structure', {}); fvgob = ict.get('fvg_ob', {})
    return {'ts': state.get('last_update', now_utc().isoformat()),
            'balance': state['balance'],
            'current_price': state.get('last_analysis', {}).get('price', 0),
            'server': 'online',
            'decision': {
                'action': dec.get('action','HOLD'),
                'confidence': dec.get('confidence',0),
                'score': dec.get('score',0),
                'reason': dec.get('reason',''),
                'macro_context': dec.get('regime_assessment',''),
                'risk_note': ', '.join(dec.get('anomalies', [])) if dec.get('anomalies') else '',
                'recommendation': dec.get('narrative_quality', ''),
                'sl': dec.get('sl',0), 'tp1': dec.get('tp1',0), 'tp2': dec.get('tp2',0),
                'rr1': dec.get('rr1',0), 'rr2': dec.get('rr2',0),
                'pos_size': dec.get('pos_size',0),
                'source': dec.get('source',''),
                'killzone': dec.get('killzone',''),
                'htf_bias': dec.get('htf_bias',''),
                'liq_target': dec.get('liq_target',{}),
                'cognitive_veto': dec.get('cognitive_veto', False),
                'cognitive_multiplier': dec.get('cognitive_multiplier'),
                'narrative_quality': dec.get('narrative_quality'),
                'narrative': dec.get('narrative', ''),
                'anomalies': dec.get('anomalies', []),
                'regime_assessment': dec.get('regime_assessment')},
            'ict': {
                'sweep_detected': sweep.get('detected',False),
                'sweep_direction': sweep.get('direction',''),
                'sweep_level': sweep.get('level',0),
                'sweep_level_type': sweep.get('level_type',''),
                'sweep_quality': sweep.get('quality',''),
                'sweep_wick': sweep.get('wick_pct',0),
                'structure_bos': struct.get('bos',False),
                'structure_bos_q': struct.get('bos_quality',''),
                'score_total': score.get('total',0),
                'score_exec': score.get('executable',False),
                'score_reasons': score.get('reasons',[]),
                'score_factors': score.get('factors',{}),
                'ob': fvgob.get('ob'), 'fvg': fvgob.get('fvg'),
                'entry_zone': fvgob.get('entry_zone',{'high':0,'low':0}),
                'atr': ict.get('atr',0), 'atr_pips': ict.get('atr_pips',0),
                'htf_bias': ict.get('htf_bias',''),
                'htf_strength': ict.get('htf_strength',0),
                'killzone': ict.get('killzone',''),
                'adr_pct': ict.get('adr_pct',0),
                'liq_target': ict.get('liq_target',{}),
                'inducement': ict.get('inducement',{}),
                'displacement': ict.get('displacement',{}),
                # FASE 2: Regime + Anomalies expuestos en dashboard
                'regime': ict.get('regime', {}),
                'anomalies_detected': ict.get('anomalies', {})},
            'history': state.get('history',[])[-20:],
            'open_trades': state.get('open_trades',[]),
            'active_trades_meta': state.get('active_trades_meta', {}),
            'risk_status': {
                'trading_paused': state['trading_paused'],
                'pause_reason': state['pause_reason'],
                'consecutive_losses': state['consecutive_losses'],
                'daily_loss_usd': state['daily_loss_usd'],
                'risk_pct_current': state['risk_pct_current']},
            'memory': {
                'edge_score': memory.get('edge_score',100.0),
                'session_stats': memory.get('session_stats',{}),
                'recent_trades_count': len(memory.get('recent_trades',[])),
                'defensive_mode': state.get('defensive_mode',False),
                'defensive_reason': state.get('defensive_reason','')},
            'learning': {'trades_analyzed': len(bt_state.get('log',[])),
                         'current_weights': SCORE_WEIGHTS},
            'cognitive': {'disabled': cognitive_is_disabled(),
                          'recent_calls': len(_cognitive_health.get('calls', [])),
                          'recent_failures': len(_cognitive_health.get('failures', []))}}

@app.get('/prices')
async def prices():
    try:
        price = await get_price(); ot = await get_open_trades()
        return {'price': price, 'open_trades': ot, 'balance': state['balance']}
    except Exception as e:
        return {'price': 0, 'open_trades': [], 'error': str(e)}

@app.get('/candles')
async def candles_endpoint(tf: str = 'H1', count: int = 80):
    tf = tf.upper()
    valid_tf = ('M5', 'M15', 'H1', 'H4', 'D')
    if tf not in valid_tf: tf = 'H1'
    count = max(10, min(500, int(count)))
    cache_key = f'{tf}_{count}'
    ttl = _CANDLES_CACHE_TTL.get(tf, 300)
    cached = _candles_cache.get(cache_key)
    if cached and (time.time() - cached['ts'] < ttl): return cached['data']
    try:
        candles_raw = await get_candles(tf, count)
        result = []
        for c in candles_raw:
            if not c.get('complete'): continue
            try:
                result.append({'t': c.get('time', ''),
                               'o': float(c['mid']['o']), 'h': float(c['mid']['h']),
                               'l': float(c['mid']['l']), 'c': float(c['mid']['c']),
                               'v': int(c.get('volume', 0))})
            except (KeyError, ValueError, TypeError): continue
        response = {'candles': result, 'granularity': tf, 'count': len(result),
                    'pair': PAIR, 'source': 'oanda', 'ts': now_utc().isoformat()}
        _candles_cache[cache_key] = {'data': response, 'ts': time.time()}
        return response
    except Exception as e:
        log.error(f'[CANDLES] {e}')
        return {'candles': [], 'granularity': tf, 'count': 0, 'error': str(e)}

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

@app.get('/audit/decisions')
async def audit_decisions(month: Optional[str] = None, limit: int = 100):
    if not month: month = now_et().strftime('%Y-%m')
    records = storage_read_jsonl(f'audit/decisions_{month}.jsonl', limit=limit)
    return {'month': month, 'count': len(records), 'decisions': list(reversed(records))}

@app.get('/audit/cognitive-health')
async def audit_cognitive_health():
    return {'disabled': cognitive_is_disabled(),
            'disabled_until': _cognitive_health['disabled_until'],
            'recent_calls_count': len(_cognitive_health['calls']),
            'recent_failures_count': len(_cognitive_health['failures']),
            'failure_rate_1h': (len(_cognitive_health['failures']) / len(_cognitive_health['calls'])
                                if _cognitive_health['calls'] else 0)}

@app.get('/regime')
async def regime_endpoint():
    """FASE 2: Estado actual del regimen de mercado + anomalias detectadas"""
    ict = state.get('last_analysis', {}).get('ict', {})
    return {
        'regime': ict.get('regime', {}),
        'anomalies_detected': ict.get('anomalies', {}),
        'killzone': ict.get('killzone'),
        'htf_bias': ict.get('htf_bias'),
        'htf_strength': ict.get('htf_strength'),
        'atr_pips': ict.get('atr_pips'),
        'adr_pct': ict.get('adr_pct'),
        'last_update': state.get('last_update'),
    }


# Endpoints de notificacion (NUEVOS v2.1)

@app.get('/notify/test')
async def notify_test():
    """Envia correo de prueba para validar Resend"""
    et = now_et()
    body = f"""<div class="card"><div class="label">Correo de Prueba</div>
<div style="margin:10px 0;font-size:14px;color:#00e87a">✓ Sistema de notificaciones funcionando correctamente</div></div>
<div class="card"><div class="label">Configuración</div>
<div class="row"><span class="k">Versión sistema</span><span class="v">2.1</span></div>
<div class="row"><span class="k">Destino</span><span class="v">{NOTIFY_EMAIL_TO}</span></div>
<div class="row"><span class="k">Remitente</span><span class="v">{NOTIFY_EMAIL_FROM}</span></div>
<div class="row"><span class="k">Resend API</span><span class="v green">configurada</span></div>
<div class="row"><span class="k">Notificaciones habilitadas</span><span class="v {'green' if NOTIFICATIONS_ENABLED else 'red'}">{NOTIFICATIONS_ENABLED}</span></div>
<div class="row"><span class="k">Auto Execute</span><span class="v {'green' if AUTO_EXECUTE else 'gold'}">{AUTO_EXECUTE}</span></div>
<div class="row"><span class="k">Generado</span><span class="v">{et.strftime('%H:%M:%S ET')}</span></div></div>
<div class="card"><div class="label">Reportes Programados</div>
<div class="value" style="font-size:11px;line-height:1.8">
🌅 <strong>07:00 AM ET</strong> - Briefing Pre-Londres<br>
🌇 <strong>09:00 AM ET</strong> - NY Open Analysis<br>
🟢 <strong>Al abrir trade</strong> - Notificación de entrada<br>
🔴 <strong>Al cerrar trade</strong> - Outcome + análisis<br>
🟡 <strong>Veto cognitivo</strong> - Cuando Claude rechaza setup<br>
⚠️ <strong>Alertas críticas</strong> - Daily loss / modo defensivo</div></div>"""
    subj = '🧪 TPDCM-IA · Test de Notificaciones'
    html = _email_wrapper('🧪 Test de Notificaciones', body)
    ok = await send_email(subj, html)
    return {'status': 'sent' if ok else 'failed',
            'email_to': NOTIFY_EMAIL_TO,
            'configured': bool(RESEND_API_KEY),
            'enabled': NOTIFICATIONS_ENABLED}

@app.get('/notify/pre-london-report')
async def notify_pre_london_now():
    asyncio.create_task(scheduled_pre_london_report())
    return {'status': 'triggered', 'report': 'pre_london_7am'}

@app.get('/notify/ny-open-report')
async def notify_ny_open_now():
    asyncio.create_task(scheduled_ny_open_report())
    return {'status': 'triggered', 'report': 'ny_open_9am'}

@app.get('/notify/history')
async def notify_history(limit: int = 50):
    records = storage_read_jsonl('notifications/sent.jsonl', limit=limit)
    return {'count': len(records), 'sent': list(reversed(records))}


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS NUEVOS v2.3
# ═══════════════════════════════════════════════════════════════════════════════

class ChatMessageIn(BaseModel):
    message: str
    history: Optional[list] = None


@app.post('/claude-conversation')
async def claude_conversation_endpoint(payload: ChatMessageIn):
    """
    Chat libre con Claude. Recibe un mensaje del usuario y opcionalmente
    historial de la conversacion. Retorna la respuesta de Claude con
    el contexto actual del mercado.
    """
    if not payload.message or len(payload.message.strip()) < 2:
        return {'error': 'Mensaje vacio o muy corto', 'response': None}
    if len(payload.message) > 1000:
        return {'error': 'Mensaje muy largo (max 1000 caracteres)', 'response': None}

    result = await claude_conversation(payload.message.strip(), payload.history or [])
    return result


@app.get('/claude-analysis-history')
async def claude_analysis_history(limit: int = 30):
    """Retorna historial de narrativas que Claude ha generado en validaciones."""
    history = []
    now = datetime.now(timezone.utc)
    months_to_check = [now.strftime('%Y-%m'),
                       (now.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')]
    for month in months_to_check:
        audit_file = f'{DATA_PATH}/audit/decisions_{month}.jsonl'
        if not Path(audit_file).exists():
            continue
        with open(audit_file) as f:
            for line in f:
                try:
                    d = json.loads(line)
                    if d.get('cognitive_called') and d.get('narrative'):
                        history.append({
                            'timestamp_et': d.get('timestamp_et'),
                            'timestamp_utc': d.get('timestamp_utc'),
                            'technical_action': d.get('technical_action'),
                            'final_action': d.get('final_action'),
                            'technical_score': d.get('technical_score'),
                            'cognitive_veto': d.get('cognitive_veto'),
                            'cognitive_multiplier': d.get('cognitive_multiplier'),
                            'narrative_quality': d.get('narrative_quality'),
                            'narrative': d.get('narrative'),
                            'regime_assessment': d.get('regime_assessment'),
                            'anomalies': d.get('anomalies'),
                            'price': d.get('price'),
                        })
                except Exception:
                    continue
    history.sort(key=lambda x: x.get('timestamp_utc', ''), reverse=True)
    return {'history': history[:limit], 'total_with_narrative': len(history)}


@app.get('/claude-conversation-history')
async def claude_conversation_history(limit: int = 50):
    """Historial de conversaciones del chat libre con Claude."""
    if not Path(f'{DATA_PATH}/claude_conversations/history.jsonl').exists():
        return {'history': [], 'total': 0}
    history = []
    with open(f'{DATA_PATH}/claude_conversations/history.jsonl') as f:
        for line in f:
            try: history.append(json.loads(line))
            except: pass
    return {'history': history[-limit:][::-1], 'total': len(history)}


@app.get('/weekly-stats')
async def weekly_stats_endpoint():
    """Estadisticas de la ultima semana (sin Claude)."""
    return compute_weekly_stats()


@app.post('/notify/weekly-stats-now')
async def trigger_weekly_stats():
    """Disparar manualmente el reporte semanal."""
    await scheduled_weekly_stats_report()
    return {'status': 'triggered', 'report': 'weekly_stats'}


def _is_market_active():
    now = now_et()
    return now.weekday() < 5 and 3 <= now.hour < 17


@app.get('/caution-days-stats')
async def caution_days_stats():
    """Estadísticas del Days of Caution Engine."""
    now = now_et()
    is_today_caution = is_caution_day()
    
    # Contar vetos por caution day en el audit log
    month_key = now.strftime('%Y-%m')
    audit_file = f'{DATA_PATH}/audit/decisions_{month_key}.jsonl'
    
    caution_stats = {
        'is_today_caution_day': is_today_caution,
        'today_day_name': now.strftime('%A'),
        'caution_days_configured': CAUTION_DAYS,
        'caution_risk_multiplier': CAUTION_RISK_MULTIPLIER,
        'caution_min_score': CAUTION_MIN_SCORE,
        'caution_min_claude_mult': CAUTION_MIN_CLAUDE_MULT,
        'caution_min_htf_strength': CAUTION_MIN_HTF_STRENGTH,
        'blocked_regimes': CAUTION_BLOCKED_REGIMES,
        'blocked_anomalies': CAUTION_BLOCKED_ANOMALIES,
        'historical_data': {
            'monday': {'wr': '20%', 'pnl': '-$1,585', 'trades': 10},
            'tuesday': {'wr': '80%', 'pnl': '+$14,316', 'trades': 10},
            'wednesday': {'wr': '80%', 'pnl': '+$5,618', 'trades': 5},
            'thursday': {'wr': '50%', 'pnl': '+$79', 'trades': 2},
            'friday': {'wr': '33%', 'pnl': '-$1,665', 'trades': 12}
        },
        'this_month_vetos': 0,
        'this_month_caution_trades': 0
    }
    
    # Contar vetos del mes actual
    if Path(audit_file).exists():
        with open(audit_file) as f:
            for line in f:
                try:
                    d = json.loads(line)
                    if d.get('source') == 'caution_day_veto':
                        caution_stats['this_month_vetos'] += 1
                    elif 'validated_caution' in str(d.get('source', '')):
                        caution_stats['this_month_caution_trades'] += 1
                except Exception:
                    continue
    
    return caution_stats


@app.get('/healthcheck-monitor')
async def healthcheck_monitor_endpoint():
    """Estado del sistema de monitoreo de salud."""
    last_update = state.get('last_update')
    hours_since = None
    if last_update:
        try:
            last_dt = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            hours_since = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
        except Exception:
            pass
    return {
        'last_update': last_update,
        'hours_since_last_analysis': round(hours_since, 2) if hours_since else None,
        'analysis_active': hours_since is not None and hours_since < 2.0,
        'is_market_hours': _is_market_active(),
        'alerts_sent_recently': len(_last_alert_sent),
        'last_alerts': _last_alert_sent,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SECCION 17: STARTUP + SCHEDULER
# ═══════════════════════════════════════════════════════════════════════════════

@app.on_event('startup')
async def startup():
    _ensure_data_dirs()
    log.info(f'[STARTUP] Data volume: {DATA_PATH} (exists: {Path(DATA_PATH).exists()})')
    log.info(f'[STARTUP] AUTO_EXECUTE={AUTO_EXECUTE} NOTIFICATIONS={NOTIFICATIONS_ENABLED}')

    try:
        acc = await get_account()
        state['balance'] = float(acc.get('balance', 110000.0))
        log.info(f'[STARTUP] Balance: ${state["balance"]:,.2f}')
    except Exception as e: log.error(f'[STARTUP] Balance: {e}')
    try:
        trades = await get_open_trades()
        state['open_trades'] = trades
        et = now_et()
        if (et.hour >= SESSION_END_ET or et.hour < SESSION_START_ET or et.weekday() in (5,6)) and trades:
            for t in trades:
                try: await close_trade(t['id'])
                except Exception as e: log.error(f'[STARTUP] {e}')
        else:
            log.info(f'[STARTUP] {len(trades)} trades abiertos')
    except Exception as e: log.error(f'[STARTUP] Trades: {e}')

    sched = AsyncIOScheduler(timezone=ZoneInfo('America/New_York'))
    sched.add_job(run_analysis,    'interval', hours=1,   id='analysis', args=[AUTO_EXECUTE])
    sched.add_job(monitor_trades,  'interval', minutes=5, id='monitor')
    sched.add_job(run_backtesting, CronTrigger(hour=3, minute=30, timezone=ZoneInfo('America/New_York')), id='bt_daily')
    sched.add_job(run_analysis, CronTrigger(hour=3,  minute=15, timezone=ZoneInfo('America/New_York')), id='london', args=[False])
    sched.add_job(run_analysis, CronTrigger(hour=8,  minute=0,  timezone=ZoneInfo('America/New_York')), id='ny',     args=[AUTO_EXECUTE])
    sched.add_job(run_analysis, CronTrigger(hour=10, minute=0,  timezone=ZoneInfo('America/New_York')), id='ny2',    args=[AUTO_EXECUTE])
    # Reportes por correo (v2.1)
    sched.add_job(scheduled_pre_london_report, CronTrigger(hour=7, minute=0,
                  timezone=ZoneInfo('America/New_York')), id='report_pre_london')
    sched.add_job(scheduled_ny_open_report,    CronTrigger(hour=9, minute=0,
                  timezone=ZoneInfo('America/New_York')), id='report_ny_open')
    # NUEVOS v2.3: Health monitoring, drawdown monitor, weekly stats
    sched.add_job(healthcheck_monitor, 'interval', minutes=30, id='healthcheck')
    sched.add_job(drawdown_monitor, CronTrigger(hour=12, minute=0,
                  timezone=ZoneInfo('America/New_York')), id='dd_monitor')
    sched.add_job(scheduled_weekly_stats_report, CronTrigger(day_of_week='sun', hour=18, minute=0,
                  timezone=ZoneInfo('America/New_York')), id='weekly_stats')
    sched.start()
    log.info('[SCHEDULER] Activo - analisis/hora | monitor/5min | healthcheck/30min | reportes 7/9 AM | DD daily | Weekly stats domingos 18h ET')

    async def delayed_start():
        await asyncio.sleep(5)
        log.info('[INIT] Analisis inicial...')
        try: await run_analysis(auto_execute=AUTO_EXECUTE)
        except Exception as e: log.error(f'[INIT] {e}')
        bt_flag = f'{DATA_PATH}/bt_done.flag'; should_bt = True
        try:
            with open(bt_flag) as f: last = float(f.read().strip())
            if time.time() - last < 21600: should_bt = False
        except Exception: pass
        if should_bt:
            await asyncio.sleep(3)
            log.info('[INIT] Backtesting...')
            await run_backtesting()
            try:
                with open(bt_flag, 'w') as f: f.write(str(time.time()))
            except Exception: pass
    asyncio.create_task(delayed_start())
    log.info('TPDCM-IA v2.3 - Decision Gate + Cognitive + Regime + Notifications + Health Monitoring + Chat libre - Sistema activo')
