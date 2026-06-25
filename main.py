#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════╗
║   SPACEMAN BOT — Triple Predictor                       ║
║   • 2x  : predictor por timing (ventana avg_gap)        ║
║   • 3x  : predictor por timing eventos ≥3x              ║
║            (incluye rango de cuota derivado del hash)    ║
║   Cada señal tiene 2 intentos                           ║
║   Marcador diario: aciertos 2x                          ║
╚══════════════════════════════════════════════════════════╝
"""

import asyncio
import hashlib
import threading
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional
from flask import Flask, request
import websockets
from telebot.async_telebot import AsyncTeleBot
from telebot import types
import aiohttp

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ.get("BOT_TOKEN", "8889373350:AAFU7R1ENyANVR-DiZbBMbeyAHZOi9DLlXY")
CHANNEL_ID = -1003815888467

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcdk00000005349"
CURRENCY  = "BRL"
GAME_ID   = 1301

# ── Umbrales ──────────────────────────────────────────────────────────────────
TARGET_2X        = 2.00   # señal 2x: acierto si resultado ≥ 2x
TARGET_3X        = 3.00   # señal 3x: objetivo principal
INSURANCE_2X     = 2.00   # seguro 3x: si llega a 2x pero no a 3x → acierto igual
MAX_ATTEMPTS     = 2      # intentos por señal (aplica a ambos tipos)

# ── Predictor 2x ──────────────────────────────────────────────────────────────
MIN_EVENTS_2X     = 15    # mínimo de eventos ≥2x para activar predictor
SIGNAL_WINDOW_2X  = 20    # segundos antes del ETA para disparar señal 2x
MAX_HIST_2X       = 200

# ── Predictor 3x ──────────────────────────────────────────────────────────────
# Usa eventos ≥3x (= 3x, 5x, 10x). Aprende el avg gap entre ellos.
# Dispara señal en ventana ±10s alrededor del ETA exacto:
#   → empieza 10s ANTES del ETA  (window_before=10)
#   → cierra   10s DESPUÉS del ETA (window_after=10)
MIN_EVENTS_3X      = 10   # mínimo de eventos ≥3x para activar predictor 3x
SIGNAL_WINDOW_3X_B = 10   # segundos ANTES del ETA (before)
SIGNAL_WINDOW_3X_A = 10   # segundos DESPUÉS del ETA (after)
MAX_HIST_3X        = 150
COOLDOWN_3X        = 45   # antiflood señales 3x

# ── General ────────────────────────────────────────────────────────────────────
SIGNAL_COOLDOWN_2X = 30
MAX_MULTS          = 400
TRIM_MULTS         = 300
PERSIST_FILE       = "spaceman_history.json"

# ─── ESTADO GLOBAL ────────────────────────────────────────────────────────────
g_mults:    list = []
g_seen_ids: set  = set()

# Historial de eventos por tier
g_events2x:  list = []   # timestamps ≥2x
g_gaps2x:    list = []
g_events3x:  list = []   # timestamps ≥3x (incluye 5x, 10x)
g_gaps3x:    list = []

# Control de señal 2x
g_armed2x:          bool  = False
g_last_fire2x:      float = 0
g_cooldown2x_mod:   int   = 0

# Control de señal 3x
g_armed3x:          bool  = False
g_last_fire3x:      float = 0
g_cooldown3x_mod:   int   = 0

g_all_chats: set = set()

# ─── MARCADOR DIARIO (estadísticas 2x solamente) ──────────────────────────────
# Acierto = en cualquiera de los 2 intentos el resultado llegó ≥ 2x
# (aplica tanto a señales 2x como a señales 3x con seguro)
g_daily_hits:   int = 0
g_daily_misses: int = 0
g_daily_date:   str = ""
g_scoreboard_msg_id: Optional[int] = None

g_last_signal_msgs: dict = {}
_persist_counter:   int  = 0

bot = AsyncTeleBot(BOT_TOKEN, parse_mode='HTML')
_main_loop: asyncio.AbstractEventLoop = None


# ─── HORA ARGENTINA ───────────────────────────────────────────────────────────
def argentina_time() -> str:
    return (datetime.utcnow() - timedelta(hours=3)).strftime("%H:%M")


# ─── BROADCAST ────────────────────────────────────────────────────────────────
async def broadcast(msg: str):
    try:
        await bot.send_message(CHANNEL_ID, msg, parse_mode='HTML')
    except Exception as e:
        logger.warning(f"broadcast error: {e}")


async def broadcast_signal(msg: str):
    global g_last_signal_msgs
    g_last_signal_msgs = {}
    try:
        sent = await bot.send_message(CHANNEL_ID, msg, parse_mode='HTML')
        g_last_signal_msgs[CHANNEL_ID] = sent.message_id
        logger.info(f"✅ Señal enviada msg_id={sent.message_id}")
    except Exception as e:
        logger.warning(f"broadcast_signal error: {e}")


# ─── UTILIDADES ───────────────────────────────────────────────────────────────
def avg_sec(lst: list) -> Optional[float]:
    return sum(lst) / len(lst) if lst else None


def _register_event(ts: float, events: list, gaps: list, max_hist: int):
    """Registra timestamp y calcula gap; mantiene historial acotado."""
    if events:
        gap = ts - events[-1]
        if gap > 0:
            gaps.append(gap)
    events.append(ts)
    if len(events) > max_hist:
        del events[:-max_hist]
    if len(gaps) > max_hist:
        del gaps[:-max_hist]


def _in_signal_window(events: list, gaps: list, min_events: int,
                      window_before: float = 20.0,
                      window_after: float = 0.0) -> tuple:
    """
    Retorna (in_window: bool, eta_s: float, avg_gap: float, confidence: float).

    Dos modos según los parámetros:
      • Señal 2x  → window_before=20, window_after=0
            dispara cuando  0 <= eta <= 20s  (antes del ETA)
      • Señal 3x  → window_before=10, window_after=10
            dispara cuando  -10s <= eta <= +10s  (±10s alrededor del ETA exacto)
            es decir: empieza 10s antes y cierra 10s después del ETA.
    """
    if len(events) < min_events or len(gaps) < 3:
        return False, 0.0, 0.0, 0.0
    avg = avg_sec(gaps)
    if not avg or avg <= 0:
        return False, 0.0, 0.0, 0.0
    elapsed = time.time() - events[-1]
    eta = avg - elapsed          # positivo = faltan segundos; negativo = ya pasó el ETA
    in_win = -window_after <= eta <= window_before
    # regularidad / confianza
    mean     = avg
    variance = sum((g - mean) ** 2 for g in gaps) / len(gaps)
    cv       = (variance ** 0.5) / mean if mean else 1.0
    regularity = max(0.0, 1.0 - cv)
    proximity  = 1.0 - abs(min(1.0, elapsed / avg) - 1.0)
    confidence = min(98.0, proximity * 60 + regularity * 40)
    return in_win, eta, avg, confidence


# ─── RANGO DE CUOTA (derivePrediction del HTML) ───────────────────────────────
def derive_prediction(trigger: float, ts: float) -> str:
    """
    Replica derivePrediction() del HTML usando SHA-256.
    Retorna una string con el rango estimado de cuota, p.ej. '3.0x – 3.5x'.
    Solo se usa en señales 3x para darle contexto al usuario.
    """
    sec_str = str(int(ts))[-4:]
    raw = f"{trigger:.2f}|{sec_str}|3x"
    hex_digest = hashlib.sha256(raw.encode()).hexdigest()
    n = int(hex_digest[:8], 16)
    r = (n % 1000) / 10.0   # 0.0 – 99.9
    if r < 20:
        coef = 3.0 + (int(r / 5)) * 0.5          # 3.0 – 4.5
    elif r < 45:
        coef = 5.0 + (int((r - 20) / 5)) * 1.0   # 5.0 – 9.0
    elif r < 70:
        coef = 10.0 + (int((r - 45) / 5)) * 2.0  # 10.0 – 20.0
    elif r < 90:
        coef = 20.0 + (int((r - 70) / 5)) * 5.0  # 20.0 – 40.0
    else:
        coef = 50.0 + (int((r - 90) / 5)) * 10.0 # 50.0+
    # El rango es ±20% del coef estimado
    lo = max(3.0, coef * 0.8)
    hi = coef * 1.2
    return f"{lo:.1f}x – {hi:.1f}x"


# ─── ESTADO DE SEÑAL ACTIVA ───────────────────────────────────────────────────
class ActiveSignal:
    """Una señal activa con hasta MAX_ATTEMPTS intentos."""
    def __init__(self, kind: str, trigger: float):
        self.kind    = kind    # '2x' o '3x'
        self.trigger = trigger
        self.attempt = 1


active_signal: Optional[ActiveSignal] = None

def _clear_signal():
    global active_signal, g_armed2x, g_armed3x
    active_signal = None
    g_armed2x = False
    g_armed3x = False


# ─── MARCADOR DIARIO ──────────────────────────────────────────────────────────
def _check_daily_reset():
    global g_daily_hits, g_daily_misses, g_daily_date, g_scoreboard_msg_id
    today = (datetime.utcnow() - timedelta(hours=3)).strftime("%Y-%m-%d")
    if today != g_daily_date:
        g_daily_hits = g_daily_misses = 0
        g_daily_date = today
        g_scoreboard_msg_id = None
        logger.info(f"📅 Marcador reseteado → {today}")


async def _broadcast_scoreboard():
    global g_scoreboard_msg_id
    total = g_daily_hits + g_daily_misses
    pct   = (g_daily_hits / total * 100) if total else 0.0
    hora  = argentina_time()
    txt = (
        f"<b>📆 MARCADOR DEL DÍA — 🕐 {hora}</b>\n"
        f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
        f"<b>✅ ACIERTOS ≥2x: {g_daily_hits}</b>\n"
        f"<b>❌ FALLOS: {g_daily_misses}</b>\n"
        f"<b>📈 PRECISIÓN: {pct:.1f}%</b>\n"
        f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
        f"<i>Acierto = ≥2x en cualquier intento</i>"
    )
    if g_scoreboard_msg_id:
        try:
            await bot.delete_message(CHANNEL_ID, g_scoreboard_msg_id)
        except Exception:
            pass
        g_scoreboard_msg_id = None
    sent = await bot.send_message(CHANNEL_ID, txt, parse_mode='HTML')
    g_scoreboard_msg_id = sent.message_id


# ─── PERSISTENCIA ─────────────────────────────────────────────────────────────
def save_to_disk():
    global _persist_counter
    try:
        payload = {
            'mults':       [{'id': m['id'], 'value': m['value'], 'ts': m['ts']} for m in g_mults],
            'events2x':    g_events2x, 'gaps2x': g_gaps2x,
            'events3x':    g_events3x, 'gaps3x': g_gaps3x,
            'daily_hits':  g_daily_hits,
            'daily_misses':g_daily_misses,
            'daily_date':  g_daily_date,
        }
        tmp = PERSIST_FILE + ".tmp"
        with open(tmp, 'w') as f:
            json.dump(payload, f)
        os.replace(tmp, PERSIST_FILE)
    except Exception as e:
        logger.warning(f"save error: {e}")


def load_from_disk():
    global g_mults, g_events2x, g_gaps2x, g_events3x, g_gaps3x
    global g_daily_hits, g_daily_misses, g_daily_date
    if not os.path.exists(PERSIST_FILE):
        return
    try:
        with open(PERSIST_FILE) as f:
            data = json.load(f)
        loaded = data.get('mults', [])
        if len(loaded) > MAX_MULTS:
            loaded = loaded[-TRIM_MULTS:]
        g_mults[:] = loaded
        for m in g_mults:
            g_seen_ids.add(str(m['id']))
        g_events2x[:] = data.get('events2x', [])[-MAX_HIST_2X:]
        g_gaps2x[:]   = data.get('gaps2x',   [])[-MAX_HIST_2X:]
        g_events3x[:] = data.get('events3x', [])[-MAX_HIST_3X:]
        g_gaps3x[:]   = data.get('gaps3x',   [])[-MAX_HIST_3X:]
        today = (datetime.utcnow() - timedelta(hours=3)).strftime("%Y-%m-%d")
        if data.get('daily_date', '') == today:
            g_daily_hits   = data.get('daily_hits',   0)
            g_daily_misses = data.get('daily_misses', 0)
            g_daily_date   = today
        else:
            g_daily_hits = g_daily_misses = 0
            g_daily_date = today
        logger.info(f"Cargado: {len(g_mults)} mults | {len(g_events2x)} ev2x | {len(g_events3x)} ev3x")
    except Exception as e:
        logger.warning(f"load error: {e}")


# ─── MENSAJES DE SEÑAL ────────────────────────────────────────────────────────
async def _send_signal_2x(trigger: float, attempt: int,
                          avg_gap: float, eta_s: float, conf: float):
    hora = argentina_time()
    eventos = len(g_events2x)
    logger.info(f"📤 Señal 2x intento {attempt} | trigger={trigger:.2f}x")
    txt = (
        f"<b>🆔 SEÑAL 2x SPACEMAN — 🕐 {hora}</b>\n"
        f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
        f"<b>⏱ ÚLTIMA CUOTA: {trigger:.2f}x</b>\n"
        f"<b>🎯 OBJETIVO: {TARGET_2X:.2f}x</b>\n"
        f"<b>🛡️ SEGURO: {INSURANCE_2X:.2f}x</b>\n"
        f"<b>⏳ ETA próximo 2x: ~{eta_s:.0f}s</b>\n"
        f"<b>📊 Confianza: {conf:.0f}%</b>\n"
        f"<b>🔄 Intento {attempt}/{MAX_ATTEMPTS}</b>"
    )
    await broadcast_signal(txt)


async def _send_signal_3x(trigger: float, attempt: int,
                          avg_gap: float, eta_s: float,
                          conf: float, coef_range: str):
    hora = argentina_time()
    eventos = len(g_events3x)
    # eta_s puede ser negativo (ya pasó el ETA) o positivo (aún falta)
    eta_abs = abs(eta_s)
    if eta_s > 0:
        eta_txt = f"en ~{eta_abs:.0f}s"
    elif eta_s < -1:
        eta_txt = f"hace {eta_abs:.0f}s (ventana +{SIGNAL_WINDOW_3X_A}s)"
    else:
        eta_txt = "¡AHORA!"
    logger.info(f"📤 Señal 3x intento {attempt} | trigger={trigger:.2f}x | rango={coef_range} | eta={eta_s:.1f}s")
    txt = (
        f"<b>🚀 SEÑAL 3x SPACEMAN — 🕐 {hora}</b>\n"
        f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
        f"<b>⏱ ÚLTIMA CUOTA ≥3x: {trigger:.2f}x</b>\n"
        f"<b>🎯 OBJETIVO: {TARGET_3X:.2f}x</b>\n"
        f"<b>🛡️ SEGURO: {INSURANCE_2X:.2f}x</b>\n"
        f"<b>📈 RANGO ESTIMADO: {coef_range}</b>\n"
        f"<b>⏳ ETA próximo ≥3x: {eta_txt}</b>\n"
        f"<b>🪟 VENTANA: ±{SIGNAL_WINDOW_3X_B}s del ETA</b>\n"
        f"<b>📊 Confianza: {conf:.0f}% | Eventos: {eventos}</b>\n"
        f"<b>🔄 Intento {attempt}/{MAX_ATTEMPTS}</b>"
    )
    await broadcast_signal(txt)


async def _send_hit(value: float, attempt: int, kind: str):
    hora = argentina_time()
    if value >= TARGET_3X:
        emoji, label = "✅", f"GANAMOS {value:.2f}x"
    else:
        emoji, label = "🛡️", f"SEGURO ACTIVADO {value:.2f}x"
    txt = (
        f"<b>{emoji} {label} — 🕐 {hora}</b>\n"
        f"<b>Señal {kind.upper()} · Intento {attempt}/{MAX_ATTEMPTS}</b>"
    )
    await broadcast(txt)


async def _send_miss(value: float, attempt: int, kind: str, last: bool):
    hora = argentina_time()
    if last:
        txt = (
            f"<b>❌ PERDIMOS {value:.2f}x — 🕐 {hora}</b>\n"
            f"<b>Señal {kind.upper()} · Intento {attempt}/{MAX_ATTEMPTS} — Señal fallida 😭</b>"
        )
    else:
        txt = (
            f"<b>❌ {value:.2f}x — 🕐 {hora}</b>\n"
            f"<b>Señal {kind.upper()} · Intento {attempt}/{MAX_ATTEMPTS} fallido → preparando intento {attempt+1}...</b>"
        )
    await broadcast(txt)


# ─── PROCESADOR DE MULTIPLICADORES ────────────────────────────────────────────
async def process_multiplier(value: float, round_id: str):
    global active_signal
    global g_armed2x, g_last_fire2x, g_cooldown2x_mod
    global g_armed3x, g_last_fire3x, g_cooldown3x_mod
    global g_mults, g_seen_ids, _persist_counter
    global g_daily_hits, g_daily_misses

    logger.info(f"🎲 {value:.2f}x | ID:{round_id} | señal:{active_signal.kind if active_signal else 'idle'}")

    _check_daily_reset()

    # ── Fase 1: Resolver señal activa ─────────────────────────────────────────
    if active_signal is not None:
        sig  = active_signal
        kind = sig.kind
        att  = sig.attempt

        # ¿Acierto? Para señal 2x: ≥2x. Para señal 3x: ≥2x (seguro) o ≥3x.
        hit = value >= INSURANCE_2X   # mismo umbral para ambas señales

        if hit:
            g_daily_hits += 1
            await _send_hit(value, att, kind)
            await _broadcast_scoreboard()
            save_to_disk()
            _clear_signal()
            g_cooldown2x_mod = max(g_cooldown2x_mod, 3)
            g_cooldown3x_mod = max(g_cooldown3x_mod, 3)
        else:
            if att < MAX_ATTEMPTS:
                sig.attempt += 1
                await _send_miss(value, att, kind, last=False)
                # No se borra la señal; el siguiente round será el intento 2
            else:
                g_daily_misses += 1
                await _send_miss(value, att, kind, last=True)
                await _broadcast_scoreboard()
                save_to_disk()
                _clear_signal()
            g_cooldown2x_mod = max(g_cooldown2x_mod, 2)
            g_cooldown3x_mod = max(g_cooldown3x_mod, 2)

    g_cooldown2x_mod = max(0, g_cooldown2x_mod - 1)
    g_cooldown3x_mod = max(0, g_cooldown3x_mod - 1)

    # ── Fase 2: Registrar multiplicador ───────────────────────────────────────
    now_ts = time.time()
    g_mults.append({'id': round_id, 'value': value, 'ts': now_ts})
    if len(g_mults) >= MAX_MULTS:
        g_mults[:] = g_mults[-TRIM_MULTS:]
        save_to_disk()
    else:
        _persist_counter += 1
        if _persist_counter >= 10:
            _persist_counter = 0
            save_to_disk()

    if len(g_seen_ids) > 2000:
        for oid in sorted(g_seen_ids)[:1000]:
            g_seen_ids.discard(oid)

    # ── Fase 3: Registrar eventos por tier ────────────────────────────────────
    if value >= 2.0:
        _register_event(now_ts, g_events2x, g_gaps2x, MAX_HIST_2X)
        if g_armed2x:
            g_armed2x = False
            logger.debug(f"Señal 2x reseteada — llegó {value:.2f}x")

    if value >= 3.0:
        _register_event(now_ts, g_events3x, g_gaps3x, MAX_HIST_3X)
        if g_armed3x:
            g_armed3x = False
            logger.debug(f"Señal 3x reseteada — llegó {value:.2f}x")

    # ── Fase 4: Disparar señal (solo si no hay señal activa) ──────────────────
    if active_signal is not None:
        return   # ya hay señal activa esperando

    # ── 4a. Señal 3x (tiene prioridad si el predictor está listo) ─────────────
    if g_cooldown3x_mod == 0:
        in_win3, eta3, avg3, conf3 = _in_signal_window(
            g_events3x, g_gaps3x, MIN_EVENTS_3X,
            window_before=SIGNAL_WINDOW_3X_B,
            window_after=SIGNAL_WINDOW_3X_A,
        )
        if in_win3 and not g_armed3x:
            elapsed_since = now_ts - g_last_fire3x
            if elapsed_since >= COOLDOWN_3X:
                g_armed3x      = True
                g_last_fire3x  = now_ts
                g_cooldown3x_mod = 8
                coef_range = derive_prediction(value, now_ts)
                active_signal = ActiveSignal('3x', value)
                logger.info(
                    f"🎯 SEÑAL 3x | trigger={value:.2f}x | ETA~{eta3:.0f}s | "
                    f"avg={avg3:.1f}s | conf={conf3:.0f}% | rango={coef_range}"
                )
                await _send_signal_3x(value, 1, avg3, eta3, conf3, coef_range)
                return
        elif not in_win3 and g_armed3x:
            g_armed3x = False

    # ── 4b. Señal 2x ──────────────────────────────────────────────────────────
    if g_cooldown2x_mod == 0:
        in_win2, eta2, avg2, conf2 = _in_signal_window(
            g_events2x, g_gaps2x, MIN_EVENTS_2X,
            window_before=SIGNAL_WINDOW_2X,
            window_after=0.0,
        )
        if in_win2 and not g_armed2x:
            elapsed_since = now_ts - g_last_fire2x
            if elapsed_since >= SIGNAL_COOLDOWN_2X:
                g_armed2x      = True
                g_last_fire2x  = now_ts
                g_cooldown2x_mod = 6
                active_signal = ActiveSignal('2x', value)
                logger.info(
                    f"🎯 SEÑAL 2x | trigger={value:.2f}x | ETA~{eta2:.0f}s | "
                    f"avg={avg2:.1f}s | conf={conf2:.0f}%"
                )
                await _send_signal_2x(value, 1, avg2, eta2, conf2)
                return
        elif not in_win2 and g_armed2x:
            g_armed2x = False


# ─── WEBSOCKET ────────────────────────────────────────────────────────────────
async def ws_collector():
    last_value = None
    while True:
        try:
            async with websockets.connect(
                WS_URL, ping_interval=30, ping_timeout=10, close_timeout=10
            ) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe", "casinoId": CASINO_ID,
                    "currency": CURRENCY, "key": [GAME_ID]
                }))
                logger.info("✅ Suscrito a Spaceman WebSocket")
                async for raw in ws:
                    try:
                        data = json.loads(raw)
                        gr   = data.get('gameResult', [])
                        if not gr:
                            continue
                        first = gr[0]
                        value = float(first.get('result', 0))
                        if value <= 0:
                            continue
                        rid = str(
                            first.get('roundId') or first.get('gameRoundId') or
                            first.get('id') or f"{value}_{int(time.time()*1000)}"
                        )
                        if rid in g_seen_ids or value == last_value:
                            continue
                        g_seen_ids.add(rid)
                        last_value = value
                        await process_multiplier(value, rid)
                    except Exception as e:
                        logger.debug(f"msg error: {e}")
        except Exception as e:
            logger.error(f"WS error: {e}")
        await asyncio.sleep(5)


# ─── FLASK ────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    sig = active_signal
    sig_info = f"{sig.kind} intento {sig.attempt}" if sig else "idle"
    return (
        f"🤖 SpacemanBot | mults:{len(g_mults)} | señal:{sig_info} | "
        f"ev2x:{len(g_events2x)} ev3x:{len(g_events3x)} | "
        f"✅{g_daily_hits} ❌{g_daily_misses}"
    ), 200

@flask_app.route('/ping')
def ping():
    return "pong", 200

@flask_app.route('/webhook', methods=['POST'])
def webhook():
    if _main_loop is None:
        return "Loop no iniciado", 503
    try:
        update = types.Update.de_json(request.get_json())
        asyncio.run_coroutine_threadsafe(bot.process_new_updates([update]), _main_loop)
        return '', 200
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return "Error interno", 500

@flask_app.route('/stats')
def stats_route():
    avg2 = avg_sec(g_gaps2x)
    avg3 = avg_sec(g_gaps3x)
    total = g_daily_hits + g_daily_misses
    return {
        "status": "ok",
        "mults": len(g_mults),
        "signal": active_signal.kind if active_signal else None,
        "events_2x": len(g_events2x),
        "avg_gap_2x_s": round(avg2, 1) if avg2 else None,
        "events_3x": len(g_events3x),
        "avg_gap_3x_s": round(avg3, 1) if avg3 else None,
        "daily_hits": g_daily_hits,
        "daily_misses": g_daily_misses,
        "accuracy_pct": round(g_daily_hits / total * 100, 1) if total else None,
    }

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)


async def self_ping_loop():
    render_url = os.environ.get('RENDER_EXTERNAL_URL', '')
    if not render_url:
        return
    url = f"{render_url.rstrip('/')}/ping"
    while True:
        await asyncio.sleep(14 * 60)
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=10) as r:
                    logger.info(f"Self-ping OK: {r.status}")
        except Exception as e:
            logger.warning(f"Self-ping falló: {e}")


# ─── COMANDOS TELEGRAM ────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def cmd_start(message):
    name = message.from_user.first_name or "usuario"
    g_all_chats.add(message.chat.id)
    avg2 = avg_sec(g_gaps2x)
    avg3 = avg_sec(g_gaps3x)
    ev2  = len(g_events2x)
    ev3  = len(g_events3x)
    info2 = f"<code>{avg2:.1f}s</code> avg ({ev2} eventos)" if avg2 else f"<code>{ev2}/{MIN_EVENTS_2X}</code> eventos"
    info3 = f"<code>{avg3:.1f}s</code> avg ({ev3} eventos)" if avg3 else f"<code>{ev3}/{MIN_EVENTS_3X}</code> eventos"
    await bot.reply_to(message,
        f"🚀 <b>¡Bienvenido {name}!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <b>Bot de Señales Spaceman — Triple Predictor</b>\n\n"
        f"🔵 <b>Señal 2x</b> · Objetivo ≥<code>2.00x</code>\n"
        f"   Predictor timing: {info2}\n\n"
        f"🚀 <b>Señal 3x</b> · Objetivo ≥<code>3.00x</code> · Seguro <code>2.00x</code>\n"
        f"   Predictor timing (eventos ≥3x): {info3}\n"
        f"   + Rango de cuota estimado por hash\n\n"
        f"🔄 Cada señal tiene <code>{MAX_ATTEMPTS}</code> intentos\n"
        f"✅ Acierto = ≥2x en cualquier intento\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')


@bot.message_handler(commands=['predictor'])
async def cmd_predictor(message):
    g_all_chats.add(message.chat.id)
    hora = argentina_time()
    now  = time.time()

    # 2x
    ev2  = len(g_events2x)
    avg2 = avg_sec(g_gaps2x)
    if avg2 and ev2 >= MIN_EVENTS_2X:
        el2  = now - g_events2x[-1] if g_events2x else 0
        eta2 = max(0, avg2 - el2)
        gaps = g_gaps2x
        mean = avg2
        cv2  = (sum((g-mean)**2 for g in gaps)/len(gaps))**0.5 / mean if mean else 1
        reg2 = max(0, 1-cv2)
        info2 = (f"⏱ Avg gap: <code>{avg2:.1f}s</code> | Transcurrido: <code>{el2:.1f}s</code>\n"
                 f"   ETA: <code>~{eta2:.0f}s</code> | Regularidad: <code>{reg2*100:.0f}%</code>")
    else:
        info2 = f"⏳ Acumulando: <code>{ev2}/{MIN_EVENTS_2X}</code> eventos ≥2x"

    # 3x
    ev3  = len(g_events3x)
    avg3 = avg_sec(g_gaps3x)
    if avg3 and ev3 >= MIN_EVENTS_3X:
        el3  = now - g_events3x[-1] if g_events3x else 0
        eta3 = max(0, avg3 - el3)
        gaps3= g_gaps3x
        mean3= avg3
        cv3  = (sum((g-mean3)**2 for g in gaps3)/len(gaps3))**0.5 / mean3 if mean3 else 1
        reg3 = max(0, 1-cv3)
        info3 = (f"⏱ Avg gap: <code>{avg3:.1f}s</code> | Transcurrido: <code>{el3:.1f}s</code>\n"
                 f"   ETA: <code>~{eta3:.0f}s</code> | Regularidad: <code>{reg3*100:.0f}%</code>")
    else:
        info3 = f"⏳ Acumulando: <code>{ev3}/{MIN_EVENTS_3X}</code> eventos ≥3x"

    await bot.reply_to(message,
        f"📡 <b>PREDICTORES — 🕐 {hora}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔵 <b>Predictor 2x</b>\n{info2}\n\n"
        f"🚀 <b>Predictor 3x (eventos ≥3x)</b>\n{info3}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')


@bot.message_handler(commands=['estadisticas'])
async def cmd_estadisticas(message):
    g_all_chats.add(message.chat.id)
    hora  = argentina_time()
    total = g_daily_hits + g_daily_misses
    pct   = (g_daily_hits / total * 100) if total else 0.0
    sig   = active_signal
    sig_txt = f"<code>{sig.kind}</code> · intento {sig.attempt}/{MAX_ATTEMPTS}" if sig else "<code>idle</code>"
    await bot.reply_to(message,
        f"📊 <b>ESTADÍSTICAS — 🕐 {hora}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Aciertos ≥2x hoy: <code>{g_daily_hits}</code>\n"
        f"❌ Fallos hoy: <code>{g_daily_misses}</code>\n"
        f"📈 Precisión: <code>{pct:.1f}%</code>\n"
        f"📡 Señal activa: {sig_txt}\n"
        f"📦 Eventos 2x: <code>{len(g_events2x)}</code> | 3x: <code>{len(g_events3x)}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')


# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main_async():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    logger.info("🤖 Iniciando SpacemanBot (Triple Predictor 2x/3x)...")
    load_from_disk()
    await bot.set_my_commands([
        types.BotCommand('predictor',    '📡 Estado de los predictores'),
        types.BotCommand('estadisticas', '📊 Estadísticas del día'),
    ])
    asyncio.create_task(ws_collector())
    asyncio.create_task(self_ping_loop())
    render_url = os.environ.get('RENDER_EXTERNAL_URL', '').rstrip('/')
    if render_url:
        await bot.remove_webhook()
        await asyncio.sleep(1)
        await bot.set_webhook(url=f"{render_url}/webhook")
        logger.info(f"✅ Webhook: {render_url}/webhook")
        while True:
            await asyncio.sleep(3600)
    else:
        logger.warning("⚠️ Usando polling (dev local)")
        await bot.infinity_polling(skip_pending=True)


if __name__ == '__main__':
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_async())
