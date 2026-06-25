#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════╗
║   SPACEMAN BOT — Triple Predictor (Adaptativo)          ║
║   • Predictor 2x y 3x con EMA, tendencia y corrección   ║
║   • Adaptación dinámica a rachas altas/bajas            ║
║   • Intentos: sin mensaje de fallo en intento 1         ║
║   • Marcador diario: aciertos ≥2x                       ║
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
from typing import Optional, List, Tuple
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

# ── Predictor 2x (adaptativo) ──────────────────────────────────────────────
MIN_EVENTS_2X     = 15    # mínimo de eventos ≥2x para activar predictor
SIGNAL_WINDOW_2X  = 20    # segundos antes del ETA para disparar señal 2x
MAX_HIST_2X       = 200
ALPHA_2X          = 0.25  # factor de suavizado EMA

# ── Predictor 3x (adaptativo) ──────────────────────────────────────────────
MIN_EVENTS_3X      = 10
SIGNAL_WINDOW_3X_B = 10   # segundos ANTES del ETA
SIGNAL_WINDOW_3X_A = 10   # segundos DESPUÉS del ETA
MAX_HIST_3X        = 150
COOLDOWN_3X        = 45
ALPHA_3X           = 0.30

# ── General ──────────────────────────────────────────────────────────────────
SIGNAL_COOLDOWN_2X = 30
MAX_MULTS          = 400
TRIM_MULTS         = 300
PERSIST_FILE       = "spaceman_history.json"

# ─── ESTADO GLOBAL ──────────────────────────────────────────────────────────
g_mults:    list = []
g_seen_ids: set  = set()

# Nuevas estructuras: listas de tuplas (timestamp, value)
g_events_data_2x: List[Tuple[float, float]] = []   # eventos ≥2.0
g_events_data_3x: List[Tuple[float, float]] = []   # eventos ≥3.0

# Control de señal 2x
g_armed2x:          bool  = False
g_last_fire2x:      float = 0
g_cooldown2x_mod:   int   = 0

# Control de señal 3x
g_armed3x:          bool  = False
g_last_fire3x:      float = 0
g_cooldown3x_mod:   int   = 0

g_all_chats: set = set()

# ─── MARCADOR DIARIO ────────────────────────────────────────────────────────
g_daily_hits:   int = 0
g_daily_misses: int = 0
g_daily_date:   str = ""
g_scoreboard_msg_id: Optional[int] = None

g_last_signal_msgs: dict = {}
_persist_counter:   int  = 0

bot = AsyncTeleBot(BOT_TOKEN, parse_mode='HTML')
_main_loop: asyncio.AbstractEventLoop = None

# ─── HORA ARGENTINA ─────────────────────────────────────────────────────────
def argentina_time() -> str:
    return (datetime.utcnow() - timedelta(hours=3)).strftime("%H:%M")

# ─── BROADCAST ──────────────────────────────────────────────────────────────
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

# ─── UTILIDADES ─────────────────────────────────────────────────────────────
def avg_sec(lst: List[float]) -> Optional[float]:
    return sum(lst) / len(lst) if lst else None

def _register_event(ts: float, value: float, events_data: List[Tuple[float, float]], max_hist: int):
    """Registra un evento (timestamp, valor) y mantiene el historial acotado."""
    events_data.append((ts, value))
    if len(events_data) > max_hist:
        del events_data[:-max_hist]

# ─── PREDICTOR ADAPTATIVO (EMA + TENDENCIA + CORRECCIÓN POR VALOR) ──────
def _predict_next_event(
    events_data: List[Tuple[float, float]],
    min_events: int,
    window_before: float,
    window_after: float,
    alpha: float = 0.3,
    use_value_correction: bool = True
) -> Tuple[bool, float, float, float, float]:
    """
    Predice el próximo evento usando EMA con ajuste por tendencia y valor de cuota.
    Retorna: (in_window, eta, predicted_gap, confidence, trend_adj)
    """
    if len(events_data) < min_events:
        return False, 0.0, 0.0, 0.0, 0.0

    # Extraer gaps y valores
    gaps = []
    values = []
    for i in range(1, len(events_data)):
        ts_prev, val_prev = events_data[i-1]
        ts_curr, val_curr = events_data[i]
        gap = ts_curr - ts_prev
        if gap > 0:
            gaps.append(gap)
            values.append(val_curr)

    n = len(gaps)
    if n < 3:
        return False, 0.0, 0.0, 0.0, 0.0

    # 1. EMA de los gaps con pesos exponenciales
    weights = [alpha * (1 - alpha) ** (n - 1 - i) for i in range(n)]
    total_w = sum(weights)
    weights = [w / total_w for w in weights]
    ema_gap = sum(weights[i] * gaps[i] for i in range(n))

    # 2. Tendencia (pendiente lineal)
    x = list(range(n))
    mean_x = (n - 1) / 2
    mean_y = sum(gaps) / n
    slope = sum((x[i] - mean_x) * (gaps[i] - mean_y) for i in range(n)) / sum((x[i] - mean_x) ** 2 for i in range(n)) if n > 1 else 0.0

    # 3. Volatilidad (CV)
    mean_gap = sum(gaps) / n
    variance = sum((g - mean_gap) ** 2 for g in gaps) / n
    std_gap = variance ** 0.5 if variance > 0 else 1.0
    cv = std_gap / mean_gap if mean_gap > 0 else 1.0

    # 4. Ajuste por tendencia (atenuado por volatilidad)
    volatility_factor = min(1.0, 1.0 / (1 + cv))
    trend_adj = slope * 0.5 * volatility_factor

    # 5. Corrección por valor de la última cuota
    correction = 0.0
    if use_value_correction and len(values) > 0:
        # Promedio de valores recientes (últimos 5)
        recent_vals = values[-5:] if len(values) >= 5 else values
        avg_val = sum(recent_vals) / len(recent_vals)
        # Determinamos el umbral según el tipo de evento (2x o 3x)
        # Lo inferimos del tamaño de events_data comparado con los globales
        threshold = 2.0 if len(events_data) == len(g_events_data_2x) else 3.0
        correction = (avg_val - threshold) * 0.1   # factor empírico

    predicted_gap = ema_gap + trend_adj + correction
    if predicted_gap <= 0:
        predicted_gap = 1.0

    elapsed = time.time() - events_data[-1][0]
    eta = predicted_gap - elapsed

    in_window = -window_after <= eta <= window_before

    # Confianza: regularidad + proximidad al ETA
    regularity = max(0.0, 1.0 - cv)
    proximity = 1.0 - abs(min(1.0, elapsed / predicted_gap) - 1.0) if predicted_gap > 0 else 0.0
    confidence = min(98.0, proximity * 50 + regularity * 50)

    return in_window, eta, predicted_gap, confidence, trend_adj + correction

# ─── RANGO DE CUOTA (derivePrediction) ──────────────────────────────────────
def derive_prediction(trigger: float, ts: float) -> str:
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
    lo = max(3.0, coef * 0.8)
    hi = coef * 1.2
    return f"{lo:.1f}x – {hi:.1f}x"

# ─── ESTADO DE SEÑAL ACTIVA ─────────────────────────────────────────────────
class ActiveSignal:
    def __init__(self, kind: str, trigger: float):
        self.kind    = kind
        self.trigger = trigger
        self.attempt = 1

active_signal: Optional[ActiveSignal] = None

def _clear_signal():
    global active_signal, g_armed2x, g_armed3x
    active_signal = None
    g_armed2x = False
    g_armed3x = False

# ─── MARCADOR DIARIO ────────────────────────────────────────────────────────
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

# ─── PERSISTENCIA ───────────────────────────────────────────────────────────
def save_to_disk():
    global _persist_counter
    try:
        payload = {
            'mults': [{'id': m['id'], 'value': m['value'], 'ts': m['ts']} for m in g_mults],
            'events2x': g_events_data_2x,
            'events3x': g_events_data_3x,
            'daily_hits':  g_daily_hits,
            'daily_misses': g_daily_misses,
            'daily_date':  g_daily_date,
        }
        tmp = PERSIST_FILE + ".tmp"
        with open(tmp, 'w') as f:
            json.dump(payload, f)
        os.replace(tmp, PERSIST_FILE)
    except Exception as e:
        logger.warning(f"save error: {e}")

def load_from_disk():
    global g_mults, g_events_data_2x, g_events_data_3x
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
        # Cargar listas de eventos (se guardan como listas de listas)
        g_events_data_2x = [(e[0], e[1]) for e in data.get('events2x', [])[-MAX_HIST_2X:]]
        g_events_data_3x = [(e[0], e[1]) for e in data.get('events3x', [])[-MAX_HIST_3X:]]
        today = (datetime.utcnow() - timedelta(hours=3)).strftime("%Y-%m-%d")
        if data.get('daily_date', '') == today:
            g_daily_hits   = data.get('daily_hits',   0)
            g_daily_misses = data.get('daily_misses', 0)
            g_daily_date   = today
        else:
            g_daily_hits = g_daily_misses = 0
            g_daily_date = today
        logger.info(f"Cargado: {len(g_mults)} mults | {len(g_events_data_2x)} ev2x | {len(g_events_data_3x)} ev3x")
    except Exception as e:
        logger.warning(f"load error: {e}")

# ─── MENSAJES DE SEÑAL ──────────────────────────────────────────────────────
async def _send_signal_2x(trigger: float, attempt: int,
                          avg_gap: float, eta_s: float, conf: float):
    hora = argentina_time()
    txt = (
        f"<b>🆔 SEÑAL SPACEMAN — 🕐 {hora}</b>\n"
        f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
        f"<b>🧨 ÚLTIMA CUOTA: {trigger:.2f}x</b>\n"
        f"<b>🛡️ OBJECTIVO: {TARGET_2X:.2f}x — 3.00x</b>\n"
        f"<b>♣️ ETA próximo 2X: ~{eta_s:.0f}s</b>\n"
        f"<b>💡 CONFIANZA: {conf:.0f}%</b>\n"
        f"<b>🔄 INTENTO {attempt}/{MAX_ATTEMPTS}</b>"
    )
    await broadcast_signal(txt)

async def _send_signal_3x(trigger: float, attempt: int,
                          avg_gap: float, eta_s: float,
                          conf: float, coef_range: str):
    hora = argentina_time()
    eta_abs = abs(eta_s)
    if eta_s > 0:
        eta_txt = f"en ~{eta_abs:.0f}s"
    elif eta_s < -1:
        eta_txt = f"hace {eta_abs:.0f}s"
    else:
        eta_txt = "¡AHORA!"
    txt = (
        f"<b>💎 SEÑAL SPACEMAN — 🕐 {hora}</b>\n"
        f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
        f"<b>🧨 ÚLTIMA CUOTA ≥3X: {trigger:.2f}x</b>\n"
        f"<b>🎯 OBJETIVO: {TARGET_3X:.2f}x</b>\n"
        f"<b>🛡️ SEGURO: {INSURANCE_2X:.2f}x</b>\n"
        f"<b>♣️ ETA próximo ≥3X: {eta_txt}</b>\n"
        f"<b>💡 CONFIANZA: {conf:.0f}%</b>\n"
        f"<b>🔄 INTENTO {attempt}/{MAX_ATTEMPTS}</b>"
    )
    await broadcast_signal(txt)

async def _send_hit(value: float, attempt: int, kind: str):
    """Mensaje de acierto (para cualquier intento)."""
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

async def _send_miss(value: float, attempt: int, kind: str):
    """
    Mensaje de fallo. Solo se llama cuando el intento actual es el último
    y ha fallado, o cuando el intento 1 falla y no hay más intentos (no ocurre aquí).
    """
    hora = argentina_time()
    # Siempre será el último intento (2) porque en el intento 1 no se envía miss.
    txt = (
        f"<b>❌ PERDIMOS {value:.2f}x — 🕐 {hora}</b>\n"
        f"<b>Señal {kind.upper()} · Intento {attempt}/{MAX_ATTEMPTS} — Señal fallida 😭</b>"
    )
    await broadcast(txt)

# ─── PROCESADOR DE MULTIPLICADORES ──────────────────────────────────────────
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
        hit = value >= INSURANCE_2X

        if hit:
            g_daily_hits += 1
            await _send_hit(value, att, kind)   # Envía mensaje de acierto (intento actual)
            await _broadcast_scoreboard()
            save_to_disk()
            _clear_signal()
            g_cooldown2x_mod = max(g_cooldown2x_mod, 3)
            g_cooldown3x_mod = max(g_cooldown3x_mod, 3)
        else:
            if att < MAX_ATTEMPTS:
                # Intento 1 fallido → solo incrementamos el intento, NO enviamos mensaje de pérdida
                sig.attempt += 1
                logger.info(f"Intento {att} fallido para señal {kind}. Pasando a intento {sig.attempt}.")
                # No se limpia la señal, se espera el próximo multiplicador
            else:
                # Intento 2 (último) fallido → enviamos mensaje de pérdida
                g_daily_misses += 1
                await _send_miss(value, att, kind)   # att es 2 aquí
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

    # ── Fase 3: Registrar eventos y resetear timer si cuota ≥2x o ≥3x ──────
    if value >= 2.0:
        _register_event(now_ts, value, g_events_data_2x, MAX_HIST_2X)
        g_armed2x = False
        logger.info(f"🔄 Timer 2x reseteado — cuota {value:.2f}x")

    if value >= 3.0:
        _register_event(now_ts, value, g_events_data_3x, MAX_HIST_3X)
        g_armed3x = False
        logger.info(f"🔄 Timer 3x reseteado — cuota {value:.2f}x")

    # ── Fase 4: Disparar señal (solo si no hay señal activa) ──────────────────
    if active_signal is not None:
        return   # ya hay señal activa

    # ⛔ Solo disparar si la cuota actual es <2x (el timer se reseteó arriba si era ≥2x)
    if value >= 2.0:
        return

    # ── 4a. Señal 3x (prioridad) ──────────────────────────────────────────────
    if g_cooldown3x_mod == 0:
        in_win3, eta3, avg3, conf3, _ = _predict_next_event(
            g_events_data_3x, MIN_EVENTS_3X,
            SIGNAL_WINDOW_3X_B, SIGNAL_WINDOW_3X_A,
            alpha=ALPHA_3X,
            use_value_correction=True
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
        in_win2, eta2, avg2, conf2, _ = _predict_next_event(
            g_events_data_2x, MIN_EVENTS_2X,
            SIGNAL_WINDOW_2X, 0.0,
            alpha=ALPHA_2X,
            use_value_correction=True
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

# ─── WEBSOCKET ──────────────────────────────────────────────────────────────
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

# ─── FLASK ──────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    sig = active_signal
    sig_info = f"{sig.kind} intento {sig.attempt}" if sig else "idle"
    return (
        f"🤖 SpacemanBot | mults:{len(g_mults)} | señal:{sig_info} | "
        f"ev2x:{len(g_events_data_2x)} ev3x:{len(g_events_data_3x)} | "
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
    # Calcular promedios a partir de los gaps
    gaps2 = [g_events_data_2x[i][0] - g_events_data_2x[i-1][0] for i in range(1, len(g_events_data_2x)) if g_events_data_2x[i][0] > g_events_data_2x[i-1][0]]
    gaps3 = [g_events_data_3x[i][0] - g_events_data_3x[i-1][0] for i in range(1, len(g_events_data_3x)) if g_events_data_3x[i][0] > g_events_data_3x[i-1][0]]
    avg2 = sum(gaps2)/len(gaps2) if gaps2 else None
    avg3 = sum(gaps3)/len(gaps3) if gaps3 else None
    total = g_daily_hits + g_daily_misses
    return {
        "status": "ok",
        "mults": len(g_mults),
        "signal": active_signal.kind if active_signal else None,
        "events_2x": len(g_events_data_2x),
        "avg_gap_2x_s": round(avg2, 1) if avg2 else None,
        "events_3x": len(g_events_data_3x),
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

# ─── COMANDOS TELEGRAM ──────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def cmd_start(message):
    name = message.from_user.first_name or "usuario"
    g_all_chats.add(message.chat.id)
    # Calcular promedios de gaps para mostrar
    gaps2 = [g_events_data_2x[i][0] - g_events_data_2x[i-1][0] for i in range(1, len(g_events_data_2x)) if g_events_data_2x[i][0] > g_events_data_2x[i-1][0]]
    gaps3 = [g_events_data_3x[i][0] - g_events_data_3x[i-1][0] for i in range(1, len(g_events_data_3x)) if g_events_data_3x[i][0] > g_events_data_3x[i-1][0]]
    avg2 = sum(gaps2)/len(gaps2) if gaps2 else None
    avg3 = sum(gaps3)/len(gaps3) if gaps3 else None
    ev2 = len(g_events_data_2x)
    ev3 = len(g_events_data_3x)
    info2 = f"<code>{avg2:.1f}s</code> avg ({ev2} eventos)" if avg2 else f"<code>{ev2}/{MIN_EVENTS_2X}</code> eventos"
    info3 = f"<code>{avg3:.1f}s</code> avg ({ev3} eventos)" if avg3 else f"<code>{ev3}/{MIN_EVENTS_3X}</code> eventos"
    await bot.reply_to(message,
        f"🚀 <b>¡Bienvenido {name}!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <b>Bot de Señales Spaceman — Triple Predictor (Adaptativo)</b>\n\n"
        f"🔵 <b>Señal 2x</b> · Objetivo ≥<code>2.00x</code>\n"
        f"   Predictor adaptativo (EMA+tendencia): {info2}\n\n"
        f"🚀 <b>Señal 3x</b> · Objetivo ≥<code>3.00x</code> · Seguro <code>2.00x</code>\n"
        f"   Predictor adaptativo (EMA+tendencia): {info3}\n"
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
    ev2  = len(g_events_data_2x)
    gaps2 = [g_events_data_2x[i][0] - g_events_data_2x[i-1][0] for i in range(1, ev2) if g_events_data_2x[i][0] > g_events_data_2x[i-1][0]]
    avg2 = sum(gaps2)/len(gaps2) if gaps2 else None
    if avg2 and ev2 >= MIN_EVENTS_2X:
        el2  = now - g_events_data_2x[-1][0] if g_events_data_2x else 0
        eta2 = max(0, avg2 - el2)
        # Calcular regularidad (CV inverso)
        cv2 = (sum((g-avg2)**2 for g in gaps2)/len(gaps2))**0.5 / avg2 if avg2 else 1
        reg2 = max(0, 1-cv2)
        info2 = (f"⏱ Avg gap: <code>{avg2:.1f}s</code> | Transcurrido: <code>{el2:.1f}s</code>\n"
                 f"   ETA: <code>~{eta2:.0f}s</code> | Regularidad: <code>{reg2*100:.0f}%</code>")
    else:
        info2 = f"⏳ Acumulando: <code>{ev2}/{MIN_EVENTS_2X}</code> eventos ≥2x"

    # 3x
    ev3  = len(g_events_data_3x)
    gaps3 = [g_events_data_3x[i][0] - g_events_data_3x[i-1][0] for i in range(1, ev3) if g_events_data_3x[i][0] > g_events_data_3x[i-1][0]]
    avg3 = sum(gaps3)/len(gaps3) if gaps3 else None
    if avg3 and ev3 >= MIN_EVENTS_3X:
        el3  = now - g_events_data_3x[-1][0] if g_events_data_3x else 0
        eta3 = max(0, avg3 - el3)
        cv3 = (sum((g-avg3)**2 for g in gaps3)/len(gaps3))**0.5 / avg3 if avg3 else 1
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
        f"📦 Eventos 2x: <code>{len(g_events_data_2x)}</code> | 3x: <code>{len(g_events_data_3x)}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')

# ─── MAIN ────────────────────────────────────────────────────────────────────
async def main_async():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    logger.info("🤖 Iniciando SpacemanBot (Triple Predictor Adaptativo)...")
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
