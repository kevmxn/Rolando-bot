#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════╗
║   SPACEMAN BOT — Triple Predictor + Trend + High Range  ║
║   • Timing (2x/3x) con EMA y tendencia de gaps          ║
║   • Tendencia (continuación alcista)                    ║
║   • Rango alto (≥10x / ≥15x) por intervalos             ║
║   • Seguro universal: 2.00x                             ║
║   • Marcador diario: aciertos ≥2x                       ║
║   • Aprendizaje adaptativo por señales falladas         ║
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
TARGET_2X        = 2.00
TARGET_3X        = 3.00
INSURANCE_2_0    = 2.00   # seguro universal (igual al objetivo 2x)
MAX_ATTEMPTS     = 2
MIN_CONFIDENCE   = 60.0   # solo para señales de timing

# ── Predictor temporal (timing) ─────────────────────────────────────────────
MIN_EVENTS_2X     = 5
SIGNAL_WINDOW_2X  = 20
MAX_HIST_2X       = 200
ALPHA_2X          = 0.25

MIN_EVENTS_3X      = 5
SIGNAL_WINDOW_3X_B = 10
SIGNAL_WINDOW_3X_A = 10
MAX_HIST_3X        = 150
COOLDOWN_3X        = 45
ALPHA_3X           = 0.30

# ── Predictor de tendencia ──────────────────────────────────────────────────
TREND_COOLDOWN          = 60
TREND_MIN_CONDITIONS    = 3
TREND_EMA_FAST          = 4
TREND_EMA_SLOW          = 12
TREND_BULLISH_LOOKBACK  = 8
TREND_STRENGTH_WINDOW   = 4

# ── Predictor de rango alto ──────────────────────────────────────────────────
HIGH_RANGE_COOLDOWN            = 90
HIGH_RANGE_MIN_OCCURRENCES     = 3
HIGH_RANGE_CONFIDENCE_THRESHOLD= 60.0

# ── General ──────────────────────────────────────────────────────────────────
SIGNAL_COOLDOWN_2X = 30
MAX_MULTS          = 400
TRIM_MULTS         = 300
PERSIST_FILE       = "spaceman_history.json"

# ─── ESTADO GLOBAL ──────────────────────────────────────────────────────────
g_mults:    list = []
g_seen_ids: set  = set()

# Listas de eventos (timestamp, value) para timing
g_events_data_2x: List[Tuple[float, float]] = []   # eventos ≥2.0
g_events_data_3x: List[Tuple[float, float]] = []   # eventos ≥3.0

# Últimos valores reales de cuota alta (para mostrar en mensajes)
g_last_high_2x_value = 0.0
g_last_high_3x_value = 0.0

# Control de señal de timing
g_armed2x:          bool  = False
g_last_fire2x:      float = 0
g_cooldown2x_mod:   int   = 0

g_armed3x:          bool  = False
g_last_fire3x:      float = 0
g_cooldown3x_mod:   int   = 0

# Control de señal de tendencia
g_last_trend_fire:  float = 0
g_trend_cooldown_mod: int = 0

# Control de señal de rango alto
g_last_high_range_fire: float = 0
g_high_range_cooldown_mod: int = 0

# Historial de índices de fuerzas 9 y 10 (rangos altos)
g_high_force_9_indices = []
g_high_force_10_indices = []
g_high_range_search = None

g_all_chats: set = set()

# ─── APRENDIZAJE ADAPTATIVO (señales falladas) ───────────────────────────────
# Historial de resultados recientes para ajuste dinámico de umbrales
g_recent_results: list = []          # lista de dicts: {kind, result, value, attempt}
MAX_RECENT_RESULTS  = 50
g_consecutive_misses: dict = {       # fallos consecutivos por tipo de señal
    '2x': 0, '3x': 0, 'trend': 0, 'high_range': 0
}
g_learning_conf_penalty: dict = {    # penalización dinámica de confianza mínima
    '2x': 0.0, '3x': 0.0, 'trend': 0.0, 'high_range': 0.0
}
# Bonus de confianza acumulado por señales exitosas consecutivas
g_consecutive_hits: dict = {
    '2x': 0, '3x': 0, 'trend': 0, 'high_range': 0
}


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
    events_data.append((ts, value))
    if len(events_data) > max_hist:
        del events_data[:-max_hist]

# ─── CLASIFICACIÓN DE FUERZA ──────────────────────────────────────────────
def classify(value: float) -> int:
    v = float(value)
    if v >= 1.00 and v <= 1.09: return -10
    if v >= 1.10 and v <= 1.19: return -9
    if v >= 1.20 and v <= 1.29: return -8
    if v >= 1.30 and v <= 1.39: return -7
    if v >= 1.40 and v <= 1.49: return -6
    if v >= 1.50 and v <= 1.59: return -5
    if v >= 1.60 and v <= 1.69: return -4
    if v >= 1.70 and v <= 1.79: return -3
    if v >= 1.80 and v <= 1.89: return -2
    if v >= 1.90 and v <= 1.99: return -1
    if v >= 2.00 and v <= 2.99: return 1
    if v >= 3.00 and v <= 3.99: return 2
    if v >= 4.00 and v <= 4.99: return 3
    if v >= 5.00 and v <= 5.99: return 4
    if v >= 6.00 and v <= 6.99: return 5
    if v >= 7.00 and v <= 7.99: return 6
    if v >= 8.00 and v <= 8.99: return 7
    if v >= 9.00 and v <= 9.99: return 8
    if v >= 10.00 and v <= 14.99: return 9
    if v >= 15.00: return 10
    return 0

# ─── PREDICTOR DE TENDENCIA ──────────────────────────────────────────────────
def check_trend_conditions() -> bool:
    if len(g_mults) < TREND_EMA_SLOW + 2:
        return False

    vals = [m['value'] for m in g_mults]
    n = len(vals)

    def calc_ema(data, period):
        k = 2 / (period + 1)
        ema = data[0]
        for val in data[1:]:
            ema = val * k + ema * (1 - k)
        return ema

    fast_ema = calc_ema(vals[-TREND_EMA_FAST:], TREND_EMA_FAST)
    slow_ema = calc_ema(vals[-TREND_EMA_SLOW:], TREND_EMA_SLOW)
    cond1 = fast_ema > slow_ema and (fast_ema > vals[-2] if len(vals) >= 2 else False)

    lookback = TREND_BULLISH_LOOKBACK
    if n >= lookback + 1:
        recent = vals[-lookback:]
        bullish_count = sum(1 for i in range(1, len(recent)) if recent[i] > recent[i-1])
        cond2 = bullish_count >= lookback * 0.6
    else:
        cond2 = False

    window = TREND_STRENGTH_WINDOW
    if n >= 2 * window:
        forces = [classify(v) for v in vals]
        recent_avg = sum(forces[-window:]) / window
        prev_avg = sum(forces[-2*window:-window]) / window
        cond3 = recent_avg > prev_avg
    else:
        cond3 = False

    if n >= 5:
        diff_vals = [vals[i] - vals[i-1] for i in range(1, len(vals))]
        last_diffs = diff_vals[-5:] if len(diff_vals) >= 5 else diff_vals
        cond4 = sum(last_diffs) > 0
    else:
        cond4 = False

    if n >= 2:
        last_val = vals[-1]
        prev_val = vals[-2]
        force = classify(last_val)
        cond5 = last_val > prev_val and force > 0
    else:
        cond5 = False

    conditions = [cond1, cond2, cond3, cond4, cond5]
    true_count = sum(conditions)
    logger.debug(f"Trend conditions: {true_count}/5 -> {conditions}")
    return true_count >= TREND_MIN_CONDITIONS

# ─── PREDICTOR TEMPORAL (timing) ────────────────────────────────────────────
def _predict_next_event(
    events_data: List[Tuple[float, float]],
    min_events: int,
    window_before: float,
    window_after: float,
    alpha: float = 0.3,
    use_value_correction: bool = True,
    kind: str = '2x'
) -> Tuple[bool, float, float, float, float]:
    if len(events_data) < min_events:
        return False, 0.0, 0.0, 0.0, 0.0

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

    # EMA ponderada exponencial
    weights = [alpha * (1 - alpha) ** (n - 1 - i) for i in range(n)]
    total_w = sum(weights)
    weights = [w / total_w for w in weights]
    ema_gap = sum(weights[i] * gaps[i] for i in range(n))

    # Regresión lineal sobre los últimos gaps (tendencia de ciclo)
    x = list(range(n))
    mean_x = (n - 1) / 2
    mean_y = sum(gaps) / n
    denom = sum((x[i] - mean_x) ** 2 for i in range(n))
    slope = sum((x[i] - mean_x) * (gaps[i] - mean_y) for i in range(n)) / denom if denom > 0 else 0.0

    mean_gap = mean_y
    variance = sum((g - mean_gap) ** 2 for g in gaps) / n
    std_gap = variance ** 0.5 if variance > 0 else 1.0
    cv = std_gap / mean_gap if mean_gap > 0 else 1.0

    volatility_factor = min(1.0, 1.0 / (1 + cv))
    trend_adj = slope * 0.5 * volatility_factor

    # Corrección por valor reciente (señales de alta cuota predicen ciclos más largos)
    correction = 0.0
    if use_value_correction and len(values) > 0:
        recent_vals = values[-5:] if len(values) >= 5 else values
        avg_val = sum(recent_vals) / len(recent_vals)
        threshold = 2.0 if kind == '2x' else 3.0
        correction = (avg_val - threshold) * 0.1

    predicted_gap = ema_gap + trend_adj + correction
    if predicted_gap <= 0:
        predicted_gap = 1.0

    elapsed = time.time() - events_data[-1][0]
    eta = predicted_gap - elapsed

    in_window = -window_after <= eta <= window_before

    # ── Cálculo de confianza mejorado ──────────────────────────────────────────
    # 1. Regularidad del ciclo (inversa del CV)
    regularity = max(0.0, 1.0 - cv)

    # 2. Proximidad al momento predicho (curva gaussiana centrada en eta=0)
    norm_pos = elapsed / predicted_gap if predicted_gap > 0 else 1.0
    # Máxima confianza cuando estamos justo en el punto predicho (norm_pos≈1.0)
    proximity = max(0.0, 1.0 - abs(norm_pos - 1.0) * 1.5)

    # 3. Consistencia de la tendencia del slope (baja varianza en gaps recientes)
    recent_gaps = gaps[-min(10, n):]
    recent_mean = sum(recent_gaps) / len(recent_gaps)
    recent_var = sum((g - recent_mean) ** 2 for g in recent_gaps) / len(recent_gaps)
    recent_cv = (recent_var ** 0.5) / recent_mean if recent_mean > 0 else 1.0
    consistency = max(0.0, 1.0 - recent_cv)

    # 4. Bonus por cantidad de eventos históricos (más datos → más confianza)
    data_bonus = min(10.0, (n - 3) * 0.5)

    base_confidence = proximity * 40 + regularity * 35 + consistency * 15 + data_bonus
    base_confidence = min(98.0, base_confidence)

    # Aplicar ajuste adaptativo por historial de fallos/aciertos
    final_confidence = weighted_confidence_boost(events_data, base_confidence, kind)

    return in_window, eta, predicted_gap, final_confidence, trend_adj + correction


# ─── APRENDIZAJE ADAPTATIVO ──────────────────────────────────────────────────
def record_signal_result(kind: str, result: str, value: float, attempt: int):
    """Registra el resultado de una señal y ajusta umbrales dinámicamente."""
    global g_consecutive_misses, g_consecutive_hits, g_learning_conf_penalty

    entry = {'kind': kind, 'result': result, 'value': value, 'attempt': attempt,
             'ts': time.time()}
    g_recent_results.append(entry)
    if len(g_recent_results) > MAX_RECENT_RESULTS:
        del g_recent_results[:-MAX_RECENT_RESULTS]

    if result == 'hit':
        g_consecutive_misses[kind] = 0
        g_consecutive_hits[kind] += 1
        # Reducir penalización si encadenamos aciertos
        g_learning_conf_penalty[kind] = max(0.0, g_learning_conf_penalty[kind] - 3.0)
        logger.info(f"📚 Aprendizaje HIT {kind}: hits_consec={g_consecutive_hits[kind]} penalty={g_learning_conf_penalty[kind]:.1f}")
    else:  # miss o insurance_miss
        g_consecutive_hits[kind] = 0
        g_consecutive_misses[kind] += 1
        # Aumentar penalización de confianza según fallos consecutivos
        penalty_inc = 5.0 * g_consecutive_misses[kind]
        g_learning_conf_penalty[kind] = min(30.0, g_learning_conf_penalty[kind] + penalty_inc)
        logger.info(f"📚 Aprendizaje MISS {kind}: misses_consec={g_consecutive_misses[kind]} penalty={g_learning_conf_penalty[kind]:.1f}")

def get_effective_confidence_threshold(kind: str) -> float:
    """Retorna el umbral de confianza efectivo según el historial de fallos."""
    base = MIN_CONFIDENCE
    penalty = g_learning_conf_penalty.get(kind, 0.0)
    return min(90.0, base + penalty)

def compute_recent_accuracy(kind: str, window: int = 20) -> float:
    """Calcula la precisión reciente para un tipo de señal."""
    relevant = [r for r in g_recent_results if r['kind'] == kind][-window:]
    if not relevant:
        return 0.5  # neutro
    hits = sum(1 for r in relevant if r['result'] == 'hit')
    return hits / len(relevant)

def weighted_confidence_boost(events_data: List[Tuple[float, float]], base_conf: float, kind: str) -> float:
    """
    Ajusta la confianza base según:
    - Precisión reciente del tipo de señal
    - Penalización por fallos consecutivos
    - Bonus por valor promedio reciente alto
    """
    if not events_data:
        return base_conf

    # Factor de precisión reciente
    recent_acc = compute_recent_accuracy(kind)
    acc_factor = (recent_acc - 0.5) * 20.0  # -10 a +10 puntos

    # Penalización por fallos consecutivos
    penalty = g_learning_conf_penalty.get(kind, 0.0)

    # Bonus si el valor promedio reciente de eventos es significativamente alto
    recent_vals = [v for _, v in events_data[-10:]] if len(events_data) >= 10 else [v for _, v in events_data]
    avg_recent_val = sum(recent_vals) / len(recent_vals) if recent_vals else 2.0
    threshold = 2.0 if kind == '2x' else 3.0
    val_bonus = min(5.0, (avg_recent_val - threshold) * 0.5)

    adjusted = base_conf + acc_factor + val_bonus - (penalty * 0.3)
    return max(0.0, min(98.0, adjusted))


def compute_average_interval(indices: List[int]) -> Optional[float]:
    if len(indices) < 2:
        return None
    diffs = [indices[i] - indices[i-1] for i in range(1, len(indices))]
    return sum(diffs) / len(diffs)

def record_high_range_occurrence(force: int, bar_index: int):
    if force == 9:
        g_high_force_9_indices.append(bar_index)
    elif force == 10:
        g_high_force_10_indices.append(bar_index)
    cutoff = max(0, bar_index - 200)
    g_high_force_9_indices = [i for i in g_high_force_9_indices if i >= cutoff]
    g_high_force_10_indices = [i for i in g_high_force_10_indices if i >= cutoff]

def update_high_range_search(bar_index: int):
    global g_high_range_search
    if g_high_range_search:
        elapsed = bar_index - g_high_range_search['created_at_bar']
        remaining = max(0, g_high_range_search['window'] - elapsed)
        g_high_range_search['remaining'] = remaining
        if remaining == 0:
            logger.info("Búsqueda de rango alto fallida")
            g_high_range_search = None
        return

    avg9 = compute_average_interval(g_high_force_9_indices)
    avg10 = compute_average_interval(g_high_force_10_indices)

    if avg9 is not None and avg10 is not None:
        if avg9 < avg10:
            target = 9; avg = avg9
        elif avg10 < avg9:
            target = 10; avg = avg10
        else:
            target = 9 if len(g_high_force_9_indices) >= len(g_high_force_10_indices) else 10
            avg = avg9 if target == 9 else avg10
    elif avg9 is not None:
        target = 9; avg = avg9
    elif avg10 is not None:
        target = 10; avg = avg10
    else:
        return

    if len(g_high_force_9_indices) < HIGH_RANGE_MIN_OCCURRENCES and len(g_high_force_10_indices) < HIGH_RANGE_MIN_OCCURRENCES:
        return

    window = max(2, int(round(avg)) + 2)
    g_high_range_search = {
        'target': target,
        'expected_in': int(round(avg)),
        'remaining': window,
        'created_at_bar': bar_index,
        'window': window,
        'avg': avg
    }
    logger.info(f"Iniciada búsqueda rango alto {target} ventana {window}")

def check_high_range_signal(bar_index: int) -> Tuple[bool, float]:
    if not g_high_range_search:
        return False, 0.0
    remaining = g_high_range_search['remaining']
    window = g_high_range_search['window']
    elapsed = bar_index - g_high_range_search['created_at_bar']

    if remaining <= 3 and remaining > 0 and elapsed >= window * 0.5:
        target = g_high_range_search['target']
        indices = g_high_force_9_indices if target == 9 else g_high_force_10_indices
        if len(indices) >= 3:
            diffs = [indices[i] - indices[i-1] for i in range(1, len(indices))]
            mean = sum(diffs) / len(diffs)
            variance = sum((d - mean) ** 2 for d in diffs) / len(diffs)
            std = variance ** 0.5
            cv = std / mean if mean > 0 else 1.0
            confidence = max(0, 100 * (1 - min(cv, 1.0)))
            if confidence >= HIGH_RANGE_CONFIDENCE_THRESHOLD:
                return True, confidence
    return False, 0.0

# ─── RANGO DE CUOTA (para 3x) ──────────────────────────────────────────────
def derive_prediction(trigger: float, ts: float) -> str:
    sec_str = str(int(ts))[-4:]
    raw = f"{trigger:.2f}|{sec_str}|3x"
    hex_digest = hashlib.sha256(raw.encode()).hexdigest()
    n = int(hex_digest[:8], 16)
    r = (n % 1000) / 10.0
    if r < 20:
        coef = 3.0 + (int(r / 5)) * 0.5
    elif r < 45:
        coef = 5.0 + (int((r - 20) / 5)) * 1.0
    elif r < 70:
        coef = 10.0 + (int((r - 45) / 5)) * 2.0
    elif r < 90:
        coef = 20.0 + (int((r - 70) / 5)) * 5.0
    else:
        coef = 50.0 + (int((r - 90) / 5)) * 10.0
    lo = max(3.0, coef * 0.8)
    hi = coef * 1.2
    return f"{lo:.1f}x – {hi:.1f}x"

# ─── ESTADO DE SEÑAL ACTIVA ─────────────────────────────────────────────────
class ActiveSignal:
    def __init__(self, kind: str, trigger: float):
        self.kind    = kind    # '2x', '3x', 'trend', 'high_range'
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
            'high_force_9': g_high_force_9_indices,
            'high_force_10': g_high_force_10_indices,
            'recent_results': g_recent_results[-MAX_RECENT_RESULTS:],
            'consecutive_misses': g_consecutive_misses,
            'consecutive_hits': g_consecutive_hits,
            'learning_conf_penalty': g_learning_conf_penalty,
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
    global g_last_high_2x_value, g_last_high_3x_value
    global g_high_force_9_indices, g_high_force_10_indices
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
        g_events_data_2x = [(e[0], e[1]) for e in data.get('events2x', [])[-MAX_HIST_2X:]]
        g_events_data_3x = [(e[0], e[1]) for e in data.get('events3x', [])[-MAX_HIST_3X:]]
        if g_events_data_2x:
            g_last_high_2x_value = g_events_data_2x[-1][1]
        if g_events_data_3x:
            g_last_high_3x_value = g_events_data_3x[-1][1]
        g_high_force_9_indices = data.get('high_force_9', [])
        g_high_force_10_indices = data.get('high_force_10', [])
        cutoff = max(0, len(g_mults) - 200)
        g_high_force_9_indices = [i for i in g_high_force_9_indices if i >= cutoff]
        g_high_force_10_indices = [i for i in g_high_force_10_indices if i >= cutoff]
        # Cargar estado de aprendizaje adaptativo
        loaded_results = data.get('recent_results', [])
        g_recent_results.clear()
        g_recent_results.extend(loaded_results[-MAX_RECENT_RESULTS:])
        saved_misses = data.get('consecutive_misses', {})
        saved_hits   = data.get('consecutive_hits', {})
        saved_penalty = data.get('learning_conf_penalty', {})
        for k in ('2x', '3x', 'trend', 'high_range'):
            g_consecutive_misses[k] = saved_misses.get(k, 0)
            g_consecutive_hits[k]   = saved_hits.get(k, 0)
            g_learning_conf_penalty[k] = saved_penalty.get(k, 0.0)
        today = (datetime.utcnow() - timedelta(hours=3)).strftime("%Y-%m-%d")
        if data.get('daily_date', '') == today:
            g_daily_hits   = data.get('daily_hits',   0)
            g_daily_misses = data.get('daily_misses', 0)
            g_daily_date   = today
        else:
            g_daily_hits = g_daily_misses = 0
            g_daily_date = today
        logger.info(f"Cargado: {len(g_mults)} mults | ev2x:{len(g_events_data_2x)} ev3x:{len(g_events_data_3x)} | high9:{len(g_high_force_9_indices)} high10:{len(g_high_force_10_indices)}")
    except Exception as e:
        logger.warning(f"load error: {e}")

# ─── MENSAJES DE SEÑAL ──────────────────────────────────────────────────────
# Señales de tiempo (2x y 3x) mantienen su formato completo con ETA y confianza
async def _send_signal_2x(trigger: float, attempt: int,
                          avg_gap: float, eta_s: float, conf: float,
                          last_high_value: float):
    hora = argentina_time()
    txt = (
        f"<b>🆔 SEÑAL SPACEMAN — 🕐 {hora}</b>\n"
        f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
        f"<b>🧨 ÚLTIMA CUOTA: {last_high_value:.2f}x</b>\n"
        f"<b>💠 OBJETIVO: {TARGET_2X:.2f}x</b>\n"
        f"<b>🛡️ SEGURO: {INSURANCE_2_0:.2f}x</b>\n"
        f"<b>♣️ ETA próximo 2X: ~{eta_s:.0f}s</b>\n"
        f"<b>💡 CONFIANZA: {conf:.0f}%</b>\n"
        f"<b>🔄 INTENTO {attempt}/{MAX_ATTEMPTS}</b>"
    )
    await broadcast_signal(txt)

async def _send_signal_3x(trigger: float, attempt: int,
                          avg_gap: float, eta_s: float,
                          conf: float, coef_range: str,
                          last_high_value: float):
    hora = argentina_time()
    eta_abs = abs(eta_s)
    if eta_s > 0:
        eta_txt = f"en ~{eta_abs:.0f}s"
    elif eta_s < -1:
        eta_txt = f"hace {eta_abs:.0f}s (ventana +{SIGNAL_WINDOW_3X_A}s)"
    else:
        eta_txt = "¡AHORA!"
    txt = (
        f"<b>💎 SEÑAL SPACEMAN — 🕐 {hora}</b>\n"
        f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
        f"<b>🧨 ÚLTIMA CUOTA: {last_high_value:.2f}x</b>\n"
        f"<b>💠 OBJETIVO: {TARGET_3X:.2f}x</b>\n"
        f"<b>🛡️ SEGURO: {INSURANCE_2_0:.2f}x</b>\n"
        f"<b>♣️ ETA próximo ≥3X: {eta_txt}</b>\n"
        f"<b>💡 CONFIANZA: {conf:.0f}%</b>\n"
        f"<b>🔄 INTENTO {attempt}/{MAX_ATTEMPTS}</b>"
    )
    await broadcast_signal(txt)

# Señales de tendencia y rango alto usan el formato básico (sin ETA ni confianza)
async def _send_signal_trend(trigger: float, attempt: int, last_value: float):
    hora = argentina_time()
    txt = (
        f"<b>♦️ SEÑAL SPACEMAN — 🕐 {hora}</b>\n"
        f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
        f"<b>🧨 ÚLTIMA CUOTA: {last_value:.2f}x</b>\n"
        f"<b>💠 OBJETIVO: {TARGET_2X:.2f}x</b>\n"
        f"<b>🛡️ SEGURO: {INSURANCE_2_0:.2f}x</b>\n"
        f"<b>🔄 INTENTO {attempt}/{MAX_ATTEMPTS}</b>"
    )
    await broadcast_signal(txt)

async def _send_signal_high_range(trigger: float, attempt: int, target_label: str):
    hora = argentina_time()
    txt = (
        f"<b>♦️ SEÑAL SPACEMAN — 🕐 {hora}</b>\n"
        f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
        f"<b>🧨 ÚLTIMA CUOTA: {trigger:.2f}x</b>\n"
        f"<b>💠 OBJETIVO: ≥{target_label}</b>\n"
        f"<b>🛡️ SEGURO: {INSURANCE_2_0:.2f}x</b>\n"
        f"<b>🔄 INTENTO {attempt}/{MAX_ATTEMPTS}</b>"
    )
    await broadcast_signal(txt)

async def _send_hit(value: float, attempt: int, kind: str):
    hora = argentina_time()
    emoji, label = "✅", f"GANAMOS {value:.2f}x"
    txt = (
        f"<b>{emoji} {label} — 🕐 {hora}</b>\n"
        f"<b>Señal {kind.upper()} · Intento {attempt}/{MAX_ATTEMPTS}</b>"
    )
    await broadcast(txt)

async def _send_insurance(value: float, attempt: int, kind: str):
    hora = argentina_time()
    txt = (
        f"<b>🛡️ SEGURO ACTIVADO {value:.2f}x — 🕐 {hora}</b>\n"
        f"<b>Señal {kind.upper()} · Intento {attempt}/{MAX_ATTEMPTS}</b>"
    )
    await broadcast(txt)

async def _send_miss(value: float, attempt: int, kind: str):
    hora = argentina_time()
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
    global g_last_trend_fire, g_trend_cooldown_mod
    global g_last_high_range_fire, g_high_range_cooldown_mod
    global g_mults, g_seen_ids, _persist_counter
    global g_daily_hits, g_daily_misses
    global g_last_high_2x_value, g_last_high_3x_value
    global g_high_range_search

    logger.info(f"🎲 {value:.2f}x | ID:{round_id} | señal:{active_signal.kind if active_signal else 'idle'}")

    _check_daily_reset()

    # ── Fase 1: Resolver señal activa ─────────────────────────────────────────
    if active_signal is not None:
        sig  = active_signal
        kind = sig.kind
        att  = sig.attempt

        if value >= TARGET_2X:  # Acierto completo
            g_daily_hits += 1
            record_signal_result(kind, 'hit', value, att)
            await _send_hit(value, att, kind)
            await _broadcast_scoreboard()
            save_to_disk()
            _clear_signal()
            g_cooldown2x_mod = max(g_cooldown2x_mod, 3)
            g_cooldown3x_mod = max(g_cooldown3x_mod, 3)
            g_trend_cooldown_mod = max(g_trend_cooldown_mod, 3)
            g_high_range_cooldown_mod = max(g_high_range_cooldown_mod, 3)
        elif value >= INSURANCE_2_0:  # Seguro activado (≥2.00x, igual que objetivo → siempre es acierto)
            if att < MAX_ATTEMPTS:
                sig.attempt += 1
                logger.info(f"Seguro en intento {att} para {kind}. Pasando a {sig.attempt}.")
            else:
                g_daily_misses += 1
                record_signal_result(kind, 'miss', value, att)
                await _send_insurance(value, att, kind)
                await _broadcast_scoreboard()
                save_to_disk()
                _clear_signal()
            g_cooldown2x_mod = max(g_cooldown2x_mod, 2)
            g_cooldown3x_mod = max(g_cooldown3x_mod, 2)
            g_trend_cooldown_mod = max(g_trend_cooldown_mod, 2)
            g_high_range_cooldown_mod = max(g_high_range_cooldown_mod, 2)
        else:  # Fallo total (<2.00x)
            if att < MAX_ATTEMPTS:
                sig.attempt += 1
                logger.info(f"Intento {att} fallido para {kind}. Pasando a {sig.attempt}.")
            else:
                g_daily_misses += 1
                record_signal_result(kind, 'miss', value, att)
                await _send_miss(value, att, kind)
                await _broadcast_scoreboard()
                save_to_disk()
                _clear_signal()
            g_cooldown2x_mod = max(g_cooldown2x_mod, 2)
            g_cooldown3x_mod = max(g_cooldown3x_mod, 2)
            g_trend_cooldown_mod = max(g_trend_cooldown_mod, 2)
            g_high_range_cooldown_mod = max(g_high_range_cooldown_mod, 2)

    g_cooldown2x_mod = max(0, g_cooldown2x_mod - 1)
    g_cooldown3x_mod = max(0, g_cooldown3x_mod - 1)
    g_trend_cooldown_mod = max(0, g_trend_cooldown_mod - 1)
    g_high_range_cooldown_mod = max(0, g_high_range_cooldown_mod - 1)

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

    # ── Fase 3: Registrar eventos de timing ─────────────────────────────────
    if value >= 2.0:
        _register_event(now_ts, value, g_events_data_2x, MAX_HIST_2X)
        g_last_high_2x_value = value
        g_armed2x = False
        logger.info(f"🔄 Timer 2x reseteado — cuota {value:.2f}x")

    if value >= 3.0:
        _register_event(now_ts, value, g_events_data_3x, MAX_HIST_3X)
        g_last_high_3x_value = value
        g_armed3x = False
        logger.info(f"🔄 Timer 3x reseteado — cuota {value:.2f}x")

    # ── Fase 3b: Actualizar búsqueda de rango alto ──────────────────────────
    force = classify(value)
    if force in (9, 10):
        record_high_range_occurrence(force, len(g_mults) - 1)
    update_high_range_search(len(g_mults) - 1)

    # ── Fase 4: Disparar señales (solo si no hay señal activa) ────────────────
    if active_signal is not None:
        return

    can_timing = (value < 2.0)

    # ── 4a. Señal 3x (timing, prioridad) ─────────────────────────────────────
    if can_timing and g_cooldown3x_mod == 0:
        in_win3, eta3, avg3, conf3, _ = _predict_next_event(
            g_events_data_3x, MIN_EVENTS_3X,
            SIGNAL_WINDOW_3X_B, SIGNAL_WINDOW_3X_A,
            alpha=ALPHA_3X,
            use_value_correction=True,
            kind='3x'
        )
        eff_thresh_3x = get_effective_confidence_threshold('3x')
        if in_win3 and not g_armed3x:
            if conf3 >= eff_thresh_3x:
                elapsed_since = now_ts - g_last_fire3x
                if elapsed_since >= COOLDOWN_3X:
                    g_armed3x      = True
                    g_last_fire3x  = now_ts
                    g_cooldown3x_mod = 8
                    coef_range = derive_prediction(value, now_ts)
                    active_signal = ActiveSignal('3x', value)
                    logger.info(
                        f"🎯 SEÑAL 3x (timing) | trigger={value:.2f}x | ETA~{eta3:.0f}s | "
                        f"avg={avg3:.1f}s | conf={conf3:.0f}% | rango={coef_range}"
                    )
                    await _send_signal_3x(value, 1, avg3, eta3, conf3, coef_range, g_last_high_3x_value)
                    return
            else:
                logger.debug(f"Señal 3x descartada por baja confianza: {conf3:.1f}% (umbral adaptativo {eff_thresh_3x:.1f}%)")
        elif not in_win3 and g_armed3x:
            g_armed3x = False

    # ── 4b. Señal 2x (timing) ──────────────────────────────────────────────────
    if can_timing and g_cooldown2x_mod == 0:
        in_win2, eta2, avg2, conf2, _ = _predict_next_event(
            g_events_data_2x, MIN_EVENTS_2X,
            SIGNAL_WINDOW_2X, 0.0,
            alpha=ALPHA_2X,
            use_value_correction=True,
            kind='2x'
        )
        eff_thresh_2x = get_effective_confidence_threshold('2x')
        if in_win2 and not g_armed2x:
            if conf2 >= eff_thresh_2x:
                elapsed_since = now_ts - g_last_fire2x
                if elapsed_since >= SIGNAL_COOLDOWN_2X:
                    g_armed2x      = True
                    g_last_fire2x  = now_ts
                    g_cooldown2x_mod = 6
                    active_signal = ActiveSignal('2x', value)
                    logger.info(
                        f"🎯 SEÑAL 2x (timing) | trigger={value:.2f}x | ETA~{eta2:.0f}s | "
                        f"avg={avg2:.1f}s | conf={conf2:.0f}% | umbral_adapt={eff_thresh_2x:.0f}%"
                    )
                    await _send_signal_2x(value, 1, avg2, eta2, conf2, g_last_high_2x_value)
                    return
            else:
                logger.debug(f"Señal 2x descartada por baja confianza: {conf2:.1f}% (umbral adaptativo {eff_thresh_2x:.1f}%)")
        elif not in_win2 and g_armed2x:
            g_armed2x = False

    # ── 4c. Señal de tendencia ────────────────────────────────────────────────
    if g_trend_cooldown_mod == 0:
        elapsed_since = now_ts - g_last_trend_fire
        if elapsed_since >= TREND_COOLDOWN:
            if check_trend_conditions():
                # Solo disparar si la precisión reciente de tendencia es aceptable
                trend_acc = compute_recent_accuracy('trend', window=10)
                if trend_acc >= 0.35 or len([r for r in g_recent_results if r['kind'] == 'trend']) < 5:
                    g_last_trend_fire = now_ts
                    g_trend_cooldown_mod = 8
                    active_signal = ActiveSignal('trend', value)
                    logger.info(f"📈 SEÑAL TREND | trigger={value:.2f}x | acc_reciente={trend_acc:.0%}")
                    await _send_signal_trend(value, 1, value)
                    return
                else:
                    logger.info(f"📈 Señal TREND bloqueada por baja precisión reciente: {trend_acc:.0%}")

    # ── 4d. Señal de rango alto ──────────────────────────────────────────────
    if g_high_range_cooldown_mod == 0 and g_high_range_search is not None:
        elapsed_since = now_ts - g_last_high_range_fire
        if elapsed_since >= HIGH_RANGE_COOLDOWN:
            signal, conf = check_high_range_signal(len(g_mults) - 1)
            if signal:
                target = g_high_range_search['target']
                target_label = "10x" if target == 9 else "15x"
                g_last_high_range_fire = now_ts
                g_high_range_cooldown_mod = 10
                active_signal = ActiveSignal('high_range', value)
                logger.info(f"🔮 SEÑAL HIGH RANGE | target={target_label}, remaining={g_high_range_search['remaining']}, conf={conf:.1f}%")
                await _send_signal_high_range(value, 1, target_label)
                return

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
    gaps2 = [g_events_data_2x[i][0] - g_events_data_2x[i-1][0] for i in range(1, len(g_events_data_2x)) if g_events_data_2x[i][0] > g_events_data_2x[i-1][0]]
    gaps3 = [g_events_data_3x[i][0] - g_events_data_3x[i-1][0] for i in range(1, len(g_events_data_3x)) if g_events_data_3x[i][0] > g_events_data_3x[i-1][0]]
    avg2 = sum(gaps2)/len(gaps2) if gaps2 else None
    avg3 = sum(gaps3)/len(gaps3) if gaps3 else None
    ev2 = len(g_events_data_2x)
    ev3 = len(g_events_data_3x)
    info2 = f"<code>{avg2:.1f}s</code> avg ({ev2} eventos)" if avg2 else f"<code>{ev2}/{MIN_EVENTS_2X}</code> eventos"
    info3 = f"<code>{avg3:.1f}s</code> avg ({ev3} eventos)" if avg3 else f"<code>{ev3}/{MIN_EVENTS_3X}</code> eventos"
    high_info = "⏳ Sin datos suficientes"
    if g_high_range_search:
        target_label = "10x" if g_high_range_search['target'] == 9 else "15x"
        remaining = g_high_range_search['remaining']
        avg = g_high_range_search['avg']
        high_info = f"🔍 Buscando {target_label} | restan {remaining} velas | promedio {avg:.1f}"
    elif len(g_high_force_9_indices) >= HIGH_RANGE_MIN_OCCURRENCES or len(g_high_force_10_indices) >= HIGH_RANGE_MIN_OCCURRENCES:
        high_info = "⏳ Esperando ventana para rango alto"
    await bot.reply_to(message,
        f"🚀 <b>¡Bienvenido {name}!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <b>Bot de Señales Spaceman — Predictor Híbrido + Aprendizaje</b>\n\n"
        f"🔵 <b>Señal 2x (Timing)</b> · Objetivo ≥<code>2.00x</code> · Seguro <code>2.00x</code>\n"
        f"   Predictor adaptativo: {info2}\n\n"
        f"🚀 <b>Señal 3x (Timing)</b> · Objetivo ≥<code>3.00x</code> · Seguro <code>2.00x</code>\n"
        f"   Predictor adaptativo: {info3}\n"
        f"   + Rango de cuota estimado por hash\n\n"
        f"📈 <b>Señal de Tendencia</b> · Objetivo ≥<code>2.00x</code> · Seguro <code>2.00x</code>\n"
        f"   Basada en continuación alcista\n\n"
        f"🔮 <b>Señal de Rango Alto</b> · Objetivo ≥<code>10x/15x</code> · Seguro <code>2.00x</code>\n"
        f"   {high_info}\n\n"
        f"🔄 Cada señal tiene <code>{MAX_ATTEMPTS}</code> intentos\n"
        f"✅ Acierto = ≥2x (o ≥3x según señal)\n"
        f"🛡️ Seguro = 2.00x\n"
        f"🧠 Umbral adaptativo según historial de fallos\n"
        f"🎯 Confianza mínima para timing: <code>{MIN_CONFIDENCE}%</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')

@bot.message_handler(commands=['predictor'])
async def cmd_predictor(message):
    g_all_chats.add(message.chat.id)
    hora = argentina_time()
    now  = time.time()

    ev2  = len(g_events_data_2x)
    gaps2 = [g_events_data_2x[i][0] - g_events_data_2x[i-1][0] for i in range(1, ev2) if g_events_data_2x[i][0] > g_events_data_2x[i-1][0]]
    avg2 = sum(gaps2)/len(gaps2) if gaps2 else None
    if avg2 and ev2 >= MIN_EVENTS_2X:
        el2  = now - g_events_data_2x[-1][0] if g_events_data_2x else 0
        eta2 = max(0, avg2 - el2)
        cv2 = (sum((g-avg2)**2 for g in gaps2)/len(gaps2))**0.5 / avg2 if avg2 else 1
        reg2 = max(0, 1-cv2)
        info2 = (f"⏱ Avg gap: <code>{avg2:.1f}s</code> | Transcurrido: <code>{el2:.1f}s</code>\n"
                 f"   ETA: <code>~{eta2:.0f}s</code> | Regularidad: <code>{reg2*100:.0f}%</code>")
    else:
        info2 = f"⏳ Acumulando: <code>{ev2}/{MIN_EVENTS_2X}</code> eventos ≥2x"

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

    trend_status = "✅ Activo" if check_trend_conditions() else "⏳ Inactivo"
    last_trend_sec = int(time.time() - g_last_trend_fire) if g_last_trend_fire else 0
    trend_cooldown = max(0, TREND_COOLDOWN - last_trend_sec)

    high_info = "⏳ Sin búsqueda activa"
    if g_high_range_search:
        target_label = "10x" if g_high_range_search['target'] == 9 else "15x"
        remaining = g_high_range_search['remaining']
        avg = g_high_range_search['avg']
        high_info = f"🔍 Buscando {target_label} | restan {remaining} velas | promedio {avg:.1f}"
    elif len(g_high_force_9_indices) >= HIGH_RANGE_MIN_OCCURRENCES or len(g_high_force_10_indices) >= HIGH_RANGE_MIN_OCCURRENCES:
        high_info = "⏳ Esperando ventana para rango alto"

    await bot.reply_to(message,
        f"📡 <b>PREDICTORES — 🕐 {hora}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔵 <b>Predictor 2x (Timing)</b>\n{info2}\n"
        f"   🧠 Umbral adapt.: <code>{get_effective_confidence_threshold('2x'):.0f}%</code> | Precisión reciente: <code>{compute_recent_accuracy('2x')*100:.0f}%</code>\n\n"
        f"🚀 <b>Predictor 3x (Timing)</b>\n{info3}\n"
        f"   🧠 Umbral adapt.: <code>{get_effective_confidence_threshold('3x'):.0f}%</code> | Precisión reciente: <code>{compute_recent_accuracy('3x')*100:.0f}%</code>\n\n"
        f"📈 <b>Predictor de Tendencia</b>\n"
        f"   Estado: {trend_status}\n"
        f"   Cooldown: <code>{trend_cooldown}s</code> | Precisión reciente: <code>{compute_recent_accuracy('trend')*100:.0f}%</code>\n\n"
        f"🔮 <b>Predictor de Rango Alto</b>\n"
        f"   {high_info}\n"
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
        f"🔮 Rangos altos: 9: <code>{len(g_high_force_9_indices)}</code> | 10: <code>{len(g_high_force_10_indices)}</code>\n"
        f"🧠 <b>Aprendizaje adaptativo</b>\n"
        f"   2x  · fallos consec: <code>{g_consecutive_misses['2x']}</code> · penalidad: <code>{g_learning_conf_penalty['2x']:.0f}pt</code>\n"
        f"   3x  · fallos consec: <code>{g_consecutive_misses['3x']}</code> · penalidad: <code>{g_learning_conf_penalty['3x']:.0f}pt</code>\n"
        f"   trend · fallos consec: <code>{g_consecutive_misses['trend']}</code> · penalidad: <code>{g_learning_conf_penalty['trend']:.0f}pt</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')

# ─── MAIN ────────────────────────────────────────────────────────────────────
async def main_async():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    logger.info("🤖 Iniciando SpacemanBot (Predictor Híbrido + Trend + High Range)...")
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
