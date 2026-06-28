#!/usr/bin/env python3
"""
SPACEMAN Dual Signal Bot — Telegram + Render
- Canal 2x  : señales 2.00x — gráfico moderado (3 condiciones EMA)
- Canal 1.5x: señales 1.50x — gráfico moderado (3 condiciones EMA)
- Ambos canales comparten WebSocket y historial, token/chat independientes
- Gestión Martingale C1/C2/C3 · máx 1 gale por columna, por canal
- Persistencia SQLite · Reset estadísticas 00:00 Colombia
"""

import asyncio
import sqlite3
import threading
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, List
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

# ─── CONFIG — CANAL 2x ────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ.get("BOT_TOKEN",  "8889373350:AAFU7R1ENyANVR-DiZbBMbeyAHZOi9DLlXY")
CHAT_ID    = int(os.environ.get("CHAT_ID", "-1003815888467"))

# ─── CONFIG — CANAL 1.5x ──────────────────────────────────────────────────────
BOT_TOKEN_150  = os.environ.get("BOT_TOKEN_150",  "8620810853:AAHw-3JXcQt7Oz6Qcdv16Yt6JBG9m05UyYo")
CHAT_ID_150    = int(os.environ.get("CHAT_ID_150", "-1003274770136"))

# ─── CONFIG — WEBSOCKET ───────────────────────────────────────────────────────
WS_URL    = os.environ.get("WS_URL",    "wss://dga.pragmaticplaylive.net/ws")
CASINO_ID = os.environ.get("CASINO_ID", "ppcdk00000005349")
CURRENCY  = os.environ.get("CURRENCY",  "BRL")
GAME_ID   = int(os.environ.get("GAME_ID", "1301"))

DB_FILE = os.environ.get("DB_FILE", "spaceman.db")

def colombia_now() -> datetime:
    return datetime.utcnow() - timedelta(hours=5)

def colombia_time() -> str:
    return colombia_now().strftime("%H:%M")

# ─── UMBRALES COMPARTIDOS ─────────────────────────────────────────────────────
UMBRAL_BELOW2      = 51.51   # canal 2x: <2x debe ser < este %
UMBRAL_2TO5        = 28.99   # canal 2x: 2-5x debe ser > este %
UMBRAL_BELOW2_150  = 54.01   # canal 1.5x: <2x debe ser < este %
UMBRAL_2TO5_150    = 25.99  # canal 1.5x: 2-5x debe ser > este %
HISTORY_MAX        = 150

# ─── ESTRATEGIA 2x ────────────────────────────────────────────────────────────
CASHOUT_TARGET_2X  = 2.00
CASHOUT_TRIGGER_2X = 2.00
MAX_GALES          = 1
MAX_COLS           = 3

# ─── ESTRATEGIA 1.5x ──────────────────────────────────────────────────────────
CASHOUT_TARGET_150  = 1.50
CASHOUT_TRIGGER_150 = 1.50

# ─── SQLITE — ESQUEMA ─────────────────────────────────────────────────────────
def db_init():
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS history (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            value   REAL    NOT NULL,
            created TEXT    NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS state (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    con.commit()
    con.close()

def _db():
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    return con

# ─── PERSISTENCIA — ESTADO 2x ─────────────────────────────────────────────────
def save_state_2x():
    values = {
        "2x_signal_active":    str(int(s2x_active)),
        "2x_signal_attempt":   str(s2x_attempt),
        "2x_signal_col":       str(s2x_col),
        "2x_signal_lost":      str(s2x_lost),
        "2x_trend_msg_id":     str(trend_msg_id)    if trend_msg_id    is not None else "",
        "2x_stats_msg_id":     str(s2x_stats_msg_id) if s2x_stats_msg_id is not None else "",
        "2x_signal_msg_id":    str(s2x_msg_id)      if s2x_msg_id      is not None else "",
        "2x_daily_wins":       str(s2x_daily_wins),
        "2x_daily_losses":     str(s2x_daily_losses),
        "2x_daily_col_losses": str(s2x_daily_col_losses),
        "2x_consecutive_wins": str(s2x_consecutive_wins),
    }
    _save_dict(values)

def load_state_2x():
    global s2x_active, s2x_attempt, s2x_col, s2x_lost
    global trend_msg_id, s2x_stats_msg_id, s2x_msg_id
    global s2x_daily_wins, s2x_daily_losses, s2x_daily_col_losses, s2x_consecutive_wins
    d = _load_dict()
    s2x_active           = bool(int(d.get("2x_signal_active",    "0")))
    s2x_attempt          = int(d.get("2x_signal_attempt",  "1"))
    s2x_col              = int(d.get("2x_signal_col",      "1"))
    s2x_lost             = float(d.get("2x_signal_lost",   "0.0"))
    _tid                 = d.get("2x_trend_msg_id", "")
    trend_msg_id         = int(_tid) if _tid else None
    _sid                 = d.get("2x_stats_msg_id", "")
    s2x_stats_msg_id     = int(_sid) if _sid else None
    _smid                = d.get("2x_signal_msg_id", "")
    s2x_msg_id           = int(_smid) if _smid else None
    s2x_daily_wins       = int(d.get("2x_daily_wins",       "0"))
    s2x_daily_losses     = int(d.get("2x_daily_losses",     "0"))
    s2x_daily_col_losses = int(d.get("2x_daily_col_losses", "0"))
    s2x_consecutive_wins = int(d.get("2x_consecutive_wins", "0"))
    logger.info(f"[2x] Estado cargado | activa={s2x_active} col={s2x_col}")

# ─── PERSISTENCIA — ESTADO 1.5x ───────────────────────────────────────────────
def save_state_150():
    values = {
        "150_signal_active":    str(int(s150_active)),
        "150_signal_attempt":   str(s150_attempt),
        "150_signal_col":       str(s150_col),
        "150_signal_lost":      str(s150_lost),
        "150_stats_msg_id":     str(s150_stats_msg_id) if s150_stats_msg_id is not None else "",
        "150_signal_msg_id":    str(s150_msg_id)       if s150_msg_id       is not None else "",
        "150_daily_wins":       str(s150_daily_wins),
        "150_daily_losses":     str(s150_daily_losses),
        "150_daily_col_losses": str(s150_daily_col_losses),
        "150_consecutive_wins": str(s150_consecutive_wins),
    }
    _save_dict(values)

def load_state_150():
    global s150_active, s150_attempt, s150_col, s150_lost
    global s150_stats_msg_id, s150_msg_id
    global s150_daily_wins, s150_daily_losses, s150_daily_col_losses, s150_consecutive_wins
    d = _load_dict()
    s150_active           = bool(int(d.get("150_signal_active",    "0")))
    s150_attempt          = int(d.get("150_signal_attempt",  "1"))
    s150_col              = int(d.get("150_signal_col",      "1"))
    s150_lost             = float(d.get("150_signal_lost",   "0.0"))
    _sid                  = d.get("150_stats_msg_id", "")
    s150_stats_msg_id     = int(_sid) if _sid else None
    _smid                 = d.get("150_signal_msg_id", "")
    s150_msg_id           = int(_smid) if _smid else None
    s150_daily_wins       = int(d.get("150_daily_wins",       "0"))
    s150_daily_losses     = int(d.get("150_daily_losses",     "0"))
    s150_daily_col_losses = int(d.get("150_daily_col_losses", "0"))
    s150_consecutive_wins = int(d.get("150_consecutive_wins", "0"))
    logger.info(f"[1.5x] Estado cargado | activa={s150_active} col={s150_col}")

def _save_dict(values: dict):
    try:
        con = _db()
        con.cursor().executemany(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            list(values.items())
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"Error guardando estado: {e}")

def _load_dict() -> dict:
    try:
        con = _db()
        rows = con.execute("SELECT key, value FROM state").fetchall()
        con.close()
        return {r["key"]: r["value"] for r in rows}
    except Exception as e:
        logger.warning(f"Error cargando estado: {e}")
        return {}

# ─── PERSISTENCIA — HISTORIAL ─────────────────────────────────────────────────
def save_value(value: float):
    try:
        con = _db()
        con.execute("INSERT INTO history(value) VALUES(?)", (value,))
        con.execute("""
            DELETE FROM history WHERE id NOT IN (
                SELECT id FROM history ORDER BY id DESC LIMIT ?
            )
        """, (HISTORY_MAX,))
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"Error insertando en history: {e}")

def load_history() -> List[float]:
    try:
        con = _db()
        rows = con.execute(
            "SELECT value FROM history ORDER BY id DESC LIMIT ?", (HISTORY_MAX,)
        ).fetchall()
        con.close()
        return [r["value"] for r in reversed(rows)]
    except Exception as e:
        logger.warning(f"Error cargando history: {e}")
        return []

# ─── ESTADO GLOBAL — COMPARTIDO ───────────────────────────────────────────────
history: List[float] = []
last_result: Optional[float] = None
trend_msg_id: Optional[int]  = None   # compartido — un solo mensaje de tendencia (canal 2x)

# ─── ESTADO GLOBAL — CANAL 2x ─────────────────────────────────────────────────
s2x_active:           bool         = False
s2x_attempt:          int          = 1
s2x_col:              int          = 1
s2x_lost:             float        = 0.0
s2x_msg_id:           Optional[int] = None
s2x_stats_msg_id:     Optional[int] = None
s2x_daily_wins:       int          = 0
s2x_daily_losses:     int          = 0
s2x_daily_col_losses: int          = 0
s2x_consecutive_wins: int          = 0

# ─── ESTADO GLOBAL — CANAL 1.5x ───────────────────────────────────────────────
s150_active:           bool         = False
s150_attempt:          int          = 1
s150_col:              int          = 1
s150_lost:             float        = 0.0
s150_msg_id:           Optional[int] = None
s150_stats_msg_id:     Optional[int] = None
s150_daily_wins:       int          = 0
s150_daily_losses:     int          = 0
s150_daily_col_losses: int          = 0
s150_consecutive_wins: int          = 0

# ─── BOTS + FLASK ─────────────────────────────────────────────────────────────
bot     = AsyncTeleBot(BOT_TOKEN,     parse_mode='HTML')
bot_150 = AsyncTeleBot(BOT_TOKEN_150, parse_mode='HTML')
_main_loop: asyncio.AbstractEventLoop = None
flask_app = Flask(__name__)

# ─── TELEGRAM HELPERS — CANAL 2x ──────────────────────────────────────────────
async def send_2x(text: str) -> Optional[int]:
    try:
        msg = await bot.send_message(CHAT_ID, text, parse_mode="HTML")
        return msg.message_id
    except Exception as e:
        logger.warning(f"[2x] send error: {e}")
        return None

async def edit_2x(msg_id: int, text: str) -> bool:
    try:
        await bot.edit_message_text(text, CHAT_ID, msg_id, parse_mode="HTML")
        return True
    except Exception as e:
        logger.debug(f"[2x] edit error {msg_id}: {e}")
        return False

async def delete_2x(msg_id: int) -> bool:
    try:
        await bot.delete_message(CHAT_ID, msg_id)
        return True
    except Exception as e:
        logger.debug(f"[2x] delete error {msg_id}: {e}")
        return False

# ─── TELEGRAM HELPERS — CANAL 1.5x ────────────────────────────────────────────
async def send_150(text: str) -> Optional[int]:
    try:
        msg = await bot_150.send_message(CHAT_ID_150, text, parse_mode="HTML")
        return msg.message_id
    except Exception as e:
        logger.warning(f"[1.5x] send error: {e}")
        return None

async def edit_150(msg_id: int, text: str) -> bool:
    try:
        await bot_150.edit_message_text(text, CHAT_ID_150, msg_id, parse_mode="HTML")
        return True
    except Exception as e:
        logger.debug(f"[1.5x] edit error {msg_id}: {e}")
        return False

async def delete_150(msg_id: int) -> bool:
    try:
        await bot_150.delete_message(CHAT_ID_150, msg_id)
        return True
    except Exception as e:
        logger.debug(f"[1.5x] delete error {msg_id}: {e}")
        return False

# ─── ANÁLISIS DE TENDENCIA ────────────────────────────────────────────────────
def get_stats() -> dict:
    total = len(history)
    if total == 0:
        return {"total": 0, "below2": 0, "two_to_five": 0,
                "pct_below2": 0.0, "pct_2to5": 0.0, "favorable": False}
    below2      = sum(1 for v in history if v < 2.00)
    two_to_five = sum(1 for v in history if 2.00 <= v < 5.00)
    pct_below2  = (below2 / total) * 100
    pct_2to5    = (two_to_five / total) * 100
    favorable   = (pct_below2 < UMBRAL_BELOW2) and (pct_2to5 > UMBRAL_2TO5)
    return {
        "total": total, "below2": below2, "two_to_five": two_to_five,
        "pct_below2": pct_below2, "pct_2to5": pct_2to5, "favorable": favorable,
    }

def get_stats_150() -> dict:
    """Mismos datos pero 'favorable' usa umbrales propios del canal 1.5x."""
    total = len(history)
    if total == 0:
        return {"total": 0, "below2": 0, "two_to_five": 0,
                "pct_below2": 0.0, "pct_2to5": 0.0, "favorable": False}
    below2      = sum(1 for v in history if v < 2.00)
    two_to_five = sum(1 for v in history if 2.00 <= v < 5.00)
    pct_below2  = (below2 / total) * 100
    pct_2to5    = (two_to_five / total) * 100
    favorable   = (pct_below2 < UMBRAL_BELOW2_150) and (pct_2to5 > UMBRAL_2TO5_150)
    return {
        "total": total, "below2": below2, "two_to_five": two_to_five,
        "pct_below2": pct_below2, "pct_2to5": pct_2to5, "favorable": favorable,
    }

def build_trend_message(stats: dict) -> str:
    now       = colombia_time()
    last5     = list(history)[-5:][::-1] if history else []
    last5_str = ", ".join(f"{v:.2f}x" for v in last5) if last5 else "—"
    below2_ok = stats["pct_below2"] < UMBRAL_BELOW2
    two5_ok   = stats["pct_2to5"]   > UMBRAL_2TO5
    if stats["favorable"]:
        header    = f"🟢 <b>TENDENCIA FAVORABLE — {now}</b>"
        mark2     = "✅"
    else:
        header    = f"🔴 <b>TENDENCIA DESFAVORABLE — {now}</b>"
        mark2     = "❌"
    below2_mark = "✅" if below2_ok else "❌"
    return (
        f"{header}\n"
        f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        f"<b>📈 Análisis últimos {stats['total']} multiplicadores</b>\n"
        f"<b>🔵 1.00-1.99x = {stats['below2']} — {stats['pct_below2']:.2f}%{below2_mark}</b>\n"
        f"<b>🟡 2.00-4.99x = {stats['two_to_five']} — {stats['pct_2to5']:.2f}%{mark2}</b>\n"
        f"<b>🆔 ({last5_str})</b>"
    )

# ─── EMA + POSICIONES (compartido) ────────────────────────────────────────────
def calc_ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k   = 2 / (period + 1)
    ema = [values[0]]
    for v in values[1:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema

def build_positions(vals: List[float]) -> List[float]:
    """Acumulador +1/-1 según >= 2.00 o < 2.00 (gráfico moderado)."""
    positions = [0]
    current   = 0
    for v in vals[1:]:
        current += 1 if v >= 2.00 else -1
        positions.append(current)
    return positions

# ─── DETECCIÓN DE SEÑALES ─────────────────────────────────────────────────────
def _ema_components(vals: List[float]):
    """Devuelve (positions, ema4, ema8, ema20, cur_pos, cur_e4, cur_e8, cur_e20,
                  prev_e4, prev_e8, prev_e20) o None si no hay suficientes datos."""
    if len(vals) < 8:
        return None
    positions    = build_positions(vals)
    ema4_series  = calc_ema(positions, 4)
    ema8_series  = calc_ema(positions, 8)
    ema20_series = calc_ema(positions, 20)
    if len(ema8_series) < 2 or len(ema20_series) < 2:
        return None
    return (
        positions,
        ema4_series, ema8_series, ema20_series,
        positions[-1],
        ema4_series[-1],  ema8_series[-1],  ema20_series[-1],
        ema4_series[-2],  ema8_series[-2],  ema20_series[-2],
    )

def check_signal_2x(vals: List[float]) -> bool:
    """
    3 condiciones gráfico moderado — alerta 2.00:
    C1: EMA8 cruza EMA20 hacia arriba
    C2: Patrón W en últimas 3 posiciones con pos > ema4/8/20
    C3: Dos cuotas >= 2x consecutivas (la anterior < 2x) con ema4 > ema8 > ema20
    """
    if not get_stats()["favorable"]:
        return False
    r = _ema_components(vals)
    if r is None:
        return False
    positions, _, ema8_s, ema20_s, cur_pos, cur_e4, cur_e8, cur_e20, prev_e4, prev_e8, prev_e20 = r

    cond1 = (prev_e8 <= prev_e20) and (cur_e8 > cur_e20)

    cond2 = False
    if len(positions) >= 3:
        a, b, c = positions[-3], positions[-2], positions[-1]
        if abs(a - c) <= 1 and b > a and cur_pos > cur_e4 and cur_pos > cur_e8 and cur_pos > cur_e20:
            cond2 = True

    cond3 = False
    if (len(vals) >= 2 and vals[-1] >= 2.00 and vals[-2] >= 2.00 and
            cur_e4 > cur_e8 and cur_e8 > cur_e20):
        if len(vals) < 3 or vals[-3] < 2.00:
            cond3 = True

    return cond1 or cond2 or cond3

def check_signal_150(vals: List[float]) -> bool:
    """
    3 condiciones gráfico moderado — alerta 1.50:
    C1: Patrón 3 bajos + 1 alto: últimas 4 rondas <2, <2, <2, >=2
        Y pos <= ema4 Y pos <= ema8
    C2: EMA4 cruza EMA8 hacia arriba
    C3: Soporte: pos <= min(últimas 20 pos)*1.01
        Y pos > ema4 Y pos > ema8 Y pos > ema20
    Solo dispara si tendencia 1.5x favorable (<2x<54%, 2-5x>26%).
    """
    if not get_stats_150()["favorable"]:
        return False
    r = _ema_components(vals)
    if r is None:
        return False
    positions, ema4_s, ema8_s, _, cur_pos, cur_e4, cur_e8, cur_e20, prev_e4, prev_e8, _ = r

    cond1 = False
    if len(vals) >= 4:
        cambios = [1 if v >= 2.00 else -1 for v in vals[-4:]]
        c1, c2, c3, c4 = cambios
        if c1 == -1 and c2 == -1 and c3 == -1 and c4 == 1:
            if cur_pos <= cur_e4 and cur_pos <= cur_e8:
                cond1 = True

    cond2 = (prev_e4 <= prev_e8) and (cur_e4 > cur_e8)

    cond3 = False
    if len(positions) >= 20:
        soporte = min(positions[-20:])
    else:
        soporte = min(positions)
    if (cur_pos <= soporte * 1.01 and
            cur_pos > cur_e4 and cur_pos > cur_e8 and cur_pos > cur_e20):
        cond3 = True

    return cond1 or cond2 or cond3


# ─── MENSAJES — CANAL 2x ──────────────────────────────────────────────────────
def _col_indicator(col: int) -> str:
    """🔴=perdida  🔵=activa  ⚫=pendiente"""
    icons = []
    for i in range(1, MAX_COLS + 1):
        if i < col:
            icons.append("🔴")
        elif i == col:
            icons.append("🔵")
        else:
            icons.append("⚫")
    return "".join(icons)

def build_signal_msg_2x(last_value: float, attempt: int) -> str:
    footer    = f"🔁 <b>MÁXIMO {MAX_GALES} GALE</b>" if attempt == 1 else "🔁 <b>SEGUNDA OPORTUNIDAD</b>"
    col_label = f"💎 <b>NIVEL DE COLUMNA: {_col_indicator(s2x_col)}</b>"
    return (
        "<b>✅ ENTRADA CONFIRMADA ✅</b>\n\n"
        f"<b>👉 INGRESAR DESPUÉS: {last_value:.2f}x</b>\n"
        f"<b>💰 RETIRAR EN: {CASHOUT_TARGET_2X:.2f}x</b>\n\n"
        f"{footer}\n"
        f"{col_label}\n\n"
        f"<i>🔞 +18 | Apueste con Responsabilidad</i>"
    )

def build_win_msg_2x(result: float) -> str:
    return (
        "<b>🍀🍀🍀 GANAMOS!!! 🍀🍀🍀</b>\n"
        f"<b>✅ Resultado: {result:.2f}x</b>"
    )

def build_loss_msg_2x(result: float) -> str:
    return (
        "<b>🔴 PERDIMOS!!! 🔴</b>\n"
        f"<b>❌ Resultado: {result:.2f}x</b>"
    )

def build_stats_msg_2x() -> str:
    total_ops  = s2x_daily_wins + s2x_daily_losses + s2x_daily_col_losses
    losses_txt = s2x_daily_losses + s2x_daily_col_losses
    win_pct    = (s2x_daily_wins / total_ops * 100) if total_ops > 0 else 0.0
    return (
        f"🚀 <b>Resultado del día ✅ {s2x_daily_wins} ⭕ {losses_txt}</b>\n"
        f"💎 <b>Acertamos el {win_pct:.2f}% de las veces</b>\n"
        f"📈 <b>¡Tenemos {s2x_consecutive_wins} victorias consecutivas!</b>"
    )

# ─── MENSAJES — CANAL 1.5x ────────────────────────────────────────────────────
def build_signal_msg_150(last_value: float, attempt: int) -> str:
    footer    = f"🔁 <b>MÁXIMO {MAX_GALES} GALE</b>" if attempt == 1 else "🔁 <b>SEGUNDA OPORTUNIDAD</b>"
    col_label = f"💎 <b>NIVEL DE COLUMNA: {_col_indicator(s150_col)}</b>"
    return (
        "<b>✅ ENTRADA CONFIRMADA ✅</b>\n\n"
        f"<b>👉 INGRESAR DESPUÉS: {last_value:.2f}x</b>\n"
        f"<b>💰 RETIRAR EN: {CASHOUT_TARGET_150:.2f}x</b>\n\n"
        f"{footer}\n"
        f"{col_label}\n\n"
        f"<i>🔞 +18 | Apueste con Responsabilidad</i>"
    )

def build_win_msg_150(result: float) -> str:
    return (
        "<b>🍀🍀🍀 GANAMOS!!! 🍀🍀🍀</b>\n"
        f"<b>✅ Resultado: {result:.2f}x</b>"
    )

def build_loss_msg_150(result: float) -> str:
    return (
        "<b>🔴 PERDIMOS!!! 🔴</b>\n"
        f"<b>❌ Resultado: {result:.2f}x</b>"
    )

def build_stats_msg_150() -> str:
    total_ops  = s150_daily_wins + s150_daily_losses + s150_daily_col_losses
    losses_txt = s150_daily_losses + s150_daily_col_losses
    win_pct    = (s150_daily_wins / total_ops * 100) if total_ops > 0 else 0.0
    return (
        f"🚀 <b>Resultado del día ✅ {s150_daily_wins} ⭕ {losses_txt}</b>\n"
        f"💎 <b>Acertamos el {win_pct:.2f}% de las veces</b>\n"
        f"📈 <b>¡Tenemos {s150_consecutive_wins} victorias consecutivas!</b>"
    )

# ─── STATS UPDATE ─────────────────────────────────────────────────────────────
async def send_stats_update_2x():
    global s2x_stats_msg_id
    if s2x_active:
        return
    if s2x_stats_msg_id:
        await delete_2x(s2x_stats_msg_id)
    s2x_stats_msg_id = await send_2x(build_stats_msg_2x())
    save_state_2x()

async def send_stats_update_150():
    global s150_stats_msg_id
    if s150_active:
        return
    if s150_stats_msg_id:
        await delete_150(s150_stats_msg_id)
    s150_stats_msg_id = await send_150(build_stats_msg_150())
    save_state_150()

# ─── MÁQUINA DE ESTADOS — CANAL 2x ────────────────────────────────────────────
async def resolve_2x(value: float):
    """Resuelve resultado de la señal 2x activa."""
    global s2x_active, s2x_attempt, s2x_col, s2x_lost, s2x_msg_id
    global s2x_daily_wins, s2x_daily_losses, s2x_daily_col_losses, s2x_consecutive_wins

    win = value >= CASHOUT_TARGET_2X

    if win:
        logger.info(f"[2x] ✅ GANAMOS {value:.2f}x | intento {s2x_attempt} col {s2x_col}")
        s2x_active          = False
        s2x_attempt         = 1
        s2x_col             = 1
        s2x_lost            = 0.0
        s2x_msg_id          = None
        s2x_daily_wins     += 1
        s2x_consecutive_wins += 1
        save_state_2x()
        await send_2x(build_win_msg_2x(value))
        await send_stats_update_2x()

    else:
        logger.info(f"[2x] ❌ {value:.2f}x | intento {s2x_attempt}/{MAX_GALES+1} col {s2x_col}/{MAX_COLS}")

        if s2x_attempt < (MAX_GALES + 1):
            # Gale disponible → editar mensaje
            s2x_attempt += 1
            s2x_lost    += 1
            save_state_2x()
            new_text = build_signal_msg_2x(last_value=value, attempt=s2x_attempt)
            if s2x_msg_id:
                ok = await edit_2x(s2x_msg_id, new_text)
                if not ok:
                    s2x_msg_id = await send_2x(new_text)
                    save_state_2x()
            else:
                s2x_msg_id = await send_2x(new_text)
                save_state_2x()

        else:
            # Columna agotada
            completed_col        = s2x_col
            s2x_attempt          = 1
            s2x_lost            += 1
            s2x_col             += 1
            s2x_daily_col_losses += 1

            if s2x_col > MAX_COLS:
                logger.info("[2x] ❌ 3 columnas agotadas")
                s2x_active          = False
                s2x_col             = 1
                s2x_lost            = 0.0
                s2x_msg_id          = None
                s2x_daily_losses   += 1
                s2x_consecutive_wins = 0
                save_state_2x()
                await send_2x(build_loss_msg_2x(value))
                await send_stats_update_2x()
            else:
                logger.info(f"[2x] Col {completed_col} agotada → esperando nueva señal C{s2x_col}")
                s2x_active = False
                s2x_msg_id = None
                save_state_2x()
                await send_2x(build_loss_msg_2x(value))
                await send_stats_update_2x()

# ─── MÁQUINA DE ESTADOS — CANAL 1.5x ──────────────────────────────────────────
async def resolve_150(value: float):
    """Resuelve resultado de la señal 1.5x activa."""
    global s150_active, s150_attempt, s150_col, s150_lost, s150_msg_id
    global s150_daily_wins, s150_daily_losses, s150_daily_col_losses, s150_consecutive_wins

    win = value >= CASHOUT_TARGET_150

    if win:
        logger.info(f"[1.5x] ✅ GANAMOS {value:.2f}x | intento {s150_attempt} col {s150_col}")
        s150_active           = False
        s150_attempt          = 1
        s150_col              = 1
        s150_lost             = 0.0
        s150_msg_id           = None
        s150_daily_wins      += 1
        s150_consecutive_wins += 1
        save_state_150()
        await send_150(build_win_msg_150(value))
        await send_stats_update_150()

    else:
        logger.info(f"[1.5x] ❌ {value:.2f}x | intento {s150_attempt}/{MAX_GALES+1} col {s150_col}/{MAX_COLS}")

        if s150_attempt < (MAX_GALES + 1):
            s150_attempt += 1
            s150_lost    += 1
            save_state_150()
            new_text = build_signal_msg_150(last_value=value, attempt=s150_attempt)
            if s150_msg_id:
                ok = await edit_150(s150_msg_id, new_text)
                if not ok:
                    s150_msg_id = await send_150(new_text)
                    save_state_150()
            else:
                s150_msg_id = await send_150(new_text)
                save_state_150()

        else:
            completed_col         = s150_col
            s150_attempt          = 1
            s150_lost            += 1
            s150_col             += 1
            s150_daily_col_losses += 1

            if s150_col > MAX_COLS:
                logger.info("[1.5x] ❌ 3 columnas agotadas")
                s150_active           = False
                s150_col              = 1
                s150_lost             = 0.0
                s150_msg_id           = None
                s150_daily_losses    += 1
                s150_consecutive_wins = 0
                save_state_150()
                await send_150(build_loss_msg_150(value))
                await send_stats_update_150()
            else:
                logger.info(f"[1.5x] Col {completed_col} agotada → esperando nueva señal C{s150_col}")
                s150_active = False
                s150_msg_id = None
                save_state_150()
                await send_150(build_loss_msg_150(value))
                await send_stats_update_150()

# ─── PROCESAMIENTO CENTRAL ────────────────────────────────────────────────────
async def process_new_value(value: float, silent: bool = False):
    global last_result, history, trend_msg_id
    global s2x_active, s2x_attempt, s2x_col, s2x_msg_id
    global s150_active, s150_attempt, s150_col, s150_msg_id

    # Historial compartido
    history.append(value)
    if len(history) > HISTORY_MAX:
        history = history[-HISTORY_MAX:]
    save_value(value)

    if silent:
        return

    logger.info(
        f"Nueva cuota: {value:.2f}x | hist:{len(history)} "
        f"| 2x activa:{s2x_active} C{s2x_col} | 1.5x activa:{s150_active} C{s150_col}"
    )

    vals = list(history)

    # ── CANAL 2x ──────────────────────────────────────────────────────────────
    s2x_fired_this_round = False   # rastrea si 2x disparó señal en esta ronda
    if s2x_active:
        await resolve_2x(value)
    else:
        if check_signal_2x(vals):
            s2x_active         = True
            s2x_attempt        = 1
            s2x_fired_this_round = True
            text               = build_signal_msg_2x(last_value=value, attempt=1)
            s2x_msg_id         = await send_2x(text)
            save_state_2x()
            logger.info(f"[2x] Señal enviada | col={s2x_col}")
            if trend_msg_id:
                await delete_2x(trend_msg_id)
                trend_msg_id = None
                save_state_2x()

    # ── CANAL 1.5x ────────────────────────────────────────────────────────────
    # Dispara con señal propia (1.5x) O como réplica del canal 2x SOLO si 2x
    # envió señal en esta misma ronda (s2x_fired_this_round=True).
    if s150_active:
        await resolve_150(value)
    else:
        sig150 = check_signal_150(vals)
        # Réplica 2x→1.5x solo si el canal 2x realmente disparó en esta ronda
        sig2x_replica = s2x_fired_this_round
        if sig150 or sig2x_replica:
            origen       = "1.5x" if sig150 else "2x→1.5x"
            s150_active  = True
            s150_attempt = 1
            text         = build_signal_msg_150(last_value=value, attempt=1)
            s150_msg_id  = await send_150(text)
            save_state_150()
            logger.info(f"[1.5x] Señal enviada ({origen}) | col={s150_col}")

    # ── Tendencia (canal 2x, solo cuando ninguna señal 2x activa) ─────────────
    if not s2x_active and len(history) >= 10:
        stats      = get_stats()
        trend_text = build_trend_message(stats)
        if trend_msg_id:
            ok = await edit_2x(trend_msg_id, trend_text)
            if not ok:
                trend_msg_id = await send_2x(trend_text)
                save_state_2x()
        else:
            trend_msg_id = await send_2x(trend_text)
            save_state_2x()

# ─── WEBSOCKET ────────────────────────────────────────────────────────────────
async def ws_loop():
    global last_result
    RECONNECT_DELAY = 5

    def _get_val(item: dict):
        v = item.get("result") or item.get("multiplier") or item.get("crashPoint")
        return float(v) if v is not None else None

    while True:
        try:
            logger.info(f"Conectando WebSocket: {WS_URL}")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10, close_timeout=5) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe", "casinoId": CASINO_ID,
                    "currency": CURRENCY, "key": [GAME_ID],
                }))
                logger.info(f"Suscrito a game {GAME_ID}")
                async for raw in ws:
                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue
                    game_results = data.get("gameResult", [])
                    if not game_results:
                        continue
                    val = _get_val(game_results[0])
                    if val is None or val == last_result:
                        continue
                    last_result = val
                    await process_new_value(val, silent=False)
        except Exception as e:
            logger.error(f"WS error: {e} — reconectando en {RECONNECT_DELAY}s")
            await asyncio.sleep(RECONNECT_DELAY)

# ─── FLASK ROUTES ─────────────────────────────────────────────────────────────
@flask_app.route('/')
def home():
    stats = get_stats()
    return (
        f"🤖 SpacemanBot | hist:{len(history)} "
        f"| 2x:{'activa' if s2x_active else 'idle'} C{s2x_col} "
        f"| 1.5x:{'activa' if s150_active else 'idle'} C{s150_col} "
        f"| tend:{'🟢' if stats['favorable'] else '🔴'}"
    ), 200

@flask_app.route('/webhook', methods=['POST'])
def webhook():
    try:
        update = types.Update.de_json(request.get_json())
        asyncio.run_coroutine_threadsafe(bot.process_new_updates([update]), _main_loop)
        return '', 200
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return "Error interno", 500

@flask_app.route('/webhook150', methods=['POST'])
def webhook_150():
    try:
        update = types.Update.de_json(request.get_json())
        asyncio.run_coroutine_threadsafe(bot_150.process_new_updates([update]), _main_loop)
        return '', 200
    except Exception as e:
        logger.error(f"Webhook 1.5x error: {e}")
        return "Error interno", 500

@flask_app.route('/health')
def health():
    stats = get_stats()
    return {
        "status": "ok", "history_count": len(history),
        "2x":  {"active": s2x_active,  "col": s2x_col,  "attempt": s2x_attempt},
        "150": {"active": s150_active, "col": s150_col, "attempt": s150_attempt},
        "favorable": stats["favorable"],
        "pct_below2": round(stats["pct_below2"], 2),
        "pct_2to5":   round(stats["pct_2to5"],   2),
    }, 200

@flask_app.route('/ping')
def ping():
    return 'pong', 200

# ─── TELEGRAM COMMANDS — CANAL 2x ─────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def cmd_start(message):
    name  = message.from_user.first_name or "usuario"
    stats = get_stats()
    await bot.reply_to(message,
        f"🚀 <b>¡Bienvenido {name}!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <b>SPACEMAN BOT 2x</b>\n\n"
        f"🔵 <b>Señal 2x — Gráfico Moderado</b>\n"
        f"   Objetivo: ≥ {CASHOUT_TARGET_2X:.2f}x\n"
        f"   Máx {MAX_GALES} gale(s) · {MAX_COLS} columnas\n\n"
        f"📊 <b>Filtro de Tendencia</b>\n"
        f"   &lt;2x &lt; {UMBRAL_BELOW2}% ✅\n"
        f"   2-5x &gt; {UMBRAL_2TO5}% ✅\n\n"
        f"📈 <b>Estado Actual</b>\n"
        f"   Historial: {len(history)} cuotas\n"
        f"   Favorable: {'✅' if stats['favorable'] else '❌'}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')

@bot.message_handler(commands=['stats'])
async def cmd_stats(message):
    stats   = get_stats()
    hora    = colombia_time()
    sig_txt = (f"Intento {s2x_attempt}/{MAX_GALES+1} Col {s2x_col}/{MAX_COLS}"
               if s2x_active else "Idle")
    losses_txt = s2x_daily_losses + s2x_daily_col_losses
    total_ops  = s2x_daily_wins + losses_txt
    win_pct    = (s2x_daily_wins / total_ops * 100) if total_ops > 0 else 0.0
    await bot.reply_to(message,
        f"📊 <b>ESTADÍSTICAS 2x — {hora}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Historial: <code>{stats['total']}</code> cuotas\n"
        f"🔵 &lt;2x: {stats['below2']} ({stats['pct_below2']:.1f}%)\n"
        f"🟡 2-5x: {stats['two_to_five']} ({stats['pct_2to5']:.1f}%)\n"
        f"📈 Tendencia: {'🟢 FAVORABLE' if stats['favorable'] else '🔴 DESFAVORABLE'}\n"
        f"📡 Señal: <code>{sig_txt}</code>\n"
        f"✅ Wins: {s2x_daily_wins} | ❌ Losses: {losses_txt} | 💎 {win_pct:.1f}%\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')

# ─── TELEGRAM COMMANDS — CANAL 1.5x ───────────────────────────────────────────
@bot_150.message_handler(commands=['start'])
async def cmd_start_150(message):
    name = message.from_user.first_name or "usuario"
    await bot_150.reply_to(message,
        f"🚀 <b>¡Bienvenido {name}!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <b>SPACEMAN BOT 1.5x</b>\n\n"
        f"🟠 <b>Señal 1.5x — Gráfico Moderado</b>\n"
        f"   Objetivo: ≥ {CASHOUT_TARGET_150:.2f}x\n"
        f"   Máx {MAX_GALES} gale(s) · {MAX_COLS} columnas\n\n"
        f"📈 <b>Estado Actual</b>\n"
        f"   Historial: {len(history)} cuotas\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')

@bot_150.message_handler(commands=['stats'])
async def cmd_stats_150(message):
    hora    = colombia_time()
    sig_txt = (f"Intento {s150_attempt}/{MAX_GALES+1} Col {s150_col}/{MAX_COLS}"
               if s150_active else "Idle")
    losses_txt = s150_daily_losses + s150_daily_col_losses
    total_ops  = s150_daily_wins + losses_txt
    win_pct    = (s150_daily_wins / total_ops * 100) if total_ops > 0 else 0.0
    await bot_150.reply_to(message,
        f"📊 <b>ESTADÍSTICAS 1.5x — {hora}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Historial: <code>{len(history)}</code> cuotas\n"
        f"📡 Señal: <code>{sig_txt}</code>\n"
        f"✅ Wins: {s150_daily_wins} | ❌ Losses: {losses_txt} | 💎 {win_pct:.1f}%\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')

# ─── LOOPS ────────────────────────────────────────────────────────────────────
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

async def daily_reset_loop():
    """Reinicia estadísticas de ambos canales a las 00:00 Colombia."""
    global s2x_daily_wins, s2x_daily_losses, s2x_daily_col_losses, s2x_consecutive_wins
    global s150_daily_wins, s150_daily_losses, s150_daily_col_losses, s150_consecutive_wins
    while True:
        now           = colombia_now()
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        await asyncio.sleep((next_midnight - now).total_seconds())

        meses   = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                   "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
        dia_str = f"{now.day} de {meses[now.month - 1]} {now.year}"

        # Resumen 2x
        total_2x   = s2x_daily_wins + s2x_daily_losses + s2x_daily_col_losses
        losses_2x  = s2x_daily_losses + s2x_daily_col_losses
        pct_2x     = (s2x_daily_wins / total_2x * 100) if total_2x > 0 else 0.0
        await send_2x(
            f"🤑 <b>Resultados del {dia_str}</b>\n"
            f"🚀 <b>Resultado del día ✅ {s2x_daily_wins} ⭕ {losses_2x}</b>\n"
            f"💎 <b>Acertamos el {pct_2x:.2f}% de las veces</b>\n"
            f"📈 <b>¡Tenemos {s2x_consecutive_wins} victorias consecutivas!</b>"
        )

        # Resumen 1.5x
        total_150  = s150_daily_wins + s150_daily_losses + s150_daily_col_losses
        losses_150 = s150_daily_losses + s150_daily_col_losses
        pct_150    = (s150_daily_wins / total_150 * 100) if total_150 > 0 else 0.0
        await send_150(
            f"🤑 <b>Resultados del {dia_str}</b>\n"
            f"🚀 <b>Resultado del día ✅ {s150_daily_wins} ⭕ {losses_150}</b>\n"
            f"💎 <b>Acertamos el {pct_150:.2f}% de las veces</b>\n"
            f"📈 <b>¡Tenemos {s150_consecutive_wins} victorias consecutivas!</b>"
        )

        # Reset ambos
        s2x_daily_wins = s2x_daily_losses = s2x_daily_col_losses = s2x_consecutive_wins = 0
        s150_daily_wins = s150_daily_losses = s150_daily_col_losses = s150_consecutive_wins = 0
        save_state_2x()
        save_state_150()
        logger.info("🔄 Estadísticas reiniciadas — 00:00 Colombia")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main_async():
    global _main_loop
    _main_loop = asyncio.get_running_loop()

    logger.info("🤖 Iniciando SPACEMAN Dual Bot (2x + 1.5x)...")
    db_init()
    load_state_2x()
    load_state_150()

    loaded = load_history()
    if loaded:
        history.extend(loaded)
        logger.info(f"Historial cargado: {len(history)} valores")

    await bot.set_my_commands([
        types.BotCommand('start', '🤖 Información del bot'),
        types.BotCommand('stats', '📊 Estadísticas 2x'),
    ])
    await bot_150.set_my_commands([
        types.BotCommand('start', '🤖 Información del bot'),
        types.BotCommand('stats', '📊 Estadísticas 1.5x'),
    ])

    asyncio.create_task(ws_loop())
    asyncio.create_task(self_ping_loop())
    asyncio.create_task(daily_reset_loop())

    render_url = os.environ.get('RENDER_EXTERNAL_URL', '').rstrip('/')
    if render_url:
        await bot.remove_webhook()
        await bot_150.remove_webhook()
        await asyncio.sleep(1)
        await bot.set_webhook(url=f"{render_url}/webhook")
        await bot_150.set_webhook(url=f"{render_url}/webhook150")
        logger.info(f"✅ Webhooks: {render_url}/webhook  |  {render_url}/webhook150")
        while True:
            await asyncio.sleep(3600)
    else:
        logger.warning("⚠️ Usando polling (dev local)")
        await asyncio.gather(
            bot.infinity_polling(skip_pending=True),
            bot_150.infinity_polling(skip_pending=True),
        )

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

if __name__ == '__main__':
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_async())
