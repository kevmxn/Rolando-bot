#!/usr/bin/env python3
"""
SPACEMAN 2x Signal Bot — Telegram + Render
- WebSocket Pragmatic Play (Spaceman/Aviator)
- Filtro de señales: <2x < 51% AND 2-5x > 29% (últimos 100)
- Gestión Martingale 2×3: C1, C2, C3 · máx 1 gale por columna
- Mensaje de tendencia cada nueva cuota (elimina el anterior)
- No envía tendencia cuando hay señal activa
- Flask + AsyncTeleBot + Webhook (compatible Render)
- Persistencia: SQLite (reemplaza JSON)
- Fix: señales huérfanas eliminadas
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

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ.get("BOT_TOKEN", "8889373350:AAFU7R1ENyANVR-DiZbBMbeyAHZOi9DLlXY")
CHAT_ID    = int(os.environ.get("CHAT_ID", "-1003815888467"))

WS_URL    = os.environ.get("WS_URL", "wss://dga.pragmaticplaylive.net/ws")
CASINO_ID = os.environ.get("CASINO_ID", "ppcdk00000005349")
CURRENCY  = os.environ.get("CURRENCY", "BRL")
GAME_ID   = int(os.environ.get("GAME_ID", "1301"))

DB_FILE = os.environ.get("DB_FILE", "spaceman.db")

def argentina_time() -> str:
    return (datetime.utcnow() - timedelta(hours=3)).strftime("%H:%M")

# Umbrales de filtro (últimos 100 multiplicadores)
UMBRAL_BELOW2  = 51.51
UMBRAL_2TO5    = 28.49
HISTORY_MAX    = 150

# Estrategia 2x
CASHOUT_TARGET  = 2.00
MAX_GALES       = 1
MAX_COLS        = 3

# ─── SQLITE — ESQUEMA ─────────────────────────────────────────────────────────
def db_init():
    """Crea tablas si no existen."""
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS history (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            value     REAL    NOT NULL,
            created   TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS state (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    con.commit()
    con.close()

def _db():
    """Conexión SQLite con row_factory."""
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    return con

# ─── PERSISTENCIA — ESTADO ────────────────────────────────────────────────────
_STATE_KEYS = [
    "signal_active", "signal_attempt", "signal_col", "signal_lost",
    "trend_msg_id", "stats_msg_id", "last_result",
    "daily_wins", "daily_losses", "consecutive_wins",
]

def save_state():
    """Guarda todas las variables de estado en la tabla `state`."""
    global signal_active, signal_attempt, signal_col, signal_lost
    global trend_msg_id, stats_msg_id, last_result
    global daily_wins, daily_losses, consecutive_wins

    values = {
        "signal_active":    str(int(signal_active)),
        "signal_attempt":   str(signal_attempt),
        "signal_col":       str(signal_col),
        "signal_lost":      str(signal_lost),
        "trend_msg_id":     str(trend_msg_id) if trend_msg_id is not None else "",
        "stats_msg_id":     str(stats_msg_id) if stats_msg_id is not None else "",
        "signal_msg_id":    str(signal_msg_id) if signal_msg_id is not None else "",
        "last_result":      str(last_result) if last_result is not None else "",
        "daily_wins":       str(daily_wins),
        "daily_losses":     str(daily_losses),
        "daily_col_losses": str(daily_col_losses),
        "consecutive_wins": str(consecutive_wins),
    }
    try:
        con = _db()
        cur = con.cursor()
        cur.executemany(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            list(values.items())
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"Error guardando estado: {e}")

def load_state():
    """Carga variables de estado desde la tabla `state`."""
    global signal_active, signal_attempt, signal_col, signal_lost
    global trend_msg_id, stats_msg_id, signal_msg_id, last_result
    global daily_wins, daily_losses, daily_col_losses, consecutive_wins

    try:
        con = _db()
        rows = con.execute("SELECT key, value FROM state").fetchall()
        con.close()
        d = {r["key"]: r["value"] for r in rows}

        signal_active    = bool(int(d.get("signal_active", "0")))
        signal_attempt   = int(d.get("signal_attempt", "1"))
        signal_col       = int(d.get("signal_col", "1"))
        signal_lost      = float(d.get("signal_lost", "0.0"))
        _tid             = d.get("trend_msg_id", "")
        trend_msg_id     = int(_tid) if _tid else None
        _sid             = d.get("stats_msg_id", "")
        stats_msg_id     = int(_sid) if _sid else None
        _smid            = d.get("signal_msg_id", "")
        signal_msg_id    = int(_smid) if _smid else None
        _lr              = d.get("last_result", "")
        last_result      = float(_lr) if _lr else None
        daily_wins       = int(d.get("daily_wins", "0"))
        daily_losses     = int(d.get("daily_losses", "0"))
        daily_col_losses = int(d.get("daily_col_losses", "0"))
        consecutive_wins = int(d.get("consecutive_wins", "0"))

        logger.info(f"Estado cargado desde SQLite | señal_activa={signal_active} col={signal_col}")
    except Exception as e:
        logger.warning(f"Error cargando estado: {e}")

# ─── PERSISTENCIA — HISTORIAL ─────────────────────────────────────────────────
def save_value(value: float):
    """Inserta un valor en la tabla history y poda los más viejos."""
    try:
        con = _db()
        con.execute("INSERT INTO history(value) VALUES(?)", (value,))
        # Mantener solo los últimos HISTORY_MAX registros
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
    """Carga los últimos HISTORY_MAX valores ordenados (más antiguo primero)."""
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

# ─── ESTADO GLOBAL ────────────────────────────────────────────────────────────
history: List[float] = []

signal_active   = False
signal_attempt  = 1
signal_col      = 1
signal_lost     = 0.0
signal_base_bet = 10.0

trend_msg_id: Optional[int]  = None
stats_msg_id: Optional[int]  = None
signal_msg_id: Optional[int] = None   # mensaje de señal activa (para editar)
last_result: Optional[float] = None

hist_loaded = False

daily_wins: int       = 0
daily_losses: int     = 0      # ciclos completos de 3 cols perdidos
daily_col_losses: int = 0      # columnas individuales perdidas
consecutive_wins: int = 0

# ─── Bot + Flask ───────────────────────────────────────────────────────────────
bot = AsyncTeleBot(BOT_TOKEN, parse_mode='HTML')
_main_loop: asyncio.AbstractEventLoop = None
flask_app = Flask(__name__)

# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
async def send_message(text: str, parse_mode: str = "HTML") -> Optional[int]:
    try:
        msg = await bot.send_message(CHAT_ID, text, parse_mode=parse_mode)
        return msg.message_id
    except Exception as e:
        logger.warning(f"Error enviando mensaje: {e}")
        return None

async def edit_message(msg_id: int, text: str, parse_mode: str = "HTML") -> bool:
    try:
        await bot.edit_message_text(text, CHAT_ID, msg_id, parse_mode=parse_mode)
        return True
    except Exception as e:
        logger.debug(f"Error editando mensaje {msg_id}: {e}")
        return False

async def delete_message(msg_id: int) -> bool:
    try:
        await bot.delete_message(CHAT_ID, msg_id)
        return True
    except Exception as e:
        logger.debug(f"Error borrando mensaje {msg_id}: {e}")
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
        "total": total,
        "below2": below2,
        "two_to_five": two_to_five,
        "pct_below2": pct_below2,
        "pct_2to5": pct_2to5,
        "favorable": favorable,
    }

def build_trend_message(stats: dict) -> str:
    now    = argentina_time()
    last5  = list(history)[-5:][::-1] if history else []
    last5_str = ", ".join(f"{v:.2f}x" for v in last5) if last5 else "—"

    below2_ok = stats["pct_below2"] < UMBRAL_BELOW2
    two5_ok   = stats["pct_2to5"]   > UMBRAL_2TO5

    if stats["favorable"]:
        header = f"🟢 <b>TENDENCIA FAVORABLE — {now}</b>"
        mark2  = "✅"
    else:
        header = f"🔴 <b>TENDENCIA DESFAVORABLE — {now}</b>"
        mark2  = "❌"

    below2_mark = "✅" if below2_ok else "❌"

    return (
        f"{header}\n"
        f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        f"<b>📈 Análisis últimos {stats['total']} multiplicadores</b>\n"
        f"<b>🔵 1.00-1.99x = {stats['below2']} — {stats['pct_below2']:.2f}%{below2_mark}</b>\n"
        f"<b>🟡 2.00-4.99x = {stats['two_to_five']} — {stats['pct_2to5']:.2f}%{mark2}</b>\n"
        f"<b>🆔 ({last5_str})</b>"
    )

# ─── MENSAJES DE SEÑAL ────────────────────────────────────────────────────────
def build_signal_message(last_value: float = None, attempt: int = 1) -> str:
    """
    Intento 1 → formato original con MÁXIMO N GALES.
    Intento 2 → mismo header pero con SEGUNDA OPORTUNIDAD.
    En ambos casos last_value es la última cuota registrada.
    """
    if last_value is None:
        last_value = CASHOUT_TRIGGER

    if attempt == 1:
        footer = f"🔁 <b>MÁXIMO {MAX_GALES} GALES</b>\n"
    else:
        footer = "🔁 <b>SEGUNDA OPORTUNIDAD</b>\n"

    return (
        "<b>✅ ENTRADA CONFIRMADA ✅</b>\n\n"
        f"<b>👉 INGRESAR DESPUÉS: {last_value:.2f}x</b>\n"
        f"<b>💰 RETIRAR EN: {CASHOUT_TARGET:.2f}x</b>\n\n"
        f"{footer}"
    )

def build_win_message(result: float) -> str:
    return (
        "<b>🍀🍀🍀 GANAMOS!!! 🍀🍀🍀</b>\n"
        f"<b>✅ Resultado: {result:.2f}x</b>"
    )

def build_loss_message(result: float) -> str:
    return (
        "<b>🔴 PERDIMOS!!! 🔴</b>\n"
        f"<b>❌ Resultado: {result:.2f}x</b>"
    )



def build_stats_message() -> str:
    total_ops = daily_wins + daily_losses + daily_col_losses
    win_pct   = (daily_wins / total_ops * 100) if total_ops > 0 else 0.0
    losses_txt = daily_losses + daily_col_losses  # total de señales perdidas
    return (
        f"🚀 <b>Resultado del día ✅ {daily_wins} ⭕ {losses_txt}</b>\n"
        f"💎 <b>Acertamos el {win_pct:.2f}% de las veces</b>\n"
        f"📈 <b>¡Tenemos {consecutive_wins} victorias consecutivas!</b>"
    )

async def send_stats_update():
    global stats_msg_id
    if signal_active:
        return
    if stats_msg_id:
        await delete_message(stats_msg_id)
    stats_msg_id = await send_message(build_stats_message())

# ─── LÓGICA DE SEÑALES — EMA cruce ───────────────────────────────────────────
def calc_ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k   = 2 / (period + 1)
    ema = [values[0]]
    for v in values[1:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema

def check_signal_2x(vals: List[float]) -> bool:
    if len(vals) < 20:
        return False
    stats = get_stats()
    if not stats["favorable"]:
        return False
    ema4_series  = calc_ema(vals, 4)
    ema20_series = calc_ema(vals, 20)
    cur_ema4   = ema4_series[-1]
    cur_ema20  = ema20_series[-1]
    prev_ema4  = ema4_series[-2]  if len(ema4_series)  > 1 else cur_ema4
    prev_ema20 = ema20_series[-2] if len(ema20_series) > 1 else cur_ema20
    return (prev_ema4 <= prev_ema20) and (cur_ema4 > cur_ema20)

# ─── PROCESAMIENTO DE CADA NUEVA CUOTA ───────────────────────────────────────
async def process_new_value(value: float, silent: bool = False):
    """
    Llamado con cada nueva cuota en vivo.
    MÁQUINA DE ESTADOS:
        idle  → detectar señal → activo (intento 1)
        activo:
            win  → idle + stats
            loss:
                attempt < MAX_GALES+1 → gale (attempt++)
                attempt == MAX_GALES+1:
                    col < MAX_COLS → siguiente columna (col++, signal_active=False,
                                      espera nueva señal para col siguiente)
                    col == MAX_COLS → perdida total → idle + stats
    """
    global signal_active, signal_attempt, signal_col, signal_lost
    global trend_msg_id, stats_msg_id, signal_msg_id, last_result, history
    global daily_wins, daily_losses, daily_col_losses, consecutive_wins

    # ── Actualizar historial en memoria y SQLite ──
    history.append(value)
    if len(history) > HISTORY_MAX:
        history = history[-HISTORY_MAX:]
    save_value(value)

    if silent:
        return

    logger.info(
        f"Nueva cuota: {value:.2f}x | historial: {len(history)} "
        f"| señal_activa: {signal_active} | col: {signal_col} | intento: {signal_attempt}"
    )

    # ── Resolver señal activa ──────────────────────────────────────────────────
    if signal_active:
        win = value >= CASHOUT_TARGET

        if win:
            # ── GANAMOS ──
            logger.info(f"✅ GANAMOS {value:.2f}x en intento {signal_attempt} col {signal_col}")
            signal_active  = False
            signal_attempt = 1
            signal_col     = 1
            signal_lost    = 0.0
            signal_msg_id  = None
            daily_wins    += 1
            consecutive_wins += 1
            save_state()
            await send_message(build_win_message(value))
            await send_stats_update()

        else:
            # ── PERDIMOS este intento ──
            logger.info(
                f"❌ Perdido {value:.2f}x | intento {signal_attempt}/{MAX_GALES+1} col {signal_col}/{MAX_COLS}"
            )

            if signal_attempt < (MAX_GALES + 1):
                # Hay gale disponible en esta columna → editar mensaje de señal
                signal_attempt += 1
                signal_lost    += 1
                save_state()
                logger.info(f"↩️  Gale {signal_attempt} en col {signal_col} — esperando próxima ronda")
                new_text = build_signal_message(
                    last_value=value, attempt=signal_attempt
                )
                if signal_msg_id:
                    ok = await edit_message(signal_msg_id, new_text)
                    if not ok:
                        signal_msg_id = await send_message(new_text)
                        save_state()
                else:
                    signal_msg_id = await send_message(new_text)
                    save_state()

            else:
                # Agotamos gales en la columna actual
                signal_attempt  = 1
                signal_lost    += 1
                completed_col   = signal_col
                signal_col     += 1
                daily_col_losses += 1   # contar la columna como pérdida individual

                if signal_col > MAX_COLS:
                    # ── Perdimos las 3 columnas ──
                    logger.info(f"❌ PERDIMOS — 3 columnas agotadas")
                    signal_active  = False
                    signal_col     = 1
                    signal_lost    = 0.0
                    signal_msg_id  = None
                    daily_losses  += 1
                    consecutive_wins = 0
                    save_state()
                    await send_message(build_loss_message(value))
                    await send_stats_update()

                else:
                    # ── Pasamos a siguiente columna; esperamos nueva señal ──
                    logger.info(
                        f"Col {completed_col} agotada → esperando nueva señal para Col {signal_col}"
                    )
                    signal_active = False   # idle hasta nueva señal
                    signal_msg_id = None
                    save_state()
                    await send_message(build_loss_message(value))
                    await send_stats_update()

        return  # No procesar tendencia ni nueva señal en la misma cuota

    # ── Detectar nueva señal ───────────────────────────────────────────────────
    vals = list(history)
    if check_signal_2x(vals):
        signal_active  = True
        signal_attempt = 1
        last_value     = vals[-1] if vals else CASHOUT_TRIGGER
        text           = build_signal_message(last_value=last_value, attempt=1)
        signal_msg_id  = await send_message(text)
        save_state()
        logger.info(
            f"SEÑAL 2x enviada | pct<2={get_stats()['pct_below2']:.1f}% "
            f"2-5={get_stats()['pct_2to5']:.1f}% | col={signal_col}"
        )
        if trend_msg_id:
            await delete_message(trend_msg_id)
            trend_msg_id = None
            save_state()
        return

    # ── Mensaje de tendencia ───────────────────────────────────────────────────
    stats = get_stats()
    if len(history) < 10:
        return

    trend_text = build_trend_message(stats)
    if trend_msg_id:
        ok = await edit_message(trend_msg_id, trend_text)
        if not ok:
            trend_msg_id = await send_message(trend_text)
            save_state()
    else:
        trend_msg_id = await send_message(trend_text)
        save_state()

# ─── WEBSOCKET — PRAGMATIC PLAY ──────────────────────────────────────────────
async def ws_loop():
    global hist_loaded, last_result
    RECONNECT_DELAY = 5

    while True:
        try:
            logger.info(f"Conectando WebSocket: {WS_URL}")
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                sub = {
                    "type":     "subscribe",
                    "casinoId": CASINO_ID,
                    "currency": CURRENCY,
                    "key":      [GAME_ID],
                }
                await ws.send(json.dumps(sub))
                logger.info(f"Suscrito a game {GAME_ID}")

                async for raw in ws:
                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue

                    game_results = data.get("gameResult", [])
                    if not game_results:
                        continue

                    # ── Historial inicial: primer mensaje con múltiples rondas ──
                    if not hist_loaded:
                        hist = list(reversed(game_results))   # más antiguo primero
                        logger.info(f"Cargando historial WS: {len(hist)} rondas")
                        for item in hist:
                            val = (item.get("result")
                                   or item.get("multiplier")
                                   or item.get("crashPoint"))
                            if val is not None:
                                await process_new_value(float(val), silent=True)
                        # El [0] es el más reciente; lo guardamos para deduplicar en vivo
                        newest = game_results[0]
                        last_result = float(
                            newest.get("result")
                            or newest.get("multiplier")
                            or newest.get("crashPoint")
                        )
                        hist_loaded = True
                        logger.info(f"Historial cargado ({len(hist)} rondas) — en vivo")
                        continue

                    # ── Mensajes en vivo: solo gameResults[0] (el más reciente) ──
                    newest = game_results[0]
                    val = (newest.get("result")
                           or newest.get("multiplier")
                           or newest.get("crashPoint"))
                    if val is None:
                        continue
                    val = float(val)
                    if val != last_result:
                        last_result = val
                        await process_new_value(val, silent=False)

        except Exception as e:
            logger.error(f"WS error: {e} — reconectando en {RECONNECT_DELAY}s")
            hist_loaded = False
            await asyncio.sleep(RECONNECT_DELAY)

# ─── FLASK ROUTES ─────────────────────────────────────────────────────────────
@flask_app.route('/')
def home():
    stats = get_stats()
    sig   = 'activa' if signal_active else 'idle'
    tend  = '🟢' if stats['favorable'] else '🔴'
    return f"🤖 SpacemanBot | historial:{len(history)} | señal:{sig} | tendencia:{tend}", 200

@flask_app.route('/webhook', methods=['POST'])
def webhook():
    try:
        update = types.Update.de_json(request.get_json())
        asyncio.run_coroutine_threadsafe(bot.process_new_updates([update]), _main_loop)
        return '', 200
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return "Error interno", 500

@flask_app.route('/health')
def health():
    stats = get_stats()
    return {
        "status":        "ok",
        "history_count": len(history),
        "signal_active": signal_active,
        "signal_col":    signal_col,
        "signal_attempt": signal_attempt,
        "favorable":     stats["favorable"],
        "pct_below2":    round(stats["pct_below2"], 2),
        "pct_2to5":      round(stats["pct_2to5"], 2),
    }, 200

@flask_app.route('/ping')
def ping():
    return 'pong', 200

# ─── TELEGRAM COMMANDS ────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def cmd_start(message):
    name  = message.from_user.first_name or "usuario"
    stats = get_stats()
    await bot.reply_to(message,
        f"🚀 <b>¡Bienvenido {name}!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <b>SPACEMAN BOT 2x</b>\n\n"
        f"🔵 <b>Señal 2x (EMA Cruce)</b>\n"
        f"   Objetivo: ≥ {CASHOUT_TARGET:.2f}x\n"
        f"   Entrada: Después de {CASHOUT_TRIGGER:.2f}x\n"
        f"   Máx {MAX_GALES} gale(s) por columna\n\n"
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
    hora    = argentina_time()
    sig_txt = (
        f"Intento {signal_attempt}/{MAX_GALES+1} Col {signal_col}/{MAX_COLS}"
        if signal_active else "Idle"
    )
    await bot.reply_to(message,
        f"📊 <b>ESTADÍSTICAS — {hora}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Historial: <code>{stats['total']}</code> cuotas\n"
        f"🔵 &lt;2x: {stats['below2']} ({stats['pct_below2']:.1f}%)\n"
        f"🟡 2-5x: {stats['two_to_five']} ({stats['pct_2to5']:.1f}%)\n"
        f"📈 Tendencia: {'🟢 FAVORABLE' if stats['favorable'] else '🔴 DESFAVORABLE'}\n"
        f"📡 Señal: <code>{sig_txt}</code>\n"
        f"✅ Wins hoy: {daily_wins} | ❌ Losses: {daily_losses}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')

# ─── ASYNCIO LOOP ─────────────────────────────────────────────────────────────
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

async def main_async():
    global _main_loop
    _main_loop = asyncio.get_running_loop()

    logger.info("🤖 Iniciando SPACEMAN BOT 2x...")
    db_init()
    load_state()

    # Recuperar historial desde SQLite (sin recontar señales huérfanas)
    loaded = load_history()
    if loaded:
        history.extend(loaded)
        logger.info(f"Historial cargado desde SQLite: {len(history)} valores")

    await bot.set_my_commands([
        types.BotCommand('start', '🤖 Información del bot'),
        types.BotCommand('stats', '📊 Estadísticas'),
    ])

    asyncio.create_task(ws_loop())
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

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_async())
