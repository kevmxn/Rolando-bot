#!/usr/bin/env python3
"""
AVIATOR 2x Signal Bot — Telegram + Render
Basado en versionanterior.py (estructura probada en Render)
- WebSocket Pragmatic Play (Spaceman/Aviator)
- Filtro de señales: <2x < 51% AND 2-5x > 29% (últimos 100)
- Gestión Martingale 2×3: C1, C2, C3 · máx 1 gale por columna
- Mensaje de tendencia cada nueva cuota (elimina el anterior)
- No envía tendencia cuando hay señal activa
- Flask + AsyncTeleBot + Webhook (compatible Render)
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
BOT_TOKEN  = os.environ.get("BOT_TOKEN", os.environ.get("TELEGRAM_TOKEN", "8620810853:AAHw-3JXcQt7Oz6Qcdv16Yt6JBG9m05UyYo"))
CHAT_ID    = int(os.environ.get("CHAT_ID", "-1003274770136"))

WS_URL    = "wss://cgp.pragmaticplaylive.net/ws"
CASINO_ID = os.environ.get("CASINO_ID", "ppcdk00000005349")
CURRENCY  = os.environ.get("CURRENCY", "BRL")
GAME_ID   = os.environ.get("GAME_ID", "1301")

# Zona horaria Argentina
def argentina_time() -> str:
    return (datetime.utcnow() - timedelta(hours=3)).strftime("%H:%M")

# Umbrales de filtro (últimos 100 multiplicadores)
UMBRAL_BELOW2  = 51   # <2x debe ser MENOR a este %
UMBRAL_2TO5    = 29   # 2-5x debe ser MAYOR a este %
HISTORY_MAX    = 100

# Estrategia 2x
CASHOUT_TRIGGER = 1.27   # entrada después de este multiplicador
CASHOUT_TARGET  = 2.00   # retirar en
MAX_GALES       = 1      # máximo 1 gale (2 intentos por columna)
MAX_COLS        = 3      # C1, C2, C3

# ─── ESTADO GLOBAL ──────────────────────────────────────────────────────────
history: list = []

# Señal activa
signal_active   = False
signal_attempt  = 1    # 1 = intento principal, 2 = gale
signal_col      = 1    # columna actual 1..3
signal_lost     = 0.0  # acumulado perdido en este ciclo
signal_base_bet = 10.0 # apuesta base

# Mensaje de tendencia (para editar/eliminar)
trend_msg_id: Optional[int] = None

# Última cuota registrada (evita duplicados)
last_result: Optional[float] = None

# Historial cargado
hist_loaded = False

# Contador para persistencia
_persist_counter: int = 0
PERSIST_FILE = "spaceman_history.json"

# Variables para asyncio y threading
bot = AsyncTeleBot(BOT_TOKEN, parse_mode='HTML')
_main_loop: asyncio.AbstractEventLoop = None

# ─── FLASK APP ────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

# ─── PERSISTENCIA ─────────────────────────────────────────────────────────────
def save_to_disk():
    global _persist_counter
    try:
        _persist_counter += 1
        data = {
            "history": list(history)[-HISTORY_MAX:],
            "signal_active": signal_active,
            "signal_attempt": signal_attempt,
            "signal_col": signal_col,
            "signal_lost": signal_lost,
            "trend_msg_id": trend_msg_id,
            "last_result": last_result,
        }
        with open(PERSIST_FILE, 'w') as f:
            json.dump(data, f)
        if _persist_counter % 50 == 0:
            logger.debug(f"Guardado en disco (contador={_persist_counter})")
    except Exception as e:
        logger.warning(f"Error guardando estado: {e}")

def load_from_disk():
    global history, signal_active, signal_attempt, signal_col, signal_lost, trend_msg_id, last_result
    try:
        if os.path.exists(PERSIST_FILE):
            with open(PERSIST_FILE, 'r') as f:
                data = json.load(f)
            history = data.get("history", [])
            signal_active = data.get("signal_active", False)
            signal_attempt = data.get("signal_attempt", 1)
            signal_col = data.get("signal_col", 1)
            signal_lost = data.get("signal_lost", 0.0)
            trend_msg_id = data.get("trend_msg_id")
            last_result = data.get("last_result")
            logger.info(f"Estado cargado desde disco: {len(history)} valores")
    except Exception as e:
        logger.warning(f"Error cargando estado: {e}")

# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
async def send_message(text: str, parse_mode: str = "HTML") -> Optional[int]:
    """Envía mensaje y devuelve message_id."""
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
    """Estadísticas de los últimos 100 multiplicadores."""
    total = len(history)
    if total == 0:
        return {"total": 0, "below2": 0, "two_to_five": 0,
                "pct_below2": 0.0, "pct_2to5": 0.0, "favorable": False}
    below2     = sum(1 for v in history if v < 2.00)
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
    now  = argentina_time()
    last5 = list(history)[-5:][::-1] if history else []
    last5_str = ", ".join(f"{v:.2f}x" for v in last5) if last5 else "—"

    below2_ok = stats["pct_below2"] < UMBRAL_BELOW2
    two5_ok   = stats["pct_2to5"]   > UMBRAL_2TO5

    if stats["favorable"]:
        header = f"🟢 TENDENCIA FAVORABLE — {now}"
        mark2  = "✅"
    else:
        header = f"🔴 TENDENCIA DESFAVORABLE — {now}"
        mark2  = "❌"

    below2_mark = "✅" if below2_ok else "❌"

    return (
        f"{header}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 Análisis últimos {stats['total']} multiplicadores\n"
        f"🔵 1.00-1.99x = {stats['below2']} — {stats['pct_below2']:.2f}%{below2_mark}\n"
        f"🟡 2.00-4.99x = {stats['two_to_five']} — {stats['pct_2to5']:.2f}%{mark2}\n"
        f"🆔 ({last5_str})"
    )

# ─── MENSAJES DE SEÑAL ────────────────────────────────────────────────────────
def build_signal_message() -> str:
    return (
        "✅<b>ENTRADA CONFIRMADA</b>✅\n\n"
        f"👉<b>INGRESAR DESPUÉS: {CASHOUT_TRIGGER:.2f}x</b>\n"
        f"💰<b>RETIRAR EN: {CASHOUT_TARGET:.2f}x</b>\n\n"
        f"🔁<b>MÁXIMO {MAX_GALES} GALES</b>"
    )

def build_win_message(result: float) -> str:
    return (
        "🍀🍀🍀 GANAMOS!!! 🍀🍀🍀\n"
        f"✅ Resultado: {result:.2f}x"
    )

def build_loss_message(result: float) -> str:
    return (
        "🔴 PERDIMOS!!! 🔴\n"
        f"❌ Resultado: {result:.2f}x"
    )

# ─── LÓGICA DE SEÑALES — EMA cruce ───────────────────────────────────────────
def calc_ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    ema = [values[0]]
    for v in values[1:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema

def check_signal_2x(vals: List[float]) -> bool:
    """
    Detecta señal 2x:
    Cruce alcista EMA4 sobre EMA20 + filtro de porcentajes.
    """
    if len(vals) < 20:
        return False

    stats = get_stats()
    if not stats["favorable"]:
        return False

    ema4_series  = calc_ema(vals, 4)
    ema20_series = calc_ema(vals, 20)

    cur_ema4  = ema4_series[-1]
    cur_ema20 = ema20_series[-1]
    prev_ema4  = ema4_series[-2] if len(ema4_series) > 1 else cur_ema4
    prev_ema20 = ema20_series[-2] if len(ema20_series) > 1 else cur_ema20

    crossed = (prev_ema4 <= prev_ema20) and (cur_ema4 > cur_ema20)
    return crossed

# ─── PROCESAMIENTO DE CADA NUEVA CUOTA ────────────────────────────────────────
async def process_new_value(value: float, silent: bool = False):
    """Llamado con cada nueva cuota en vivo."""
    global signal_active, signal_attempt, signal_col, signal_lost
    global trend_msg_id, last_result, history

    history.append(value)
    if len(history) > HISTORY_MAX:
        history = history[-HISTORY_MAX:]

    if silent:
        return   # cargando historial, no enviar nada

    logger.info(f"Nueva cuota: {value:.2f}x | historial: {len(history)}")

    # ── Resolver señal activa ──
    if signal_active:
        win = value >= CASHOUT_TARGET

        if win:
            # GANAMOS en cualquier intento
            signal_active  = False
            signal_attempt = 1
            signal_col     = 1
            signal_lost    = 0.0
            await send_message(build_win_message(value))
            logger.info(f"GANAMOS {value:.2f}x en intento {signal_attempt} col {signal_col}")
        else:
            # PERDIMOS este intento
            if signal_attempt < (MAX_GALES + 1):
                # Hay gale disponible
                signal_attempt += 1
                signal_lost += 1
                logger.info(f"PERDIDO intento {signal_attempt - 1}, esperando gale...")
            else:
                # Agotamos gales en esta columna
                signal_attempt = 1
                signal_col += 1
                signal_lost += 1

                if signal_col > MAX_COLS:
                    # Perdimos las 3 columnas
                    signal_active  = False
                    signal_col     = 1
                    signal_lost    = 0.0
                    await send_message(build_loss_message(value))
                    logger.info(f"PERDIMOS {value:.2f}x — 3 columnas agotadas")
                else:
                    logger.info(f"Col {signal_col - 1} agotada → esperando señal en Col {signal_col}")
                    signal_active = False

        save_to_disk()
        return

    # ── Verificar si hay nueva señal ──
    vals = list(history)
    if check_signal_2x(vals):
        signal_active  = True
        signal_attempt = 1
        await send_message(build_signal_message())
        logger.info(f"SEÑAL 2x enviada | pct<2={get_stats()['pct_below2']:.1f}% 2-5={get_stats()['pct_2to5']:.1f}%")
        # Eliminar tendencia si había
        if trend_msg_id:
            await delete_message(trend_msg_id)
            trend_msg_id = None
        save_to_disk()
        return

    # ── Enviar/actualizar mensaje de tendencia ──
    stats = get_stats()
    if len(history) < 10:
        save_to_disk()
        return

    trend_text = build_trend_message(stats)

    if trend_msg_id:
        ok = await edit_message(trend_msg_id, trend_text)
        if not ok:
            # Fue borrado externamente, reenviar
            trend_msg_id = await send_message(trend_text)
    else:
        trend_msg_id = await send_message(trend_text)

    save_to_disk()

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
                # Suscribirse
                sub = {
                    "type": "subscribe",
                    "casinoId": CASINO_ID,
                    "currency": CURRENCY,
                    "key": [GAME_ID],
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

                    # Primer mensaje: historial grande
                    if not hist_loaded and len(game_results) > 1:
                        hist = list(reversed(game_results))
                        logger.info(f"Cargando historial WS: {len(hist)} rondas")
                        for item in hist:
                            val = item.get("result") or item.get("multiplier") or item.get("crashPoint")
                            if val is not None:
                                await process_new_value(float(val), silent=True)
                        hist_loaded = True
                        logger.info("Historial cargado — en vivo")
                        continue

                    # Mensaje en vivo: resultado único
                    result = game_results[0].get("result")
                    if result is not None and result != last_result:
                        last_result = result
                        hist_loaded = True
                        await process_new_value(float(result), silent=False)

        except Exception as e:
            logger.error(f"WS error: {e} — reconectando en {RECONNECT_DELAY}s")
            hist_loaded = False
            await asyncio.sleep(RECONNECT_DELAY)

# ─── FLASK ROUTES ─────────────────────────────────────────────────────────────
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
        "status": "ok",
        "history_count": len(history),
        "signal_active": signal_active,
        "favorable": stats["favorable"],
        "pct_below2": round(stats["pct_below2"], 2),
        "pct_2to5": round(stats["pct_2to5"], 2),
    }, 200

@flask_app.route('/ping')
def ping():
    return 'pong', 200

# ─── TELEGRAM COMMANDS ─────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def cmd_start(message):
    name = message.from_user.first_name or "usuario"
    stats = get_stats()
    await bot.reply_to(message,
        f"🚀 <b>¡Bienvenido {name}!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <b>AVIATOR BOT 2x</b>\n\n"
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
    stats = get_stats()
    hora = argentina_time()
    sig_txt = f"Intento {signal_attempt}/{MAX_GALES + 1} Col {signal_col}/{MAX_COLS}" if signal_active else "Idle"
    await bot.reply_to(message,
        f"📊 <b>ESTADÍSTICAS — {hora}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Historial: <code>{stats['total']}</code> cuotas\n"
        f"🔵 &lt;2x: {stats['below2']} ({stats['pct_below2']:.1f}%)\n"
        f"🟡 2-5x: {stats['two_to_five']} ({stats['pct_2to5']:.1f}%)\n"
        f"📈 Tendencia: {'🟢 FAVORABLE' if stats['favorable'] else '🔴 DESFAVORABLE'}\n"
        f"📡 Señal: <code>{sig_txt}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')

# ─── ASYNCIO + FLASK ──────────────────────────────────────────────────────────
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
    logger.info("🤖 Iniciando AVIATOR BOT 2x...")
    load_from_disk()
    
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
        # Mantener asyncio activo
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
    # Mensaje de inicio
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_async())
