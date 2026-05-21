#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════╗
║   SPACEMAN BOT — Sistema Moderado 2.00x         ║
║   WebSocket en tiempo real | Multi-sesión       ║
╚══════════════════════════════════════════════════╝
"""

import asyncio
import threading
import json
import logging
import os
import time
from datetime import datetime
from flask import Flask
import websockets
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, ConversationHandler, MessageHandler, filters
)
import aiohttp

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ.get("BOT_TOKEN", "8889373350:AAFU7R1ENyANVR-DiZbBMbeyAHZOi9DLlXY")
WS_URL     = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID  = "ppcdk00000005349"
CURRENCY   = "BRL"
GAME_ID    = 1301

MAX_MULTS  = 400      # máximo de multiplicadores antes de recortar
TRIM_MULTS = 150      # cuántos guardar después del recorte
WIN_TARGET = 2.00     # objetivo del sistema moderado

MAX_COLS   = 3        # columnas en la gestión
MAX_ATTS   = 2        # intentos por columna (intento + SO)
CYCLE_SIZE = 10       # señales para completar un ciclo

# ─── CONVERSATION STATES ──────────────────────────────────────────────────────
ASK_CAPITAL, ASK_BET = range(2)

# ─── ESTADO GLOBAL ────────────────────────────────────────────────────────────
g_mults:    list  = []          # [{id, value, ts}, ...]
g_seen_ids: set   = set()       # IDs vistos para deduplicar
g_positions: list = []          # posición acumulada para EMAs
g_ema4:  list     = []
g_ema8:  list     = []
g_ema20: list     = []

# Estado global de señal: 'idle' | 'evaluating' | 'so'
g_signal_state = 'idle'

# Sesiones de usuario: {user_id: UserSession}
g_sessions: dict = {}

# Referencia a la app de Telegram
g_app = None

# ─── MOTOR DE EMAs ────────────────────────────────────────────────────────────
def calc_ema(data: list, period: int) -> list:
    """Calcula EMA exponencial."""
    if not data:
        return []
    k = 2 / (period + 1)
    ema = [data[0]]
    for i in range(1, len(data)):
        ema.append((data[i] - ema[i - 1]) * k + ema[i - 1])
    return ema

# ─── DETECCIÓN DE SEÑAL (PORT de checkModerateAlerts del AMX) ─────────────────
def check_signal_2x() -> bool:
    """
    Detecta señal 2.00x del sistema moderado.
    Lógica portada exactamente del archivo AMX_V20:
      - EMA8 cruza EMA20 al alza
      - EMA4 cruza EMA8 al alza
      - Dos últimos >= 2.00 con EMA4 > EMA8 > EMA20
      - Precio sobre las 3 EMAs con cercanía a EMA4
    """
    pos  = g_positions
    e4   = g_ema4
    e8   = g_ema8
    e20  = g_ema20
    data = g_mults

    if len(data) < 4 or len(pos) < 4:
        return False

    cur_pos = pos[-1]
    cur_e4  = e4[-1]  if e4  else cur_pos
    cur_e8  = e8[-1]  if e8  else cur_pos
    cur_e20 = e20[-1] if e20 else cur_pos
    prv_e4  = e4[-2]  if len(e4)  > 1 else cur_e4
    prv_e8  = e8[-2]  if len(e8)  > 1 else cur_e8
    prv_e20 = e20[-2] if len(e20) > 1 else cur_e20

    # Condición 1: EMA8 cruza EMA20
    if len(e8) >= 2 and prv_e8 <= prv_e20 and cur_e8 > cur_e20:
        return True

    # Condición 2: EMA4 cruza EMA8
    if len(e4) >= 2 and prv_e4 <= prv_e8 and cur_e4 > cur_e8:
        return True

    # Condición 3: Dos consecutivos >= 2.00 con EMAs alineadas
    if (len(data) >= 2
            and data[-1]['value'] >= WIN_TARGET
            and data[-2]['value'] >= WIN_TARGET
            and cur_e4 > cur_e8 > cur_e20):
        before = data[-3] if len(data) >= 3 else None
        if before is None or before['value'] < WIN_TARGET:
            return True

    # Condición 4: Precio sobre las 3 EMAs y cercano a EMA4
    if cur_pos > cur_e4 and cur_pos > cur_e8 and cur_pos > cur_e20:
        if abs(cur_pos - cur_e4) <= 0.5:
            return True

    return False

# ─── SESIÓN DE USUARIO ────────────────────────────────────────────────────────
class UserSession:
    IDLE       = 'idle'
    EVALUATING = 'evaluating'
    WAITING_SO = 'waiting_so'
    DONE       = 'done'

    def __init__(self, user_id: int, chat_id: int, capital: float, base_bet: float):
        self.user_id  = user_id
        self.chat_id  = chat_id
        self.capital  = capital
        self.base_bet = base_bet
        self.balance  = capital
        self.state    = self.IDLE

        # Gestión de apuesta
        self.scale   = 1         # señal actual (1‑10)
        self.col     = 1         # columna actual (1‑3)
        self.attempt = 1         # intento en la columna
        self.lost    = 0.0       # pérdida acumulada en el ciclo de col
        self.cur_bet = base_bet  # apuesta actual

        # Estadísticas
        self.entries = 0
        self.wins    = 0
        self.losses  = 0
        self.history = []
        self.created = datetime.now()

    def on_result(self, win: bool) -> tuple:
        """
        Procesa el resultado de una apuesta.
        Retorna (tipo, neto) donde tipo es:
          'win' | 'cycle_win' | 'so' | 'new_col' | 'cycle_loss'
        """
        self.entries += 1
        prev_bet  = self.cur_bet
        prev_lost = self.lost

        if win:
            net = self.cur_bet - self.lost
            self.balance += net
            self.wins += 1
            self._log('WIN', net)
            self.scale   += 1
            self.lost     = 0.0
            self.cur_bet  = self.base_bet
            self.col      = 1
            self.attempt  = 1
            if self.scale > CYCLE_SIZE:
                self.state = self.DONE
                return ('cycle_win', net)
            self.state = self.IDLE
            return ('win', net)
        else:
            self.lost   += self.cur_bet
            self.losses += 1
            self.cur_bet = self.lost + self.base_bet
            self.attempt += 1
            self._log('LOSS', -prev_bet)

            if self.attempt > MAX_ATTS:
                self.attempt = 1
                self.col    += 1
                if self.col > MAX_COLS:
                    self.state = self.DONE
                    return ('cycle_loss', -(self.capital - self.balance))
                self.state = self.IDLE
                return ('new_col', -prev_bet)
            else:
                self.state = self.WAITING_SO
                return ('so', -prev_bet)

    def _log(self, result: str, net: float):
        self.history.append({
            'n': self.entries, 'scale': self.scale, 'col': self.col,
            'att': self.attempt, 'bet': self.cur_bet, 'result': result,
            'net': net, 'balance': self.balance
        })
        if len(self.history) > 100:
            self.history.pop(0)

    def status_md(self) -> str:
        diff  = self.balance - self.capital
        sign  = "+" if diff >= 0 else ""
        emoji = "🟢" if diff >= 0 else "🔴"
        state_txt = {
            self.IDLE:       "⏳ Esperando señal",
            self.EVALUATING: "⚡ Evaluando resultado",
            self.WAITING_SO: "🔄 Esperando 2ª Oportunidad",
            self.DONE:       "✅ Ciclo finalizado",
        }.get(self.state, "—")

        return (
            f"📊 *ESTADO DE TU SESIÓN*\n"
            f"{emoji} Balance: `${self.balance:.0f}` ({sign}${diff:.0f})\n"
            f"🎯 Señal: `{min(self.scale, CYCLE_SIZE)}/{CYCLE_SIZE}`\n"
            f"📍 Col: `{self.col}/{MAX_COLS}` | Intento: `{self.attempt}/{MAX_ATTS}`\n"
            f"💵 Próxima apuesta: `${self.cur_bet:.2f}`\n"
            f"📈 G/P: `{self.wins}/{self.losses}`\n"
            f"📡 Estado: {state_txt}"
        )


# ─── PROCESADOR DE MULTIPLICADORES ───────────────────────────────────────────
async def process_multiplier(value: float, round_id: str):
    """
    Punto central: llega un nuevo multiplicador.
    Orden de procesamiento:
      1. Procesar resultados de sesiones en EVALUATING
      2. Procesar resultados de sesiones en WAITING_SO
      3. Actualizar datos y EMAs
      4. Detectar nueva señal → notificar sesiones IDLE
    """
    global g_signal_state, g_positions, g_ema4, g_ema8, g_ema20

    logger.info(f"🎲 Multiplicador: {value:.2f}x | ID: {round_id} | Estado señal: {g_signal_state}")

    # ── FASE 1: Procesar resultado de evaluación principal ──
    if g_signal_state == 'evaluating':
        win = value >= WIN_TARGET
        results_to_process = [
            s for s in g_sessions.values()
            if s.state == UserSession.EVALUATING
        ]
        if win:
            g_signal_state = 'idle'
        else:
            g_signal_state = 'so'

        for session in results_to_process:
            tipo, net = session.on_result(win)
            await _dispatch_result(session, value, tipo, net, is_so=False)

    # ── FASE 2: Procesar resultado SO ──
    elif g_signal_state == 'so':
        win = value >= WIN_TARGET
        results_to_process = [
            s for s in g_sessions.values()
            if s.state == UserSession.WAITING_SO
        ]
        g_signal_state = 'idle'

        for session in results_to_process:
            tipo, net = session.on_result(win)
            await _dispatch_result(session, value, tipo, net, is_so=True)

    # ── FASE 3: Actualizar datos ──
    increment = 1 if value >= WIN_TARGET else -1
    prev = g_positions[-1] if g_positions else 0
    g_positions.append(prev + increment)
    g_mults.append({'id': round_id, 'value': value, 'ts': time.time()})

    # Recorte de datos
    if len(g_mults) >= MAX_MULTS:
        g_mults[:]     = g_mults[-TRIM_MULTS:]
        g_positions[:] = g_positions[-TRIM_MULTS:]
        logger.info(f"✂️ Datos recortados a {TRIM_MULTS} registros")

    # Recalcular EMAs
    g_ema4  = calc_ema(g_positions, 4)
    g_ema8  = calc_ema(g_positions, 8)
    g_ema20 = calc_ema(g_positions, 20)

    # Limpiar IDs vistos si crece demasiado
    if len(g_seen_ids) > 2000:
        oldest = sorted(g_seen_ids)[:1000]
        for oid in oldest:
            g_seen_ids.discard(oid)

    # ── FASE 4: Detectar nueva señal ──
    if g_signal_state == 'idle' and check_signal_2x():
        g_signal_state = 'evaluating'
        logger.info("🚀 SEÑAL 2.00x DETECTADA")
        idle_sessions = [
            s for s in g_sessions.values()
            if s.state == UserSession.IDLE and s.state != UserSession.DONE
        ]
        for session in idle_sessions:
            session.state = UserSession.EVALUATING
            await _send_signal(session)


async def _send_signal(session: UserSession):
    """Envía alerta de señal al usuario."""
    if g_app is None:
        return
    txt = (
        "🚨 *¡SEÑAL DETECTADA! 💎 2.00x*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Objetivo: `≥ {WIN_TARGET:.2f}x`\n"
        f"💵 *APUESTA AHORA: ${session.cur_bet:.2f}*\n"
        f"📊 Señal {session.scale}/{CYCLE_SIZE} | "
        f"Col {session.col}/{MAX_COLS} | "
        f"Intento {session.attempt}/{MAX_ATTS}\n"
        f"💰 Balance: `${session.balance:.0f}`\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ _Entra al PRÓXIMO juego_"
    )
    try:
        await g_app.bot.send_message(session.chat_id, txt, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error enviando señal a {session.user_id}: {e}")


async def _dispatch_result(session: UserSession, value: float, tipo: str, net: float, is_so: bool):
    """Despacha mensaje de resultado al usuario según el tipo."""
    if g_app is None:
        return

    so_label = "🔄 *2ª Oportunidad*" if is_so else "💎 *Señal Principal*"
    emoji_val = "🟢" if value >= WIN_TARGET else "🔴"

    if tipo in ('win', 'cycle_win'):
        diff = session.balance - session.capital
        sign = "+" if diff >= 0 else ""
        txt = (
            f"✅ *{'¡CICLO COMPLETO! 🏆' if tipo == 'cycle_win' else 'GANADA'}*\n"
            f"{so_label}\n"
            f"{emoji_val} Resultado: `{value:.2f}x`\n"
            f"💵 Neto: `+${net:.2f}`\n"
            f"💰 Balance: `${session.balance:.0f}` ({sign}${diff:.0f})\n"
        )
        if tipo == 'cycle_win':
            txt += (
                "\n🎉 *¡Completaste 10 señales exitosas!*\n"
                f"📈 Capital inicial: `${session.capital:.0f}`\n"
                f"🏦 Balance final: `${session.balance:.0f}`\n"
                f"💰 Ganancia: `+${diff:.0f}`\n"
                f"📊 G/P: `{session.wins}/{session.losses}`\n\n"
                "¿Deseas continuar o cerrar sesión?"
            )
            kb = [[
                InlineKeyboardButton("🔄 Nueva Sesión", callback_data=f"new_session:{session.user_id}"),
                InlineKeyboardButton("❌ Cerrar", callback_data=f"close_session:{session.user_id}")
            ]]
            try:
                await g_app.bot.send_message(
                    session.chat_id, txt,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup(kb)
                )
            except Exception as e:
                logger.error(f"Error: {e}")
            return
        else:
            txt += f"\n⏳ _Esperando próxima señal... ({session.scale}/{CYCLE_SIZE})_"

    elif tipo == 'so':
        txt = (
            f"❌ *Perdida* — {so_label}\n"
            f"{emoji_val} Resultado: `{value:.2f}x`\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🔄 *¡SEGUNDA OPORTUNIDAD!*\n"
            f"💵 *Apuesta SO: ${session.cur_bet:.2f}*\n"
            f"📊 Col {session.col}/{MAX_COLS} | Intento 2/{MAX_ATTS}\n"
            f"💰 Balance: `${session.balance:.0f}`\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚡ _Espera el próximo resultado_"
        )

    elif tipo == 'new_col':
        txt = (
            f"❌ *SO Fallida* — {so_label}\n"
            f"{emoji_val} Resultado: `{value:.2f}x`\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 *Avanzando a Columna {session.col}/{MAX_COLS}*\n"
            f"💵 *Nueva apuesta: ${session.cur_bet:.2f}*\n"
            f"💰 Balance: `${session.balance:.0f}`\n"
            "⏳ _Esperando próxima señal..._"
        )

    elif tipo == 'cycle_loss':
        diff = session.balance - session.capital
        txt = (
            f"⚠️ *CICLO TERMINADO — 3 Columnas Fallidas*\n"
            f"{emoji_val} Resultado: `{value:.2f}x`\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Capital inicial: `${session.capital:.0f}`\n"
            f"📉 Balance final: `${session.balance:.0f}`\n"
            f"🔴 Pérdida: `${diff:.0f}`\n"
            f"📊 G/P: `{session.wins}/{session.losses}`\n\n"
            "¿Deseas iniciar una nueva sesión?"
        )
        kb = [[
            InlineKeyboardButton("🔄 Nueva Sesión", callback_data=f"new_session:{session.user_id}"),
            InlineKeyboardButton("❌ Cerrar", callback_data=f"close_session:{session.user_id}")
        ]]
        try:
            await g_app.bot.send_message(
                session.chat_id, txt,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(kb)
            )
        except Exception as e:
            logger.error(f"Error: {e}")
        return
    else:
        txt = f"Resultado inesperado: {tipo}"

    try:
        await g_app.bot.send_message(session.chat_id, txt, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error enviando resultado a {session.user_id}: {e}")


# ─── RECOLECTOR WEBSOCKET ────────────────────────────────────────────────────
async def ws_collector():
    """
    Recolecta multiplicadores de Spaceman en tiempo real.
    Siempre activo en segundo plano aunque no haya sesiones.
    Reconecta automáticamente ante cualquier falla.
    """
    last_value = None

    while True:
        try:
            logger.info("🔌 Conectando al WebSocket de Spaceman...")
            async with websockets.connect(
                WS_URL,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=10
            ) as ws:
                subscribe_msg = {
                    "type":     "subscribe",
                    "casinoId": CASINO_ID,
                    "currency": CURRENCY,
                    "key":      [GAME_ID]
                }
                await ws.send(json.dumps(subscribe_msg))
                logger.info("✅ Suscrito a Spaceman WebSocket")

                async for raw_msg in ws:
                    try:
                        data = json.loads(raw_msg)
                        game_results = data.get('gameResult', [])

                        if not game_results:
                            continue

                        first = game_results[0]
                        value = float(first.get('result', 0))
                        if value <= 0:
                            continue

                        # Construir ID único para deduplicar
                        round_id = str(
                            first.get('roundId') or
                            first.get('gameRoundId') or
                            first.get('id') or
                            f"{value}_{int(time.time() * 1000)}"
                        )

                        # Deduplicar por ID y por valor consecutivo igual
                        if round_id in g_seen_ids:
                            continue
                        if value == last_value:
                            continue

                        g_seen_ids.add(round_id)
                        last_value = value

                        await process_multiplier(value, round_id)

                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        logger.debug(f"Mensaje ignorado: {e}")
                    except Exception as e:
                        logger.error(f"Error procesando mensaje WS: {e}")

        except websockets.ConnectionClosed as e:
            logger.warning(f"WebSocket cerrado ({e.code}): {e.reason}")
        except Exception as e:
            logger.error(f"Error de WebSocket: {e}")

        logger.info("🔄 Reconectando en 5 segundos...")
        await asyncio.sleep(5)


# ─── KEEP-ALIVE (Render Free Tier) ───────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    return (
        f"🤖 SpacemanBot ACTIVO | "
        f"Datos: {len(g_mults)}/400 | "
        f"Sesiones: {len(g_sessions)} | "
        f"Señal: {g_signal_state}"
    ), 200

@flask_app.route('/ping')
def ping():
    return "pong", 200

@flask_app.route('/stats')
def stats():
    last5 = [f"{m['value']:.2f}x" for m in g_mults[-5:]] if g_mults else []
    return {
        "status": "ok",
        "mults_collected": len(g_mults),
        "signal_state": g_signal_state,
        "active_sessions": len(g_sessions),
        "last_5": last5
    }

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)


async def self_ping_loop():
    """
    Hace ping a sí mismo cada 14 minutos para evitar que
    Render apague el servidor gratuito.
    """
    render_url = os.environ.get('RENDER_EXTERNAL_URL', '')
    if not render_url:
        logger.info("RENDER_EXTERNAL_URL no configurada — self-ping desactivado")
        return

    url = f"{render_url.rstrip('/')}/ping"
    logger.info(f"Self-ping cada 14 min → {url}")

    while True:
        await asyncio.sleep(14 * 60)
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector()) as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    logger.info(f"Self-ping OK: {r.status}")
        except Exception as e:
            logger.warning(f"Self-ping falló: {e}")


# ─── HANDLERS DE TELEGRAM ─────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid  = update.effective_user.id
    name = update.effective_user.first_name or "usuario"

    # Si ya existe sesión activa no finalizada
    if uid in g_sessions and g_sessions[uid].state != UserSession.DONE:
        s = g_sessions[uid]
        await update.message.reply_text(
            f"✅ ¡Hola {name}! Ya tienes una sesión activa:\n\n"
            f"{s.status_md()}\n\n"
            "Usa /status para ver detalles\n"
            "Usa /nueva para reiniciar la sesión\n"
            "Usa /cerrar para terminar",
            parse_mode='Markdown'
        )
        return ConversationHandler.END

    data_info = (
        f"📡 `{len(g_mults)}/400` multiplicadores recopilados"
        if g_mults else
        "📡 Recopilando datos en tiempo real..."
    )

    await update.message.reply_text(
        f"🚀 *Bienvenido {name}!*\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 *Bot de Señales Spaceman*\n"
        "📊 Sistema Moderado | Objetivo: `2.00x`\n"
        "🔄 Gestión: 3 Columnas × 2 Intentos\n"
        "🏆 Ciclo: 10 señales exitosas\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{data_info}\n\n"
        "💰 *¿Cuál es tu capital inicial?*\n"
        "_Ejemplo: 100_",
        parse_mode='Markdown'
    )
    return ASK_CAPITAL


async def receive_capital(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    txt = update.message.text.strip().replace(',', '.')
    try:
        capital = float(txt)
    except ValueError:
        await update.message.reply_text("⚠️ Ingresa un número válido.\n_Ejemplo: 100_", parse_mode='Markdown')
        return ASK_CAPITAL

    if capital < 10:
        await update.message.reply_text("⚠️ El capital mínimo es $10.")
        return ASK_CAPITAL

    ctx.user_data['capital'] = capital
    rec = capital * 0.05

    await update.message.reply_text(
        f"✅ Capital: `${capital:.0f}`\n\n"
        "🎯 *¿Cuál es tu apuesta base?*\n"
        f"💡 Recomendado: `${rec:.2f}` (5% del capital)\n"
        "_Esta es la apuesta mínima por señal._",
        parse_mode='Markdown'
    )
    return ASK_BET


async def receive_bet(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    txt = update.message.text.strip().replace(',', '.')
    try:
        bet = float(txt)
    except ValueError:
        await update.message.reply_text("⚠️ Ingresa un número válido.\n_Ejemplo: 5_", parse_mode='Markdown')
        return ASK_BET

    capital = ctx.user_data.get('capital', 100)

    if bet < 1:
        await update.message.reply_text("⚠️ La apuesta mínima es $1.")
        return ASK_BET

    if bet > capital * 0.25:
        limite = capital * 0.25
        await update.message.reply_text(
            f"⚠️ La apuesta no puede superar el 25% del capital (`${limite:.2f}`).\n"
            "_Ingresa un valor menor para una gestión segura._",
            parse_mode='Markdown'
        )
        return ASK_BET

    uid  = update.effective_user.id
    chat = update.effective_chat.id

    # Crear sesión
    session = UserSession(uid, chat, capital, bet)
    g_sessions[uid] = session

    # Calcular peor caso para info
    worst_col1 = bet
    worst_col2 = (bet + (bet + bet)) + bet
    worst_col3 = worst_col2 + (worst_col2 + bet) + bet
    data_count = len(g_mults)

    await update.message.reply_text(
        "✅ *¡Sesión iniciada!*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Capital: `${capital:.0f}`\n"
        f"🎯 Apuesta base: `${bet:.2f}`\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "*Gestión Moderada:*\n"
        f"  📍 C1 — Apuesta: `${worst_col1:.2f}`\n"
        f"  📍 C2 — Apuesta: `${bet + bet:.2f}` (si pierde SO)\n"
        f"  📍 C3 — Recuperación máxima\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Datos WS: `{data_count}/400`\n\n"
        "⚡ _Recibirás una notificación automática con cada señal._\n\n"
        "📌 Comandos útiles:\n"
        "/status — Ver estado\n"
        "/datos — Últimos multiplicadores\n"
        "/nueva — Nueva sesión\n"
        "/cerrar — Terminar sesión",
        parse_mode='Markdown'
    )
    return ConversationHandler.END


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in g_sessions:
        await update.message.reply_text(
            "❌ No tienes una sesión activa.\nUsa /start para comenzar."
        )
        return

    s = g_sessions[uid]
    sig_txt = {
        'idle':       "✅ En espera de señal",
        'evaluating': "⚡ Evaluando resultado del juego actual",
        'so':         "🔄 Evaluando 2ª Oportunidad",
    }.get(g_signal_state, "—")

    last_mult = f"`{g_mults[-1]['value']:.2f}x`" if g_mults else "—"

    await update.message.reply_text(
        f"{s.status_md()}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 *WebSocket:* Activo\n"
        f"🎲 *Último multi:* {last_mult}\n"
        f"📊 *Datos:* `{len(g_mults)}/400`\n"
        f"🔭 *Señal global:* {sig_txt}",
        parse_mode='Markdown'
    )


async def cmd_datos(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not g_mults:
        await update.message.reply_text("📡 Sin datos aún. Conectando al WebSocket...")
        return

    last = g_mults[-30:]
    lines = []
    above = sum(1 for m in g_mults if m['value'] >= WIN_TARGET)
    below = len(g_mults) - above
    pct   = (above / len(g_mults) * 100) if g_mults else 0

    for m in reversed(last):
        emoji = "🟢" if m['value'] >= WIN_TARGET else "🔴"
        lines.append(f"{emoji} `{m['value']:.2f}x`")

    txt = (
        "📊 *Últimos 30 multiplicadores:*\n\n"
        + "\n".join(lines)
        + f"\n\n*Estadísticas ({len(g_mults)} totales):*\n"
        + f"🟢 ≥2.00x: `{above}` ({pct:.1f}%)\n"
        + f"🔴 <2.00x: `{below}` ({100-pct:.1f}%)"
    )
    await update.message.reply_text(txt, parse_mode='Markdown')


async def cmd_nueva(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id
    if uid in g_sessions:
        del g_sessions[uid]
    return await cmd_start(update, ctx)


async def cmd_cerrar(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id
    if uid in g_sessions:
        s   = g_sessions[uid]
        diff = s.balance - s.capital
        sign = "+" if diff >= 0 else ""
        del g_sessions[uid]
        await update.message.reply_text(
            f"✅ *Sesión cerrada.*\n\n"
            f"💰 Balance final: `${s.balance:.0f}` ({sign}${diff:.0f})\n"
            f"📊 G/P: `{s.wins}/{s.losses}` | Entradas: `{s.entries}`\n\n"
            "Usa /start para comenzar una nueva sesión.",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text("ℹ️ No hay sesión activa.")
    return ConversationHandler.END


async def cmd_ayuda(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *COMANDOS DEL BOT*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "/start — Iniciar nueva sesión\n"
        "/status — Ver estado de tu sesión\n"
        "/datos — Últimos multiplicadores\n"
        "/nueva — Reiniciar sesión (mantiene config)\n"
        "/cerrar — Terminar sesión\n"
        "/ayuda — Esta ayuda\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "*Cómo funciona:*\n"
        "1. El bot analiza Spaceman en tiempo real\n"
        "2. Detecta señales del sistema moderado (2.00x)\n"
        "3. Te notifica con la apuesta exacta\n"
        "4. Gestiona automáticamente pérdidas con 2ª oportunidad\n"
        "5. Completa el ciclo en 10 señales ganadas\n",
        parse_mode='Markdown'
    )


async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    parts = query.data.split(':')
    action = parts[0]
    uid    = int(parts[1]) if len(parts) > 1 else update.effective_user.id

    if action == "new_session":
        if uid in g_sessions:
            old = g_sessions[uid]
            session = UserSession(uid, old.chat_id, old.capital, old.base_bet)
            g_sessions[uid] = session
            try:
                await query.edit_message_text(
                    "🔄 *Nueva sesión iniciada*\n\n"
                    f"💰 Capital: `${old.capital:.0f}`\n"
                    f"🎯 Apuesta base: `${old.base_bet:.2f}`\n\n"
                    "⚡ _Esperando próxima señal..._",
                    parse_mode='Markdown'
                )
            except Exception:
                pass
        else:
            try:
                await query.edit_message_text(
                    "ℹ️ Sesión no encontrada. Usa /start para comenzar.",
                    parse_mode='Markdown'
                )
            except Exception:
                pass

    elif action == "close_session":
        if uid in g_sessions:
            del g_sessions[uid]
        try:
            await query.edit_message_text(
                "✅ Sesión cerrada. Usa /start cuando quieras volver."
            )
        except Exception:
            pass


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    await cmd_cerrar(update, ctx)
    return ConversationHandler.END


# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main_async():
    global g_app

    # Construir aplicación Telegram
    g_app = Application.builder().token(BOT_TOKEN).build()

    # Conversation handler para setup de sesión
    conv = ConversationHandler(
        entry_points=[
            CommandHandler('start', cmd_start),
            CommandHandler('nueva', cmd_nueva),
        ],
        states={
            ASK_CAPITAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_capital)],
            ASK_BET:     [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_bet)],
        },
        fallbacks=[
            CommandHandler('cancel', cmd_cancel),
            CommandHandler('cerrar', cmd_cancel),
        ],
        allow_reentry=True,
    )

    g_app.add_handler(conv)
    g_app.add_handler(CommandHandler('status', cmd_status))
    g_app.add_handler(CommandHandler('datos',  cmd_datos))
    g_app.add_handler(CommandHandler('cerrar', cmd_cerrar))
    g_app.add_handler(CommandHandler('ayuda',  cmd_ayuda))
    g_app.add_handler(CallbackQueryHandler(callback_handler))

    async with g_app:
        await g_app.initialize()
        await g_app.start()

        logger.info("🤖 Bot iniciado. Lanzando tareas en segundo plano...")

        # Lanzar recolector WS y self-ping
        asyncio.create_task(ws_collector())
        asyncio.create_task(self_ping_loop())

        await g_app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES
        )

        logger.info("✅ Todo activo. Esperando eventos...")
        # Mantener corriendo indefinidamente
        await asyncio.Event().wait()


if __name__ == '__main__':
    # Iniciar Flask en hilo separado (keep-alive para Render)
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"🌐 Flask iniciado en puerto {os.environ.get('PORT', 8080)}")

    asyncio.run(main_async())
