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
from typing import Optional
from flask import Flask
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
WS_URL     = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID  = "ppcdk00000005349"
CURRENCY   = "BRL"
GAME_ID    = 1301

MAX_MULTS  = 400
TRIM_MULTS = 200
WIN_TARGET = 2.00

MAX_COLS   = 3
MAX_ATTS   = 2
CYCLE_SIZE = 10

# ─── CONVERSATION STATES ──────────────────────────────────────────────────────
ASK_CAPITAL = 'ASK_CAPITAL'
ASK_BET     = 'ASK_BET'

# ─── ESTADO GLOBAL ────────────────────────────────────────────────────────────
g_mults:    list  = []
g_seen_ids: set   = set()
g_positions: list = []
g_ema4:  list     = []
g_ema8:  list     = []
g_ema20: list     = []

# Estado global de señal: 'idle' | 'evaluating' | 'so'
g_signal_state = 'idle'
# Tipo de señal activa: 'alert200' | None
g_signal_type: Optional[str] = None
# Multiplicador que disparó la señal
g_signal_trigger_mult: float = 0.0

g_sessions: dict = {}
user_states: dict = {}
user_temp: dict   = {}

bot = AsyncTeleBot(BOT_TOKEN)

# ─── MOTOR DE EMAs ────────────────────────────────────────────────────────────
def calc_ema(data: list, period: int) -> list:
    if not data:
        return []
    k = 2 / (period + 1)
    ema = [data[0]]
    for i in range(1, len(data)):
        ema.append((data[i] - ema[i - 1]) * k + ema[i - 1])
    return ema

# ─── ESTADÍSTICAS DE CUOTAS ───────────────────────────────────────────────────
def get_quota_stats(n: int = 200) -> dict:
    """
    Calcula estadísticas de cuotas para los últimos n multiplicadores.
    Condicion desfavorable: 1.00-1.99x > 52% O 2.00-4.99x < 29%.
    Solo se usa para bloquear el INICIO de sesion, nunca durante una sesion activa.
    """
    data  = g_mults[-n:] if len(g_mults) >= n else g_mults[:]
    total = len(data)
    if total == 0:
        return {
            'total': 0, 'has_enough': False, 'favorable': None,
            'count_100_199': 0, 'count_200_499': 0,
            'count_500_999': 0, 'count_1000_plus': 0,
            'pct_100_199': 0.0, 'pct_200_499': 0.0,
            'pct_500_999': 0.0, 'pct_1000_plus': 0.0,
        }

    r1 = sum(1 for m in data if 1.00 <= m['value'] <  2.00)
    r2 = sum(1 for m in data if 2.00 <= m['value'] <  5.00)
    r3 = sum(1 for m in data if 5.00 <= m['value'] < 10.00)
    r4 = sum(1 for m in data if m['value'] >= 10.00)

    pct1 = r1 / total * 100
    pct2 = r2 / total * 100
    pct3 = r3 / total * 100
    pct4 = r4 / total * 100

    # Desfavorable: 1.00-1.99 supera 52% O 2.00-4.99 esta por debajo del 29%
    unfavorable = pct1 > 52.0 or pct2 < 29.0

    return {
        'total':           total,
        'has_enough':      total >= 200,
        'favorable':       not unfavorable,
        'count_100_199':   r1,
        'count_200_499':   r2,
        'count_500_999':   r3,
        'count_1000_plus': r4,
        'pct_100_199':     pct1,
        'pct_200_499':     pct2,
        'pct_500_999':     pct3,
        'pct_1000_plus':   pct4,
    }


def quota_stats_text(stats: dict) -> str:
    """Formatea el bloque de estadisticas de cuotas para Telegram."""
    if stats['total'] == 0:
        return "📡 _Sin datos suficientes para analizar cuotas._\n"

    n_label = str(stats['total']) + ("" if stats['has_enough'] else " (acumulando...)")

    r1_flag = " ✅" if stats['pct_100_199'] <= 52.0 else " ❌"
    r2_flag = " ✅" if stats['pct_200_499'] >= 29.0 else " ❌"

    if stats['favorable']:
        fav_line = "✅ *¡TENDENCIA FAVORABLE!*\n      _Se recomienda operar_"
    else:
        fav_line = "⚠️ *TENDENCIA DESFAVORABLE*\n      _Se recomienda esperar_"

    return (
        f"📈 *Análisis de la Tendencia últimos*\n"
        f"      *{n_label} multiplicadores*\n"
        f"🔵 Cuotas (1.00-1.99x): `{stats['count_100_199']}` — {stats['pct_100_199']:.2f}%{r1_flag}\n"
        f"🟣 Cuotas (2.00-4.99x): `{stats['count_200_499']}` — {stats['pct_200_499']:.2f}%{r2_flag}\n"
        f"🟡 Cuotas (5.00-9.99x): `{stats['count_500_999']}` — {stats['pct_500_999']:.2f}%\n"
        f"🔴 Cuotas (+10.00x):    `{stats['count_1000_plus']}` — {stats['pct_1000_plus']:.2f}%\n"
        " \n"
        f"{fav_line}\n"
    )


# ─── DETECCIÓN DE SEÑAL (checkModerateAlerts del HTML — solo alert200) ───────
def check_moderate_signal() -> Optional[str]:
    """
    Retorna 'alert200' o None.
    Las 3 condiciones del gráfico moderado del HTML, todas disparan alert200.
    alert150 no entra a la estrategia (igual que en el HTML original).
    """
    pos  = g_positions
    e4   = g_ema4
    e8   = g_ema8
    e20  = g_ema20
    data = g_mults

    if len(data) < 4 or len(pos) < 4:
        return None

    cur_pos = pos[-1]
    cur_e4  = e4[-1]  if e4           else cur_pos
    cur_e8  = e8[-1]  if e8           else cur_pos
    cur_e20 = e20[-1] if e20          else cur_pos
    prv_e8  = e8[-2]  if len(e8)  > 1 else cur_e8
    prv_e20 = e20[-2] if len(e20) > 1 else cur_e20

    # Condición 1: EMA8 cruza por encima de EMA20
    if len(e8) >= 2 and prv_e8 <= prv_e20 and cur_e8 > cur_e20:
        return 'alert200'

    # Condición 2: patrón V en los últimos 3 puntos con precio sobre las 3 EMAs
    if len(pos) >= 3:
        a, b, c = pos[-3], pos[-2], pos[-1]
        if (abs(a - c) <= 1 and b > a
                and cur_pos > cur_e4
                and cur_pos > cur_e8
                and cur_pos > cur_e20):
            return 'alert200'

    # Condición 3: 2 consecutivos ≥2.00 + EMAs alineadas (4>8>20) + anterior <2.00
    if (len(data) >= 2
            and data[-1]['value'] >= WIN_TARGET
            and data[-2]['value'] >= WIN_TARGET
            and cur_e4 > cur_e8 > cur_e20):
        before = data[-3] if len(data) >= 3 else None
        if before is None or before['value'] < WIN_TARGET:
            return 'alert200'

    return None

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

        self.scale   = 1
        self.col     = 1
        self.attempt = 1
        self.lost    = 0.0
        self.cur_bet = base_bet

        self.entries = 0
        self.wins    = 0
        self.losses  = 0
        self.history = []
        self.created = datetime.now()

        # Multiplicador que disparó la señal activa
        self.signal_trigger_mult: float = 0.0

        # ID del mensaje de "Perdida intento 1 / esperando SO"
        # Se elimina cuando se conoce el resultado del SO (ganado o perdido)
        self.attempt1_msg_id: Optional[int] = None

        # Valor del resultado del intento 1 (para mostrarlo junto al SO en new_col)
        self.attempt1_result_value: float = 0.0

    def on_result(self, win: bool) -> tuple:
        """
        Retorna (tipo, bet_amount):
          'win' | 'cycle_win' | 'so' | 'new_col' | 'cycle_loss'
        Balance se actualiza con el capital real en CADA resultado (+ o -).
        """
        self.entries += 1
        prev_bet = self.cur_bet

        if win:
            # Suma la apuesta ganada al balance real
            self.balance += prev_bet
            self.wins    += 1
            self._log('WIN', prev_bet)
            self.lost    = 0.0
            self.cur_bet = self.base_bet
            self.col     = 1
            self.attempt = 1
            self.scale  += 1
            if self.scale > CYCLE_SIZE:
                self.state = self.DONE
                return ('cycle_win', prev_bet)
            self.state = self.IDLE
            return ('win', prev_bet)
        else:
            # Descuenta la apuesta perdida del balance real inmediatamente
            self.balance -= prev_bet
            self.lost    += prev_bet
            self.losses  += 1
            self.cur_bet  = self.lost + self.base_bet
            self.attempt += 1
            self._log('LOSS', -prev_bet)

            if self.attempt > MAX_ATTS:
                self.attempt = 1
                self.col    += 1
                if self.col > MAX_COLS:
                    self.state = self.DONE
                    return ('cycle_loss', prev_bet)
                self.state = self.IDLE
                return ('new_col', prev_bet)
            else:
                self.state = self.WAITING_SO
                return ('so', prev_bet)

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


# ─── HELPERS DE TECLADO ───────────────────────────────────────────────────────
def make_session_keyboard(user_id: int) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("🔄 Nueva Sesión", callback_data=f"new_session:{user_id}"),
        types.InlineKeyboardButton("❌ Cerrar",       callback_data=f"close_session:{user_id}")
    )
    return kb


# ─── PROCESADOR DE MULTIPLICADORES ───────────────────────────────────────────
async def process_multiplier(value: float, round_id: str):
    global g_signal_state, g_signal_type, g_signal_trigger_mult
    global g_positions, g_ema4, g_ema8, g_ema20

    logger.info(
        f"🎲 {value:.2f}x | ID: {round_id} | "
        f"Señal: {g_signal_state}/{g_signal_type}"
    )

    # ── FASE 1: Procesar resultado principal ──────────────────────
    if g_signal_state == 'evaluating':
        win = value >= WIN_TARGET
        sessions_eval = [
            s for s in g_sessions.values()
            if s.state == UserSession.EVALUATING
        ]

        if sessions_eval:
            # Hay sesiones evaluando: transicion normal
            # Solo entramos en estado 'so' si alguna sesion realmente perdio
            any_so = False
            for session in sessions_eval:
                tipo, bet = session.on_result(win)
                await _dispatch_result(session, value, tipo, bet, is_so=False)
                if session.state == UserSession.WAITING_SO:
                    any_so = True

            g_signal_state = 'so' if any_so else 'idle'
            if g_signal_state == 'so':
                g_signal_type = None  # tipo de SO no aplica aqui
        else:
            # Ninguna sesion estaba evaluando (ej: alert150 fue ignorada por C2/C3)
            # Volvemos a idle de inmediato, igual que en el HTML
            g_signal_state = 'idle'
            g_signal_type  = None
            logger.info("⚠️ Señal evaluada sin sesiones activas → idle inmediato")

    # ── FASE 2: Procesar resultado SO ─────────────────────────────
    elif g_signal_state == 'so':
        win = value >= WIN_TARGET
        sessions_so = [
            s for s in g_sessions.values()
            if s.state == UserSession.WAITING_SO
        ]
        g_signal_state = 'idle'
        g_signal_type  = None

        for session in sessions_so:
            tipo, bet = session.on_result(win)
            await _dispatch_result(session, value, tipo, bet, is_so=True)

    # ── FASE 3: Actualizar datos ──────────────────────────────────
    increment = 1 if value >= WIN_TARGET else -1
    prev = g_positions[-1] if g_positions else 0
    g_positions.append(prev + increment)
    g_mults.append({'id': round_id, 'value': value, 'ts': time.time()})

    if len(g_mults) >= MAX_MULTS:
        g_mults[:]     = g_mults[-TRIM_MULTS:]
        g_positions[:] = g_positions[-TRIM_MULTS:]
        logger.info(f"✂️ Datos recortados a {TRIM_MULTS} registros")

    g_ema4  = calc_ema(g_positions, 4)
    g_ema8  = calc_ema(g_positions, 8)
    g_ema20 = calc_ema(g_positions, 20)

    if len(g_seen_ids) > 2000:
        oldest = sorted(g_seen_ids)[:1000]
        for oid in oldest:
            g_seen_ids.discard(oid)

    # ── FASE 4: Detectar nueva señal ─────────────────────────────
    if g_signal_state == 'idle':
        sig_result = check_moderate_signal()
        if sig_result:
            sig_type, sig_strictness = sig_result
            g_signal_state        = 'evaluating'
            g_signal_type         = sig_type
            g_signal_strictness   = sig_strictness
            g_signal_trigger_mult = value
            logger.info(
                f"🚀 SEÑAL {sig_type.upper()} "
                f"S{sig_strictness} | Trigger: {value:.2f}x"
            )

            for session in list(g_sessions.values()):
                if session.state != UserSession.IDLE:
                    continue

                # ── Restricción progresiva por columna ──────────────
                # La gestión moderada exige señales más restrictivas
                # conforme se pierden columnas:
                #   Col 1 → acepta S1, S2 o S3  (cualquier alert200)
                #   Col 2 → acepta solo S2 o S3  (más restrictiva)
                #   Col 3 → acepta solo S3        (la más estricta)
                # Regla: strictness de la señal debe ser >= col actual
                if sig_strictness < session.col:
                    logger.info(
                        f"⏭️ User {session.user_id}: "
                        f"S{sig_strictness} < Col {session.col} → señal omitida"
                    )
                    continue

                # Limpiar mensaje de intento1 residual por seguridad
                if session.attempt1_msg_id:
                    try:
                        await bot.delete_message(
                            session.chat_id, session.attempt1_msg_id
                        )
                    except Exception:
                        pass
                    session.attempt1_msg_id = None

                session.signal_trigger_mult = value
                session.state = UserSession.EVALUATING
                await _send_signal(session, sig_type, value)


async def _send_signal(session: UserSession, sig_type: str, trigger: float):
    """Envia alerta de señal al usuario."""
    # En el sistema moderado solo existe alert200 en la estrategia
    # El nivel de restriccion viene de g_signal_strictness (global)
    s = g_signal_strictness  # 1 | 2 | 3
    nivel = {1: "S1 — EMA Cruce", 2: "S2 — Patrón V", 3: "S3 — Doble ≥2.00"}.get(s, "💎 2.00x")

    txt = (
        f"🚨 *¡SEÑAL DETECTADA! 💎 2.00x*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Último Multiplicador — `{trigger:.2f}x`\n"
        f"💵 *Apostar Ahora: `${session.cur_bet:.2f}`*\n"
        f"🕹️ Señal `{session.scale}/{CYCLE_SIZE}` | "
        f"Col `{session.col}/{MAX_COLS}` | "
        f"Intento `{session.attempt}/{MAX_ATTS}`"
    )
    try:
        await bot.send_message(session.chat_id, txt, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error enviando señal a {session.user_id}: {e}")


async def _dispatch_result(
    session: UserSession,
    value: float,
    tipo: str,
    bet: float,
    is_so: bool
):
    """
    Despacha el mensaje de resultado.
    • is_so=True  → elimina el mensaje del intento 1 (ganado O perdido).
    • tipo='so'   → guarda el message_id del intento 1 para eliminarlo luego.
    """
    # ── Eliminar mensaje de intento 1 cuando llega el resultado del SO ──
    if is_so and session.attempt1_msg_id:
        try:
            await bot.delete_message(session.chat_id, session.attempt1_msg_id)
        except Exception:
            pass
        session.attempt1_msg_id = None

    emoji_val = "🟢" if value >= WIN_TARGET else "🔴"
    diff      = session.balance - session.capital
    sign      = "+" if diff >= 0 else ""

    # ── GANADA (intento 1 o 2ª oportunidad) ──────────────────────
    if tipo in ('win', 'cycle_win'):
        so_prefix = "🔄 2ª Oportunidad — " if is_so else ""

        if tipo == 'cycle_win':
            txt = (
                f"✅ *GANADA* — {emoji_val} Resultado: `{value:.2f}x`\n"
                "━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{so_prefix}💵 Próxima Apuesta: `${session.base_bet:.2f}`\n"
                f"💰 Balance Actual: `${session.balance:.0f}` ({sign}${abs(diff):.0f})\n"
                "\n"
                "🏆 *¡CICLO COMPLETO — 10 señales exitosas!*\n"
                f"📈 Capital inicial: `${session.capital:.0f}`\n"
                f"🏦 Balance final: `${session.balance:.0f}`\n"
                f"📊 G/P: `{session.wins}/{session.losses}`\n\n"
                "¿Deseas continuar o cerrar sesión?"
            )
            try:
                await bot.send_message(
                    session.chat_id, txt,
                    parse_mode='Markdown',
                    reply_markup=make_session_keyboard(session.user_id)
                )
            except Exception as e:
                logger.error(f"Error cycle_win: {e}")
            return

        txt = (
            f"✅ *GANADA* — {emoji_val} Resultado: `{value:.2f}x`\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{so_prefix}💵 Próxima Apuesta: `${session.base_bet:.2f}`\n"
            f"💰 Balance Actual: `${session.balance:.0f}` ({sign}${abs(diff):.0f})\n"
            "\n"
            f"⏳ _Esperando próxima señal... ({session.scale}/{CYCLE_SIZE})_"
        )

    # ── PERDIDA INTENTO 1 → avisa SO ──────────────────────────────
    elif tipo == 'so':
        # Guardar el valor del resultado para mostrarlo si el SO también falla
        session.attempt1_result_value = value
        txt = (
            f"❌ *Perdida* — {emoji_val} Resultado: `{value:.2f}x`\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🔄 *¡SEGUNDA OPORTUNIDAD!*\n"
            f"💵 Apuesta: `${session.cur_bet:.2f}`\n"
            f"🕹️ Col `{session.col}/{MAX_COLS}` | Intento `2/{MAX_ATTS}`"
        )
        try:
            msg = await bot.send_message(
                session.chat_id, txt, parse_mode='Markdown'
            )
            session.attempt1_msg_id = msg.message_id
        except Exception as e:
            logger.error(f"Error enviando SO a {session.user_id}: {e}")
        return

    # ── SO FALLIDA → avanza columna ───────────────────────────────
    elif tipo == 'new_col':
        r1 = f"{session.attempt1_result_value:.2f}x" if session.attempt1_result_value else "—"
        txt = (
            f"🔴 *Resultados: `{r1}` — `{value:.2f}x`*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 Avanzando a Columna `{session.col}/{MAX_COLS}`\n"
            f"💵 Nueva apuesta: `${session.cur_bet:.2f}`\n"
            f"💰 Balance Actual: `${session.balance:.0f}`\n"
            "\n"
            "⏳ _Esperando próxima señal..._"
        )

    # ── CICLO PERDIDO (3 columnas fallidas) ───────────────────────
    elif tipo == 'cycle_loss':
        r1 = f"{session.attempt1_result_value:.2f}x" if session.attempt1_result_value else "—"
        txt = (
            f"🔴 *Resultados: `{r1}` — `{value:.2f}x`*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚠️ *CICLO TERMINADO — 3 Columnas Fallidas*\n"
            f"💰 Capital inicial: `${session.capital:.0f}`\n"
            f"📉 Balance final: `${session.balance:.0f}`\n"
            f"🔴 Pérdida total: `${abs(diff):.0f}`\n"
            f"📊 G/P: `{session.wins}/{session.losses}`\n\n"
            "¿Deseas iniciar una nueva sesión?"
        )
        try:
            await bot.send_message(
                session.chat_id, txt,
                parse_mode='Markdown',
                reply_markup=make_session_keyboard(session.user_id)
            )
        except Exception as e:
            logger.error(f"Error cycle_loss: {e}")
        return

    else:
        txt = f"Resultado inesperado: {tipo}"

    try:
        await bot.send_message(session.chat_id, txt, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error enviando resultado a {session.user_id}: {e}")


# ─── RECOLECTOR WEBSOCKET ────────────────────────────────────────────────────
async def ws_collector():
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

                        round_id = str(
                            first.get('roundId') or
                            first.get('gameRoundId') or
                            first.get('id') or
                            f"{value}_{int(time.time() * 1000)}"
                        )

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


# ─── KEEP-ALIVE ───────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    return (
        f"🤖 SpacemanBot ACTIVO | "
        f"Datos: {len(g_mults)}/400 | "
        f"Sesiones: {len(g_sessions)} | "
        f"Señal: {g_signal_state}/{g_signal_type}"
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
        "signal_type": g_signal_type,
        "trigger_mult": g_signal_trigger_mult,
        "active_sessions": len(g_sessions),
        "last_5": last5
    }

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)


async def self_ping_loop():
    render_url = os.environ.get('RENDER_EXTERNAL_URL', '')
    if not render_url:
        logger.info("RENDER_EXTERNAL_URL no configurada — self-ping desactivado")
        return

    url = f"{render_url.rstrip('/')}/ping"
    logger.info(f"Self-ping cada 14 min → {url}")

    while True:
        await asyncio.sleep(14 * 60)
        try:
            async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector()
            ) as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    logger.info(f"Self-ping OK: {r.status}")
        except Exception as e:
            logger.warning(f"Self-ping falló: {e}")


# ─── HANDLERS DE TELEGRAM ────────────────────────────────────────────────────

@bot.message_handler(commands=['start', 'nueva'])
async def cmd_start(message):
    uid  = message.from_user.id
    name = message.from_user.first_name or "usuario"

    user_states.pop(uid, None)
    user_temp.pop(uid, None)

    if uid in g_sessions and g_sessions[uid].state != UserSession.DONE:
        s = g_sessions[uid]
        if message.text.startswith('/nueva'):
            del g_sessions[uid]
        else:
            await bot.reply_to(
                message,
                f"✅ ¡Hola {name}! Ya tienes una sesión activa:\n\n"
                f"{s.status_md()}\n\n"
                "Usa /status para ver detalles\n"
                "Usa /nueva para reiniciar\n"
                "Usa /cerrar para terminar",
                parse_mode='Markdown'
            )
            return

    data_info = (
        f"📡 `{len(g_mults)}/400` multiplicadores recopilados"
        if g_mults else
        "📡 Recopilando datos en tiempo real..."
    )

    # ── Analisis de cuotas sobre los ultimos 200 multiplicadores ──
    stats     = get_quota_stats(200)
    stats_blk = quota_stats_text(stats)

    # ── Bloquear inicio de NUEVA sesion si tendencia es desfavorable ──
    # Solo aplica cuando NO hay sesion activa (no interrumpe sesiones en curso)
    if stats['total'] >= 50 and not stats['favorable']:
        n_label   = str(stats['total']) + ("" if stats['has_enough'] else " (acumulando...)")
        r1_flag   = " ✅" if stats['pct_100_199'] <= 52.0 else " ❌"
        r2_flag   = " ✅" if stats['pct_200_499'] >= 29.0 else " ❌"
        unf_stats = (
            f"📈 Análisis de la Tendencia últimos\n"
            f"      {n_label} multiplicadores\n"
            f"🔵 Cuotas (1.00-1.99x): {stats['count_100_199']} — {stats['pct_100_199']:.2f}%{r1_flag}\n"
            f"🟣 Cuotas (2.00-4.99x): {stats['count_200_499']} — {stats['pct_200_499']:.2f}%{r2_flag}\n"
            f"🟡 Cuotas (5.00-9.99x): {stats['count_500_999']} — {stats['pct_500_999']:.2f}%\n"
            f"🔴 Cuotas (+10.00x):    {stats['count_1000_plus']} — {stats['pct_1000_plus']:.2f}%\n"
            "\n"
            "❌ TENDENCIA DESFAVORABLE\n"
            "      No se recomienda esperar"
        )
        await bot.reply_to(
            message,
            f"🚀 *Bienvenido {name}!*\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🤖 Bot de Señales Spaceman\n"
            "📊 Sistema Moderado | Objetivo: 2.00x\n"
            "🔄 Gestión: 3 Columnas × 2 Intentos\n"
            "📈 Señales más estrictas en columnas 2 y 3\n"
            "🏆 Ciclo: 10 señales exitosas\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{data_info}\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{unf_stats}\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚪ Espera una hora para volver a\n"
            "consultar la tendencia del juego.\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "❇️ Presionar /start para volver a\n"
            "consultar la tendencia del juego",
            parse_mode='Markdown'
        )
        return  # No pedir capital — sesion bloqueada por tendencia desfavorable

    await bot.reply_to(
        message,
        f"🚀 *Bienvenido {name}!*\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 *Bot de Señales Spaceman*\n"
        "📊 Sistema Moderado | Objetivo: `2.00x`\n"
        "🔄 Gestión: 3 Columnas × 2 Intentos\n"
        "📈 Señales más estrictas en columnas 2 y 3\n"
        "🏆 Ciclo: 10 señales exitosas\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{data_info}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{stats_blk}"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💰 *¿Cuál es tu capital inicial?*\n"
        "_Ejemplo: 100_",
        parse_mode='Markdown'
    )
    user_states[uid] = ASK_CAPITAL


@bot.message_handler(commands=['status'])
async def cmd_status(message):
    uid = message.from_user.id
    if uid not in g_sessions:
        await bot.reply_to(
            message,
            "❌ No tienes una sesión activa.\nUsa /start para comenzar."
        )
        return

    s = g_sessions[uid]
    sig_txt = {
        'idle':       "✅ En espera de señal",
        'evaluating': f"⚡ Evaluando ({g_signal_type or '?'}) | Trigger: `{g_signal_trigger_mult:.2f}x`",
        'so':         "🔄 Evaluando 2ª Oportunidad",
    }.get(g_signal_state, "—")

    last_mult = f"`{g_mults[-1]['value']:.2f}x`" if g_mults else "—"

    await bot.reply_to(
        message,
        f"{s.status_md()}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 *WebSocket:* Activo\n"
        f"🎲 *Último multi:* {last_mult}\n"
        f"📊 *Datos:* `{len(g_mults)}/400`\n"
        f"🔭 *Señal global:* {sig_txt}",
        parse_mode='Markdown'
    )


@bot.message_handler(commands=['datos'])
async def cmd_datos(message):
    if not g_mults:
        await bot.reply_to(
            message, "📡 Sin datos aún. Conectando al WebSocket..."
        )
        return

    last  = g_mults[-30:]
    above = sum(1 for m in g_mults if m['value'] >= WIN_TARGET)
    below = len(g_mults) - above
    pct   = (above / len(g_mults) * 100) if g_mults else 0

    lines = [
        f"{'🟢' if m['value'] >= WIN_TARGET else '🔴'} `{m['value']:.2f}x`"
        for m in reversed(last)
    ]

    txt = (
        "📊 *Últimos 30 multiplicadores:*\n\n"
        + "\n".join(lines)
        + f"\n\n*Estadísticas ({len(g_mults)} totales):*\n"
        + f"🟢 ≥2.00x: `{above}` ({pct:.1f}%)\n"
        + f"🔴 <2.00x: `{below}` ({100-pct:.1f}%)"
    )
    await bot.reply_to(message, txt, parse_mode='Markdown')


@bot.message_handler(commands=['cerrar', 'cancel'])
async def cmd_cerrar(message):
    uid = message.from_user.id
    user_states.pop(uid, None)
    user_temp.pop(uid, None)

    if uid in g_sessions:
        s    = g_sessions[uid]
        diff = s.balance - s.capital
        sign = "+" if diff >= 0 else ""
        del g_sessions[uid]
        await bot.reply_to(
            message,
            f"✅ *Sesión cerrada.*\n\n"
            f"💰 Balance final: `${s.balance:.0f}` ({sign}${diff:.0f})\n"
            f"📊 G/P: `{s.wins}/{s.losses}` | Entradas: `{s.entries}`\n\n"
            "Usa /start para comenzar una nueva sesión.",
            parse_mode='Markdown'
        )
    else:
        await bot.reply_to(message, "ℹ️ No hay sesión activa.")


@bot.message_handler(commands=['ayuda'])
async def cmd_ayuda(message):
    await bot.reply_to(
        message,
        "🤖 *COMANDOS DEL BOT*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "/start — Iniciar nueva sesión\n"
        "/status — Ver estado de tu sesión\n"
        "/datos — Últimos multiplicadores\n"
        "/nueva — Reiniciar sesión\n"
        "/cerrar — Terminar sesión\n"
        "/ayuda — Esta ayuda\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "*Cómo funciona:*\n"
        "1. Analiza Spaceman en tiempo real\n"
        "2. Señal con mult disparador visible\n"
        "3. Col 1: señales moderadas + estrictas\n"
        "4. Col 2-3: SOLO estrictas (≥2.00x) tras pérdida\n"
        "5. Intento 1 perdido → SO automática\n"
        "6. Resultado SO → mensaje intento 1 eliminado\n"
        "7. Balance real actualizado en cada pérdida\n"
        "8. Apuesta recomendada: 1% del capital\n",
        parse_mode='Markdown'
    )


@bot.message_handler(
    func=lambda m: m.content_type == 'text' and not m.text.startswith('/')
)
async def handle_text(message):
    uid   = message.from_user.id
    state = user_states.get(uid)
    if state == ASK_CAPITAL:
        await _receive_capital(message)
    elif state == ASK_BET:
        await _receive_bet(message)


async def _receive_capital(message):
    uid = message.from_user.id
    txt = message.text.strip().replace(',', '.')
    try:
        capital = float(txt)
    except ValueError:
        await bot.reply_to(
            message,
            "⚠️ Ingresa un número válido.\n_Ejemplo: 100_",
            parse_mode='Markdown'
        )
        return

    if capital < 10:
        await bot.reply_to(message, "⚠️ El capital mínimo es $10.")
        return

    user_temp[uid] = {'capital': capital}
    user_states[uid] = ASK_BET
    rec = capital * 0.01  # 1% recomendado

    await bot.reply_to(
        message,
        f"✅ Capital: `${capital:.0f}`\n\n"
        "🎯 *¿Cuál es tu apuesta base?*\n"
        f"💡 Recomendado: `${rec:.2f}` (1% del capital)\n"
        "_Esta es la apuesta mínima por señal._",
        parse_mode='Markdown'
    )


async def _receive_bet(message):
    uid = message.from_user.id
    txt = message.text.strip().replace(',', '.')
    try:
        bet = float(txt)
    except ValueError:
        await bot.reply_to(
            message,
            "⚠️ Ingresa un número válido.\n_Ejemplo: 1_",
            parse_mode='Markdown'
        )
        return

    capital = user_temp.get(uid, {}).get('capital', 100)

    if bet < 0.5:
        await bot.reply_to(message, "⚠️ La apuesta mínima es $0.50.")
        return

    if bet > capital * 0.25:
        limite = capital * 0.25
        await bot.reply_to(
            message,
            f"⚠️ Máximo 25% del capital (`${limite:.2f}`).\n"
            "_Ingresa un valor menor para gestión segura._",
            parse_mode='Markdown'
        )
        return

    chat = message.chat.id
    user_states.pop(uid, None)
    user_temp.pop(uid, None)

    # ── Verificar cuotas ANTES de crear la sesion (solo bloquea al inicio) ──
    stats = get_quota_stats(200)
    if stats['total'] >= 50 and not stats['favorable']:
        r1_ok = stats['pct_100_199'] <= 52.0
        r2_ok = stats['pct_200_499'] >= 29.0
        r1_line = (
            f"🔴 1.00-1.99x: `{stats['pct_100_199']:.1f}%` (limite <=52%) {'✅' if r1_ok else '❌'}\n"
        )
        r2_line = (
            f"🟡 2.00-4.99x: `{stats['pct_200_499']:.1f}%` (minimo >=29%) {'✅' if r2_ok else '❌'}\n"
        )
        await bot.reply_to(
            message,
            "⛔ *Condiciones DESFAVORABLES — Sesion NO iniciada*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            + r1_line + r2_line +
            f"📊 Basado en los ultimos `{stats['total']}` multiplicadores\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⏳ _Espera que las cuotas sean favorables._\n"
            "Usa /start para verificar nuevamente cuando mejoren.",
            parse_mode='Markdown'
        )
        return

    session = UserSession(uid, chat, capital, bet)
    g_sessions[uid] = session

    c1_apuesta = bet
    c2_apuesta = bet * 2          # referencia ilustrativa
    data_count = len(g_mults)

    stats     = get_quota_stats(200)
    stats_blk = quota_stats_text(stats)

    await bot.reply_to(
        message,
        "✅ *¡Sesión iniciada!*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Capital: `${capital:.0f}`\n"
        f"🎯 Apuesta base: `${bet:.2f}`\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "*Gestión Moderada:*\n"
        f"  📍 C1 — Apuesta: `${c1_apuesta:.2f}`\n"
        f"  📍 C2 — Apuesta: `${c2_apuesta:.2f}`\n"
        "  📍 C3 — Recuperación máxima\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{stats_blk}"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📌 *Comandos útiles:*\n"
        "/status — Ver estado\n"
        "/datos — Últimos multiplicadores\n"
        "/nueva — Nueva sesión\n"
        "/cerrar — Terminar sesión",
        parse_mode='Markdown'
    )


@bot.callback_query_handler(func=lambda call: True)
async def callback_handler(call):
    await bot.answer_callback_query(call.id)

    parts  = call.data.split(':')
    action = parts[0]
    uid    = int(parts[1]) if len(parts) > 1 else call.from_user.id

    if action == "new_session":
        if uid in g_sessions:
            old     = g_sessions[uid]
            session = UserSession(uid, old.chat_id, old.capital, old.base_bet)
            g_sessions[uid] = session
            try:
                await bot.edit_message_text(
                    "🔄 *Nueva sesión iniciada*\n\n"
                    f"💰 Capital: `${old.capital:.0f}`\n"
                    f"🎯 Apuesta base: `${old.base_bet:.2f}`\n\n"
                    "⚡ _Esperando próxima señal..._",
                    call.message.chat.id,
                    call.message.message_id,
                    parse_mode='Markdown'
                )
            except Exception:
                pass
        else:
            try:
                await bot.edit_message_text(
                    "ℹ️ Sesión no encontrada. Usa /start para comenzar.",
                    call.message.chat.id,
                    call.message.message_id,
                    parse_mode='Markdown'
                )
            except Exception:
                pass

    elif action == "close_session":
        if uid in g_sessions:
            del g_sessions[uid]
        try:
            await bot.edit_message_text(
                "✅ Sesión cerrada. Usa /start cuando quieras volver.",
                call.message.chat.id,
                call.message.message_id
            )
        except Exception:
            pass


# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main_async():
    logger.info("🤖 Iniciando SpacemanBot con pyTelegramBotAPI...")
    asyncio.create_task(ws_collector())
    asyncio.create_task(self_ping_loop())
    logger.info("✅ Tareas de fondo iniciadas. Iniciando polling...")
    await bot.infinity_polling(skip_pending=True)


if __name__ == '__main__':
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"🌐 Flask iniciado en puerto {os.environ.get('PORT', 8080)}")
    asyncio.run(main_async())
