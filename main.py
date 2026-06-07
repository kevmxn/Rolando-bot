#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════╗
║   SPACEMAN BOT — Sistema Moderado 2.00x         ║
║   WebSocket en tiempo real | Sesión Global      ║
╚══════════════════════════════════════════════════╝
"""

import asyncio
import threading
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple
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
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │  VARIABLES CONFIGURABLES — Modificar según necesidad                        │
# └─────────────────────────────────────────────────────────────────────────────┘

BOT_TOKEN  = os.environ.get("BOT_TOKEN", "8889373350:AAFU7R1ENyANVR-DiZbBMbeyAHZOi9DLlXY")

# ID del canal de Telegram donde se publican señales, resultados y marcador diario
CHANNEL_ID = -1003815888467

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcdk00000005349"
CURRENCY  = "BRL"
GAME_ID   = 1301

# ── Objetivos de apuesta ──────────────────────────────────────────────────────
WIN_TARGET    = 2.00   # Multiplicador objetivo de la señal
# Sin seguro — solo se juega a 2.00x

# ── Gestión de sesión ─────────────────────────────────────────────────────────
MAX_COLS   = 12        # Columnas máximas antes de ciclo perdido
MAX_ATTS   = 1         # 1 intento por columna (sin segundo intento)
CYCLE_SIZE = 10        # Señales exitosas para completar un ciclo
BASE_BET   = 0.10      # Apuesta base fija (USD)

# ── Umbrales de tendencia (detección favorable/desfavorable) ──────────────────
# Desfavorable si: cuotas 1.00-1.99x superan THRESH_LOW_MAX
#              O si: cuotas 2.00-4.99x caen por debajo de THRESH_MID_MIN
THRESH_LOW_MAX = 55.0  # % máximo permitido para cuotas 1.00-1.99x
THRESH_MID_MIN = 27.0  # % mínimo requerido para cuotas 2.00-4.99x

# ── Parámetros internos ───────────────────────────────────────────────────────
MAX_MULTS  = 400
TRIM_MULTS = 200

# Archivo de persistencia del historial de multiplicadores (sobrevive reinicios)
PERSIST_FILE = "spaceman_history.json"

# ─── ESTADO GLOBAL ────────────────────────────────────────────────────────────
g_mults:    list  = []
g_seen_ids: set   = set()
g_positions: list = []
g_ema4:  list     = []
g_ema8:  list     = []
g_ema20: list     = []

g_signal_state        = 'idle'     # 'idle' | 'evaluating' | 'so'
g_signal_type: Optional[str] = None
g_signal_strictness: int     = 0
g_signal_trigger_mult: float = 0.0

g_all_chats: set              = set()   # Todos los chats que alguna vez enviaron /start
g_trend_favorable: Optional[bool] = None

# ─── MARCADOR DIARIO ──────────────────────────────────────────────────────────
g_daily_wins:  int = 0
g_daily_losses: int = 0
g_daily_date:  str = ""        # "YYYY-MM-DD" en hora Argentina

# ─── IDs DE MENSAJES DE SEÑAL ────────────────────────────────────────────────
g_last_signal_msgs: dict = {}  # chat_id → message_id

# Contador interno para guardar en disco cada N multiplicadores
_persist_counter: int = 0

bot = AsyncTeleBot(BOT_TOKEN)

# Referencia al loop principal de asyncio (necesaria para el webhook)
_main_loop: asyncio.AbstractEventLoop = None


# ─── HORA ARGENTINA ───────────────────────────────────────────────────────────
def argentina_time() -> str:
    now_arg = datetime.utcnow() - timedelta(hours=3)
    return now_arg.strftime("%H:%M")


# ─── BROADCAST AL CANAL ───────────────────────────────────────────────────────
async def broadcast(msg: str, parse_mode: str = None):
    """Publica un mensaje en el canal de Telegram configurado."""
    try:
        await bot.send_message(CHANNEL_ID, msg, parse_mode=parse_mode)
    except Exception as e:
        logger.warning(f"Error enviando al canal {CHANNEL_ID}: {e}")


async def broadcast_trend_change(favorable: bool):
    # Ya no se emiten mensajes automáticos de tendencia al canal
    logger.info(f"Tendencia cambió → {'FAVORABLE' if favorable else 'DESFAVORABLE'} (sin broadcast)")


async def broadcast_signal(msg: str):
    """Envía señal al canal y guarda el message_id para borrado posterior."""
    global g_last_signal_msgs
    g_last_signal_msgs = {}
    try:
        sent = await bot.send_message(CHANNEL_ID, msg, parse_mode='Markdown')
        g_last_signal_msgs[CHANNEL_ID] = sent.message_id
        logger.info(f"✅ Señal enviada al canal — msg_id: {sent.message_id}")
    except Exception as e:
        logger.warning(f"Error enviando señal al canal {CHANNEL_ID}: {e}")


async def delete_last_signal():
    """Borra el mensaje de señal del intento 1 en el canal."""
    msg_id = g_last_signal_msgs.get(CHANNEL_ID)
    if msg_id:
        try:
            await bot.delete_message(CHANNEL_ID, msg_id)
            logger.info(f"🗑️ Señal intento 1 borrada del canal (msg_id: {msg_id})")
        except Exception as e:
            logger.warning(f"No se pudo borrar señal del canal: {e}")
    g_last_signal_msgs.clear()


# ─── MOTOR DE EMAs ────────────────────────────────────────────────────────────
def calc_ema(data: list, period: int) -> list:
    if not data:
        return []
    k = 2 / (period + 1)
    ema = [data[0]]
    for i in range(1, len(data)):
        ema.append((data[i] - ema[i - 1]) * k + ema[i - 1])
    return ema


# ─── PERSISTENCIA EN DISCO ────────────────────────────────────────────────────
def save_mults_to_disk():
    """
    Guarda el historial de multiplicadores y posiciones en disco.
    Llamado cada 10 nuevos multiplicadores para no perder datos en reinicios.
    """
    try:
        payload = {
            'mults':     [{'id': m['id'], 'value': m['value'], 'ts': m['ts']}
                          for m in g_mults],
            'positions': g_positions,
        }
        tmp = PERSIST_FILE + ".tmp"
        with open(tmp, 'w') as f:
            json.dump(payload, f)
        os.replace(tmp, PERSIST_FILE)   # Escritura atómica: evita archivo corrupto
        logger.debug(f"💾 Historial guardado: {len(g_mults)} multiplicadores")
    except Exception as e:
        logger.warning(f"No se pudo guardar historial en disco: {e}")


def load_mults_from_disk():
    """
    Carga el historial desde disco al arrancar.
    Restaura g_mults, g_positions, g_ema* y g_seen_ids.
    """
    global g_mults, g_positions, g_ema4, g_ema8, g_ema20
    if not os.path.exists(PERSIST_FILE):
        logger.info("📂 Sin historial previo en disco — comenzando desde cero")
        return
    try:
        with open(PERSIST_FILE) as f:
            data = json.load(f)

        loaded_mults = data.get('mults', [])
        loaded_pos   = data.get('positions', [])

        # Sanidad: recortar si excede el límite
        if len(loaded_mults) > MAX_MULTS:
            loaded_mults = loaded_mults[-TRIM_MULTS:]
            loaded_pos   = loaded_pos[-TRIM_MULTS:]

        g_mults[:]     = loaded_mults
        g_positions[:] = loaded_pos

        # Recalcular EMAs con los datos cargados
        g_ema4[:]  = calc_ema(g_positions, 4)
        g_ema8[:]  = calc_ema(g_positions, 8)
        g_ema20[:] = calc_ema(g_positions, 20)

        # Restaurar IDs vistos para evitar duplicados tras reconexión WS
        for m in g_mults:
            g_seen_ids.add(str(m['id']))

        logger.info(
            f"✅ Historial cargado desde disco: "
            f"{len(g_mults)} multiplicadores | "
            f"IDs restaurados: {len(g_seen_ids)}"
        )
    except Exception as e:
        logger.warning(f"Error cargando historial desde disco: {e} — comenzando desde cero")


# ─── ESTADÍSTICAS DE CUOTAS ───────────────────────────────────────────────────
def get_quota_stats(n: int = 200) -> dict:
    """
    Calcula estadísticas de cuotas para los últimos n multiplicadores.
    Desfavorable si: 1.00-1.99x > THRESH_LOW_MAX  O  2.00-4.99x < THRESH_MID_MIN.
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

    unfavorable = pct1 > THRESH_LOW_MAX or pct2 < THRESH_MID_MIN

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
    """Formatea el bloque de estadísticas de cuotas para Telegram."""
    if stats['total'] == 0:
        return "📡 _Sin datos suficientes para analizar cuotas._\n"

    n_label = "200" if stats['has_enough'] else str(stats['total']) + " (acumulando...)"
    r1_flag = " ✅" if stats['pct_100_199'] <= THRESH_LOW_MAX else " ❌"
    r2_flag = " ✅" if stats['pct_200_499'] >= THRESH_MID_MIN else " ❌"
    fav_line = (
        "✅ *¡TENDENCIA FAVORABLE!*\n      _Se recomienda operar_"
        if stats['favorable'] else
        "⚠️ *TENDENCIA DESFAVORABLE*\n      _Se recomienda esperar_"
    )

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


# ─── DETECCIÓN DE SEÑAL ───────────────────────────────────────────────────────
def check_moderate_signal() -> Optional[Tuple[str, int]]:
    """
    Retorna ('alert200', strictness) o None.
      S1 → EMA8 cruza por encima de EMA20
      S2 → patrón V + precio sobre las 3 EMAs
      S3 → 2 consecutivos ≥2.00 + EMAs alineadas (4>8>20)
      S4 → S3 + precio sobre EMA4 + EMA4 acelerando al alza (confluencia máxima)
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

    # S4: Confluencia máxima — S3 + precio sobre EMA4 + EMA4 acelerando al alza
    if (len(data) >= 2
            and data[-1]['value'] >= WIN_TARGET
            and data[-2]['value'] >= WIN_TARGET
            and cur_e4 > cur_e8 > cur_e20
            and cur_pos > cur_e4
            and len(e4) >= 2 and e4[-1] > e4[-2]):
        before = data[-3] if len(data) >= 3 else None
        if before is None or before['value'] < WIN_TARGET:
            return ('alert200', 4)

    # S3: 2 consecutivos ≥2.00 + EMAs alineadas + anterior <2.00
    if (len(data) >= 2
            and data[-1]['value'] >= WIN_TARGET
            and data[-2]['value'] >= WIN_TARGET
            and cur_e4 > cur_e8 > cur_e20):
        before = data[-3] if len(data) >= 3 else None
        if before is None or before['value'] < WIN_TARGET:
            return ('alert200', 3)

    # S2: patrón V + precio sobre las 3 EMAs
    if len(pos) >= 3:
        a, b, c = pos[-3], pos[-2], pos[-1]
        if (abs(a - c) <= 1 and b > a
                and cur_pos > cur_e4
                and cur_pos > cur_e8
                and cur_pos > cur_e20):
            return ('alert200', 2)

    # S1: EMA8 cruza por encima de EMA20
    if len(e8) >= 2 and prv_e8 <= prv_e20 and cur_e8 > cur_e20:
        return ('alert200', 1)

    return None


# ─── SESIÓN GLOBAL ────────────────────────────────────────────────────────────
class GlobalSession:
    """
    Sesión única compartida por todos los usuarios.
    Apuesta base fija: BASE_BET ($0.10).
    Rastrea fichas (C1…C12) para estadísticas reales.
    1 intento por columna — si falla, pasa a la siguiente columna.
    """
    IDLE       = 'idle'
    EVALUATING = 'evaluating'
    DONE       = 'done'

    def __init__(self, carry_fichas: list = None):
        self.base_bet = BASE_BET
        self.state    = self.IDLE

        self.scale   = 1
        self.col     = 1
        self.attempt = 1
        self.lost    = 0.0
        self.cur_bet = BASE_BET

        self.entries = 0
        self.wins    = 0
        self.losses  = 0
        self.created = datetime.now()

        self.signal_trigger_mult:    float = 0.0
        self.attempt1_result_value:  float = 0.0

        # Historial de fichas completas (se preserva entre ciclos via carry_fichas)
        self.fichas: list = carry_fichas if carry_fichas is not None else []
        self._cur_ficha: dict = None   # ficha en curso (abarca C1→C2→C3 si fuera necesario)

    def start_ficha(self):
        """Inicia una nueva ficha al recibir señal en columna 1."""
        self._cur_ficha = {
            'n':      len(self.fichas) + 1,
            **{f'c{i}': 0.0 for i in range(1, 13)},  # c1…c12
            'result': None,
            'ts':     argentina_time(),
        }

    def on_result(self, win: bool) -> tuple:
        """
        Retorna (tipo, bet_amount).
        Tipos: 'win' | 'cycle_win' | 'new_col' | 'cycle_loss'
        Con 1 intento por columna: si falla, avanza directamente a la siguiente columna.
        Actualiza la ficha activa con el gasto real de cada columna.
        """
        self.entries += 1
        prev_bet = self.cur_bet
        prev_col = self.col   # columna activa ANTES de que on_result la cambie

        # Acumular gasto en la columna correspondiente de la ficha activa
        if self._cur_ficha is not None:
            col_key = f'c{prev_col}'
            self._cur_ficha[col_key] = self._cur_ficha.get(col_key, 0.0) + prev_bet

        if win:
            self.wins   += 1
            self.lost    = 0.0
            self.cur_bet = self.base_bet
            self.col     = 1
            self.attempt = 1
            self.scale  += 1

            # Cerrar ficha como ganada
            if self._cur_ficha is not None:
                self._cur_ficha['result'] = 'win'
                self.fichas.append(self._cur_ficha)
                self._cur_ficha = None
            if len(self.fichas) > 100:
                self.fichas = self.fichas[-100:]

            if self.scale > CYCLE_SIZE:
                self.state = self.DONE
                return ('cycle_win', prev_bet)
            self.state = self.IDLE
            return ('win', prev_bet)

        else:
            # Pérdida → avanzar directo a siguiente columna (1 intento por columna)
            self.losses  += 1
            self.lost    += prev_bet
            self.cur_bet  = self.lost + self.base_bet
            self.col     += 1

            if self.col > MAX_COLS:
                # Ciclo perdido — cerrar ficha como pérdida
                if self._cur_ficha is not None:
                    self._cur_ficha['result'] = 'loss'
                    self.fichas.append(self._cur_ficha)
                    self._cur_ficha = None
                if len(self.fichas) > 100:
                    self.fichas = self.fichas[-100:]
                self.state = self.DONE
                return ('cycle_loss', prev_bet)

            # Avanzar a siguiente columna — la ficha continúa abierta
            self.state = self.IDLE
            return ('new_col', prev_bet)

    def status_short(self) -> str:
        estado_txt = {
            self.IDLE:       "⏳ Esperando señal",
            self.EVALUATING: "⚡ Evaluando resultado",
            self.DONE:       "✅ Ciclo finalizado",
        }.get(self.state, "—")

        return (
            f"📡 Estado: {estado_txt}\n"
            f"🎯 Señal: `{min(self.scale, CYCLE_SIZE)}/{CYCLE_SIZE}`\n"
            f"📍 Col: `{self.col}/{MAX_COLS}`\n"
            f"💵 Próxima apuesta: `${self.cur_bet:.2f}`\n"
            f"📈 G/P: `{self.wins}/{self.losses}`"
        )


# ─── INSTANCIA GLOBAL ─────────────────────────────────────────────────────────
g_session: GlobalSession = GlobalSession()


def reset_global_session():
    """Reinicia la sesión global preservando el historial de fichas."""
    global g_session
    old_fichas = list(g_session.fichas)   # preservar historial completo
    g_session  = GlobalSession(carry_fichas=old_fichas)
    logger.info("🔄 Sesión global reiniciada — fichas preservadas")


# ─── PROCESADOR DE MULTIPLICADORES ───────────────────────────────────────────
async def process_multiplier(value: float, round_id: str):
    global g_signal_state, g_signal_type, g_signal_strictness, g_signal_trigger_mult
    global g_positions, g_ema4, g_ema8, g_ema20, g_mults, g_seen_ids
    global g_trend_favorable, g_session

    logger.info(
        f"🎲 {value:.2f}x | ID: {round_id} | "
        f"Señal: {g_signal_state}/{g_signal_type} (S{g_signal_strictness})"
    )

    # ── RESET DIARIO ──────────────────────────────────────────────────────────
    _check_daily_reset()

    # ── FASE 1: Procesar resultado principal ──────────────────────────────────
    if g_signal_state == 'evaluating':
        if g_session.state == GlobalSession.EVALUATING:
            attempt_num = g_session.attempt
            if value >= WIN_TARGET:
                tipo, bet = g_session.on_result(True)
            else:
                tipo, bet = g_session.on_result(False)

            await _dispatch_result(value, tipo, bet, attempt_num=attempt_num)

        g_signal_state      = 'idle'
        g_signal_type       = None
        g_signal_strictness = 0

    # ── FASE 3: Actualizar datos y EMAs ───────────────────────────────────────
    global _persist_counter
    increment = 1 if value >= WIN_TARGET else -1
    prev = g_positions[-1] if g_positions else 0
    g_positions.append(prev + increment)
    g_mults.append({'id': round_id, 'value': value, 'ts': time.time()})

    if len(g_mults) >= MAX_MULTS:
        g_mults[:]     = g_mults[-TRIM_MULTS:]
        g_positions[:] = g_positions[-TRIM_MULTS:]
        logger.info(f"✂️ Datos recortados a {TRIM_MULTS} registros")
        save_mults_to_disk()   # Guardar siempre que se recorta
    else:
        _persist_counter += 1
        if _persist_counter >= 10:
            _persist_counter = 0
            save_mults_to_disk()   # Guardar cada 10 multiplicadores nuevos

    g_ema4  = calc_ema(g_positions, 4)
    g_ema8  = calc_ema(g_positions, 8)
    g_ema20 = calc_ema(g_positions, 20)

    if len(g_seen_ids) > 2000:
        oldest = sorted(g_seen_ids)[:1000]
        for oid in oldest:
            g_seen_ids.discard(oid)

    # ── FASE 4.5: Detectar cambio de tendencia (solo log, sin broadcast) ──────
    stats_trend = get_quota_stats(200)
    if stats_trend['total'] >= 10:
        new_fav = stats_trend['favorable']
        if new_fav != g_trend_favorable:
            g_trend_favorable = new_fav
            asyncio.create_task(broadcast_trend_change(new_fav))

    # ── FASE 4: Detectar nueva señal ─────────────────────────────────────────
    if g_signal_state == 'idle' and g_session.state == GlobalSession.IDLE:
        sig_result = check_moderate_signal()
        if sig_result:
            sig_type, strictness = sig_result
            # Restricción escalonada cada 2 columnas:
            # Col 1-2 → S1+ | Col 3-4 → S2+ | Col 5-6 → S3+ | Col 7-12 → S4+
            required_strictness = min(4, (g_session.col + 1) // 2)
            if strictness >= required_strictness:
                # Col > 1 → ficha en curso, continuar SIEMPRE sin importar tendencia
                # Col == 1 → nueva ficha, solo si tendencia favorable
                if g_session.col > 1:
                    proceed = True
                else:
                    stats_now = get_quota_stats(200)
                    proceed   = (stats_now['total'] == 0) or (stats_now['favorable'] is not False)

                if proceed:
                    g_signal_state        = 'evaluating'
                    g_signal_type         = sig_type
                    g_signal_strictness   = strictness
                    g_signal_trigger_mult = value
                    g_session.signal_trigger_mult = value
                    g_session.state = GlobalSession.EVALUATING

                    # Iniciar nueva ficha solo al arrancar desde columna 1
                    if g_session.col == 1:
                        g_session.start_ficha()

                    logger.info(f"🚀 SEÑAL S{strictness} (req≥S{required_strictness}) Col{g_session.col} | Trigger: {value:.2f}x")
                    await _send_signal(value, strictness)


# ─── MARCADOR DIARIO ──────────────────────────────────────────────────────────
def _check_daily_reset():
    """Resetea el marcador diario a las 00:00 hora Argentina."""
    global g_daily_wins, g_daily_losses, g_daily_date
    now_arg  = datetime.utcnow() - timedelta(hours=3)
    today    = now_arg.strftime("%Y-%m-%d")
    if today != g_daily_date:
        g_daily_wins   = 0
        g_daily_losses = 0
        g_daily_date   = today
        logger.info(f"📅 Marcador diario reseteado para {today}")


async def _broadcast_scoreboard():
    """Envía el marcador diario a todos los chats."""
    total = g_daily_wins + g_daily_losses
    pct   = (g_daily_wins / total * 100) if total > 0 else 0.0
    txt   = (
        f"📊 *MARCADOR DIARIO:*\n"
        f"✅ GANADAS: `{g_daily_wins}`\n"
        f"❌ PERDIDAS: `{g_daily_losses}`\n"
        f"📈 ACIERTOS = `{pct:.2f}%`"
    )
    await broadcast(txt, parse_mode='Markdown')


# ─── MENSAJERÍA ───────────────────────────────────────────────────────────────
async def _send_signal(trigger: float, strictness: int):
    """Broadcast de señal al canal."""
    txt = (
        f"🚨 Entrar después de: `{trigger:.2f}x`\n"
        f"💎 Señal para `2.00x`\n"
        f"🎯 S{strictness} | Col `{g_session.col}/{MAX_COLS}`"
    )
    await broadcast_signal(txt)   # Guarda ID para referencia


async def _check_trend_after_cycle():
    """Verifica la tendencia post-ciclo (sin broadcast automático)."""
    stats = get_quota_stats(200)
    if stats['total'] > 0 and not stats['favorable']:
        logger.info("⚠️ Post-ciclo: tendencia desfavorable — bot en espera")
    else:
        logger.info("✅ Post-ciclo: tendencia favorable — bot continúa analizando")


async def _dispatch_result(value: float, tipo: str, bet: float, attempt_num: int):
    """
    Broadcast del resultado al canal.
    Sistema de 12 columnas, 1 intento por columna.
    """
    global g_session, g_daily_wins, g_daily_losses

    # ── WIN ───────────────────────────────────────────────────────────────────
    if tipo in ('win', 'cycle_win'):
        g_daily_wins += 1
        txt = f"✅ WIN — `{value:.2f}x`"
        await broadcast(txt, parse_mode='Markdown')
        await _broadcast_scoreboard()
        if tipo == 'cycle_win':
            await broadcast(
                "🏆 *¡CICLO COMPLETO — 10 señales exitosas!*\n"
                "🔄 _Sesión reiniciada automáticamente_",
                parse_mode='Markdown'
            )
            reset_global_session()
            await _check_trend_after_cycle()
        return

    # ── LOSS: avanzar columna ─────────────────────────────────────────────────
    if tipo == 'new_col':
        g_daily_losses += 1
        txt = (
            f"❌ LOSS — `{value:.2f}x`\n"
            f"📍 _Avanzando a Columna `{g_session.col}/{MAX_COLS}`..._"
        )
        await broadcast(txt, parse_mode='Markdown')
        await _broadcast_scoreboard()
        return

    # ── LOSS: ciclo completo perdido ──────────────────────────────────────────
    if tipo == 'cycle_loss':
        g_daily_losses += 1
        txt = (
            f"❌ LOSS — `{value:.2f}x`\n"
            f"⚠️ *CICLO TERMINADO — {MAX_COLS} Columnas Fallidas*\n"
            "🔄 _Sesión reiniciada automáticamente_"
        )
        await broadcast(txt, parse_mode='Markdown')
        await _broadcast_scoreboard()
        reset_global_session()
        await _check_trend_after_cycle()
        return

    logger.warning(f"Resultado inesperado: tipo={tipo}")


# ─── RECOLECTOR WEBSOCKET ─────────────────────────────────────────────────────
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


# ─── KEEP-ALIVE FLASK ─────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    return (
        f"🤖 SpacemanBot ACTIVO | "
        f"Datos: {len(g_mults)}/400 | "
        f"Sesión: {g_session.state} | "
        f"Señal: {g_signal_state} | "
        f"Chats: {len(g_all_chats)}"
    ), 200

@flask_app.route('/ping')
def ping():
    return "pong", 200

@flask_app.route('/webhook', methods=['POST'])
def webhook():
    """
    Recibe los updates de Telegram vía webhook.
    Usa run_coroutine_threadsafe para pasar el update al loop asyncio principal.
    """
    if _main_loop is None:
        return "Loop no iniciado", 503
    try:
        update = types.Update.de_json(request.get_json())
        asyncio.run_coroutine_threadsafe(
            bot.process_new_updates([update]),
            _main_loop
        )
        return '', 200
    except Exception as e:
        logger.error(f"Error procesando webhook: {e}")
        return "Error interno", 500

@flask_app.route('/stats')
def stats_route():
    last5 = [f"{m['value']:.2f}x" for m in g_mults[-5:]] if g_mults else []
    return {
        "status":           "ok",
        "mults_collected":  len(g_mults),
        "signal_state":     g_signal_state,
        "signal_type":      g_signal_type,
        "trigger_mult":     g_signal_trigger_mult,
        "session_state":    g_session.state,
        "session_col":      g_session.col,
        "wins":             g_session.wins,
        "losses":           g_session.losses,
        "fichas_total":     len(g_session.fichas),
        "registered_chats": len(g_all_chats),
        "trend_favorable":  g_trend_favorable,
        "last_5":           last5,
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
            ) as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    logger.info(f"Self-ping OK: {r.status}")
        except Exception as e:
            logger.warning(f"Self-ping falló: {e}")


# ─── HANDLERS DE TELEGRAM ─────────────────────────────────────────────────────

@bot.message_handler(commands=['start'])
async def cmd_start(message):
    """
    Registra el chat para recibir señales y broadcasts.
    Muestra estado actual de la tendencia.
    """
    name = message.from_user.first_name or "usuario"
    g_all_chats.add(message.chat.id)

    stats     = get_quota_stats(200)
    stats_blk = quota_stats_text(stats)
    data_info = (
        f"📡 `{len(g_mults)}/400` multiplicadores recopilados"
        if g_mults else
        "📡 Recopilando datos en tiempo real..."
    )

    await bot.reply_to(
        message,
        f"🚀 *¡Bienvenido {name}!*\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 *Bot de Señales Spaceman*\n"
        "📊 Sistema Moderado | Objetivo: `2.00x`\n"
        "🔄 Gestión: 12 Columnas × 1 Intento\n"
        f"💵 Apuesta base fija: `${BASE_BET:.2f}`\n"
        "🏆 Ciclo: 10 señales exitosas\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{data_info}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{stats_blk}"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "✅ *¡Registrado!*\n"
        "_Recibirás señales automáticamente_\n"
        "_cuando la tendencia sea favorable._",
        parse_mode='Markdown'
    )


@bot.message_handler(commands=['estadisticas'])
async def cmd_estadisticas(message):
    """
    Muestra estadísticas reales de la sesión global:
    estado actual + historial de fichas con C1+C2+C3 por ficha.
    """
    g_all_chats.add(message.chat.id)

    s      = g_session
    stats  = get_quota_stats(200)
    trend  = quota_stats_text(stats)

    # ── Historial de fichas (últimas 15) ──────────────────────────────────────
    fichas_recientes = s.fichas[-15:]
    if fichas_recientes:
        lineas = []
        for f in fichas_recientes:
            # Sumar todas las columnas (c1..c12) dinámicamente
            total = sum(f.get(f'c{i}', 0.0) for i in range(1, 13))
            net   = BASE_BET if f['result'] == 'win' else -total
            res   = "✅" if f['result'] == 'win' else "❌"
            hora  = f.get('ts', '--:--')

            # Solo mostrar columnas con gasto real
            partes = []
            for i in range(1, 13):
                v = f.get(f'c{i}', 0.0)
                if v > 0:
                    partes.append(f"C{i}:${v:.2f}")
            cols_txt = " ".join(partes) if partes else "—"

            net_txt = f"+${net:.2f}" if net >= 0 else f"-${abs(net):.2f}"
            lineas.append(f"{res} #{f['n']} {hora} | {cols_txt} | {net_txt}")

        fichas_txt = "\n".join(lineas)
        total_fichas = len(s.fichas)
        wins_f  = sum(1 for f in s.fichas if f['result'] == 'win')
        loss_f  = sum(1 for f in s.fichas if f['result'] == 'loss')
        resumen = f"Total fichas: `{total_fichas}` | ✅ `{wins_f}` | ❌ `{loss_f}`"
    else:
        fichas_txt = "_Sin fichas registradas aún._"
        resumen    = "Total fichas: `0`"

    await bot.reply_to(
        message,
        "📊 *ESTADÍSTICAS DEL BOT*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{s.status_short()}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*Últimas fichas (C1 a C12):*\n"
        f"{fichas_txt}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{resumen}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{trend}",
        parse_mode='Markdown'
    )


@bot.message_handler(commands=['tendencia'])
async def cmd_tendencia(message):
    """Muestra la tendencia actual con el nuevo formato completo."""
    g_all_chats.add(message.chat.id)
    hora  = argentina_time()
    stats = get_quota_stats(200)

    if stats['total'] == 0:
        await bot.reply_to(message,
            "📡 _Sin datos suficientes para analizar la tendencia._",
            parse_mode='Markdown')
        return

    n_label = "200" if stats['has_enough'] else str(stats['total'])
    r1_flag = "✅" if stats['pct_100_199'] <= THRESH_LOW_MAX else "❌"
    r2_flag = "✅" if stats['pct_200_499'] >= THRESH_MID_MIN else "❌"

    if stats['favorable']:
        header   = f"🟢 TENDENCIA FAVORABLE — {hora}"
        footer   = "✅ ¡TENDENCIA FAVORABLE!\n      Se recomienda operar"
    else:
        header   = f"🔴 TENDENCIA DESFAVORABLE — {hora}"
        footer   = "⚠️ TENDENCIA DESFAVORABLE\n      Se recomienda esperar"

    txt = (
        f"*{header}*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 Análisis de la Tendencia últimos\n"
        f"      {n_label} multiplicadores\n"
        f"🔵 Cuotas (1.00-1.99x): `{stats['count_100_199']}` — {stats['pct_100_199']:.2f}%{r1_flag}\n"
        f"🟣 Cuotas (2.00-4.99x): `{stats['count_200_499']}` — {stats['pct_200_499']:.2f}%{r2_flag}\n"
        f"🟡 Cuotas (5.00-9.99x): `{stats['count_500_999']}` — {stats['pct_500_999']:.2f}%\n"
        f"🔴 Cuotas (+10.00x):    `{stats['count_1000_plus']}` — {stats['pct_1000_plus']:.2f}%\n"
        " \n"
        f"*{footer}*"
    )
    await bot.reply_to(message, txt, parse_mode='Markdown')


# ─── MAIN ──────────────────────────────────────────────────────────────────────
async def main_async():
    global _main_loop
    _main_loop = asyncio.get_running_loop()

    logger.info("🤖 Iniciando SpacemanBot (Sesión Global / Apuesta $0.10)...")

    # Cargar historial de multiplicadores desde disco (sobrevive reinicios)
    load_mults_from_disk()

    # Configurar botones de menú en Telegram (bandeja qwerty, junto a emojis)
    await bot.set_my_commands([
        types.BotCommand('tendencia', '📈 Ver tendencia actual'),
    ])
    logger.info("✅ Comandos de Telegram configurados: /tendencia")

    asyncio.create_task(ws_collector())
    asyncio.create_task(self_ping_loop())
    logger.info("✅ Tareas de fondo iniciadas.")

    render_url = os.environ.get('RENDER_EXTERNAL_URL', '').rstrip('/')

    if render_url:
        # ── Modo Webhook (Render) ─────────────────────────────────────────────
        # Eliminar cualquier webhook o polling previo antes de registrar el nuevo.
        # Esto evita el error 409 causado por múltiples instancias compitiendo.
        await bot.remove_webhook()
        await asyncio.sleep(1)   # Pequeña pausa para que Telegram libere la sesión

        webhook_url = f"{render_url}/webhook"
        await bot.set_webhook(url=webhook_url)
        logger.info(f"✅ Webhook configurado: {webhook_url}")
        logger.info("🔔 Bot esperando updates via webhook...")

        # Mantener el loop corriendo indefinidamente;
        # los updates llegan por Flask → /webhook → process_new_updates
        try:
            while True:
                await asyncio.sleep(3600)
        finally:
            logger.info("🛑 Cerrando bot — eliminando webhook y sesión...")
            await bot.remove_webhook()
            await bot.close_session()
    else:
        # ── Modo Polling (desarrollo local, sin RENDER_EXTERNAL_URL) ─────────
        logger.warning("⚠️ RENDER_EXTERNAL_URL no configurada — usando polling (solo para desarrollo local)")
        try:
            await bot.infinity_polling(skip_pending=True)
        finally:
            logger.info("🛑 Cerrando sesión de polling...")
            await bot.close_session()   # Evita "Unclosed client session"


if __name__ == '__main__':
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"🌐 Flask iniciado en puerto {os.environ.get('PORT', 8080)}")
    asyncio.run(main_async())
