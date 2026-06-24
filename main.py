
#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════╗
║   SPACEMAN BOT — Sistema 2x–4.99x por Tiempo   ║
║   Rango aprendizaje: 2.00x – 4.99x             ║
║   Confianza alta (CV + regularidad) requerida   ║
║   Objetivo de apuesta: 2.00x · Ciclo: 2/2      ║
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
BOT_TOKEN  = os.environ.get("BOT_TOKEN", "8889373350:AAFU7R1ENyANVR-DiZbBMbeyAHZOi9DLlXY")
CHANNEL_ID = -1003815888467

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcdk00000005349"
CURRENCY  = "BRL"
GAME_ID   = 1301

WIN_TARGET_LO  = 2.00   # Rango inferior del evento de aprendizaje
WIN_TARGET_HI  = 5.00   # Rango superior exclusivo (2.00x – 4.99x)
BET_WIN_TARGET = 2.00   # Objetivo real de la apuesta: se gana si resultado >= 2.00x
MAX_COLS       = 8
MAX_ATTS       = 1
WINS_PER_CYCLE = 2      # Victorias necesarias por ciclo: 2/2
BASE_BET       = 0.10

# ─── SISTEMA 2x–4.99x POR TIEMPO ─────────────────────────────────────────────
MIN_EVENTS_2X     = 25     # Mínimo de eventos en rango para evaluar señales
SIGNAL_WINDOW_SEC = 20     # Ventana (segundos antes del ETA) para disparar señal
MAX_EVENTS_2X     = 200    # Historial máximo de timestamps de eventos en rango

# ─── UMBRAL DE CONFIANZA ──────────────────────────────────────────────────────
# Confianza = proximity*0.6 + regularidad*0.4  (igual que el HTML)
# Solo se dispara señal si confianza >= MIN_CONFIDENCE
MIN_CONFIDENCE    = 0.60   # 60% mínimo para disparar señal

MAX_MULTS  = 400
TRIM_MULTS = 300
PERSIST_FILE = "spaceman_history.json"

# ─── ESTADO GLOBAL ────────────────────────────────────────────────────────────
g_mults:    list  = []
g_seen_ids: set   = set()
g_positions: list = []

# ─── ESTADO DEL SISTEMA 2x–4.99x POR TIEMPO ─────────────────────────────────
g_events2x: list = []      # Timestamps (float) de cada ronda en rango 2.00–4.99x
g_gaps2x:   list = []      # Intervalos en segundos entre eventos consecutivos
g_signal_armed: bool = False   # True cuando ya entramos en ventana de señal
g_last_signal_fire: float = 0  # Timestamp del último disparo (antiflood)

SIGNAL_COOLDOWN = 30    # Antiflood: segundos mínimos entre señales consecutivas
g_cooldown_mod  = 0

g_signal_state        = 'idle'
g_signal_trigger_mult: float = 0.0

g_all_chats: set = set()

g_daily_wins:        int = 0
g_daily_losses:      int = 0
g_daily_cycles_won:  int = 0
g_daily_cycles_lost: int = 0
g_daily_date:        str = ""
g_scoreboard_msg_id: Optional[int] = None

g_last_signal_msgs: dict = {}
_persist_counter: int = 0

bot = AsyncTeleBot(BOT_TOKEN, parse_mode='HTML')
_main_loop: asyncio.AbstractEventLoop = None


# ─── HORA ARGENTINA ───────────────────────────────────────────────────────────
def argentina_time() -> str:
    now_arg = datetime.utcnow() - timedelta(hours=3)
    return now_arg.strftime("%H:%M")


# ─── BROADCAST ────────────────────────────────────────────────────────────────
async def broadcast(msg: str, parse_mode: str = 'HTML'):
    try:
        await bot.send_message(CHANNEL_ID, msg, parse_mode=parse_mode)
    except Exception as e:
        logger.warning(f"Error enviando al canal: {e}")


async def broadcast_signal(msg: str, parse_mode: str = 'HTML'):
    global g_last_signal_msgs
    g_last_signal_msgs = {}
    try:
        sent = await bot.send_message(CHANNEL_ID, msg, parse_mode=parse_mode)
        g_last_signal_msgs[CHANNEL_ID] = sent.message_id
        logger.info(f"✅ Señal enviada al canal — msg_id: {sent.message_id}")
    except Exception as e:
        logger.warning(f"Error enviando señal: {e}")


async def delete_last_signal():
    msg_id = g_last_signal_msgs.get(CHANNEL_ID)
    if msg_id:
        try:
            await bot.delete_message(CHANNEL_ID, msg_id)
            logger.info(f"🗑️ Señal borrada (msg_id: {msg_id})")
        except Exception as e:
            logger.warning(f"No se pudo borrar: {e}")
    g_last_signal_msgs.clear()




# ─── SISTEMA 2x–4.99x POR TIEMPO + CONFIANZA ─────────────────────────────────
def avg_sec(lst: list) -> Optional[float]:
    """Promedio de una lista de floats. Retorna None si está vacía."""
    return sum(lst) / len(lst) if lst else None


def calc_confidence(gaps: list, elapsed: float) -> tuple:
    """
    Calcula confianza igual que update2xPredictor() del HTML.
    Retorna (confianza_0_1, eta_seg, avg_gap_seg).

    Fórmula:
      mean      = avgGap
      stdDev    = sqrt(varianza)
      CV        = stdDev / mean           (coeficiente variación)
      normalized= min(1, elapsed / mean)
      proximity = 1 - |normalized - 1|   (1 cuando elapsed == mean)
      regularity= max(0, 1 - CV)         (1 cuando todos los gaps son iguales)
      confidence= min(0.98, proximity*0.6 + regularity*0.4)
    """
    if len(gaps) < 3:
        return 0.0, 0.0, 0.0

    mean = avg_sec(gaps)
    if not mean or mean <= 0:
        return 0.0, 0.0, 0.0

    variance  = sum((g - mean) ** 2 for g in gaps) / len(gaps)
    std_dev   = variance ** 0.5
    cv        = std_dev / mean
    normalized= min(1.0, elapsed / mean)
    proximity = 1.0 - abs(normalized - 1.0)
    regularity= max(0.0, 1.0 - cv)
    confidence= min(0.98, proximity * 0.6 + regularity * 0.4)
    eta       = max(0.0, mean - elapsed)

    return confidence, eta, mean


def check_2x_timing_signal() -> tuple:
    """
    Evalúa si se debe disparar señal para el rango 2.00x–4.99x.
    Retorna (disparar: bool, confianza: float, eta_seg: float).

    Condiciones para disparar:
      1. >= MIN_EVENTS_2X eventos en rango acumulados
      2. >= 3 intervalos calculados
      3. Confianza >= MIN_CONFIDENCE
      4. ETA dentro de la ventana SIGNAL_WINDOW_SEC
    """
    if len(g_events2x) < MIN_EVENTS_2X:
        return False, 0.0, 0.0

    if len(g_gaps2x) < 3:
        return False, 0.0, 0.0

    last_event = g_events2x[-1]
    elapsed    = time.time() - last_event
    conf, eta, avg_gap = calc_confidence(g_gaps2x, elapsed)

    in_window  = 0.0 <= eta <= SIGNAL_WINDOW_SEC
    high_conf  = conf >= MIN_CONFIDENCE

    logger.debug(
        f"2x-Timer | avg={avg_gap:.1f}s elapsed={elapsed:.1f}s eta={eta:.1f}s "
        f"conf={conf:.2f} window={in_window} high_conf={high_conf} events={len(g_events2x)}"
    )
    return (in_window and high_conf), conf, eta


def register_event_2x(ts: float):
    """Registra un nuevo evento en rango 2.00x–4.99x y actualiza los intervalos."""
    global g_events2x, g_gaps2x
    if g_events2x:
        gap = ts - g_events2x[-1]
        if gap > 0:
            g_gaps2x.append(gap)
    g_events2x.append(ts)
    if len(g_events2x) > MAX_EVENTS_2X:
        g_events2x = g_events2x[-MAX_EVENTS_2X:]
    if len(g_gaps2x) > MAX_EVENTS_2X:
        g_gaps2x = g_gaps2x[-MAX_EVENTS_2X:]


def is_in_range(value: float) -> bool:
    """Retorna True si el valor está en el rango de aprendizaje 2.00x–4.99x."""
    return WIN_TARGET_LO <= value < WIN_TARGET_HI


def quota_stats_text(events: int, gaps: list) -> str:
    """Texto informativo del estado del predictor 2x–4.99x."""
    if events < MIN_EVENTS_2X:
        return f"📡 <i>Acumulando datos... ({events}/{MIN_EVENTS_2X} eventos 2x–4.99x)</i>\n"
    avg = avg_sec(gaps)
    if not avg:
        return "📡 <i>Sin intervalos calculados aún.</i>\n"

    last_event = g_events2x[-1] if g_events2x else 0
    elapsed    = time.time() - last_event
    conf, eta, _ = calc_confidence(gaps, elapsed)
    conf_pct   = round(conf * 100)
    last_gap   = gaps[-1] if gaps else 0

    return (f"📈 <b>Predictor 2x–4.99x por Tiempo</b>\n"
            f"⏱ Intervalo promedio: <code>{avg:.1f}s</code>\n"
            f"⏱ Último intervalo: <code>{last_gap:.1f}s</code>\n"
            f"🎯 Confianza actual: <code>{conf_pct}%</code>\n"
            f"📊 Eventos en rango: <code>{events}</code>\n")


# ─── PERSISTENCIA ─────────────────────────────────────────────────────────────
def save_mults_to_disk():
    try:
        payload = {
            'mults': [{'id': m['id'], 'value': m['value'], 'ts': m['ts']} for m in g_mults],
            'events2x': g_events2x,
            'gaps2x': g_gaps2x,
            'daily_wins': g_daily_wins,
            'daily_losses': g_daily_losses,
            'daily_cycles_won': g_daily_cycles_won,
            'daily_cycles_lost': g_daily_cycles_lost,
            'daily_date': g_daily_date,
        }
        tmp = PERSIST_FILE + ".tmp"
        with open(tmp, 'w') as f:
            json.dump(payload, f)
        os.replace(tmp, PERSIST_FILE)
    except Exception as e:
        logger.warning(f"No se pudo guardar historial: {e}")


def load_mults_from_disk():
    global g_mults, g_events2x, g_gaps2x
    global g_daily_wins, g_daily_losses, g_daily_date, g_daily_cycles_won, g_daily_cycles_lost
    if not os.path.exists(PERSIST_FILE):
        logger.info("Sin historial previo")
        return
    try:
        with open(PERSIST_FILE) as f:
            data = json.load(f)
        loaded_mults = data.get('mults', [])
        if len(loaded_mults) > MAX_MULTS:
            loaded_mults = loaded_mults[-TRIM_MULTS:]
        g_mults[:] = loaded_mults
        for m in g_mults:
            g_seen_ids.add(str(m['id']))
        # Restaurar timing 2x
        g_events2x[:] = data.get('events2x', [])[-MAX_EVENTS_2X:]
        g_gaps2x[:]   = data.get('gaps2x', [])[-MAX_EVENTS_2X:]
        saved_date = data.get('daily_date', '')
        today_arg = (datetime.utcnow() - timedelta(hours=3)).strftime("%Y-%m-%d")
        if saved_date == today_arg:
            g_daily_wins = data.get('daily_wins', 0)
            g_daily_losses = data.get('daily_losses', 0)
            g_daily_cycles_won = data.get('daily_cycles_won', 0)
            g_daily_cycles_lost = data.get('daily_cycles_lost', 0)
            g_daily_date = saved_date
        else:
            g_daily_wins = g_daily_losses = g_daily_cycles_won = g_daily_cycles_lost = 0
            g_daily_date = today_arg
        logger.info(f"Historial cargado: {len(g_mults)} mults | {len(g_events2x)} eventos2x")
    except Exception as e:
        logger.warning(f"Error cargando: {e}")


# ─── SESIÓN GLOBAL ────────────────────────────────────────────────────────────
class GlobalSession:
    IDLE, EVALUATING, DONE = 'idle', 'evaluating', 'done'

    def __init__(self, carry_fichas: list = None):
        self.base_bet = BASE_BET
        self.state = self.IDLE
        self.col = 1
        self.attempt = 1
        self.lost = 0.0
        self.cur_bet = BASE_BET
        self.entries_in_cycle = 0
        self.wins_in_cycle = 0
        self.entries = 0
        self.wins = 0
        self.losses = 0
        self.created = datetime.now()
        self.signal_trigger_mult = 0.0
        self.attempt1_result_value = 0.0
        self.fichas = carry_fichas if carry_fichas is not None else []
        self._cur_ficha = None
        self.col_history: list = []  # 'win'/'loss' por cada entrada del ciclo actual (orden cronológico)

    def start_ficha(self):
        self._cur_ficha = {'n': len(self.fichas) + 1,
                           **{f'c{i}': 0.0 for i in range(1, 13)},
                           'result': None, 'ts': argentina_time()}

    def on_result(self, win: bool) -> tuple:
        self.entries += 1
        self.entries_in_cycle += 1
        prev_bet = self.cur_bet
        prev_col = self.col
        if self._cur_ficha:
            self._cur_ficha[f'c{prev_col}'] = self._cur_ficha.get(f'c{prev_col}', 0.0) + prev_bet
        if win:
            self.wins += 1
            self.wins_in_cycle += 1
            self.col_history.append('win')
            self.lost = 0.0
            self.cur_bet = self.base_bet
            self.col = 1
            self.attempt = 1
            if self._cur_ficha:
                self._cur_ficha['result'] = 'win'
                self.fichas.append(self._cur_ficha)
                self._cur_ficha = None
            if len(self.fichas) > 100:
                self.fichas = self.fichas[-100:]
            if self.wins_in_cycle >= WINS_PER_CYCLE:
                self.state = self.DONE
                return ('cycle_win', prev_bet)
            self.state = self.IDLE
            return ('win', prev_bet)
        else:
            self.losses += 1
            self.col_history.append('loss')
            self.lost += prev_bet
            self.cur_bet = self.lost + self.base_bet
            self.col += 1
            if self.entries_in_cycle >= MAX_COLS:
                if self._cur_ficha:
                    self._cur_ficha['result'] = 'loss'
                    self.fichas.append(self._cur_ficha)
                    self._cur_ficha = None
                if len(self.fichas) > 100:
                    self.fichas = self.fichas[-100:]
                self.state = self.DONE
                return ('cycle_loss', prev_bet)
            self.state = self.IDLE
            return ('new_col', prev_bet)

    def status_short(self) -> str:
        estado = {self.IDLE: "⏳ Esperando señal", self.EVALUATING: "⚡ Evaluando", self.DONE: "✅ Ciclo finalizado"}.get(self.state, "—")
        return (f"📡 Estado: {estado}\n"
                f"🎯 Ciclo: <code>{self.wins_in_cycle}/{WINS_PER_CYCLE}</code> victorias | <code>{self.entries_in_cycle}/{MAX_COLS}</code> entradas\n"
                f"📍 Col: <code>{self.col}/{MAX_COLS}</code>\n"
                f"💵 Próxima apuesta: <code>${self.cur_bet:.2f}</code>\n"
                f"📈 G/P sesión: <code>{self.wins}/{self.losses}</code>")


g_session = GlobalSession()

def reset_global_session():
    global g_session
    old_fichas = list(g_session.fichas)
    g_session = GlobalSession(carry_fichas=old_fichas)
    logger.info("🔄 Sesión reiniciada — fichas preservadas")


# ─── PROCESADOR DE MULTIPLICADORES (sistema 2x por tiempo) ────────────────────
async def process_multiplier(value: float, round_id: str):
    global g_signal_state, g_signal_trigger_mult
    global g_mults, g_seen_ids
    global g_session, g_cooldown_mod, _persist_counter
    global g_signal_armed, g_last_signal_fire

    logger.info(f"🎲 {value:.2f}x | ID: {round_id} | Señal: {g_signal_state}")

    _check_daily_reset()

    # ── Fase 1: Evaluar resultado pendiente ──────────────────────────────────
    if g_signal_state == 'evaluating' and g_session.state == GlobalSession.EVALUATING:
        win = value >= BET_WIN_TARGET
        tipo, bet = g_session.on_result(win)
        await _dispatch_result(value, tipo, bet, attempt_num=g_session.attempt)
        g_signal_state = 'idle'
        g_signal_armed = False
        g_cooldown_mod = max(g_cooldown_mod, 2)

    g_cooldown_mod = max(0, g_cooldown_mod - 1)

    # ── Fase 2: Registrar multiplicador ──────────────────────────────────────
    now_ts = time.time()
    g_mults.append({'id': round_id, 'value': value, 'ts': now_ts})
    if len(g_mults) >= MAX_MULTS:
        g_mults[:] = g_mults[-TRIM_MULTS:]
        save_mults_to_disk()
    else:
        _persist_counter += 1
        if _persist_counter >= 10:
            _persist_counter = 0
            save_mults_to_disk()

    if len(g_seen_ids) > 2000:
        oldest = sorted(g_seen_ids)[:1000]
        for oid in oldest:
            g_seen_ids.discard(oid)

    # ── Fase 3: Registrar evento en rango 2.00x–4.99x ───────────────────────
    if is_in_range(value):
        register_event_2x(now_ts)
        # Si había señal armada → resetear (ya llegó el evento, ciclo reinicia)
        if g_signal_armed:
            g_signal_armed = False
            logger.info(f"🔄 Señal armada reseteada — llegó {value:.2f}x (en rango)")

    # ── Fase 4: Detectar ventana de señal 2x–4.99x ───────────────────────────
    if (g_signal_state == 'idle'
            and g_session.state == GlobalSession.IDLE
            and g_cooldown_mod == 0):

        should_fire, conf, eta = check_2x_timing_signal()

        if should_fire and not g_signal_armed:
            elapsed_since_fire = now_ts - g_last_signal_fire
            if elapsed_since_fire >= SIGNAL_COOLDOWN:
                g_signal_armed = True
                g_last_signal_fire = now_ts
                g_signal_state = 'evaluating'
                g_signal_trigger_mult = value
                g_session.signal_trigger_mult = value
                g_session.state = GlobalSession.EVALUATING
                g_cooldown_mod = 6
                if g_session.col == 1:
                    g_session.start_ficha()
                avg_gap = avg_sec(g_gaps2x)
                logger.info(
                    f"🎯 SEÑAL 2x–4.99x | Trigger: {value:.2f}x | ETA ~{eta:.0f}s | "
                    f"Conf: {conf*100:.0f}% | Eventos: {len(g_events2x)} | AvgGap: {avg_gap:.1f}s"
                )
                await _send_signal(value, conf)
            else:
                logger.debug(f"⏳ En ventana pero cooldown ({elapsed_since_fire:.0f}s/{SIGNAL_COOLDOWN}s)")

        elif not should_fire and g_signal_armed:
            g_signal_armed = False


# ─── MARCADOR DIARIO ──────────────────────────────────────────────────────────
def _check_daily_reset():
    global g_daily_wins, g_daily_losses, g_daily_cycles_won, g_daily_cycles_lost, g_daily_date, g_scoreboard_msg_id
    now_arg = datetime.utcnow() - timedelta(hours=3)
    today = now_arg.strftime("%Y-%m-%d")
    if today != g_daily_date:
        g_daily_wins = g_daily_losses = g_daily_cycles_won = g_daily_cycles_lost = 0
        g_daily_date = today
        g_scoreboard_msg_id = None
        logger.info(f"📅 Marcador reseteado para {today}")


async def _broadcast_scoreboard():
    global g_scoreboard_msg_id
    total_sig = g_daily_wins + g_daily_losses
    pct_sig = (g_daily_wins / total_sig * 100) if total_sig > 0 else 0.0
    total_cyc = g_daily_cycles_won + g_daily_cycles_lost
    pct_cyc = (g_daily_cycles_won / total_cyc * 100) if total_cyc > 0 else 0.0
    hora = argentina_time()
    txt = (f"<b>📆 MARCADOR DEL DÍA — 🕐 {hora}</b>\n"
           f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
           f"<b>✅ Ganadas: {g_daily_wins}</b>\n"
           f"<b>❌ Perdidas: {g_daily_losses}</b>\n"
           f"<b>📈 Acierto: {pct_sig:.1f}%</b>\n"
           f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
           f"<b>🔄 Sesión {MAX_COLS} Entradas · {WINS_PER_CYCLE}/2 Victorias</b>\n"
           f"<b>✅ Ganados: {g_daily_cycles_won}</b>\n"
           f"<b>❌ Perdidos: {g_daily_cycles_lost}</b>\n"
           f"<b>📈 Acierto: {pct_cyc:.1f}%</b>")
    if g_scoreboard_msg_id:
        try:
            await bot.delete_message(CHANNEL_ID, g_scoreboard_msg_id)
        except:
            pass
        g_scoreboard_msg_id = None
    sent = await bot.send_message(CHANNEL_ID, txt, parse_mode='HTML')
    g_scoreboard_msg_id = sent.message_id


# ─── MENSAJERÍA ───────────────────────────────────────────────────────────────
def render_gestion_bar(history: list, total: int, pending: bool = False) -> str:
    """
    Construye la barra de 'Nivel de Gestión' con el historial real de la sesión.
    🟢 = entrada ganada · 🔴 = entrada perdida · 🔵 = entrada en curso (pendiente) · ⚫ = aún no jugada
    """
    chars = ['🟢' if r == 'win' else '🔴' for r in history]
    if pending:
        chars.append('🔵')
    if len(chars) < total:
        chars.extend(['⚫'] * (total - len(chars)))
    else:
        chars = chars[:total]
    return ''.join(chars)


async def _send_signal(trigger: float, conf: float = 0.0):
    hora = argentina_time()
    col = g_session.col
    ents = g_session.entries_in_cycle + 1
    wins = g_session.wins_in_cycle
    gestion_bar = render_gestion_bar(g_session.col_history, MAX_COLS, pending=True)
    avg_gap = avg_sec(g_gaps2x)
    conf_pct = round(conf * 100)
    logger.info(f"📤 Señal 2x–4.99x | Col{col} | Entrada {ents}/{MAX_COLS} | Ciclo {wins}/{WINS_PER_CYCLE} | Conf:{conf_pct}%")
    txt = (f"<b>🆔 ENTRADA SPACEMAN — 🕐 {hora}</b>\n"
           f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
           f"<b>⏱ Cuota registrada: {trigger:.2f}x</b>\n"
           f"<b>🎯 Objetivo: {BET_WIN_TARGET:.2f}x</b>\n"
           f"<b>📊 Confianza: {conf_pct}%</b>\n"
           f"<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
           f"<b>💎GESTION DE ENTRADAS:</b>\n"
           f"<b>{gestion_bar}</b>")
    await broadcast_signal(txt)


async def _dispatch_result(value: float, tipo: str, bet: float, attempt_num: int):
    global g_session, g_daily_wins, g_daily_losses, g_daily_cycles_won, g_daily_cycles_lost
    hora = argentina_time()
    if tipo == 'win':
        g_daily_wins += 1
        wins = g_session.wins_in_cycle
        ents = g_session.entries_in_cycle
        wins_bar = '🟢' * wins + '⚫' * (WINS_PER_CYCLE - wins)
        txt = f"<b>✅ GANAMOS {value:.2f}x — 🕐 {hora}</b>"
        await broadcast(txt)
        await _broadcast_scoreboard()
    elif tipo == 'cycle_win':
        g_daily_wins += 1
        g_daily_cycles_won += 1
        wins = g_session.wins_in_cycle
        wins_bar = '🟢' * wins
        txt = (f"<b>✅ GANAMOS {value:.2f}x — 🕐 {hora}</b>\n"
               f"<b>💎 CICLO COMPLETO {wins}/{WINS_PER_CYCLE} VICTORIAS {wins_bar}</b>")
        await broadcast(txt)
        await _broadcast_scoreboard()
        reset_global_session()
    elif tipo == 'new_col':
        g_daily_losses += 1
        wins = g_session.wins_in_cycle
        ents = g_session.entries_in_cycle
        col = g_session.col
        wins_bar = '🟢' * wins + '⚫' * (WINS_PER_CYCLE - wins)
        txt = f"<b>❌ PERDIMOS {value:.2f}x — 🕐 {hora}</b>"
        await broadcast(txt)
        await _broadcast_scoreboard()
    elif tipo == 'cycle_loss':
        g_daily_losses += 1
        g_daily_cycles_lost += 1
        txt = (f"<b>❌ PERDIMOS {value:.2f}x — 🕐 {hora}</b>\n"
               f"<b>💎 CICLO PERDIDO 😭</b>")
        await broadcast(txt)
        await _broadcast_scoreboard()
        reset_global_session()
    else:
        logger.warning(f"Resultado inesperado: {tipo}")


# ─── WEBSOCKET ───────────────────────────────────────────────────────────────
async def ws_collector():
    last_value = None
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=10, close_timeout=10) as ws:
                subscribe_msg = {"type": "subscribe", "casinoId": CASINO_ID, "currency": CURRENCY, "key": [GAME_ID]}
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
                        round_id = str(first.get('roundId') or first.get('gameRoundId') or first.get('id') or f"{value}_{int(time.time()*1000)}")
                        if round_id in g_seen_ids or value == last_value:
                            continue
                        g_seen_ids.add(round_id)
                        last_value = value
                        await process_multiplier(value, round_id)
                    except Exception as e:
                        logger.debug(f"Error procesando mensaje: {e}")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        await asyncio.sleep(5)


# ─── FLASK KEEP-ALIVE ────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    return f"🤖 SpacemanBot ACTIVO | Datos: {len(g_mults)}/400 | Sesión: {g_session.state} | Señal: {g_signal_state}", 200

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
    last5 = [f"{m['value']:.2f}x" for m in g_mults[-5:]] if g_mults else []
    avg = avg_sec(g_gaps2x)
    return {"status": "ok", "mults_collected": len(g_mults), "signal_state": g_signal_state,
            "events_2x": len(g_events2x), "avg_gap_sec": round(avg, 1) if avg else None,
            "session_state": g_session.state, "session_col": g_session.col,
            "wins": g_session.wins, "losses": g_session.losses,
            "last_5": last5}

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


# ─── COMANDOS DE TELEGRAM ─────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def cmd_start(message):
    name = message.from_user.first_name or "usuario"
    g_all_chats.add(message.chat.id)
    stats_blk = quota_stats_text(len(g_events2x), g_gaps2x)
    data_info = f"📡 <code>{len(g_mults)}/400</code> multiplicadores" if g_mults else "📡 Recopilando datos..."
    await bot.reply_to(message,
        f"🚀 <b>¡Bienvenido {name}!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <b>Bot de Señales Spaceman</b>\n"
        f"📊 Sistema 2x–4.99x por Tiempo | Objetivo: <code>{BET_WIN_TARGET:.2f}x</code>\n"
        f"🔄 Gestión: <code>{MAX_COLS}</code> Entradas × <code>{WINS_PER_CYCLE}</code> Victorias/Ciclo\n"
        f"💵 Apuesta base fija: <code>${BASE_BET:.2f}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{data_info}\n\n{stats_blk}\n"
        "✅ <b>¡Registrado!</b>\n<i>Señales con confianza ≥60% cuando el patrón de tiempo es regular.</i>",
        parse_mode='HTML')


@bot.message_handler(commands=['estadisticas'])
async def cmd_estadisticas(message):
    g_all_chats.add(message.chat.id)
    s = g_session
    trend = quota_stats_text(len(g_events2x), g_gaps2x)
    fichas_recientes = s.fichas[-15:]
    if fichas_recientes:
        lineas = []
        for f in fichas_recientes:
            total = sum(f.get(f'c{i}', 0.0) for i in range(1, 13))
            net = BASE_BET if f['result'] == 'win' else -total
            res = "✅" if f['result'] == 'win' else "❌"
            hora_f = f.get('ts', '--:--')
            partes = [f"C{i}:${f.get(f'c{i}',0.0):.2f}" for i in range(1,13) if f.get(f'c{i}',0.0)>0]
            cols_txt = " ".join(partes) if partes else "—"
            net_txt = f"+${net:.2f}" if net >= 0 else f"-${abs(net):.2f}"
            lineas.append(f"{res} #{f['n']} {hora_f} | {cols_txt} | {net_txt}")
        fichas_txt = "\n".join(lineas)
        total_fichas = len(s.fichas)
        wins_f = sum(1 for f in s.fichas if f['result'] == 'win')
        loss_f = total_fichas - wins_f
        resumen = f"Total fichas: <code>{total_fichas}</code> | ✅ <code>{wins_f}</code> | ❌ <code>{loss_f}</code>"
    else:
        fichas_txt = "<i>Sin fichas registradas aún.</i>"
        resumen = "Total fichas: <code>0</code>"
    await bot.reply_to(message,
        f"📊 <b>ESTADÍSTICAS DEL BOT</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n{s.status_short()}\n━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Últimas fichas (C1 a C12):</b>\n{fichas_txt}\n━━━━━━━━━━━━━━━━━━━━━━━\n{resumen}\n━━━━━━━━━━━━━━━━━━━━━━━\n{trend}",
        parse_mode='HTML')


@bot.message_handler(commands=['predictor'])
async def cmd_predictor(message):
    g_all_chats.add(message.chat.id)
    hora = argentina_time()
    eventos = len(g_events2x)
    avg = avg_sec(g_gaps2x)
    if eventos < MIN_EVENTS_2X:
        await bot.reply_to(message,
            f"📡 <b>PREDICTOR 2x–4.99x — 🕐 {hora}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ Acumulando datos: <code>{eventos}/{MIN_EVENTS_2X}</code> eventos en rango",
            parse_mode='HTML')
        return
    last_event = g_events2x[-1] if g_events2x else 0
    elapsed    = time.time() - last_event
    conf, eta, avg_gap = calc_confidence(g_gaps2x, elapsed)
    conf_pct   = round(conf * 100)
    last_gap   = g_gaps2x[-1] if g_gaps2x else 0
    estado_conf = "🟢 ALTA" if conf >= MIN_CONFIDENCE else "🟡 MEDIA" if conf >= 0.40 else "🔴 BAJA"
    txt = (f"📡 <b>PREDICTOR 2x–4.99x — 🕐 {hora}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━━━━\n"
           f"⏱ Intervalo promedio: <code>{avg_gap:.1f}s</code>\n"
           f"⏱ Último intervalo: <code>{last_gap:.1f}s</code>\n"
           f"⏱ Transcurrido: <code>{elapsed:.1f}s</code>\n"
           f"🎯 ETA próximo evento: <code>~{eta:.0f}s</code>\n"
           f"📊 Confianza: <code>{conf_pct}%</code> — {estado_conf}\n"
           f"📊 Eventos en rango: <code>{eventos}</code>\n"
           f"🪟 Ventana de señal: <code>{SIGNAL_WINDOW_SEC}s</code>\n"
           f"🔒 Umbral mínimo: <code>{round(MIN_CONFIDENCE*100)}%</code>")
    await bot.reply_to(message, txt, parse_mode='HTML')


# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main_async():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    logger.info("🤖 Iniciando SpacemanBot (Sistema 2x–4.99x · Confianza · Ciclo 2/2)...")
    load_mults_from_disk()
    await bot.set_my_commands([
        types.BotCommand('predictor', '📡 Estado del predictor 2x'),
        types.BotCommand('estadisticas', '📊 Estadísticas de la sesión'),
    ])
    asyncio.create_task(ws_collector())
    asyncio.create_task(self_ping_loop())
    render_url = os.environ.get('RENDER_EXTERNAL_URL', '').rstrip('/')
    if render_url:
        await bot.remove_webhook()
        await asyncio.sleep(1)
        await bot.set_webhook(url=f"{render_url}/webhook")
        logger.info(f"✅ Webhook configurado: {render_url}/webhook")
        while True:
            await asyncio.sleep(3600)
    else:
        logger.warning("⚠️ Usando polling (desarrollo local)")
        await bot.infinity_polling(skip_pending=True)


if __name__ == '__main__':
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_async())
