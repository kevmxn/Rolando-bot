#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════╗
║   SPACEMAN BOT — Sistema Moderado 2.00x         ║
║   Señales: Maestro HTML (analyzeTrend)          ║
║   Niveles: Medio | Moderado | Alto              ║
║   Exigencia creciente por columna               ║
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

WIN_TARGET    = 2.00
MAX_COLS       = 7
MAX_ATTS       = 1
WINS_PER_CYCLE = 2
BASE_BET       = 0.10

THRESH_LOW_MAX = 48.0
THRESH_MID_MIN = 31.0

MAX_MULTS  = 400
TRIM_MULTS = 200
PERSIST_FILE = "spaceman_history.json"

# ─── NIVEL MÍNIMO DE SEÑAL POR COLUMNA (exigencia creciente) ──────────────────
# 1 = Moderado, 2 = Medio, 3 = Alto
MIN_LEVEL_BY_COL = {
    1: 1,  # col1 acepta Moderado, Medio o Alto
    2: 1,
    3: 2,  # col3 requiere al menos Medio
    4: 2,
    5: 3,  # col5 requiere Alto
    6: 3,
    7: 3,
}

# Mapeo de nivel numérico a nombre para logs y mensajes
LEVEL_NAMES = {
    1: "Moderado",
    2: "Medio",
    3: "Alto",
}

# ─── ESTADO GLOBAL ────────────────────────────────────────────────────────────
g_mults:    list  = []
g_seen_ids: set   = set()
g_positions: list = []
g_ema3:  list     = []
g_ema4:  list     = []
g_ema8:  list     = []
g_ema20: list     = []

SIGNAL_COOLDOWN = 6
g_cooldown_mod  = 0

g_signal_state        = 'idle'
g_signal_type: Optional[str] = None
g_signal_strictness: int     = 0
g_signal_trigger_mult: float = 0.0

g_all_chats: set              = set()
g_trend_favorable: Optional[bool] = None

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


# ─── EMAs ─────────────────────────────────────────────────────────────────────
def calc_ema(data: list, period: int) -> list:
    if not data:
        return []
    k = 2 / (period + 1)
    ema = [data[0]]
    for i in range(1, len(data)):
        ema.append((data[i] - ema[i - 1]) * k + ema[i - 1])
    return ema


# ─── PERSISTENCIA ─────────────────────────────────────────────────────────────
def save_mults_to_disk():
    try:
        payload = {
            'mults': [{'id': m['id'], 'value': m['value'], 'ts': m['ts']} for m in g_mults],
            'positions': g_positions,
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
    global g_mults, g_positions, g_ema3, g_ema4, g_ema8, g_ema20
    global g_daily_wins, g_daily_losses, g_daily_date
    if not os.path.exists(PERSIST_FILE):
        logger.info("Sin historial previo")
        return
    try:
        with open(PERSIST_FILE) as f:
            data = json.load(f)
        loaded_mults = data.get('mults', [])
        loaded_pos = data.get('positions', [])
        if len(loaded_mults) > MAX_MULTS:
            loaded_mults = loaded_mults[-TRIM_MULTS:]
            loaded_pos = loaded_pos[-TRIM_MULTS:]
        g_mults[:] = loaded_mults
        g_positions[:] = loaded_pos
        g_ema3 = calc_ema(g_positions, 3)
        g_ema4 = calc_ema(g_positions, 4)
        g_ema8 = calc_ema(g_positions, 8)
        g_ema20 = calc_ema(g_positions, 20)
        for m in g_mults:
            g_seen_ids.add(str(m['id']))
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
        logger.info(f"Historial cargado: {len(g_mults)} mults")
    except Exception as e:
        logger.warning(f"Error cargando: {e}")


# ─── ESTADÍSTICAS DE CUOTAS ───────────────────────────────────────────────────
def get_quota_stats(n: int = 200) -> dict:
    data = g_mults[-n:] if len(g_mults) >= n else g_mults[:]
    total = len(data)
    if total == 0:
        return {'total': 0, 'has_enough': False, 'favorable': None,
                'count_100_199': 0, 'count_200_499': 0, 'count_500_999': 0, 'count_1000_plus': 0,
                'pct_100_199': 0.0, 'pct_200_499': 0.0, 'pct_500_999': 0.0, 'pct_1000_plus': 0.0}
    r1 = sum(1 for m in data if 1.00 <= m['value'] < 2.00)
    r2 = sum(1 for m in data if 2.00 <= m['value'] < 5.00)
    r3 = sum(1 for m in data if 5.00 <= m['value'] < 10.00)
    r4 = sum(1 for m in data if m['value'] >= 10.00)
    pct1 = r1 / total * 100
    pct2 = r2 / total * 100
    unfavorable = pct1 > THRESH_LOW_MAX or pct2 < THRESH_MID_MIN
    return {
        'total': total, 'has_enough': total >= 200, 'favorable': not unfavorable,
        'count_100_199': r1, 'count_200_499': r2, 'count_500_999': r3, 'count_1000_plus': r4,
        'pct_100_199': pct1, 'pct_200_499': pct2, 'pct_500_999': r3/total*100, 'pct_1000_plus': r4/total*100,
    }


def quota_stats_text(stats: dict) -> str:
    if stats['total'] == 0:
        return "📡 <i>Sin datos suficientes.</i>\n"
    n_label = "200" if stats['has_enough'] else str(stats['total']) + " (acumulando...)"
    r1_flag = " ✅" if stats['pct_100_199'] <= THRESH_LOW_MAX else " ❌"
    r2_flag = " ✅" if stats['pct_200_499'] >= THRESH_MID_MIN else " ❌"
    fav_line = ("✅ <b>¡TENDENCIA FAVORABLE!</b>\n      <i>Se recomienda operar</i>"
                if stats['favorable'] else
                "⚠️ <b>TENDENCIA DESFAVORABLE</b>\n      <i>Se recomienda esperar</i>")
    return (f"📈 <b>Análisis de la Tendencia últimos</b>\n"
            f"      <b>{n_label} multiplicadores</b>\n"
            f"🔵 Cuotas (1.00-1.99x): <code>{stats['count_100_199']}</code> — {stats['pct_100_199']:.2f}%{r1_flag}\n"
            f"🟣 Cuotas (2.00-4.99x): <code>{stats['count_200_499']}</code> — {stats['pct_200_499']:.2f}%{r2_flag}\n"
            f"🟡 Cuotas (5.00-9.99x): <code>{stats['count_500_999']}</code> — {stats['pct_500_999']:.2f}%\n"
            f"🔴 Cuotas (+10.00x):    <code>{stats['count_1000_plus']}</code> — {stats['pct_1000_plus']:.2f}%\n\n{fav_line}\n")


# ─── DETECCIÓN DE SEÑALES (HTML analyzeTrend) ─────────────────────────────────
def check_html_signal(data: list) -> Tuple[bool, Optional[str], Optional[int]]:
    """Retorna (detectada, nombre_nivel, nivel_numérico)."""
    if len(data) < 3:
        return False, None, None
    recent = data[-10:] if len(data) >= 10 else data[:]
    vals = [d['value'] for d in recent]
    total = len(vals)
    avg = sum(vals) / total
    last3avg = sum(vals[:3]) / 3 if total >= 3 else avg
    last_is_green = vals[0] >= 2.0
    second_last = vals[1] if total > 1 else vals[0]
    streak = 0
    for v in vals:
        if v >= 2.0:
            break
        streak += 1
    # Condiciones en orden: S1 -> Moderado, S2 -> Medio, S3 -> Alto
    if streak >= 3 and last_is_green:
        return True, "Moderado", 1
    if second_last < 2.0 and last_is_green:
        return True, "Medio", 2
    if last_is_green and last3avg > avg:
        return True, "Alto", 3
    return False, None, None


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


# ─── PROCESADOR DE MULTIPLICADORES (señales con exigencia por columna) ────────
async def process_multiplier(value: float, round_id: str):
    global g_signal_state, g_signal_type, g_signal_strictness, g_signal_trigger_mult
    global g_positions, g_ema3, g_ema4, g_ema8, g_ema20, g_mults, g_seen_ids
    global g_trend_favorable, g_session, g_cooldown_mod, _persist_counter

    logger.info(f"🎲 {value:.2f}x | ID: {round_id} | Señal: {g_signal_state}/{g_signal_type}")

    _check_daily_reset()

    # Fase 1: Evaluar resultado pendiente
    if g_signal_state == 'evaluating' and g_session.state == GlobalSession.EVALUATING:
        win = value >= WIN_TARGET
        tipo, bet = g_session.on_result(win)
        await _dispatch_result(value, tipo, bet, attempt_num=g_session.attempt)
        g_signal_state = 'idle'
        g_signal_type = None
        g_signal_strictness = 0
        g_cooldown_mod = max(g_cooldown_mod, 2)

    g_cooldown_mod = max(0, g_cooldown_mod - 1)

    # Actualizar datos
    increment = 1 if value >= WIN_TARGET else -1
    prev = g_positions[-1] if g_positions else 0
    g_positions.append(prev + increment)
    g_mults.append({'id': round_id, 'value': value, 'ts': time.time()})
    if len(g_mults) >= MAX_MULTS:
        g_mults[:] = g_mults[-TRIM_MULTS:]
        g_positions[:] = g_positions[-TRIM_MULTS:]
        save_mults_to_disk()
    else:
        _persist_counter += 1
        if _persist_counter >= 10:
            _persist_counter = 0
            save_mults_to_disk()

    g_ema3 = calc_ema(g_positions, 3)
    g_ema4 = calc_ema(g_positions, 4)
    g_ema8 = calc_ema(g_positions, 8)
    g_ema20 = calc_ema(g_positions, 20)

    if len(g_seen_ids) > 2000:
        oldest = sorted(g_seen_ids)[:1000]
        for oid in oldest:
            g_seen_ids.discard(oid)

    # Tendencia
    stats_trend = get_quota_stats(200)
    if stats_trend['total'] >= 10 and stats_trend['favorable'] != g_trend_favorable:
        g_trend_favorable = stats_trend['favorable']
        logger.info(f"Tendencia → {'FAVORABLE' if g_trend_favorable else 'DESFAVORABLE'}")

    # Detección de señal con exigencia por columna
    if (g_signal_state == 'idle' and g_session.state == GlobalSession.IDLE
            and g_cooldown_mod == 0 and g_trend_favorable is True):
        detected, level_name, level_num = check_html_signal(g_mults)
        if detected:
            col_actual = g_session.col
            min_level = MIN_LEVEL_BY_COL.get(col_actual, 3)
            if level_num >= min_level:
                g_signal_state = 'evaluating'
                g_signal_type = level_name
                g_signal_strictness = level_num
                g_signal_trigger_mult = value
                g_session.signal_trigger_mult = value
                g_session.state = GlobalSession.EVALUATING
                g_cooldown_mod = SIGNAL_COOLDOWN
                if g_session.col == 1:
                    g_session.start_ficha()
                logger.info(f"🎯 SEÑAL {level_name} (nivel {level_num}) | Col{col_actual} (mínimo {min_level}) | Trigger: {value:.2f}x")
                await _send_signal(value, level_name, level_num)
            else:
                logger.info(f"🚫 Señal {level_name} (nivel {level_num}) DESCARTADA para Col{col_actual} — requiere mínimo {min_level}")


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
    txt = (f"📆 <b>MARCADOR DEL DÍA</b> — 🕐 <b>{hora}</b>\n"
           f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
           f"✅ Ganadas: {g_daily_wins}\n"
           f"❌ Perdidas: {g_daily_losses}\n"
           f"📈 Acierto: {pct_sig:.1f}%\n"
           f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
           f"🔄 Ciclos {MAX_COLS} entradas · {WINS_PER_CYCLE} victorias\n"
           f"✅ Ganados: {g_daily_cycles_won}\n"
           f"❌ Perdidos: {g_daily_cycles_lost}\n"
           f"📈 Acierto: {pct_cyc:.1f}%")
    if g_scoreboard_msg_id:
        try:
            await bot.delete_message(CHANNEL_ID, g_scoreboard_msg_id)
        except:
            pass
        g_scoreboard_msg_id = None
    sent = await bot.send_message(CHANNEL_ID, txt, parse_mode='HTML')
    g_scoreboard_msg_id = sent.message_id


# ─── MENSAJERÍA ───────────────────────────────────────────────────────────────
async def _send_signal(trigger: float, level_name: str, level_num: int):
    hora = argentina_time()
    col = g_session.col
    ents = g_session.entries_in_cycle + 1
    wins = g_session.wins_in_cycle
    ents_bar = '⚫' * (ents - 1) + '🔵' + '⚫' * (MAX_COLS - ents)
    wins_bar = '⚪' * WINS_PER_CYCLE
    logger.info(f"📤 Señal {level_name} | Col{col} | Entrada {ents}/{MAX_COLS} | Ciclo {wins}/{WINS_PER_CYCLE}")
    txt = (f"🆔 <b>ENTRADA SPACEMAN</b> — 🕐 <b>{hora}</b>\n"
           f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
           f"🧨 Después de: {trigger:.2f}x\n"
           f"🎯 Objetivo: 2.00x\n"
           f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
           f"🔰 Col {col}  |  Entradas {ents}/{MAX_COLS}\n"
           f"{ents_bar}\n"
           f"💎 Ciclo {wins}/{WINS_PER_CYCLE} victorias\n"
           f"{wins_bar}")
    await broadcast_signal(txt)


async def _dispatch_result(value: float, tipo: str, bet: float, attempt_num: int):
    global g_session, g_daily_wins, g_daily_losses, g_daily_cycles_won, g_daily_cycles_lost
    hora = argentina_time()
    if tipo == 'win':
        g_daily_wins += 1
        wins = g_session.wins_in_cycle
        ents = g_session.entries_in_cycle
        wins_bar = '🟢' * wins + '⚪' * (WINS_PER_CYCLE - wins)
        txt = (f"✅ <b>GANAMOS</b> <code>{value:.2f}x</code> — 🕐 <b>{hora}</b>\n"
               f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
               f"💎 Ciclo  {wins_bar}  {wins}/{WINS_PER_CYCLE}\n"
               f"🔰 Entradas usadas: {ents}/{MAX_COLS}")
        await broadcast(txt)
        await _broadcast_scoreboard()
    elif tipo == 'cycle_win':
        g_daily_wins += 1
        g_daily_cycles_won += 1
        total_cyc = g_daily_cycles_won + g_daily_cycles_lost
        pct_cyc = (g_daily_cycles_won / total_cyc * 100) if total_cyc > 0 else 0.0
        wins_bar = '🟢' * WINS_PER_CYCLE
        txt = (f"✅ <b>GANAMOS</b> <code>{value:.2f}x</code> — 🕐 <b>{hora}</b>\n"
               f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
               f"❤️ <b>¡CICLO COMPLETADO!</b>\n"
               f"{wins_bar}  {WINS_PER_CYCLE}/{WINS_PER_CYCLE} victorias\n"
               f"💎 Ciclos ganados hoy: {g_daily_cycles_won} — {pct_cyc:.0f}%\n"
               f"🔄 Nueva sesión iniciada")
        await broadcast(txt)
        await _broadcast_scoreboard()
        reset_global_session()
    elif tipo == 'new_col':
        g_daily_losses += 1
        wins = g_session.wins_in_cycle
        ents = g_session.entries_in_cycle
        col = g_session.col
        ents_bar = '⚫' * (ents - 1) + '🔴' + '⚫' * (MAX_COLS - ents)
        wins_bar = '🟢' * wins + '⚪' * (WINS_PER_CYCLE - wins)
        txt = (f"❌ <b>PERDIMOS</b> <code>{value:.2f}x</code> — 🕐 {hora}\n"
               f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
               f"🔰 {ents_bar}  {ents}/{MAX_COLS}\n"
               f"💎 Ciclo  {wins_bar}  {wins}/{WINS_PER_CYCLE}\n"
               f"➡️ Siguiente entrada: Col {col}")
        await broadcast(txt)
        await _broadcast_scoreboard()
    elif tipo == 'cycle_loss':
        g_daily_losses += 1
        g_daily_cycles_lost += 1
        total_cyc = g_daily_cycles_won + g_daily_cycles_lost
        pct_cyc = (g_daily_cycles_won / total_cyc * 100) if total_cyc > 0 else 0.0
        ents_bar = '🔴' * MAX_COLS
        txt = (f"❌ <b>PERDIMOS</b> <code>{value:.2f}x</code> — 🕐 <b>{hora}</b>\n"
               f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
               f"😭 <b>¡CICLO PERDIDO!</b>\n"
               f"{ents_bar}  {MAX_COLS}/{MAX_COLS} entradas\n"
               f"💎 Ciclos ganados hoy: {g_daily_cycles_won} — {pct_cyc:.0f}%\n"
               f"🔄 Nueva sesión iniciada")
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
    return {"status": "ok", "mults_collected": len(g_mults), "signal_state": g_signal_state,
            "signal_type": g_signal_type, "session_state": g_session.state, "session_col": g_session.col,
            "wins": g_session.wins, "losses": g_session.losses, "trend_favorable": g_trend_favorable,
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
    stats = get_quota_stats(200)
    stats_blk = quota_stats_text(stats)
    data_info = f"📡 <code>{len(g_mults)}/400</code> multiplicadores" if g_mults else "📡 Recopilando datos..."
    await bot.reply_to(message,
        f"🚀 <b>¡Bienvenido {name}!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <b>Bot de Señales Spaceman</b>\n"
        "📊 Sistema Moderado | Objetivo: <code>2.00x</code>\n"
        f"🔄 Gestión: <code>{MAX_COLS}</code> Entradas × <code>{WINS_PER_CYCLE}</code> Victorias/Ciclo\n"
        f"💵 Apuesta base fija: <code>${BASE_BET:.2f}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{data_info}\n\n{stats_blk}\n"
        "✅ <b>¡Registrado!</b>\n<i>Recibirás señales cuando la tendencia sea favorable.</i>",
        parse_mode='HTML')


@bot.message_handler(commands=['estadisticas'])
async def cmd_estadisticas(message):
    g_all_chats.add(message.chat.id)
    s = g_session
    stats = get_quota_stats(200)
    trend = quota_stats_text(stats)
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


@bot.message_handler(commands=['tendencia'])
async def cmd_tendencia(message):
    g_all_chats.add(message.chat.id)
    hora = argentina_time()
    stats = get_quota_stats(200)
    if stats['total'] == 0:
        await bot.reply_to(message, "📡 <i>Sin datos suficientes.</i>", parse_mode='HTML')
        return
    n_label = "200" if stats['has_enough'] else str(stats['total'])
    r1_flag = "✅" if stats['pct_100_199'] <= THRESH_LOW_MAX else "❌"
    r2_flag = "✅" if stats['pct_200_499'] >= THRESH_MID_MIN else "❌"
    header = f"🟢 TENDENCIA FAVORABLE — {hora}" if stats['favorable'] else f"🔴 TENDENCIA DESFAVORABLE — {hora}"
    footer = ("✅ <b>¡TENDENCIA FAVORABLE!</b>\n      <i>Se recomienda operar</i>"
              if stats['favorable'] else
              "⚠️ <b>TENDENCIA DESFAVORABLE</b>\n      <i>Se recomienda esperar</i>")
    txt = (f"<b>{header}</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n"
           f"📈 Análisis últimos {n_label} multiplicadores\n"
           f"🔵 1.00-1.99x: <code>{stats['count_100_199']}</code> — {stats['pct_100_199']:.2f}%{r1_flag}\n"
           f"🟣 2.00-4.99x: <code>{stats['count_200_499']}</code> — {stats['pct_200_499']:.2f}%{r2_flag}\n"
           f"🟡 5.00-9.99x: <code>{stats['count_500_999']}</code> — {stats['pct_500_999']:.2f}%\n"
           f"🔴 +10.00x:    <code>{stats['count_1000_plus']}</code> — {stats['pct_1000_plus']:.2f}%\n\n{footer}")
    await bot.reply_to(message, txt, parse_mode='HTML')


# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main_async():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    logger.info("🤖 Iniciando SpacemanBot (Niveles: Moderado/Medio/Alto)...")
    load_mults_from_disk()
    await bot.set_my_commands([types.BotCommand('tendencia', '📈 Ver tendencia actual')])
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
