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
MAX_COLS       = 7     # Entradas máximas por ciclo (7 entradas = 1 ciclo)
MAX_ATTS       = 1     # 1 intento por columna (sin segundo intento)
WINS_PER_CYCLE = 2     # Victorias necesarias dentro de MAX_COLS entradas para ganar el ciclo
BASE_BET       = 0.10  # Apuesta base fija (USD)

# ── Umbrales de tendencia (detección favorable/desfavorable) ──────────────────
# Desfavorable si: cuotas 1.00-1.99x superan THRESH_LOW_MAX
#              O si: cuotas 2.00-4.99x caen por debajo de THRESH_MID_MIN
THRESH_LOW_MAX = 52.0  # % máximo permitido para cuotas 1.00-1.99x
THRESH_MID_MIN = 29.0  # % mínimo requerido para cuotas 2.00-4.99x

# ── Parámetros internos ───────────────────────────────────────────────────────
MAX_MULTS  = 400
TRIM_MULTS = 200

# Archivo de persistencia del historial de multiplicadores (sobrevive reinicios)
PERSIST_FILE = "spaceman_history.json"

# ─── ESTADO GLOBAL ────────────────────────────────────────────────────────────
g_mults:    list  = []
g_seen_ids: set   = set()
g_positions: list = []
g_ema3:  list     = []
g_ema4:  list     = []
g_ema8:  list     = []
g_ema20: list     = []

# ── Sistema de señales — Gráfica Moderada AMX 2x ─────────────────────────────
# Tres niveles según la columna activa (estrategia dinero real):
#   S1 (C1,C2)   → condiciones alerta150: rebote EMA4↑EMA8, patrón [-1,-1,-1,+1], soporte
#   S2 (C3,C4)   → condiciones alerta200: EMA8↑EMA20, consolidación, 2× consecutivos
#   S3 (C5,C6,C7)→ condición más selectiva: solo cruce EMA8↑EMA20
SIGNAL_COOLDOWN = 6   # ticks mínimos entre señales para evitar spam
g_cooldown_mod  = 0   # cooldown único para el sistema moderado

g_signal_state        = 'idle'     # 'idle' | 'evaluating' | 'so'
g_signal_type: Optional[str] = None
g_signal_strictness: int     = 0
g_signal_trigger_mult: float = 0.0


# ─── RESTRICCIÓN DE SEÑALES POR COLUMNA (Estrategia Dinero Real) ──────────────
# Gráfica moderada AMX 2x → posición: +1 si valor ≥ 2.00x, -1 si valor < 2.00x
#   C1, C2      → nivel 1 (S1): señales alerta150 del HTML moderado
#   C3, C4      → nivel 2 (S2): señales alerta200 del HTML moderado
#   C5, C6, C7  → nivel 3 (S3): señal más selectiva (solo cruce EMA8↑EMA20)
def signal_level_for_col(col: int) -> int:
    """Retorna el nivel de señal requerido para la columna activa (1, 2 o 3)."""
    if col <= 2:
        return 1
    elif col <= 4:
        return 2
    else:
        return 3

g_all_chats: set              = set()   # Todos los chats que alguna vez enviaron /start
g_trend_favorable: Optional[bool] = None

# ─── MARCADOR DIARIO ──────────────────────────────────────────────────────────
g_daily_wins:        int          = 0
g_daily_losses:      int          = 0
g_daily_cycles_won:  int          = 0    # Ciclos ganados hoy
g_daily_cycles_lost: int          = 0    # Ciclos perdidos hoy
g_daily_date:        str          = ""   # "YYYY-MM-DD" en hora Argentina
g_scoreboard_msg_id: Optional[int] = None  # ID del último mensaje de marcador diario

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


async def broadcast_signal(msg: str, parse_mode: str = None):
    """Envía señal al canal y guarda el message_id para borrado posterior."""
    global g_last_signal_msgs
    g_last_signal_msgs = {}
    try:
        sent = await bot.send_message(CHANNEL_ID, msg, parse_mode=parse_mode)
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
    Guarda el historial de multiplicadores, posiciones y marcador diario en disco.
    Llamado cada 10 nuevos multiplicadores para no perder datos en reinicios.
    """
    try:
        payload = {
            'mults':     [{'id': m['id'], 'value': m['value'], 'ts': m['ts']}
                          for m in g_mults],
            'positions': g_positions,
            # ── Marcador diario (sobrevive reinicios) ──
            'daily_wins':        g_daily_wins,
            'daily_losses':      g_daily_losses,
            'daily_cycles_won':  g_daily_cycles_won,
            'daily_cycles_lost': g_daily_cycles_lost,
            'daily_date':        g_daily_date,
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
    El marcador diario se restaura solo si corresponde al día de hoy (hora Argentina);
    si el archivo es de otro día, los contadores arrancan en 0.
    """
    global g_mults, g_positions, g_ema3, g_ema4, g_ema8, g_ema20
    global g_daily_wins, g_daily_losses, g_daily_date
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
        g_ema3[:]  = calc_ema(g_positions, 3)
        g_ema4[:]  = calc_ema(g_positions, 4)
        g_ema8[:]  = calc_ema(g_positions, 8)
        g_ema20[:] = calc_ema(g_positions, 20)

        # Restaurar IDs vistos para evitar duplicados tras reconexión WS
        for m in g_mults:
            g_seen_ids.add(str(m['id']))

        # ── Marcador diario: restaurar solo si es el mismo día ────────────────
        saved_date = data.get('daily_date', '')
        today_arg  = (datetime.utcnow() - timedelta(hours=3)).strftime("%Y-%m-%d")
        if saved_date == today_arg:
            g_daily_wins        = data.get('daily_wins',        0)
            g_daily_losses      = data.get('daily_losses',      0)
            g_daily_cycles_won  = data.get('daily_cycles_won',  0)
            g_daily_cycles_lost = data.get('daily_cycles_lost', 0)
            g_daily_date        = saved_date
            logger.info(
                f"📊 Marcador diario restaurado: "
                f"✅ {g_daily_wins} | ❌ {g_daily_losses} | "
                f"🏆 Ciclos: {g_daily_cycles_won}W/{g_daily_cycles_lost}L ({today_arg})"
            )
        else:
            # Archivo de otro día → marcador arranca en 0, fecha actualizada
            g_daily_wins        = 0
            g_daily_losses      = 0
            g_daily_cycles_won  = 0
            g_daily_cycles_lost = 0
            g_daily_date        = today_arg
            logger.info(f"📅 Nuevo día detectado al cargar ({today_arg}) — marcador en 0")

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


# ─── DETECCIÓN DE SEÑALES — GRÁFICA MODERADA AMX 2x ─────────────────────────
def check_moderate_signal(
    positions: list,
    ema4: list, ema8: list, ema20: list,
    data: list,
    level: int
) -> bool:
    """
    Detecta señales del gráfico moderado AMX con umbral 2.00x.
    Equivalente a checkModerateAlerts() del HTML AMX_V20.
    Posiciones: +1 si value >= 2.00x | -1 si value < 2.00x.

    level 1 (S1, C1-C2) → condiciones alerta150: más frecuentes.
    level 2 (S2, C3-C4) → condiciones alerta200: moderadas.
    level 3 (S3, C5-C7) → solo cruce EMA8↑EMA20: más selectivo.
    """
    if len(data) < 4 or not positions:
        return False

    cur_pos  = positions[-1]
    cur_e4   = ema4[-1]  if ema4  else cur_pos
    cur_e8   = ema8[-1]  if ema8  else cur_pos
    cur_e20  = ema20[-1] if ema20 else cur_pos
    prev_e4  = ema4[-2]  if len(ema4)  >= 2 else cur_e4
    prev_e8  = ema8[-2]  if len(ema8)  >= 2 else cur_e8
    prev_e20 = ema20[-2] if len(ema20) >= 2 else cur_e20

    soporte = min(positions[-20:]) if len(positions) >= 20 else min(positions)

    if level == 1:
        # ── S1: condiciones alerta150 (HTML moderado) ─────────────────────────
        # Cond 1: patrón [-1,-1,-1,+1] con posición por debajo de EMA4 y EMA8
        cambios = [1 if d['value'] >= 2.00 else -1 for d in data[-4:]]
        c1, c2, c3, c4 = cambios
        if c1 == -1 and c2 == -1 and c3 == -1 and c4 == 1:
            if cur_pos <= cur_e4 and cur_pos <= cur_e8:
                return True
        # Cond 2: EMA4 cruza EMA8 al alza
        if len(ema4) >= 2 and prev_e4 <= prev_e8 and cur_e4 > cur_e8:
            return True
        # Cond 3: rebote desde soporte, posición sobre las 3 EMAs
        if (cur_pos <= soporte * 1.01
                and cur_pos > cur_e4
                and cur_pos > cur_e8
                and cur_pos > cur_e20):
            return True

    elif level == 2:
        # ── S2: condiciones alerta200 (HTML moderado) ─────────────────────────
        # Cond 1: EMA8 cruza EMA20 al alza
        if len(ema8) >= 2 and prev_e8 <= prev_e20 and cur_e8 > cur_e20:
            return True
        # Cond 2: consolidación → |a-c| ≤ 1, b > a, posición sobre las 3 EMAs
        if len(positions) >= 3:
            a, b, c = positions[-3], positions[-2], positions[-1]
            if (abs(a - c) <= 1 and b > a
                    and cur_pos > cur_e4
                    and cur_pos > cur_e8
                    and cur_pos > cur_e20):
                return True
        # Cond 3: 2 valores >= 2.00x consecutivos + alineación alcista EMAs
        #         y el valor previo a ellos fue < 2.00x
        if (data[-1]['value'] >= 2.00 and data[-2]['value'] >= 2.00
                and cur_e4 > cur_e8 > cur_e20):
            before = data[-3] if len(data) >= 3 else None
            if before is None or before['value'] < 2.00:
                return True

    elif level == 3:
        # ── S3: solo cruce EMA8↑EMA20 — condición más selectiva ──────────────
        if len(ema8) >= 2 and prev_e8 <= prev_e20 and cur_e8 > cur_e20:
            return True

    return False


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

        self.col     = 1
        self.attempt = 1
        self.lost    = 0.0
        self.cur_bet = BASE_BET

        # ── Contadores de ciclo actual ─────────────────────────────────────────
        self.entries_in_cycle = 0   # entradas jugadas en el ciclo actual (máx MAX_COLS)
        self.wins_in_cycle    = 0   # victorias acumuladas en el ciclo actual (meta WINS_PER_CYCLE)

        # ── Contadores globales de sesión ──────────────────────────────────────
        self.entries = 0
        self.wins    = 0
        self.losses  = 0
        self.created = datetime.now()

        self.signal_trigger_mult:    float = 0.0
        self.attempt1_result_value:  float = 0.0

        # Historial de fichas completas (se preserva entre ciclos via carry_fichas)
        self.fichas: list = carry_fichas if carry_fichas is not None else []
        self._cur_ficha: dict = None   # ficha en curso (abarca C1→C2→... si hubiera pérdidas)

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

        Lógica de ciclo:
          - Un ciclo tiene MAX_COLS (12) entradas en total.
          - Se necesitan WINS_PER_CYCLE (4) victorias dentro de esas 12 entradas.
          - Un win resetea col a 1 y la apuesta a base_bet, pero el ciclo continúa.
          - Al alcanzar 4 victorias → cycle_win.
          - Al agotar las 12 entradas sin 4 victorias → cycle_loss.
        """
        self.entries          += 1
        self.entries_in_cycle += 1
        prev_bet = self.cur_bet
        prev_col = self.col

        # Acumular gasto en la columna correspondiente de la ficha activa
        if self._cur_ficha is not None:
            col_key = f'c{prev_col}'
            self._cur_ficha[col_key] = self._cur_ficha.get(col_key, 0.0) + prev_bet

        if win:
            self.wins         += 1
            self.wins_in_cycle += 1
            self.lost    = 0.0
            self.cur_bet = self.base_bet
            self.col     = 1
            self.attempt = 1

            # Cerrar ficha como ganada
            if self._cur_ficha is not None:
                self._cur_ficha['result'] = 'win'
                self.fichas.append(self._cur_ficha)
                self._cur_ficha = None
            if len(self.fichas) > 100:
                self.fichas = self.fichas[-100:]

            # ¿Ciclo ganado? (4 victorias dentro de las 12 entradas)
            if self.wins_in_cycle >= WINS_PER_CYCLE:
                self.state = self.DONE
                return ('cycle_win', prev_bet)

            # Victoria parcial — el ciclo continúa
            self.state = self.IDLE
            return ('win', prev_bet)

        else:
            # Pérdida → avanzar directo a siguiente columna (1 intento por columna)
            self.losses  += 1
            self.lost    += prev_bet
            self.cur_bet  = self.lost + self.base_bet
            self.col     += 1

            # ¿Ciclo agotado? (12 entradas sin llegar a 4 victorias)
            if self.entries_in_cycle >= MAX_COLS:
                if self._cur_ficha is not None:
                    self._cur_ficha['result'] = 'loss'
                    self.fichas.append(self._cur_ficha)
                    self._cur_ficha = None
                if len(self.fichas) > 100:
                    self.fichas = self.fichas[-100:]
                self.state = self.DONE
                return ('cycle_loss', prev_bet)

            # Avanzar a siguiente columna — el ciclo continúa
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
            f"🎯 Ciclo: `{self.wins_in_cycle}/{WINS_PER_CYCLE}` victorias | `{self.entries_in_cycle}/{MAX_COLS}` entradas\n"
            f"📍 Col: `{self.col}/{MAX_COLS}`\n"
            f"💵 Próxima apuesta: `${self.cur_bet:.2f}`\n"
            f"📈 G/P sesión: `{self.wins}/{self.losses}`"
        )


# ─── INSTANCIA GLOBAL ─────────────────────────────────────────────────────────
g_session: GlobalSession = GlobalSession()


def reset_global_session():
    """Reinicia la sesión global preservando el historial de fichas."""
    global g_session
    old_fichas = list(g_session.fichas)   # preservar historial completo
    g_session  = GlobalSession(carry_fichas=old_fichas)
    logger.info("🔄 Sesión global reiniciada — fichas preservadas, ciclo en 0")


# ─── PROCESADOR DE MULTIPLICADORES ───────────────────────────────────────────
async def process_multiplier(value: float, round_id: str):
    global g_signal_state, g_signal_type, g_signal_strictness, g_signal_trigger_mult
    global g_positions, g_ema3, g_ema4, g_ema8, g_ema20, g_mults, g_seen_ids
    global g_trend_favorable, g_session
    global g_cooldown_mod

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

    # ── FASE 2: Decrementar cooldown único del sistema moderado ──────────────
    g_cooldown_mod = max(0, g_cooldown_mod - 1)

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
        save_mults_to_disk()
    else:
        _persist_counter += 1
        if _persist_counter >= 10:
            _persist_counter = 0
            save_mults_to_disk()

    # EMAs actualizadas (incluyen el valor actual, igual que el HTML)
    g_ema3  = calc_ema(g_positions, 3)
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

    # ── FASE 4: Detectar nueva señal — Gráfica Moderada AMX 2x ──────────────
    if g_signal_state == 'idle' and g_session.state == GlobalSession.IDLE and g_cooldown_mod == 0:

        level = signal_level_for_col(g_session.col)
        sig_names = {1: 'Mod_S1', 2: 'Mod_S2', 3: 'Mod_S3'}

        if check_moderate_signal(g_positions, g_ema4, g_ema8, g_ema20, g_mults, level):
            sig_type   = sig_names[level]
            strictness = level

            g_signal_state        = 'evaluating'
            g_signal_type         = sig_type
            g_signal_strictness   = strictness
            g_signal_trigger_mult = value
            g_session.signal_trigger_mult = value
            g_session.state       = GlobalSession.EVALUATING
            g_cooldown_mod        = SIGNAL_COOLDOWN

            if g_session.col == 1:
                g_session.start_ficha()

            logger.info(
                f"🎯 SEÑAL {sig_type} (nivel {level}) "
                f"Col{g_session.col} | Trigger: {value:.2f}x"
            )
            await _send_signal(value, sig_type, strictness)



# ─── MARCADOR DIARIO ──────────────────────────────────────────────────────────
def _check_daily_reset():
    """
    Resetea el marcador diario a las 00:00 hora Argentina.
    El último mensaje de marcador del día anterior se DEJA publicado (no se borra).
    Se anula el ID guardado para que el próximo resultado envíe un mensaje nuevo.
    """
    global g_daily_wins, g_daily_losses, g_daily_date
    global g_daily_cycles_won, g_daily_cycles_lost, g_scoreboard_msg_id
    now_arg  = datetime.utcnow() - timedelta(hours=3)
    today    = now_arg.strftime("%Y-%m-%d")
    if today != g_daily_date:
        g_daily_wins        = 0
        g_daily_losses      = 0
        g_daily_cycles_won  = 0
        g_daily_cycles_lost = 0
        g_daily_date        = today
        g_scoreboard_msg_id = None   # Próximo marcador será un mensaje nuevo
        logger.info(f"📅 Marcador diario reseteado para {today}")


async def _broadcast_scoreboard():
    """
    Borra el marcador diario anterior y envía uno nuevo actualizado.
    Muestra señales ganadas/perdidas Y ciclos exitosos/fallidos.
    """
    global g_scoreboard_msg_id

    # ── Estadísticas de señales ────────────────────────────────────────────────
    total_sig = g_daily_wins + g_daily_losses
    pct_sig   = (g_daily_wins / total_sig * 100) if total_sig > 0 else 0.0

    # ── Estadísticas de ciclos ─────────────────────────────────────────────────
    total_cyc = g_daily_cycles_won + g_daily_cycles_lost
    pct_cyc   = (g_daily_cycles_won / total_cyc * 100) if total_cyc > 0 else 0.0

    hora  = argentina_time()

    txt = (
        f"📆 *MARCADOR DEL DÍA* — 🕐 {hora}\n"
        f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        f"✅ Ganadas: {g_daily_wins}\n"
        f"❌ Perdidas: {g_daily_losses}\n"
        f"📈 Acierto: {pct_sig:.1f}%\n"
        f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        f"🔄 Ciclos {MAX_COLS} entradas · {WINS_PER_CYCLE} victorias\n"
        f"✅ Ganados: {g_daily_cycles_won}\n"
        f"❌ Perdidos: {g_daily_cycles_lost}\n"
        f"📈 Acierto: {pct_cyc:.1f}%"
    )

    # Borrar marcador anterior si existe
    if g_scoreboard_msg_id:
        try:
            await bot.delete_message(CHANNEL_ID, g_scoreboard_msg_id)
            logger.info(f"🗑️ Marcador anterior borrado (msg_id: {g_scoreboard_msg_id})")
        except Exception as e:
            logger.warning(f"No se pudo borrar marcador anterior: {e}")
        g_scoreboard_msg_id = None

    # Enviar marcador actualizado
    try:
        sent = await bot.send_message(CHANNEL_ID, txt, parse_mode='Markdown')
        g_scoreboard_msg_id = sent.message_id
        logger.info(f"📊 Marcador diario enviado (msg_id: {sent.message_id})")
    except Exception as e:
        logger.warning(f"Error enviando marcador diario: {e}")


# ─── MENSAJERÍA ───────────────────────────────────────────────────────────────
async def _send_signal(trigger: float, signal_name: str, strictness: int):
    """Broadcast de señal al canal — formato Estrategia Dinero Real."""
    hora = argentina_time()
    col  = g_session.col
    ents = g_session.entries_in_cycle + 1   # número de entrada que se va a jugar
    wins = g_session.wins_in_cycle

    ents_bar = '⚫' * (ents - 1) + '🔵' + '⚫' * (MAX_COLS - ents)
    wins_bar = '⚪' * WINS_PER_CYCLE     # 2 círculos blancos (victorias meta)

    # Etiqueta de señal para log interno
    sig_label = {1: 'S1 Wabe↑', 2: 'S2 Doble Piso', 3: 'S3 EMA3↑EMA4'}.get(strictness, f'S{strictness}')
    logger.info(f"📤 Señal {sig_label} | Col{col} | Entrada {ents}/{MAX_COLS} | Ciclo {wins}/{WINS_PER_CYCLE}")

    txt = (
        f"🆔 ENTRADA SPACEMAN — 🕐 {hora}\n"
        f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        f"🧨 Después de: {trigger:.2f}x\n"
        f"🎯 Objetivo: 2.00x\n"
        f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        f"🔰 Col {col}  |  Entradas {ents}/{MAX_COLS}\n"
        f"{ents_bar}\n"
        f"💎 Ciclo {wins}/{WINS_PER_CYCLE} victorias\n"
        f"{wins_bar}"
    )
    await broadcast_signal(txt)   # sin parse_mode — texto plano con emojis


async def _check_trend_after_cycle():
    """Verifica la tendencia post-ciclo (sin broadcast automático)."""
    stats = get_quota_stats(200)
    if stats['total'] > 0 and not stats['favorable']:
        logger.info("⚠️ Post-ciclo: tendencia desfavorable — bot en espera")
    else:
        logger.info("✅ Post-ciclo: tendencia favorable — bot continúa analizando")


async def _dispatch_result(value: float, tipo: str, bet: float, attempt_num: int):
    """Broadcast del resultado al canal. La señal NO se borra."""
    global g_session, g_daily_wins, g_daily_losses, g_daily_cycles_won, g_daily_cycles_lost

    hora = argentina_time()

    # ── WIN parcial ───────────────────────────────────────────────────────────
    if tipo == 'win':
        g_daily_wins += 1
        wins = g_session.wins_in_cycle
        ents = g_session.entries_in_cycle
        wins_bar = '🟢' * wins + '⚪' * (WINS_PER_CYCLE - wins)
        txt = (
            f"✅ *GANAMOS*  `{value:.2f}x` — 🕐 {hora}\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"💎 Ciclo  {wins_bar}  {wins}/{WINS_PER_CYCLE}\n"
            f"🔰 Entradas usadas: {ents}/{MAX_COLS}"
        )
        await broadcast(txt, parse_mode='Markdown')
        await _broadcast_scoreboard()
        return

    # ── CYCLE WIN ─────────────────────────────────────────────────────────────
    if tipo == 'cycle_win':
        g_daily_wins       += 1
        g_daily_cycles_won += 1
        total_cyc = g_daily_cycles_won + g_daily_cycles_lost
        pct_cyc   = (g_daily_cycles_won / total_cyc * 100) if total_cyc > 0 else 0.0
        wins_bar  = '🟢' * WINS_PER_CYCLE
        txt = (
            f"✅ *GANAMOS*  `{value:.2f}x` — 🕐 {hora}\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"❤️ *¡CICLO COMPLETADO!*\n"
            f"{wins_bar}  {WINS_PER_CYCLE}/{WINS_PER_CYCLE} victorias\n"
            f"💎 Ciclos ganados hoy: {g_daily_cycles_won} — {pct_cyc:.0f}%\n"
            f"🔄 Nueva sesión iniciada"
        )
        await broadcast(txt, parse_mode='Markdown')
        await _broadcast_scoreboard()
        reset_global_session()
        await _check_trend_after_cycle()
        return

    # ── LOSS: ciclo continúa ──────────────────────────────────────────────────
    if tipo == 'new_col':
        g_daily_losses += 1
        wins = g_session.wins_in_cycle
        ents = g_session.entries_in_cycle
        col  = g_session.col
        ents_bar = '⚫' * (ents - 1) + '🔴' + '⚫' * (MAX_COLS - ents)
        wins_bar = '🟢' * wins + '⚪' * (WINS_PER_CYCLE - wins)
        txt = (
            f"❌ *PERDIMOS*  `{value:.2f}x` — 🕐 {hora}\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"🔰 {ents_bar}  {ents}/{MAX_COLS}\n"
            f"💎 Ciclo  {wins_bar}  {wins}/{WINS_PER_CYCLE}\n"
            f"➡️ Siguiente entrada: Col {col}"
        )
        await broadcast(txt, parse_mode='Markdown')
        await _broadcast_scoreboard()
        return

    # ── CYCLE LOSS ────────────────────────────────────────────────────────────
    if tipo == 'cycle_loss':
        g_daily_losses      += 1
        g_daily_cycles_lost += 1
        total_cyc = g_daily_cycles_won + g_daily_cycles_lost
        pct_cyc   = (g_daily_cycles_won / total_cyc * 100) if total_cyc > 0 else 0.0
        ents_bar  = '🔴' * MAX_COLS
        txt = (
            f"❌ *PERDIMOS*  `{value:.2f}x` — 🕐 {hora}\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"😭 *¡CICLO PERDIDO!*\n"
            f"{ents_bar}  {MAX_COLS}/{MAX_COLS} entradas\n"
            f"💎 Ciclos ganados hoy: {g_daily_cycles_won} — {pct_cyc:.0f}%\n"
            f"🔄 Nueva sesión iniciada"
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
        f"🔄 Gestión: `{MAX_COLS}` Entradas × `{WINS_PER_CYCLE}` Victorias/Ciclo\n"
        f"💵 Apuesta base fija: `${BASE_BET:.2f}`\n"
        f"🏆 Ciclo: `{WINS_PER_CYCLE}` victorias en `{MAX_COLS}` entradas\n"
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
