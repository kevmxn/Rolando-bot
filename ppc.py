#!/usr/bin/env python3
"""
Mega Roulette Bot — Key 204
Lógica: Docenas, Columnas, Call+PLE, Color (patrones), Rango
Sin gestión, sin estadísticas ML. 1 intento por señal.
"""

import asyncio
import logging
import os
import time
import urllib.request
import threading
from typing import Optional

import telebot
import websockets
import json
from flask import Flask, jsonify
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [MegaRoulette] %(levelname)s %(message)s')
logger = logging.getLogger("MegaRoulette")
for _ln in ['werkzeug', 'flask.app', 'flask', 'urllib3']:
    logging.getLogger(_ln).setLevel(logging.ERROR)

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
TOKEN   = "8932208184:AAHj_7XYSQQmZZQWrk6LJtJj4CkwuCxrNLI"
CHAT_ID = -1003610988961

_session = requests.Session()
_retry = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504],
               allowed_methods=["GET", "POST"], raise_on_status=False)
_session.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
_session.mount("http://",  HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
bot = telebot.TeleBot(TOKEN, threaded=False)
bot.session = _session

# ─── RULETA ───────────────────────────────────────────────────────────────────
ROULETTE_KEY  = 204
ROULETTE_NAME = "MEGA ROULETTE"
ROULETTE_URL  = "https://1win.lat/casino/play/v_pragmatic:megaroulette"

WS_URL    = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID = "ppcjd00000007254"

# ─── LÓGICA (portada desde logica.txt) ────────────────────────────────────────
RED    = {1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}
BLACK  = {2,4,6,8,10,11,13,15,17,20,22,24,26,28,29,31,33,35}
COL1   = {1,4,7,10,13,16,19,22,25,28,31,34}
COL2   = {2,5,8,11,14,17,20,23,26,29,32,35}
COL3   = {3,6,9,12,15,18,21,24,27,30,33,36}
CALLPLE_NUMS = [1, 3, 4, 6, 7, 9]

COLOR_PATTERNS = [
    {'p': ['N','N','N','R','N'],         'bet': 'Negro'},
    {'p': ['R','R','R','N','R'],         'bet': 'Rojo'},
    {'p': ['N','N','N','R','R','N','N'], 'bet': 'Rojo'},
    {'p': ['R','R','R','N','N','R','R'], 'bet': 'Negro'},
]

def _color(n):   return 'green' if n == 0 else ('red' if n in RED else 'black')
def _letter(n):  return 'X' if n == 0 else ('R' if n in RED else 'N')
def _dozen(n):   return 'X' if n == 0 else ('D1' if n <= 12 else ('D2' if n <= 24 else 'D3'))
def _column(n):  return 'X' if n == 0 else ('C1' if n in COL1 else ('C2' if n in COL2 else 'C3'))

def check_docenas(h):
    if len(h) < 2: return None
    a, b = _dozen(h[-1]), _dozen(h[-2])
    if a == 'X' or b == 'X' or a != b: return None
    bets = {'D1': 'D2 y D3', 'D2': 'D1 y D3', 'D3': 'D1 y D2'}
    return {'name': 'Docenas', 'bet': f"Apostar {bets[a]}", 'detail': f"{a} repetida"}

def check_columnas(h):
    if len(h) < 2: return None
    a, b = _column(h[-1]), _column(h[-2])
    if a == 'X' or b == 'X' or a != b: return None
    bets = {'C1': 'C2 y C3', 'C2': 'C1 y C3', 'C3': 'C1 y C2'}
    return {'name': 'Columnas', 'bet': f"Apostar {bets[a]}", 'detail': f"{a} repetida"}

def check_callple(h):
    if len(h) < 2: return None
    if _dozen(h[-1]) == 'D3' and _dozen(h[-2]) == 'D3':
        nums = ', '.join(str(n) for n in CALLPLE_NUMS)
        return {'name': 'Call + PLE', 'bet': f"Apostar: {nums}", 'detail': 'D3 repetida'}
    return None

def check_color(h):
    if len(h) < 5: return None
    letters = [_letter(n) for n in h]
    for pat in COLOR_PATTERNS:
        l = len(pat['p'])
        if len(h) < l: continue
        if letters[-l:] == pat['p']:
            return {
                'name': 'Color',
                'bet': f"Apostar {pat['bet']}",
                'detail': 'Patrón: ' + '-'.join(pat['p'])
            }
    return None

def check_rango(h):
    if len(h) < 2: return None
    a, b = _dozen(h[-1]), _dozen(h[-2])
    if a == 'X' or b == 'X' or a != b: return None
    if a == 'D3': return {'name': 'Rango', 'bet': 'Apostar 1-18', 'detail': 'D3 repetida'}
    if a == 'D1': return {'name': 'Rango', 'bet': 'Apostar 19-36', 'detail': 'D1 repetida'}
    return None

def run_all_checks(history):
    return [
        check_docenas(history),
        check_columnas(history),
        check_callple(history),
        check_color(history),
        check_rango(history),
    ]

# ─── MARCADOR DIARIO ──────────────────────────────────────────────────────────
class DailyScore:
    def __init__(self):
        self.wins   = 0
        self.losses = 0

    def add_win(self):  self.wins   += 1
    def add_loss(self): self.losses += 1

    def get_text(self):
        total = self.wins + self.losses
        pct   = (self.wins / total * 100) if total > 0 else 0.0
        return (
            f"📊 MARCADOR DIARIO:\n"
            f"✅ GANADAS: {self.wins}\n"
            f"❌ PERDIDAS: {self.losses}\n\n"
            f"📈 ACIERTOS = {pct:.1f}%"
        )

    def reset(self):
        self.wins   = 0
        self.losses = 0

SCORE = DailyScore()

# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
_TG_RETRIES = 12

def _tg_call(fn, *a, **kw):
    delay = 2.0
    for attempt in range(1, _TG_RETRIES + 1):
        try: return fn(*a, **kw)
        except Exception as e:
            err = str(e)
            if "retry after" in err.lower():
                try: wait = int(''.join(filter(str.isdigit, err))) + 1
                except: wait = 30
                time.sleep(wait); continue
            if attempt == _TG_RETRIES: return None
            time.sleep(delay); delay = min(delay * 2, 60)
    return None

def tg_send(text: str) -> Optional[int]:
    msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text, parse_mode="HTML")
    return msg.message_id if msg else None

def tg_send_with_button(text: str) -> Optional[int]:
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(telebot.types.InlineKeyboardButton("🎰 ACCEDER A LA RULETA", url=ROULETTE_URL))
    msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text,
                   parse_mode="HTML", reply_markup=markup,
                   disable_web_page_preview=True)
    return msg.message_id if msg else None

def tg_edit(message_id: int, text: str):
    try:
        _tg_call(bot.edit_message_text, text=text, chat_id=CHAT_ID,
                 message_id=message_id, parse_mode="HTML")
    except: pass

def tg_reply(reply_to_id: int, text: str) -> Optional[int]:
    """Envía un mensaje como respuesta (comentario) a otro mensaje."""
    msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text,
                   parse_mode="HTML", reply_to_message_id=reply_to_id)
    return msg.message_id if msg else None

def tg_delete(message_id: int):
    try: _tg_call(bot.delete_message, chat_id=CHAT_ID, message_id=message_id)
    except: pass

# ─── MOTOR DE SEÑALES ─────────────────────────────────────────────────────────
WARMUP_SPINS = 20

# Nombres de todas las estrategias (en orden fijo)
STRATEGY_NAMES = ['Docenas', 'Columnas', 'Call + PLE', 'Color', 'Rango']

CHECKERS = [
    check_docenas,
    check_columnas,
    check_callple,
    check_color,
    check_rango,
]

def _ball(n: int) -> str:
    c = _color(n)
    icons = {'red': '🔴', 'black': '⚫', 'green': '🟢'}
    return f"{icons.get(c, '⚪')}{n}"

def _evaluate(n: int, bet: str) -> bool:
    """Evalúa si el número n gana la apuesta dada."""
    if n == 0:
        return False
    bet = bet.lower()

    # Docenas
    if 'd2' in bet and 'd3' in bet and 'd1' not in bet:
        return _dozen(n) in ('D2', 'D3')
    if 'd1' in bet and 'd3' in bet and 'd2' not in bet:
        return _dozen(n) in ('D1', 'D3')
    if 'd1' in bet and 'd2' in bet and 'd3' not in bet:
        return _dozen(n) in ('D1', 'D2')

    # Columnas
    if 'c2' in bet and 'c3' in bet and 'c1' not in bet:
        return _column(n) in ('C2', 'C3')
    if 'c1' in bet and 'c3' in bet and 'c2' not in bet:
        return _column(n) in ('C1', 'C3')
    if 'c1' in bet and 'c2' in bet and 'c3' not in bet:
        return _column(n) in ('C1', 'C2')

    # Call + PLE
    if 'apostar:' in bet or '1, 3, 4' in bet:
        return n in CALLPLE_NUMS

    # Color
    if 'rojo' in bet:
        return n in RED
    if 'negro' in bet:
        return n in BLACK

    # Rango
    if '1-18' in bet:
        return 1 <= n <= 18
    if '19-36' in bet:
        return 19 <= n <= 36

    return False


class StrategySlot:
    """
    Representa UNA estrategia independiente.
    Ciclo: IDLE → WAITING (señal enviada, espera resultado) → IDLE
    """
    def __init__(self, name: str, checker):
        self.name    = name
        self.checker = checker
        self.waiting  = False   # True = señal activa esperando resultado
        self.sig      = None    # dict con bet/detail
        self.msg_id   = None    # message_id del mensaje en Telegram

    def try_detect(self, history: list) -> bool:
        """Intenta detectar señal. Retorna True si se activó."""
        if self.waiting:
            return False
        result = self.checker(history)
        if result is None:
            return False

        self.waiting = True
        self.sig     = result

        last5 = history[-5:][::-1]
        balls = '  '.join(_ball(x) for x in last5)
        text = (
            f"🎰 <b>{ROULETTE_NAME}</b>\n\n"
            f"🔔 <b>SEÑAL DETECTADA</b>\n\n"
            f"📌 <b>Estrategia:</b> {self.name}\n"
            f"🎯 <b>Apuesta:</b> {result['bet']}\n"
            f"📋 <b>Patrón:</b> {result['detail']}\n\n"
            f"🕐 Últimos números:\n{balls}\n\n"
            f"⚠️ <b>1 intento — sin gestión</b>"
        )
        self.msg_id = tg_send_with_button(text)
        logger.info(f"📡 [{self.name}] Señal enviada | {result['bet']}")
        return True

    def resolve(self, n: int) -> Optional[bool]:
        """Resuelve la señal activa con el número n. Retorna True/False o None si no había señal."""
        if not self.waiting:
            return None

        won = _evaluate(n, self.sig['bet'])
        if won:
            SCORE.add_win()
            emoji, label = "✅", "GANADA"
        else:
            SCORE.add_loss()
            emoji, label = "❌", "PERDIDA"

        # Texto del resultado (sin marcador)
        result_text = (
            f"🎰 <b>{ROULETTE_NAME}</b>\n\n"
            f"{emoji} <b>SEÑAL {label}</b>\n\n"
            f"📌 <b>Estrategia:</b> {self.name}\n"
            f"🎯 <b>Apuesta:</b> {self.sig['bet']}\n"
            f"🔢 <b>Número:</b> {_ball(n)}"
        )

        if self.msg_id:
            tg_reply(self.msg_id, result_text)          # responde como comentario al mensaje de señal
        else:
            tg_send(result_text)

        logger.info(f"{emoji} [{self.name}] Señal {label} | Número: {n}")

        # reset
        self.waiting = False
        self.sig     = None
        self.msg_id  = None
        return won


class SignalEngine:
    def __init__(self):
        self.history     = []
        self.warmup_done = False
        # Un slot independiente por cada estrategia
        self.slots = [StrategySlot(name, checker)
                      for name, checker in zip(STRATEGY_NAMES, CHECKERS)]

    def on_number(self, n: int):
        self.history.append(n)

        if not self.warmup_done:
            if len(self.history) >= WARMUP_SPINS:
                self.warmup_done = True
                logger.info(f"✅ Warmup completo ({WARMUP_SPINS} giros)")
            return

        # 1) Resolver primero todas las señales que estaban esperando
        resolved_any = False
        for slot in self.slots:
            if slot.waiting:
                result = slot.resolve(n)
                if result is not None:
                    resolved_any = True

        # Enviar marcador una sola vez después de resolver todas las señales del giro
        if resolved_any:
            tg_send(SCORE.get_text())

        # 2) Luego detectar nuevas señales con el historial actualizado
        for slot in self.slots:
            slot.try_detect(self.history)

    @property
    def active_signals(self):
        return [s for s in self.slots if s.waiting]


ENGINE = SignalEngine()

# ─── WEBSOCKET READER ─────────────────────────────────────────────────────────
async def ws_reader():
    reconnect_delay = 3
    seen_ids: set = set()

    def is_new(gid: str) -> bool:
        if gid in seen_ids: return False
        seen_ids.add(gid)
        if len(seen_ids) > 5000:
            oldest = list(seen_ids)[:1000]
            for x in oldest: seen_ids.discard(x)
        return True

    while True:
        try:
            sub_msg = json.dumps({
                "type": "subscribe",
                "casinoId": CASINO_ID,
                "key": str(ROULETTE_KEY),
            })
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=30) as ws:
                await ws.send(sub_msg)
                reconnect_delay = 3
                logger.info(f"[WS] Conectado — Mega Roulette key={ROULETTE_KEY}")

                async for raw in ws:
                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue

                    # carga inicial de historial
                    if isinstance(data, dict) and "results" in data:
                        results = data["results"]
                        if isinstance(results, list):
                            loaded = 0
                            for item in reversed(results):
                                gid = str(item.get("gameId", ""))
                                if not gid or not is_new(gid): continue
                                try:
                                    n = int(item.get("result", ""))
                                    if 0 <= n <= 36:
                                        ENGINE.history.append(n)
                                        loaded += 1
                                except (ValueError, TypeError): pass
                            if not ENGINE.warmup_done and len(ENGINE.history) >= WARMUP_SPINS:
                                ENGINE.warmup_done = True
                                logger.info(f"✅ Warmup por carga inicial ({loaded} giros)")
                            logger.info(f"[WS] Carga inicial: {loaded} giros | Total: {len(ENGINE.history)}")
                        continue

                    # nuevo giro en tiempo real
                    results = data.get("results") if isinstance(data, dict) else None
                    if results and isinstance(results, list) and len(results) > 0:
                        latest = results[0]
                        gid = str(latest.get("gameId", ""))
                        if not is_new(gid): continue
                        try:
                            n = int(latest.get("result", ""))
                            if 0 <= n <= 36:
                                logger.info(f"[WS] Nuevo número: {n}")
                                ENGINE.on_number(n)
                        except (ValueError, TypeError): pass
                        continue

                    # fallback
                    for key in ("result", "number", "outcome", "winningNumber"):
                        if key in data:
                            gid = str(data.get("gameId", f"{ROULETTE_KEY}_{data[key]}_{int(time.time())}"))
                            if not is_new(gid): break
                            try:
                                n = int(data[key])
                                if 0 <= n <= 36:
                                    logger.info(f"[WS-fallback] Nuevo número: {n}")
                                    ENGINE.on_number(n)
                            except (ValueError, TypeError): pass
                            break

        except Exception as e:
            logger.warning(f"[WS] Desconectado: {e}. Reconectando en {reconnect_delay}s")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)


# ─── FLASK ────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return jsonify({"status": "ok", "bot": "Mega Roulette Bot — key 204"})

@flask_app.route("/ping")
def ping():
    return jsonify({"status": "pong", "ts": time.time()})

@flask_app.route("/health")
def health():
    active = ENGINE.active_signals
    return jsonify({
        "roulette":       ROULETTE_NAME,
        "warmup":         ENGINE.warmup_done,
        "history_len":    len(ENGINE.history),
        "signals_active": len(active),
        "active_signals": [{"name": s.name, "bet": s.sig["bet"]} for s in active],
        "score":          {"wins": SCORE.wins, "losses": SCORE.losses},
    })

async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url: return
    await asyncio.sleep(30)
    while True:
        try: urllib.request.urlopen(f"{url}/ping", timeout=15)
        except: pass
        await asyncio.sleep(240)

# ─── RESET DIARIO DEL MARCADOR ────────────────────────────────────────────────
async def daily_reset_loop():
    import datetime
    while True:
        now_utc = datetime.datetime.utcnow()
        now_arg = now_utc + datetime.timedelta(hours=-3)
        target  = now_arg.replace(hour=0, minute=0, second=0, microsecond=0) \
                  + datetime.timedelta(days=1)
        wait    = (target - now_arg).total_seconds()
        logger.info(f"[Score] Reset diario en {wait/3600:.1f}h")
        await asyncio.sleep(wait)
        SCORE.reset()
        logger.info("[Score] Marcador reiniciado para el nuevo día")

# ─── BOT COMMANDS ─────────────────────────────────────────────────────────────
@bot.message_handler(commands=['start', 'help'])
def cmd_start(m):
    bot.reply_to(m,
        "<b>🎰 Mega Roulette Bot</b>\n\n"
        "Ruleta: MEGA ROULETTE (key 204)\n"
        "Señales: Docenas · Columnas · Call+PLE · Color · Rango\n"
        "Sin gestión · 1 intento por señal\n\n"
        "/status — Estado actual\n"
        "/score — Marcador diario",
        parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(m):
    active = ENGINE.active_signals
    if active:
        lines = "\n".join(f"📡 <b>{s.name}</b> — {s.sig['bet']}" for s in active)
        estado = f"Señales activas ({len(active)}):\n{lines}"
    else:
        estado = "⚪ Sin señales activas"
    bot.reply_to(m,
        f"🎰 <b>{ROULETTE_NAME}</b>\n"
        f"Warmup: {'✅' if ENGINE.warmup_done else '⏳'}\n"
        f"Giros vistos: {len(ENGINE.history)}\n"
        f"{estado}",
        parse_mode="HTML")

@bot.message_handler(commands=['score'])
def cmd_score(m):
    bot.reply_to(m, SCORE.get_text(), parse_mode="HTML")

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"[Flask] Iniciando en 0.0.0.0:{port}")
    flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main():
    threading.Thread(target=run_flask, daemon=True).start()
    logger.info("[Main] ✅ Flask thread iniciado")

    threading.Thread(
        target=lambda: bot.polling(none_stop=True, interval=1, timeout=30),
        daemon=True
    ).start()
    logger.info("[Main] ✅ Telegram bot thread iniciado")

    await asyncio.sleep(1)
    logger.info(f"[Main] 🎰 Bot iniciado — {ROULETTE_NAME} key={ROULETTE_KEY}")

    await asyncio.gather(
        ws_reader(),
        daily_reset_loop(),
        self_ping_loop(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("[Main] 🛑 Bot detenido")
    except Exception as e:
        logger.error(f"[Main] 💥 Error fatal: {e}")
        raise
