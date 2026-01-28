import os
import json
import time
import asyncio
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import requests
import hmac, hashlib, urllib.parse

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes


# =========================
# ENV
# =========================
BINANCE_API_KEY = (os.getenv("BINANCE_API_KEY") or "").strip()
BINANCE_API_SECRET = (os.getenv("BINANCE_API_SECRET") or "").strip()

TELEGRAM_TOKEN = (os.getenv("TELEGRAM_TOKEN") or "").strip()
ALLOWED_USER_ID = int((os.getenv("ALLOWED_USER_ID") or "0").strip() or "0")

# =========================
# Binance endpoints (fallback)
# =========================
BINANCE_BASES = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
]
SESSION = requests.Session()


# =========================
# SETTINGS
# =========================
INTERVAL = os.getenv("INTERVAL", "3m")
QUOTE_ASSET = (os.getenv("QUOTE_ASSET") or "USDT").strip().upper()

KLINE_LIMIT = int(os.getenv("KLINE_LIMIT", "200"))
TOP_REFRESH_SEC = int(os.getenv("TOP_REFRESH_SEC", "180"))

ENTRY_SCAN_SEC = float(os.getenv("ENTRY_SCAN_SEC", "12"))
POS_POLL_SEC = float(os.getenv("POS_POLL_SEC", "1"))

SYMBOL_MODE = (os.getenv("SYMBOL_MODE") or "TOP10").strip().upper()  # TOP10 or LIST
SYMBOLS = [s.strip().upper() for s in (os.getenv("SYMBOLS") or "").split(",") if s.strip()]

SIZE_MODE = (os.getenv("SIZE_MODE") or "USDT").strip().upper()  # USDT or RISK
DEFAULT_USDT = float(os.getenv("DEFAULT_USDT") or "10")
DEFAULT_RISK = float(os.getenv("DEFAULT_RISK") or "0.10")

MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "1"))

SL_OFFSET = float(os.getenv("SL_OFFSET") or "0.001")      # 0.1% = 0.001
BE_TRIGGER = float(os.getenv("BE_TRIGGER") or "0.0")

MIN_GREEN_RUN = int(os.getenv("MIN_GREEN_RUN", "2"))
MIN_PULLBACK_REDS = int(os.getenv("MIN_PULLBACK_REDS", "1"))

APPROVE_TIMEOUT_SEC = int(os.getenv("APPROVE_TIMEOUT_SEC") or "120")
MAX_PENDING = int(os.getenv("MAX_PENDING") or "25")

# Optional state snapshot (not required, but useful)
STATE_FILE = os.getenv("STATE_FILE") or "state.json"
STATE_SNAPSHOT_SEC = int(os.getenv("STATE_SNAPSHOT_SEC") or "60")  # set 0 to disable


# =========================
# Utils
# =========================
def now_ts() -> int:
    return int(time.time())

def safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def fmt(p: float) -> str:
    if p >= 1:
        return f"{p:.6f}".rstrip("0").rstrip(".")
    return f"{p:.10f}".rstrip("0").rstrip(".")

def http_get_json(path: str, params: Optional[dict] = None, timeout: int = 12):
    last_err = None
    for base in BINANCE_BASES:
        try:
            r = SESSION.get(base + path, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
    raise RuntimeError(f"All Binance endpoints failed: {path}. Last error: {last_err}")


# =========================
# In-memory STATE (NO RACE)
# =========================
STATE_LOCK = asyncio.Lock()

def _default_state() -> dict:
    return {
        "symbols": {},
        "top10": [],
        "last_top_refresh": 0,
        "pending": {},    # oid -> payload
        "positions": {},  # symbol -> pos
        "prefs": {
            "size_mode": SIZE_MODE,
            "default_usdt": DEFAULT_USDT,
            "default_risk": DEFAULT_RISK,
        }
    }

STATE: dict = _default_state()

def _load_state_file_once():
    global STATE
    if not STATE_FILE:
        return
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # merge carefully
        base = _default_state()
        for k in base.keys():
            if k in data:
                base[k] = data[k]
        # ensure prefs keys
        base.setdefault("prefs", {})
        base["prefs"].setdefault("size_mode", SIZE_MODE)
        base["prefs"].setdefault("default_usdt", DEFAULT_USDT)
        base["prefs"].setdefault("default_risk", DEFAULT_RISK)
        STATE = base
    except Exception:
        STATE = _default_state()

def _save_state_file_snapshot():
    if not STATE_FILE:
        return
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(STATE, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)

def get_prefs(state: dict) -> dict:
    state.setdefault("prefs", {})
    p = state["prefs"]
    p.setdefault("size_mode", SIZE_MODE)
    p.setdefault("default_usdt", DEFAULT_USDT)
    p.setdefault("default_risk", DEFAULT_RISK)
    return p


# =========================
# Candle helpers
# =========================
def parse_klines(raw: list) -> List[dict]:
    out = []
    for k in raw:
        out.append({
            "open_time": int(k[0]),
            "open": safe_float(k[1]),
            "high": safe_float(k[2]),
            "low": safe_float(k[3]),
            "close": safe_float(k[4]),
            "close_time": int(k[6]),
        })
    return out

def is_green(c: dict) -> bool:
    return c["close"] > c["open"]

def is_red(c: dict) -> bool:
    return c["close"] < c["open"]


# =========================
# Binance public (sync)
# =========================
def get_exchange_info_symbols_usdt() -> set:
    data = http_get_json("/api/v3/exchangeInfo")
    ok = set()
    for s in data.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        sym = s.get("symbol", "")
        if sym.endswith("USDT"):
            ok.add(sym)
    return ok

def get_top10_gainers_usdt(tradable_usdt: set) -> List[str]:
    tickers = http_get_json("/api/v3/ticker/24hr")
    rows = []
    for t in tickers:
        sym = t.get("symbol", "")
        if sym not in tradable_usdt:
            continue
        if sym.endswith(("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue
        p = safe_float(t.get("priceChangePercent", 0))
        rows.append((sym, p))
    rows.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in rows[:10]]

def get_klines(symbol: str, interval: str, limit: int) -> List[dict]:
    data = http_get_json("/api/v3/klines", params={"symbol": symbol, "interval": interval, "limit": limit})
    return parse_klines(data)

def get_spot_price(symbol: str) -> float:
    data = http_get_json("/api/v3/ticker/price", params={"symbol": symbol})
    return safe_float(data.get("price", 0))

# async wrappers (non-blocking)
async def a_get_exchange_info_symbols_usdt():
    return await asyncio.to_thread(get_exchange_info_symbols_usdt)

async def a_get_top10_gainers_usdt(tradable_usdt: set):
    return await asyncio.to_thread(get_top10_gainers_usdt, tradable_usdt)

async def a_get_klines(symbol: str, interval: str, limit: int):
    return await asyncio.to_thread(get_klines, symbol, interval, limit)

async def a_get_spot_price(symbol: str):
    return await asyncio.to_thread(get_spot_price, symbol)


# =========================
# Binance signed (sync)
# =========================
def sign_query(params: Dict) -> str:
    qs = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(BINANCE_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig

def signed_request(method: str, path: str, params: Dict):
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET env yo'q")

    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000)
    qs = sign_query(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    last_err = None
    for base in BINANCE_BASES:
        url = base + path + "?" + qs
        try:
            r = SESSION.request(method, url, headers=headers, timeout=12)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Signed request failed: {path}. Last error: {last_err}")

def get_account_info_signed() -> dict:
    return signed_request("GET", "/api/v3/account", {})

def get_free_balance(asset: str) -> float:
    info = get_account_info_signed()
    for b in info.get("balances", []):
        if b.get("asset") == asset:
            return safe_float(b.get("free"))
    return 0.0

def get_symbol_info(symbol: str) -> dict:
    data = http_get_json("/api/v3/exchangeInfo", params={"symbol": symbol})
    syms = data.get("symbols", [])
    return syms[0] if syms else {}

def lot_step_for_symbol(symbol_info: dict) -> Tuple[float, float]:
    min_qty, step = 0.0, 0.0
    for f in symbol_info.get("filters", []):
        if f.get("filterType") == "LOT_SIZE":
            min_qty = safe_float(f.get("minQty"))
            step = safe_float(f.get("stepSize"))
            break
    return min_qty, step

def round_step(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    return (qty // step) * step

def compute_qty(symbol: str, price: float, size_mode: str, usdt_amount: float, risk_pct: float) -> float:
    if size_mode == "USDT":
        spend = usdt_amount
    else:
        usdt_free = get_free_balance(QUOTE_ASSET)
        spend = usdt_free * risk_pct

    if spend <= 0:
        raise RuntimeError(f"{QUOTE_ASSET} amount too small: {spend}")

    raw_qty = spend / price
    info = get_symbol_info(symbol)
    min_qty, step = lot_step_for_symbol(info)
    qty = round_step(raw_qty, step)
    if qty < min_qty:
        raise RuntimeError(f"Qty < minQty. qty={qty} minQty={min_qty}")
    return float(qty)

def place_market_order(symbol: str, side: str, quantity: float) -> dict:
    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": f"{quantity:.18f}".rstrip("0").rstrip("."),
        "newOrderRespType": "FULL",
    }
    return signed_request("POST", "/api/v3/order", params)

# async wrappers
async def a_compute_qty(symbol: str, price: float, size_mode: str, usdt_amount: float, risk_pct: float):
    return await asyncio.to_thread(compute_qty, symbol, price, size_mode, usdt_amount, risk_pct)

async def a_place_market_order(symbol: str, side: str, qty: float):
    return await asyncio.to_thread(place_market_order, symbol, side, qty)


# =========================
# Telegram helpers
# =========================
async def tg_send(app: Application, text: str):
    try:
        await app.bot.send_message(chat_id=ALLOWED_USER_ID, text=text)
    except Exception:
        pass

def make_order_id() -> str:
    return f"o{int(time.time()*1000)}"

def build_keyboard(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ BUY (default)", callback_data=f"APPROVE|{order_id}|DEF"),
            InlineKeyboardButton("❌ Reject", callback_data=f"REJECT|{order_id}"),
        ],
        [
            InlineKeyboardButton("BUY 10 USDT", callback_data=f"APPROVE|{order_id}|USDT:10"),
            InlineKeyboardButton("BUY 20 USDT", callback_data=f"APPROVE|{order_id}|USDT:20"),
            InlineKeyboardButton("BUY 50 USDT", callback_data=f"APPROVE|{order_id}|USDT:50"),
        ],
    ])

async def send_approval(app: Application, payload: dict):
    # pending state is protected by lock -> never disappears
    async with STATE_LOCK:
        pending = STATE.get("pending", {}) or {}
        if len(pending) >= MAX_PENDING:
            return
        oid = make_order_id()
        pending[oid] = payload
        STATE["pending"] = pending

    txt = (
        f"📣 BUY setup (ruxsat kerak)\n"
        f"{payload['symbol']}\n"
        f"TF: {payload['interval']}\n"
        f"BUY trigger (wick): price > last closed pullback candle HIGH\n"
        f"Break HIGH: {fmt(payload['break_high'])}\n"
        f"Price: {fmt(payload['price'])}\n"
        f"SL: {fmt(payload['sl'])}\n"
        f"(Timeout: {APPROVE_TIMEOUT_SEC}s)\n"
    )
    await app.bot.send_message(chat_id=ALLOWED_USER_ID, text=txt, reply_markup=build_keyboard(oid))


# =========================
# Strategy state (your logic)
# =========================
@dataclass
class SymbolState:
    stage: str = "SEEK_MAKEHIGH_START"
    green_run_count: int = 0
    turn_red_low: float = 0.0
    turn_red_close_time: int = 0
    pullback_red_count: int = 0
    last_pullback_close_time: int = 0
    last_pullback_high: float = 0.0
    last_pullback_low: float = 0.0
    last_signal: str = ""

def ensure_symbol_state(symbol: str) -> SymbolState:
    s = (STATE.get("symbols", {}) or {}).get(symbol)
    if not s:
        st = SymbolState()
        STATE.setdefault("symbols", {})[symbol] = asdict(st)
        return st
    st = SymbolState(**{**asdict(SymbolState()), **s})
    STATE.setdefault("symbols", {})[symbol] = asdict(st)
    return st

def set_symbol_state(symbol: str, st: SymbolState) -> None:
    STATE.setdefault("symbols", {})[symbol] = asdict(st)

def reset_symbol(symbol: str) -> None:
    STATE.setdefault("symbols", {})[symbol] = asdict(SymbolState())

def analyze_buy_setup(symbol: str, st: SymbolState, klines: List[dict], price: float) -> Tuple[SymbolState, Optional[dict]]:
    if len(klines) < 6:
        return st, None

    last_closed = klines[-2]
    prev_closed = klines[-3]

    def sig_key(kind: str, t: int) -> str:
        return f"{kind}:{t}"

    if st.stage == "WAIT_APPROVAL":
        return st, None

    if st.stage == "SEEK_MAKEHIGH_START":
        st.green_run_count = 0
        st.pullback_red_count = 0
        st.turn_red_low = 0.0
        if is_red(prev_closed) and is_green(last_closed):
            st.stage = "GREEN_RUN"
            st.green_run_count = 1
        return st, None

    if st.stage == "GREEN_RUN":
        if is_green(last_closed):
            st.green_run_count += 1
            return st, None

        if is_red(last_closed):
            if st.green_run_count < MIN_GREEN_RUN:
                return SymbolState(), None
            st.turn_red_low = float(last_closed["low"])
            st.turn_red_close_time = int(last_closed["close_time"])
            st.stage = "WAIT_PULLBACK_BREAK"
        return st, None

    if st.stage == "WAIT_PULLBACK_BREAK":
        if st.turn_red_low > 0 and price < st.turn_red_low:
            st.stage = "PULLBACK_BEARISH"
            st.pullback_red_count = 0
            st.last_pullback_close_time = 0
            st.last_pullback_high = 0.0
            st.last_pullback_low = 0.0
            if is_red(last_closed):
                st.pullback_red_count = 1
                st.last_pullback_close_time = int(last_closed["close_time"])
                st.last_pullback_high = float(last_closed["high"])
                st.last_pullback_low = float(last_closed["low"])
        return st, None

    if st.stage == "PULLBACK_BEARISH":
        if is_red(last_closed):
            st.pullback_red_count += 1
            st.last_pullback_close_time = int(last_closed["close_time"])
            st.last_pullback_high = float(last_closed["high"])
            st.last_pullback_low = float(last_closed["low"])
            return st, None

        if is_green(last_closed):
            if st.pullback_red_count < MIN_PULLBACK_REDS or st.last_pullback_high <= 0:
                return SymbolState(), None
            st.stage = "WAIT_BUY_BREAK"
        return st, None

    if st.stage == "WAIT_BUY_BREAK":
        if is_red(last_closed):
            st.stage = "PULLBACK_BEARISH"
            st.pullback_red_count = max(st.pullback_red_count, 1)
            st.last_pullback_close_time = int(last_closed["close_time"])
            st.last_pullback_high = float(last_closed["high"])
            st.last_pullback_low = float(last_closed["low"])
            return st, None

        if st.last_pullback_high > 0 and price > st.last_pullback_high:
            key = sig_key("BUY", st.last_pullback_close_time or int(last_closed["close_time"]))
            if st.last_signal == key:
                return st, None
            st.last_signal = key

            sl = st.last_pullback_low * (1.0 - SL_OFFSET)
            payload = {
                "symbol": symbol,
                "price": price,
                "break_high": st.last_pullback_high,
                "sl": sl,
                "ts": now_ts(),
                "interval": INTERVAL,
            }
            st.stage = "WAIT_APPROVAL"
            return st, payload

    return st, None


# =========================
# Exit logic (SELL auto)
# =========================
async def update_trailing_low_from_last_closed_green(symbol: str, pos: dict) -> dict:
    try:
        kl = await a_get_klines(symbol, INTERVAL, 20)
        last_closed = kl[-2]
        ct = int(last_closed["close_time"])
        if is_green(last_closed) and ct != int(pos.get("last_green_close_time", 0)):
            pos["last_green_close_time"] = ct
            pos["trail_low"] = float(last_closed["low"])
    except Exception:
        pass
    return pos

async def do_sell_and_notify(app: Application, sym: str, qty: float, text: str):
    try:
        await a_place_market_order(sym, "SELL", qty)
        await tg_send(app, text)
    except Exception as e:
        await tg_send(app, f"⚠️ SELL FAILED {sym}: {e}")

async def monitor_positions(app: Application):
    while True:
        async with STATE_LOCK:
            positions = dict(STATE.get("positions", {}) or {})

        if not positions:
            await asyncio.sleep(POS_POLL_SEC)
            continue

        changed = False
        new_positions = dict(positions)

        for sym, pos in list(positions.items()):
            try:
                price = await a_get_spot_price(sym)

                entry = float(pos.get("entry", 0.0))
                sl = float(pos.get("sl", 0.0))

                # Break-even optional
                if BE_TRIGGER > 0 and entry > 0 and (not pos.get("be_on", False)) and price >= entry * (1.0 + BE_TRIGGER):
                    pos["sl"] = entry
                    pos["be_on"] = True
                    changed = True
                    await tg_send(app, f"🟡 BE ON\n{sym}\nSL -> entry {fmt(entry)}")

                # trail update occasionally
                if (now_ts() - int(pos.get("last_trail_update_ts", 0))) >= max(int(ENTRY_SCAN_SEC), 8):
                    pos["last_trail_update_ts"] = now_ts()
                    pos = await update_trailing_low_from_last_closed_green(sym, pos)
                    new_positions[sym] = pos
                    changed = True

                trail_low = float(pos.get("trail_low", 0.0))

                # SL
                if sl > 0 and price <= sl:
                    qty = float(pos.get("qty", 0.0))
                    text = f"🟥 SL SELL\n{sym}\nPrice: {fmt(price)}\nSL: {fmt(sl)}"
                    asyncio.create_task(do_sell_and_notify(app, sym, qty, text))
                    new_positions.pop(sym, None)
                    changed = True
                    continue

                # SELL on last green low break
                if trail_low > 0 and price < trail_low:
                    qty = float(pos.get("qty", 0.0))
                    text = f"🟩 SELL (LOW break)\n{sym}\nPrice: {fmt(price)}\nLast green LOW: {fmt(trail_low)}"
                    asyncio.create_task(do_sell_and_notify(app, sym, qty, text))
                    new_positions.pop(sym, None)
                    changed = True
                    continue

            except Exception as e:
                await tg_send(app, f"⚠️ position monitor error {sym}: {e}")

        if changed:
            async with STATE_LOCK:
                STATE["positions"] = new_positions

        await asyncio.sleep(POS_POLL_SEC)


# =========================
# Entry scanner
# =========================
async def entry_scanner(app: Application):
    tradable_usdt = set()
    try:
        tradable_usdt = await a_get_exchange_info_symbols_usdt()
    except Exception as e:
        await tg_send(app, f"⚠️ exchangeInfo error: {e}")

    while True:
        try:
            # refresh top10 if needed
            if SYMBOL_MODE == "TOP10":
                async with STATE_LOCK:
                    top10 = list(STATE.get("top10", []) or [])
                    last_refresh = int(STATE.get("last_top_refresh", 0))

                if (now_ts() - last_refresh >= TOP_REFRESH_SEC) or (not top10):
                    top10_new = await a_get_top10_gainers_usdt(tradable_usdt)
                    async with STATE_LOCK:
                        STATE["top10"] = top10_new
                        STATE["last_top_refresh"] = now_ts()
                        # cleanup symbol states not in top10
                        syms = STATE.get("symbols", {}) or {}
                        for s in list(syms.keys()):
                            if s not in top10_new:
                                syms.pop(s, None)
                        STATE["symbols"] = syms
                    symbols = top10_new
                else:
                    symbols = top10
            else:
                symbols = SYMBOLS[:]

            for sym in symbols:
                async with STATE_LOCK:
                    positions = STATE.get("positions", {}) or {}
                    if sym in positions:
                        continue
                    if len(positions) >= MAX_OPEN_POSITIONS:
                        break
                    st_sym = ensure_symbol_state(sym)

                kl = await a_get_klines(sym, INTERVAL, KLINE_LIMIT)
                price = await a_get_spot_price(sym)

                st_sym, payload = analyze_buy_setup(sym, st_sym, kl, price)

                async with STATE_LOCK:
                    set_symbol_state(sym, st_sym)

                if payload:
                    await send_approval(app, payload)

        except Exception as e:
            await tg_send(app, f"⚠️ entry scan error: {e}")

        await asyncio.sleep(ENTRY_SCAN_SEC)


# =========================
# Snapshot task (optional)
# =========================
async def snapshot_state_task():
    if STATE_SNAPSHOT_SEC <= 0:
        return
    while True:
        try:
            async with STATE_LOCK:
                _save_state_file_snapshot()
        except Exception:
            pass
        await asyncio.sleep(max(STATE_SNAPSHOT_SEC, 10))


# =========================
# Telegram commands + callbacks
# =========================
def is_allowed(update: Update) -> bool:
    return (not ALLOWED_USER_ID) or (update.effective_user and update.effective_user.id == ALLOWED_USER_ID)

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await update.message.reply_text(
        "✅ Bot ishga tushdi.\n"
        "BUY: ruxsat bilan (yopilgan sham HIGH wick break)\n"
        "SELL: avtomat (yopilgan yashil sham LOW wick break)\n"
        f"TF={INTERVAL}\n"
        f"MIN_GREEN_RUN={MIN_GREEN_RUN}, MIN_PULLBACK_REDS={MIN_PULLBACK_REDS}\n"
        f"Timeout={APPROVE_TIMEOUT_SEC}s\n"
    )

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    async with STATE_LOCK:
        prefs = get_prefs(STATE)
        pending_n = len(STATE.get("pending", {}) or {})
        pos_n = len(STATE.get("positions", {}) or {})
        top10 = STATE.get("top10", []) or []

    msg = (
        f"TF: {INTERVAL}\n"
        f"ENTRY_SCAN_SEC: {ENTRY_SCAN_SEC}\n"
        f"POS_POLL_SEC: {POS_POLL_SEC}\n"
        f"Size mode: {prefs['size_mode']}\n"
        f"Default USDT: {prefs['default_usdt']}\n"
        f"Default risk: {prefs['default_risk']}\n"
        f"MAX_OPEN_POSITIONS: {MAX_OPEN_POSITIONS}\n"
        f"Pending: {pending_n}\n"
        f"Positions: {pos_n}\n"
        f"Top10: {', '.join(top10[:10])}\n"
    )
    await update.message.reply_text(msg)

async def do_buy_and_reply(app: Application, q, oid: str, mode_arg: str):
    # take payload atomically and reserve spot (prevents double click / overwrite)
    async with STATE_LOCK:
        pending = STATE.get("pending", {}) or {}
        p = pending.get(oid)
        if not p:
            try:
                await q.edit_message_text("⚠️ Eskirgan yoki topilmadi.")
            except Exception:
                pass
            return

        # timeout check
        if now_ts() - int(p.get("ts", 0)) > APPROVE_TIMEOUT_SEC:
            pending.pop(oid, None)
            STATE["pending"] = pending
            reset_symbol(p.get("symbol", ""))
            try:
                await q.edit_message_text("⌛ Timeout. Bekor bo‘ldi.")
            except Exception:
                pass
            return

        # limits
        positions = STATE.get("positions", {}) or {}
        if len(positions) >= MAX_OPEN_POSITIONS:
            pending.pop(oid, None)
            STATE["pending"] = pending
            reset_symbol(p.get("symbol", ""))
            try:
                await q.edit_message_text(f"⚠️ Limit: MAX_OPEN_POSITIONS={MAX_OPEN_POSITIONS}.")
            except Exception:
                pass
            return

        symbol = p["symbol"]
        if symbol in positions:
            pending.pop(oid, None)
            STATE["pending"] = pending
            reset_symbol(symbol)
            try:
                await q.edit_message_text("⚠️ Bu coinda allaqachon pozitsiya bor.")
            except Exception:
                pass
            return

        prefs = get_prefs(STATE)
        size_mode = prefs["size_mode"]
        usdt_amt = float(prefs["default_usdt"])
        risk_pct = float(prefs["default_risk"])
        if mode_arg.startswith("USDT:"):
            size_mode = "USDT"
            usdt_amt = safe_float(mode_arg.split(":", 1)[1]) or usdt_amt

        sl = float(p["sl"])

        # IMPORTANT: keep pending until success/fail (no loss)

    # outside lock -> do network calls
    try:
        price = await a_get_spot_price(symbol)
        qty = await a_compute_qty(symbol, price, size_mode, usdt_amt, risk_pct)
        await a_place_market_order(symbol, "BUY", qty)

        # commit state after success
        async with STATE_LOCK:
            positions = STATE.get("positions", {}) or {}
            positions[symbol] = {
                "qty": qty,
                "entry": price,
                "sl": sl,
                "be_on": False,
                "trail_low": 0.0,
                "last_green_close_time": 0,
                "last_trail_update_ts": 0,
                "opened_ts": now_ts(),
            }
            STATE["positions"] = positions
            # remove pending
            pending = STATE.get("pending", {}) or {}
            pending.pop(oid, None)
            STATE["pending"] = pending
            reset_symbol(symbol)

        try:
            await q.edit_message_text(
                f"✅ BUY EXECUTED\n{symbol}\n"
                f"Qty: {qty}\n"
                f"Entry: {fmt(price)}\n"
                f"SL: {fmt(sl)}\n"
                f"SELL: last closed green LOW wick break\n"
            )
        except Exception:
            pass

    except Exception as e:
        # remove pending (so it doesn't clog), reset symbol state
        async with STATE_LOCK:
            pending = STATE.get("pending", {}) or {}
            p2 = pending.pop(oid, None)
            STATE["pending"] = pending
            if p2 and p2.get("symbol"):
                reset_symbol(p2["symbol"])
        try:
            await q.edit_message_text(f"⚠️ BUY FAILED: {e}")
        except Exception:
            pass

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return

    # ACK immediately (prevents Telegram timeout)
    try:
        await q.answer("⏳ Qabul qilindi...")
    except Exception:
        try:
            await q.answer()
        except Exception:
            pass

    if ALLOWED_USER_ID and q.from_user and q.from_user.id != ALLOWED_USER_ID:
        try:
            await q.edit_message_text("⛔ Ruxsat yo‘q.")
        except Exception:
            pass
        return

    parts = (q.data or "").split("|")
    if not parts:
        return

    action = parts[0]
    oid = parts[1] if len(parts) > 1 else ""
    mode_arg = parts[2] if len(parts) > 2 else "DEF"

    if action == "REJECT":
        async with STATE_LOCK:
            pending = STATE.get("pending", {}) or {}
            payload = pending.pop(oid, None)
            STATE["pending"] = pending
            if payload and payload.get("symbol"):
                reset_symbol(payload["symbol"])
        try:
            await q.edit_message_text("❌ Bekor qilindi.")
        except Exception:
            pass
        return

    if action == "APPROVE":
        # quick UI update
        try:
            await q.edit_message_text("⏳ Order yuborilyapti...")
        except Exception:
            pass
        # run in background
        context.application.create_task(do_buy_and_reply(context.application, q, oid, mode_arg))
        return


# =========================
# Main
# =========================
def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN env yo'q")
    if not ALLOWED_USER_ID:
        raise RuntimeError("ALLOWED_USER_ID env yo'q")
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET env yo'q")

    _load_state_file_once()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CallbackQueryHandler(callback_handler))

    async def post_init(app_: Application):
        app_.create_task(entry_scanner(app_))
        app_.create_task(monitor_positions(app_))
        app_.create_task(snapshot_state_task())
        await tg_send(app_, f"✅ START | TF={INTERVAL} | BUY=approval | SELL=auto | RAM state (no 'Eskirgan')")

    app.post_init = post_init
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
