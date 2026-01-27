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

# Faqat shu user tasdiqlay oladi (sening Telegram ID)
ALLOWED_USER_ID = int((os.getenv("ALLOWED_USER_ID") or "0").strip() or "0")

# Binance SPOT endpoint fallback
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
SCAN_EVERY_SEC = int(os.getenv("SCAN_EVERY_SEC", "10"))
TOP_REFRESH_SEC = int(os.getenv("TOP_REFRESH_SEC", "180"))
KLINE_LIMIT = int(os.getenv("KLINE_LIMIT", "200"))

SYMBOL_MODE = (os.getenv("SYMBOL_MODE") or "TOP10").strip().upper()  # TOP10 or LIST
SYMBOLS = [s.strip().upper() for s in (os.getenv("SYMBOLS") or "").split(",") if s.strip()]

STATE_FILE = os.getenv("STATE_FILE") or "state.json"

QUOTE_ASSET = (os.getenv("QUOTE_ASSET") or "USDT").strip().upper()

# Default order sizing mode:
# SIZE_MODE = "USDT" or "RISK"
SIZE_MODE = (os.getenv("SIZE_MODE") or "USDT").strip().upper()
DEFAULT_USDT = float(os.getenv("DEFAULT_USDT") or "20")      # masalan 20 USDT
DEFAULT_RISK = float(os.getenv("DEFAULT_RISK") or "0.02")    # masalan balans 2%

# Stoploss offset (0.1% = 0.001)
SL_OFFSET = float(os.getenv("SL_OFFSET") or "0.002")  # 0.2% default (3m uchun yaxshiroq)

# Buy tasdiqlash timeout (sekund)
APPROVE_TIMEOUT_SEC = int(os.getenv("APPROVE_TIMEOUT_SEC") or "120")

# Pending orderlarda safety
MAX_PENDING = int(os.getenv("MAX_PENDING") or "20")


# =========================
# State
# =========================
def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {
            "symbols": {},
            "top10": [],
            "last_top_refresh": 0,
            "pending": {},   # order_id -> payload
            "prefs": {
                "size_mode": SIZE_MODE,
                "default_usdt": DEFAULT_USDT,
                "default_risk": DEFAULT_RISK,
            }
        }
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "symbols": {},
            "top10": [],
            "last_top_refresh": 0,
            "pending": {},
            "prefs": {"size_mode": SIZE_MODE, "default_usdt": DEFAULT_USDT, "default_risk": DEFAULT_RISK}
        }

def save_state(state: dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)

def now_ts() -> int:
    return int(time.time())

def safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def http_get_json(path: str, params: Optional[dict] = None, timeout: int = 12):
    last_err = None
    for base in BINANCE_BASES:
        url = base + path
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"All Binance endpoints failed: {path}. Last error: {last_err}")


# =========================
# Binance (public)
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
    out = []
    for k in data:
        out.append({
            "open_time": int(k[0]),
            "open": safe_float(k[1]),
            "high": safe_float(k[2]),
            "low": safe_float(k[3]),
            "close": safe_float(k[4]),
            "close_time": int(k[6]),
        })
    return out

def get_spot_price(symbol: str) -> float:
    data = http_get_json("/api/v3/ticker/price", params={"symbol": symbol})
    return safe_float(data.get("price", 0))


# =========================
# Binance (signed)
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
            continue
    raise RuntimeError(f"Signed request failed: {path}. Last error: {last_err}")

def get_account_info_signed() -> dict:
    return signed_request("GET", "/api/v3/account", {})

def get_symbol_info(symbol: str) -> dict:
    data = http_get_json("/api/v3/exchangeInfo", params={"symbol": symbol})
    syms = data.get("symbols", [])
    return syms[0] if syms else {}

def lot_step_for_symbol(symbol_info: dict) -> Tuple[float, float]:
    min_qty = 0.0
    step = 0.0
    for f in symbol_info.get("filters", []):
        if f.get("filterType") == "LOT_SIZE":
            min_qty = safe_float(f.get("minQty"))
            step = safe_float(f.get("stepSize"))
            break
    return (min_qty, step)

def round_step(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    return (qty // step) * step

def get_free_balance(asset: str) -> float:
    info = get_account_info_signed()
    for b in info.get("balances", []):
        if b.get("asset") == asset:
            return safe_float(b.get("free"))
    return 0.0

def place_market_order(symbol: str, side: str, quantity: float) -> dict:
    params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": f"{quantity:.18f}".rstrip("0").rstrip("."),
        "newOrderRespType": "FULL",
    }
    return signed_request("POST", "/api/v3/order", params)


# =========================
# Strategy (entry only for approval)
# =========================
def is_green(c: dict) -> bool:
    return c["close"] > c["open"]

def is_red(c: dict) -> bool:
    return c["close"] < c["open"]

@dataclass
class SymbolState:
    stage: str = "SEEK_MAKEHIGH_START"
    turn_red_low: float = 0.0
    last_bear_close_time: int = 0
    last_bear_high: float = 0.0
    last_bear_low: float = 0.0
    last_signal: str = ""

def ensure_symbol_state(state: dict, symbol: str) -> SymbolState:
    s = state["symbols"].get(symbol)
    if not s:
        st = SymbolState()
        state["symbols"][symbol] = asdict(st)
        return st
    st = SymbolState(**{**asdict(SymbolState()), **s})
    state["symbols"][symbol] = asdict(st)
    return st

def update_symbol_state_dict(state: dict, symbol: str, st: SymbolState) -> None:
    state["symbols"][symbol] = asdict(st)

def analyze_entry(symbol: str, st: SymbolState, klines: List[dict], price: float) -> Tuple[SymbolState, Optional[dict]]:
    """
    BUY signal:
    - make high start: red->first green
    - green run -> first red (turn red)
    - price < turn_red_low => pullback start
    - track last red candle during pullback
    - BUY when price > last red high
    SL = last red low * (1 - SL_OFFSET)
    """
    if len(klines) < 6:
        return st, None

    last_closed = klines[-2]
    prev_closed = klines[-3]

    def sig_key(kind: str, t: int) -> str:
        return f"{kind}:{t}"

    # 1) make-high start
    if st.stage == "SEEK_MAKEHIGH_START":
        if is_red(prev_closed) and is_green(last_closed):
            st.stage = "GREEN_RUN"
            st.turn_red_low = 0.0
            st.last_bear_close_time = 0
            st.last_bear_high = 0.0
            st.last_bear_low = 0.0

    # 2) green run ends on first red
    elif st.stage == "GREEN_RUN":
        if is_red(last_closed):
            st.turn_red_low = last_closed["low"]
            st.stage = "WAIT_PULLBACK_BREAK"

    # 3) pullback starts when price breaks below turn-red low
    elif st.stage == "WAIT_PULLBACK_BREAK":
        if st.turn_red_low > 0 and price < st.turn_red_low:
            st.stage = "PULLBACK_BEARISH"
            if is_red(last_closed):
                st.last_bear_close_time = last_closed["close_time"]
                st.last_bear_high = last_closed["high"]
                st.last_bear_low = last_closed["low"]

    # 4) in pullback track last red candle
    elif st.stage == "PULLBACK_BEARISH":
        if is_red(last_closed):
            st.last_bear_close_time = last_closed["close_time"]
            st.last_bear_high = last_closed["high"]
            st.last_bear_low = last_closed["low"]
        elif is_green(last_closed):
            if st.last_bear_high > 0:
                st.stage = "WAIT_BUY_BREAK"

    # 5) buy on break above last red high
    elif st.stage == "WAIT_BUY_BREAK":
        if is_red(last_closed):
            st.stage = "PULLBACK_BEARISH"
            st.last_bear_close_time = last_closed["close_time"]
            st.last_bear_high = last_closed["high"]
            st.last_bear_low = last_closed["low"]
        else:
            if st.last_bear_high > 0 and price > st.last_bear_high:
                key = sig_key("BUY", st.last_bear_close_time or last_closed["close_time"])
                if st.last_signal == key:
                    return st, None
                st.last_signal = key

                sl = st.last_bear_low * (1.0 - SL_OFFSET)
                payload = {
                    "symbol": symbol,
                    "price": price,
                    "break_high": st.last_bear_high,
                    "sl": sl,
                    "ts": now_ts(),
                    "interval": INTERVAL,
                }
                # reset state to search next cycle
                st = SymbolState()
                return st, payload

    return st, None


# =========================
# Telegram UI (approval)
# =========================
def fmt(p: float) -> str:
    if p >= 1:
        return f"{p:.6f}".rstrip("0").rstrip(".")
    return f"{p:.10f}".rstrip("0").rstrip(".")

def make_order_id() -> str:
    return f"o{int(time.time()*1000)}"

def build_keyboard(order_id: str) -> InlineKeyboardMarkup:
    # quick sizes + approve/reject
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

def get_prefs(state: dict) -> dict:
    state.setdefault("prefs", {})
    prefs = state["prefs"]
    prefs.setdefault("size_mode", SIZE_MODE)
    prefs.setdefault("default_usdt", DEFAULT_USDT)
    prefs.setdefault("default_risk", DEFAULT_RISK)
    return prefs

def compute_qty_for_symbol(symbol: str, price: float, mode: str, usdt_amount: float, risk_pct: float) -> float:
    if mode == "USDT":
        spend = usdt_amount
    else:
        usdt_free = get_free_balance(QUOTE_ASSET)
        spend = usdt_free * risk_pct

    if spend <= 10:
        raise RuntimeError(f"{QUOTE_ASSET} amount too small: {spend}")

    raw_qty = spend / price
    info = get_symbol_info(symbol)
    min_qty, step = lot_step_for_symbol(info)
    qty = round_step(raw_qty, step)
    if qty < min_qty:
        raise RuntimeError(f"Qty < minQty. qty={qty} minQty={min_qty}")
    return float(qty)


# =========================
# Commands
# =========================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ALLOWED_USER_ID and update.effective_user and update.effective_user.id != ALLOWED_USER_ID:
        return
    await update.message.reply_text(
        "✅ Bot ishlayapti.\n\n"
        "Buyruqlar:\n"
        "/mode usdt   — default sizing USDT\n"
        "/mode risk   — default sizing balans %\n"
        "/setusdt 25  — default USDT miqdor\n"
        "/setrisk 0.02 — default risk 2%\n"
        "/status      — holat\n"
    )

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ALLOWED_USER_ID and update.effective_user and update.effective_user.id != ALLOWED_USER_ID:
        return
    st = load_state()
    prefs = get_prefs(st)
    top10 = st.get("top10", [])
    pending = st.get("pending", {})
    msg = (
        f"TF: {INTERVAL}\n"
        f"MODE: {SYMBOL_MODE}\n"
        f"Size mode: {prefs['size_mode']}\n"
        f"Default USDT: {prefs['default_usdt']}\n"
        f"Default risk: {prefs['default_risk']}\n"
        f"Pending: {len(pending)}\n\n"
        f"TOP10:\n" + ("\n".join(top10) if top10 else "—")
    )
    await update.message.reply_text(msg)

async def mode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ALLOWED_USER_ID and update.effective_user and update.effective_user.id != ALLOWED_USER_ID:
        return
    if not context.args:
        await update.message.reply_text("Foydalanish: /mode usdt  yoki  /mode risk")
        return
    m = context.args[0].strip().upper()
    if m not in ("USDT", "RISK"):
        await update.message.reply_text("Noto‘g‘ri. /mode usdt yoki /mode risk")
        return
    st = load_state()
    prefs = get_prefs(st)
    prefs["size_mode"] = m
    save_state(st)
    await update.message.reply_text(f"✅ Size mode = {m}")

async def setusdt_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ALLOWED_USER_ID and update.effective_user and update.effective_user.id != ALLOWED_USER_ID:
        return
    if not context.args:
        await update.message.reply_text("Foydalanish: /setusdt 25")
        return
    val = safe_float(context.args[0])
    if val <= 0:
        await update.message.reply_text("USDT miqdor > 0 bo‘lsin")
        return
    st = load_state()
    prefs = get_prefs(st)
    prefs["default_usdt"] = val
    save_state(st)
    await update.message.reply_text(f"✅ Default USDT = {val}")

async def setrisk_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ALLOWED_USER_ID and update.effective_user and update.effective_user.id != ALLOWED_USER_ID:
        return
    if not context.args:
        await update.message.reply_text("Foydalanish: /setrisk 0.02 (2%)")
        return
    val = safe_float(context.args[0])
    if val <= 0 or val > 1:
        await update.message.reply_text("Risk 0..1 oralig‘ida bo‘lsin. Masalan 0.02")
        return
    st = load_state()
    prefs = get_prefs(st)
    prefs["default_risk"] = val
    save_state(st)
    await update.message.reply_text(f"✅ Default risk = {val} ({val*100:.2f}%)")


# =========================
# Callback (approve/reject)
# =========================
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if ALLOWED_USER_ID and q.from_user and q.from_user.id != ALLOWED_USER_ID:
        await q.edit_message_text("⛔ Ruxsat yo‘q.")
        return

    data = (q.data or "").split("|")
    if not data:
        return

    st = load_state()
    pending = st.get("pending", {}) or {}

    if data[0] == "REJECT":
        oid = data[1] if len(data) > 1 else ""
        if oid in pending:
            pending.pop(oid, None)
            st["pending"] = pending
            save_state(st)
        await q.edit_message_text("❌ Bekor qilindi.")
        return

    if data[0] == "APPROVE":
        oid = data[1] if len(data) > 1 else ""
        mode_arg = data[2] if len(data) > 2 else "DEF"

        p = pending.get(oid)
        if not p:
            await q.edit_message_text("⚠️ Bu order eskirgan yoki topilmadi.")
            return

        # timeout
        if now_ts() - int(p.get("ts", 0)) > APPROVE_TIMEOUT_SEC:
            pending.pop(oid, None)
            st["pending"] = pending
            save_state(st)
            await q.edit_message_text("⌛ Timeout. Order bekor bo‘ldi.")
            return

        prefs = get_prefs(st)
        size_mode = prefs["size_mode"]
        usdt_amt = float(prefs["default_usdt"])
        risk_pct = float(prefs["default_risk"])

        if mode_arg.startswith("USDT:"):
            size_mode = "USDT"
            usdt_amt = safe_float(mode_arg.split(":", 1)[1]) or usdt_amt

        symbol = p["symbol"]
        price = float(p["price"])

        try:
            qty = compute_qty_for_symbol(symbol, price, size_mode, usdt_amt, risk_pct)
            order = place_market_order(symbol, "BUY", qty)
            pending.pop(oid, None)
            st["pending"] = pending
            save_state(st)

            await q.edit_message_text(
                f"✅ BUY EXECUTED\n"
                f"{symbol}\n"
                f"Qty: {qty}\n"
                f"Mode: {size_mode}\n"
                f"USDT: {usdt_amt}\n"
                f"Risk: {risk_pct}\n"
                f"Approx price: {fmt(price)}"
            )
        except Exception as e:
            await q.edit_message_text(f"⚠️ BUY FAILED: {e}")
        return


# =========================
# Scanner loop -> sends approval requests
# =========================
async def send_approval(app: Application, symbol_payload: dict):
    # Send to allowed user (private chat with bot)
    chat_id = ALLOWED_USER_ID
    if not chat_id:
        return

    st = load_state()
    pending = st.get("pending", {}) or {}
    if len(pending) >= MAX_PENDING:
        return

    oid = make_order_id()
    pending[oid] = symbol_payload
    st["pending"] = pending
    save_state(st)

    sym = symbol_payload["symbol"]
    price = float(symbol_payload["price"])
    sl = float(symbol_payload["sl"])
    bh = float(symbol_payload["break_high"])

    prefs = get_prefs(st)
    txt = (
        f"📣 SIGNAL BUY (approval)\n"
        f"{sym}\n"
        f"TF: {symbol_payload.get('interval','')}\n"
        f"Break HIGH: {fmt(bh)}\n"
        f"Price: {fmt(price)}\n"
        f"SL: {fmt(sl)}\n\n"
        f"Default size: {prefs['size_mode']} | "
        f"{prefs['default_usdt']} USDT | risk {prefs['default_risk']}"
    )

    await app.bot.send_message(chat_id=chat_id, text=txt, reply_markup=build_keyboard(oid))


async def scanner_loop(app: Application):
    state = load_state()
    tradable_usdt = set()
    try:
        tradable_usdt = get_exchange_info_symbols_usdt()
    except Exception as e:
        await app.bot.send_message(chat_id=ALLOWED_USER_ID, text=f"⚠️ exchangeInfo error: {e}")

    top10 = state.get("top10", []) or []

    while True:
        try:
            state = load_state()

            # refresh top10
            if SYMBOL_MODE == "TOP10":
                if now_ts() - int(state.get("last_top_refresh", 0)) >= TOP_REFRESH_SEC or not top10:
                    top10 = get_top10_gainers_usdt(tradable_usdt)
                    state["top10"] = top10
                    state["last_top_refresh"] = now_ts()

                    # clean removed symbol states
                    for sym in list(state.get("symbols", {}).keys()):
                        if sym not in top10:
                            state["symbols"].pop(sym, None)

                    save_state(state)
                symbols = top10
            else:
                symbols = SYMBOLS[:]

            for sym in symbols:
                st_sym = ensure_symbol_state(state, sym)
                kl = get_klines(sym, INTERVAL, KLINE_LIMIT)
                price = get_spot_price(sym)

                st_sym, payload = analyze_entry(sym, st_sym, kl, price)
                update_symbol_state_dict(state, sym, st_sym)

                if payload:
                    # send approval request
                    await send_approval(app, payload)

            save_state(state)

        except Exception as e:
            if ALLOWED_USER_ID:
                try:
                    await app.bot.send_message(chat_id=ALLOWED_USER_ID, text=f"⚠️ Scan error: {e}")
                except Exception:
                    pass

        await asyncio.sleep(SCAN_EVERY_SEC)


# =========================
# Main
# =========================
def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN env yo'q")
    if not ALLOWED_USER_ID:
        raise RuntimeError("ALLOWED_USER_ID env yo'q (sening Telegram ID)")
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET env yo'q")

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("mode", mode_cmd))
    app.add_handler(CommandHandler("setusdt", setusdt_cmd))
    app.add_handler(CommandHandler("setrisk", setrisk_cmd))
    app.add_handler(CallbackQueryHandler(callback_handler))

    async def post_init(app_: Application):
        app_.create_task(scanner_loop(app_))
        await app_.bot.send_message(chat_id=ALLOWED_USER_ID, text=f"✅ Approval bot started | TF={INTERVAL} | MODE={SYMBOL_MODE}")

    app.post_init = post_init
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
