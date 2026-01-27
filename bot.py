import os
import json
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import requests
import hmac
import hashlib
import urllib.parse

# =========================
# ENV
# =========================
BINANCE_API_KEY = (os.getenv("BINANCE_API_KEY") or "").strip()
BINANCE_API_SECRET = (os.getenv("BINANCE_API_SECRET") or "").strip()

TELEGRAM_TOKEN = (os.getenv("TELEGRAM_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

BINANCE_BASES = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
]

# =========================
# SETTINGS
# =========================
INTERVAL = os.getenv("INTERVAL", "3m")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "10"))
KLINES_LIMIT = int(os.getenv("KLINES_LIMIT", "200"))

SYMBOL_MODE = (os.getenv("SYMBOL_MODE") or "TOP10").strip().upper()  # TOP10 or LIST
SYMBOLS = [s.strip().upper() for s in (os.getenv("SYMBOLS") or "BTCUSDT,ETHUSDT").split(",") if s.strip()]
TOP_REFRESH_SECONDS = int(os.getenv("TOP_REFRESH_SECONDS", "180"))

DRY_RUN = (os.getenv("DRY_RUN") or "1").strip() != "0"

QUOTE_ASSET = (os.getenv("QUOTE_ASSET") or "USDT").strip().upper()
RISK_PCT = float(os.getenv("RISK_PCT") or "0.10")  # balansdan necha % bilan kirish (xavfsiz: 0.01–0.03)
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS") or "1")
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY") or "10")
MAX_DAILY_LOSS_USDT = float(os.getenv("MAX_DAILY_LOSS_USDT") or "50")

# SL offset: 0.1% past
SL_OFFSET = float(os.getenv("SL_OFFSET") or "0.001")  # 0.001 = 0.1%

STATE_FILE = os.getenv("STATE_FILE") or "state.json"
SESSION = requests.Session()


# =========================
# Utilities
# =========================
def now_ts() -> int:
    return int(time.time())

def day_key_utc() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())

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

def notify(msg: str) -> None:
    print(msg)
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        SESSION.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=12)
    except Exception:
        pass

def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {
            "symbols": {},
            "top10": [],
            "last_top_refresh": 0,
            "open_positions": {},  # symbol -> {qty, entry, sl, trail_low, time}
            "daily": {"day": "", "trades": 0, "realized_pnl": 0.0},
        }
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "symbols": {},
            "top10": [],
            "last_top_refresh": 0,
            "open_positions": {},
            "daily": {"day": "", "trades": 0, "realized_pnl": 0.0},
        }

def save_state(state: dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)

def parse_klines(klines: list) -> List[dict]:
    out = []
    for k in klines:
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
# Binance SPOT data
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


# =========================
# Binance signed requests
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

def avg_fill_price(order: dict, fallback_price: float) -> float:
    fills = order.get("fills") or []
    if not fills:
        return fallback_price
    total_quote = 0.0
    total_qty = 0.0
    for f in fills:
        q = safe_float(f.get("qty"))
        p = safe_float(f.get("price"))
        total_quote += q * p
        total_qty += q
    return (total_quote / total_qty) if total_qty > 0 else fallback_price


# =========================
# Strategy state (ENTRY)
# =========================
@dataclass
class SymbolState:
    # SEEK_MAKEHIGH_START -> GREEN_RUN -> WAIT_PULLBACK_BREAK -> PULLBACK_BEARISH -> WAIT_BUY_BREAK
    stage: str = "SEEK_MAKEHIGH_START"

    # turn red candle after green run
    turn_red_low: float = 0.0

    # last bearish candle during pullback (the one whose HIGH gets broken for BUY)
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


def analyze_entry(symbol: str, st: SymbolState, klines: List[dict], price: float) -> Tuple[SymbolState, Optional[str], Optional[dict]]:
    """
    Entry rules (sen aytgandek):
    1) Make-high START: red -> first green
    2) Green run -> first red appears (turn-red)
    3) Price breaks below turn-red LOW => pullback started
    4) Pullback red sequence: track last red candle
    5) BUY when price breaks above last red candle HIGH
       SL = last red candle LOW * (1 - 0.001)
    """
    if len(klines) < 6:
        return st, None, None

    last_closed = klines[-2]
    prev_closed = klines[-3]

    signal = None
    buy_payload = None

    def sig_key(kind: str, t: int) -> str:
        return f"{kind}:{t}"

    if st.stage == "SEEK_MAKEHIGH_START":
        if is_red(prev_closed) and is_green(last_closed):
            st.stage = "GREEN_RUN"
            st.turn_red_low = 0.0
            st.last_bear_close_time = 0
            st.last_bear_high = 0.0
            st.last_bear_low = 0.0

    elif st.stage == "GREEN_RUN":
        if is_red(last_closed):
            st.turn_red_low = last_closed["low"]
            st.stage = "WAIT_PULLBACK_BREAK"

    elif st.stage == "WAIT_PULLBACK_BREAK":
        if st.turn_red_low > 0 and price < st.turn_red_low:
            st.stage = "PULLBACK_BEARISH"
            if is_red(last_closed):
                st.last_bear_close_time = last_closed["close_time"]
                st.last_bear_high = last_closed["high"]
                st.last_bear_low = last_closed["low"]

    elif st.stage == "PULLBACK_BEARISH":
        if is_red(last_closed):
            st.last_bear_close_time = last_closed["close_time"]
            st.last_bear_high = last_closed["high"]
            st.last_bear_low = last_closed["low"]
        elif is_green(last_closed):
            if st.last_bear_high > 0:
                st.stage = "WAIT_BUY_BREAK"

    elif st.stage == "WAIT_BUY_BREAK":
        if is_red(last_closed):
            st.stage = "PULLBACK_BEARISH"
            st.last_bear_close_time = last_closed["close_time"]
            st.last_bear_high = last_closed["high"]
            st.last_bear_low = last_closed["low"]
        else:
            if st.last_bear_high > 0 and price > st.last_bear_high:
                # BUY trigger candle = last bearish candle
                sl = st.last_bear_low * (1.0 - SL_OFFSET)

                key = sig_key("BUY", st.last_bear_close_time or last_closed["close_time"])
                if st.last_signal != key:
                    st.last_signal = key
                    signal = (
                        f"{symbol} ✅ BUY\n"
                        f"Break pullback last RED HIGH: {st.last_bear_high}\n"
                        f"SL: {sl}  (0.1% below last RED low)\n"
                        f"Price: {price}\n"
                        f"TF: {INTERVAL}"
                    )

                buy_payload = {"sl": sl}
                # after buy, reset entry state for next cycle
                st = SymbolState()

    return st, signal, buy_payload


# =========================
# Risk / Trade helpers
# =========================
def can_trade_today(state: dict) -> bool:
    today = day_key_utc()
    d = state.get("daily", {})
    if d.get("day") != today:
        state["daily"] = {"day": today, "trades": 0, "realized_pnl": 0.0}
        return True
    if float(d.get("realized_pnl", 0.0)) <= -abs(MAX_DAILY_LOSS_USDT):
        return False
    if int(d.get("trades", 0)) >= MAX_TRADES_PER_DAY:
        return False
    return True

def open_positions_count(state: dict) -> int:
    return len(state.get("open_positions", {}) or {})

def inc_trades(state: dict) -> None:
    state["daily"]["trades"] = int(state["daily"].get("trades", 0)) + 1

def update_realized_pnl(state: dict, symbol: str, exit_price: float) -> None:
    pos = (state.get("open_positions", {}) or {}).get(symbol)
    if not pos:
        return
    entry = float(pos.get("entry", 0.0))
    qty = float(pos.get("qty", 0.0))
    pnl = (exit_price - entry) * qty
    state["daily"]["realized_pnl"] = float(state["daily"].get("realized_pnl", 0.0)) + pnl

def compute_buy_qty(symbol: str, price: float) -> float:
    usdt_free = get_free_balance(QUOTE_ASSET)
    spend = usdt_free * RISK_PCT
    if spend <= 10:
        raise RuntimeError(f"Balance too small. {QUOTE_ASSET} free={usdt_free}")
    raw_qty = spend / price

    info = get_symbol_info(symbol)
    min_qty, step = lot_step_for_symbol(info)
    qty = round_step(raw_qty, step)
    if qty < min_qty:
        raise RuntimeError(f"Qty < minQty. qty={qty} minQty={min_qty}")
    return float(qty)


# =========================
# Exit rules (SL + TP)
# =========================
def check_exit(symbol: str, pos: dict, price: float, last_closed: dict) -> Tuple[Optional[str], dict]:
    """
    SL: price <= sl
    TP: price < trail_low (trail_low = oxirgi yopilgan sham low)
        trail_low har yangi candle yopilganda last_closed['low'] ga yangilanadi
    """
    sl = float(pos.get("sl", 0.0))
    trail_low = float(pos.get("trail_low", 0.0))
    last_low = float(last_closed.get("low", 0.0))
    last_ct = int(last_closed.get("close_time", 0))

    # update trailing low with last closed candle low
    # (TP qoidang: o'sish davomida oxirgi yopilgan sham low yorilsa)
    if last_ct and last_low > 0:
        # always update to most recent closed candle low
        pos["trail_low"] = last_low

    # 1) Stop loss first (priority)
    if sl > 0 and price <= sl:
        return "SL", pos

    # 2) Take profit trailing: break last closed low
    # (using updated pos["trail_low"])
    if pos.get("trail_low", 0.0) > 0 and price < float(pos["trail_low"]):
        return "TP", pos

    return None, pos


# =========================
# Main
# =========================
def main():
    state = load_state()

    # daily init
    if state.get("daily", {}).get("day") != day_key_utc():
        state["daily"] = {"day": day_key_utc(), "trades": 0, "realized_pnl": 0.0}

    tradable_usdt = set()
    try:
        tradable_usdt = get_exchange_info_symbols_usdt()
    except Exception as e:
        notify(f"⚠️ exchangeInfo error: {e}")

    notify(f"✅ SPOT AUTO BOT START | TF={INTERVAL} | MODE={SYMBOL_MODE} | DRY_RUN={int(DRY_RUN)}")

    while True:
        try:
            # symbols universe
            if SYMBOL_MODE == "TOP10":
                if now_ts() - int(state.get("last_top_refresh", 0)) >= TOP_REFRESH_SECONDS or not state.get("top10"):
                    top10 = get_top10_gainers_usdt(tradable_usdt)
                    state["top10"] = top10
                    state["last_top_refresh"] = now_ts()
                    # clean entry states for removed
                    for sym in list(state.get("symbols", {}).keys()):
                        if sym not in top10:
                            state["symbols"].pop(sym, None)
                    save_state(state)
                symbols = list(state.get("top10", []) or [])
            else:
                symbols = SYMBOLS[:]

            # daily safety
            if not can_trade_today(state):
                notify("⛔️ Trading paused (daily limits reached).")
                save_state(state)
                time.sleep(max(POLL_SECONDS, 30))
                continue

            open_positions = state.get("open_positions", {}) or {}

            for sym in symbols:
                kl = get_klines(sym, INTERVAL, KLINES_LIMIT)
                price = get_spot_price(sym)
                last_closed = kl[-2]  # closed candle

                # ===== EXIT (if in position) =====
                if sym in open_positions:
                    pos = open_positions[sym]
                    reason, pos = check_exit(sym, pos, price, last_closed)
                    open_positions[sym] = pos  # persist trail_low updates

                    if reason in ("SL", "TP"):
                        qty = float(pos.get("qty", 0.0))
                        if qty > 0:
                            if DRY_RUN:
                                notify(f"[DRY_RUN] {reason} SELL {sym} @ {price} | SL={pos.get('sl')} | trail_low={pos.get('trail_low')}")
                                update_realized_pnl(state, sym, price)
                                open_positions.pop(sym, None)
                                inc_trades(state)
                            else:
                                order = place_market_order(sym, "SELL", qty)
                                exit_price = avg_fill_price(order, price)
                                update_realized_pnl(state, sym, exit_price)
                                open_positions.pop(sym, None)
                                inc_trades(state)
                                notify(f"🟥 {reason} SELL {sym} qty={qty} exit≈{exit_price} | SL={pos.get('sl')} | trail_low={pos.get('trail_low')}")
                        continue  # after exit, go next symbol

                # ===== ENTRY (if not in position) =====
                if sym not in open_positions:
                    if open_positions_count(state) >= MAX_OPEN_POSITIONS:
                        continue

                    st = ensure_symbol_state(state, sym)
                    st, sig, buy_payload = analyze_entry(sym, st, kl, price)
                    update_symbol_state_dict(state, sym, st)
                    if sig:
                        notify(sig)

                    if buy_payload and can_trade_today(state):
                        sl = float(buy_payload["sl"])

                        if DRY_RUN:
                            # simulate
                            qty_sim = 1.0
                            open_positions[sym] = {
                                "qty": qty_sim,
                                "entry": price,
                                "sl": sl,
                                "trail_low": last_closed["low"],  # start trailing from last closed candle low
                                "time": now_ts(),
                            }
                            inc_trades(state)
                            notify(f"[DRY_RUN] BUY {sym} @ {price} | SL={sl} | trail_low={last_closed['low']}")
                        else:
                            qty = compute_buy_qty(sym, price)
                            order = place_market_order(sym, "BUY", qty)
                            entry_price = avg_fill_price(order, price)

                            open_positions[sym] = {
                                "qty": qty,
                                "entry": entry_price,
                                "sl": sl,
                                "trail_low": last_closed["low"],
                                "time": now_ts(),
                            }
                            inc_trades(state)
                            notify(f"✅ ORDER BUY {sym} qty={qty} entry≈{entry_price} | SL={sl} | trail_low={last_closed['low']}")

            state["open_positions"] = open_positions
            save_state(state)

        except Exception as e:
            notify(f"⚠️ ERROR: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
