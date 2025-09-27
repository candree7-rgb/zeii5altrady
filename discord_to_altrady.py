#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Discord → Altrady Signal-Forwarder (Multi-Block + Leg-Filter + Futures-Touch + Basis-Adjust)
- Extrahiert ALLE Signal-Blöcke aus der neuesten Discord-Message
- Strict: nur */USD bzw. */USDT (z. B. */BTC wird übersprungen)
- Mappings: LUNA→LUNA2 (für Altrady), SHIB→1000SHIB, USD→USDT
- Tick-Rundung
- Dynamische Leverage via SL-% (gecappt global + coin-spezifisch)
- TP-Splits aus ENV (x,y)
- Wait-for-Touch:
  * Prüft TOUCH am **Futures-Preis** (Perp, USDT-Margined)
  * Optionales **Basis-Adjust** der Levels (Entry/TP/SL) Spot→Futures (Cap via ENV)
  * Order bei Touch: **LIMIT** (oder MARKET via ENV)
  * Timeout via ENTRY_WAIT_MAX_SEC
- Leg-Filter via Spot-Klines + ZigZag:
  * LEG_MAX (z. B. 3)
  * Trend-Match optional
"""

import os, re, sys, time, json, traceback
from pathlib import Path
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

# =========================
# ENV / CONFIG
# =========================

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
CHANNEL_ID    = os.getenv("CHANNEL_ID", "").strip()
ALTRADY_WEBHOOK_URL = os.getenv("ALTRADY_WEBHOOK_URL", "").strip()

ALTRADY_API_KEY    = os.getenv("ALTRADY_API_KEY", "").strip()
ALTRADY_API_SECRET = os.getenv("ALTRADY_API_SECRET", "").strip()
ALTRADY_EXCHANGE   = os.getenv("ALTRADY_EXCHANGE", "BIFU").strip()
QUOTE              = os.getenv("QUOTE", "USDT").strip().upper()

MAX_LEVERAGE = int(os.getenv("MAX_LEVERAGE", "75"))
SAFETY_PCT   = float(os.getenv("SAFETY_PCT", "80"))

COIN_LEV_CAPS = {
    "LUNA2": int(os.getenv("LEV_MAX_LUNA2", "50")),
    "LUNA":  int(os.getenv("LEV_MAX_LUNA",  "50")),
}

TP_SPLITS_RAW = os.getenv("TP_SPLITS", "40,60").strip()  # Default 40/60
ENTRY_EXPIRATION_MIN = int(os.getenv("ENTRY_EXPIRATION_MIN", "10"))

POLL_BASE   = int(os.getenv("POLL_BASE_SECONDS", "60"))
POLL_OFFSET = int(os.getenv("POLL_OFFSET_SECONDS", "3"))
STATE_FILE  = Path(os.getenv("STATE_FILE", "state.json"))

# --- LEG-FILTER ENVs ---
LEG_FILTER              = os.getenv("LEG_FILTER", "on").lower() == "on"
LEG_TIMEFRAME_DEFAULT   = os.getenv("LEG_TIMEFRAME_DEFAULT", "M5").upper()
LEG_ZIGZAG_PCT          = float(os.getenv("LEG_ZIGZAG_PCT", "1.0"))
LEG_MAX                 = int(os.getenv("LEG_MAX", "3"))
LEG_MAX_LOOKBACK        = int(os.getenv("LEG_MAX_LOOKBACK", "400"))
LEG_REQUIRE_TREND_MATCH = os.getenv("LEG_REQUIRE_TREND_MATCH", "on").lower() == "on"
LEG_FAIL_MODE           = os.getenv("LEG_FAIL_MODE", "skip").lower()  # "skip" | "open"
TF_MAP = {"M5": "5m", "M15": "15m", "H1": "1h", "1D": "1d"}

# --- WAIT-FOR-TOUCH ENVs ---
ENTRY_WAIT_MAX_SEC      = int(os.getenv("ENTRY_WAIT_MAX_SEC", "1200"))  # M5 default: 20m
ENTRY_POLL_SEC          = float(os.getenv("ENTRY_POLL_SEC", "1"))
ENTRY_TOL_PCT           = float(os.getenv("ENTRY_TOL_PCT", "0.05"))     # 0.05% = 5 bps
ENTRY_TOUCH_ORDER_TYPE  = os.getenv("ENTRY_TOUCH_ORDER_TYPE", "limit").lower()  # limit | market

# --- BASIS/ADJUST ENVs ---
# adjust: hole Spot & Futures, skaliere Level 1x mit fut/spot (cap via BASIS_MAX_PCT), Touch am Futures
# spot:   keine Anpassung, Touch trotzdem am Futures
# off:    keine Anpassung, Touch am Futures (identisch zu spot, nur Flag klarer)
BASIS_MODE     = os.getenv("BASIS_MODE", "adjust").lower()  # adjust | spot | off
BASIS_MAX_PCT  = float(os.getenv("BASIS_MAX_PCT", "0.30"))  # Max 0.30% Skalierung

# Sanity-Check
if not DISCORD_TOKEN or not CHANNEL_ID or not ALTRADY_WEBHOOK_URL:
    print("Bitte ENV setzen: DISCORD_TOKEN, CHANNEL_ID, ALTRADY_WEBHOOK_URL (und Keys).")
    sys.exit(1)

HEADERS = {
    "Authorization": DISCORD_TOKEN,   # User-Session
    "User-Agent": "DiscordToAltrady/1.5"
}

# =========================
# Helpers / Exceptions
# =========================

class SkipSignal(Exception):
    pass

def parse_tp_splits(raw: str) -> tuple[int,int]:
    try:
        a,b = [int(float(x.strip())) for x in raw.split(",")]
        if a <= 0 or b <= 0 or a+b != 100: raise ValueError
        return a,b
    except Exception:
        print(f"[WARN] Ungültige TP_SPLITS='{raw}', Fallback 40,60.")
        return 40,60

TP1_PCT, TP2_PCT = parse_tp_splits(TP_SPLITS_RAW)

def load_state() -> dict:
    if STATE_FILE.exists():
        try: return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception: pass
    return {"last_id": None}

def save_state(state: dict):
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state), encoding="utf-8")
    tmp.replace(STATE_FILE)

def sleep_until_next_tick():
    now = time.time()
    period_start = (now // POLL_BASE) * POLL_BASE
    next_tick = period_start + POLL_BASE + POLL_OFFSET
    if now < period_start + POLL_OFFSET:
        next_tick = period_start + POLL_OFFSET
    time.sleep(max(0, next_tick - now))

# =========================
# Discord
# =========================

def fetch_latest_message(channel_id: str):
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    r = requests.get(url, headers=HEADERS, params={"limit":1}, timeout=15)
    if r.status_code == 429:
        retry = 5
        try: retry = float(r.json().get("retry_after", 5))
        except Exception: pass
        time.sleep(retry + 0.5)
        r = requests.get(url, headers=HEADERS, params={"limit":1}, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data[0] if data else None

BUYSELL_LINE = re.compile(r"(?im)^\s*.*\b(BUY|SELL)\b.*$")

def extract_signal_blocks(msg: dict) -> list[str]:
    def source_text():
        parts = []
        parts.append((msg.get("content") or "").replace("\r",""))
        embeds = msg.get("embeds") or []
        if embeds and isinstance(embeds, list):
            e0 = embeds[0] or {}
            desc = (e0.get("description") or "").replace("\r","")
            if desc: parts.append(desc)
        return "\n".join([p for p in parts if p]).strip()

    raw = source_text()
    if not raw: return []
    starts = [m.start() for m in BUYSELL_LINE.finditer(raw)]
    if not starts: return []

    blocks = []
    for i,s in enumerate(starts):
        tail = raw[s:]
        chunk = tail[:(starts[i+1]-s)] if i+1 < len(starts) else tail
        m_blank = re.search(r"\n\s*\n", chunk)
        if m_blank: chunk = chunk[:m_blank.start()]
        chunk = re.sub(r"(?im)^\s*Timeframe:.*$", "", chunk).strip()
        if BUYSELL_LINE.search(chunk):
            blocks.append(chunk)
    return blocks

def find_timeframe_in_msg(msg: dict) -> str:
    parts = []
    parts.append(msg.get("content") or "")
    e = (msg.get("embeds") or [])
    if e and isinstance(e,list):
        e0 = e[0] or {}
        parts.append(e0.get("description") or "")
        ft = (e0.get("footer") or {}).get("text","") if isinstance(e0.get("footer"), dict) else ""
        if ft: parts.append(ft)
    txt = "\n".join(parts)
    m = re.search(r"Timeframe:\s*(M5|M15|H1|1D)", txt, re.I)
    return (m.group(1).upper() if m else LEG_TIMEFRAME_DEFAULT)

# =========================
# Parsing & Rounding
# =========================

TICK_MAP = {
    "SHIB":8,"1000SHIB":8,"DOGE":5,"XRP":4,"SOL":2,"AVAX":3,"AAVE":2,"LINK":3,
    "BTC":2,"ETH":2,"BNB":2,"LTC":2,"ADA":5,"MATIC":5,"EOS":4,"BCH":2,
    "ATOM":3,"ALGO":5,"LUNA2":3
}
def round_tick(sym: str, v: float) -> float:
    d = TICK_MAP.get(sym, 4); p = 10**d; return round(v*p)/p

SIG_SIDE  = re.compile(r"\b(BUY|SELL)\b", re.I)
SIG_PAIR  = re.compile(r"on\s+([A-Z0-9]+)[/\-]([A-Z0-9]+)", re.I)
NUM       = r"([0-9]*\.?[0-9]+)"
SIG_ENTRY = re.compile(rf"Price:\s*{NUM}", re.I)
SIG_TP1   = re.compile(rf"TP\s*1:\s*{NUM}", re.I)
SIG_TP2   = re.compile(rf"TP\s*2:\s*{NUM}", re.I)
SIG_SL    = re.compile(rf"\bSL\s*:\s*{NUM}", re.I)

def parse_signal_text(text: str) -> dict:
    t = (text or "").replace("\r","").strip()
    if not t: raise AssertionError("Leerer Signaltext.")
    m_side = SIG_SIDE.search(t);   assert m_side, "BUY/SELL nicht gefunden."
    m_pair = SIG_PAIR.search(t);   assert m_pair, "Paar (z. B. SOL/USD) nicht gefunden."
    m_e    = SIG_ENTRY.search(t);  assert m_e,    "Entry nicht gefunden."
    m_tp1  = SIG_TP1.search(t);    assert m_tp1,  "TP1 nicht gefunden."
    m_tp2  = SIG_TP2.search(t);    assert m_tp2,  "TP2 nicht gefunden."
    m_sl   = SIG_SL.search(t);     assert m_sl,   "SL nicht gefunden."

    side_raw = m_side.group(1).upper()
    side = "long" if side_raw == "BUY" else "short"
    base = m_pair.group(1).upper()
    quoted_raw = m_pair.group(2).upper()
    if quoted_raw not in ("USD","USDT"):
        raise SkipSignal(f"Non-USD Quote erkannt: {base}/{quoted_raw}")

    # Normalisieren/Mapping
    if base == "LUNA": base = "LUNA2"
    if base == "SHIB": base = "1000SHIB"
    quoted = "USDT"

    entry = float(m_e.group(1))
    tp1   = float(m_tp1.group(1))
    tp2   = float(m_tp2.group(1))
    sl    = float(m_sl.group(1))

    if side == "long" and not (sl < entry and tp1 > entry and tp2 > entry):
        raise ValueError("Long: TP/SL nicht plausibel.")
    if side == "short" and not (sl > entry and tp1 < entry and tp2 < entry):
        raise ValueError("Short: TP/SL nicht plausibel.")

    sl_pct = ((entry - sl)/entry*100.0) if side=="long" else ((sl - entry)/entry*100.0)
    lev = int(SAFETY_PCT // max(sl_pct, 1e-12))
    lev = max(1, min(lev, MAX_LEVERAGE))
    coin_cap = COIN_LEV_CAPS.get(base)
    if coin_cap is not None: lev = min(lev, coin_cap)

    symbol = f"{ALTRADY_EXCHANGE}_{QUOTE}_{base}"

    # Spot-Level zunächst runden – Adjust folgt (falls aktiv)
    entry = round_tick(base, entry)
    tp1   = round_tick(base, tp1)
    tp2   = round_tick(base, tp2)
    sl    = round_tick(base, sl)

    return {
        "side": side, "base": base, "quote_from_signal": quoted,
        "entry": entry, "tp1": tp1, "tp2": tp2, "sl": sl,
        "sl_pct": float(f"{sl_pct:.6f}"), "leverage": lev, "symbol": symbol
    }

# =========================
# Leg-Filter (Spot-Klines + ZigZag)
# =========================

def market_base_for_data(base: str) -> str:
    return "LUNA" if base=="LUNA2" else base

def fetch_klines_binance_spot(base: str, quote: str, interval: str, limit: int):
    sym = f"{base}{quote}"
    url = "https://api.binance.com/api/v3/klines"
    r = requests.get(url, params={"symbol":sym,"interval":interval,"limit":limit}, timeout=10)
    r.raise_for_status()
    data = r.json()
    out = []
    for k in data:
        o,h,l,c = float(k[1]), float(k[2]), float(k[3]), float(k[4])
        out.append((o,h,l,c))
    return out

def zigzag_pivots(closes, pct: float):
    if not closes: return []
    thr = pct / 100.0
    piv = []; last_i=0; last_v=closes[0]; direction=0
    for i in range(1,len(closes)):
        up   = (closes[i]-last_v)/last_v
        down = (last_v-closes[i])/last_v
        if direction >= 0:
            if up >= thr:
                piv.append(last_i); direction=1; last_i=i; last_v=closes[i]
            elif closes[i] < last_v:
                last_i=i; last_v=closes[i]
        if direction <= 0:
            if down >= thr:
                piv.append(last_i); direction=-1; last_i=i; last_v=closes[i]
            elif closes[i] > last_v:
                last_i=i; last_v=closes[i]
    if last_i not in piv: piv.append(last_i)
    return sorted(set(piv))

def infer_trend_and_leg(closes, pivots):
    if len(pivots) < 3: return "unknown", 1
    recent = pivots[-10:]
    last, prev = recent[-1], recent[-2]
    trend = "up" if closes[last] > closes[prev] else "down"
    start = recent[0]
    for i in range(2,len(recent)):
        a,b,c = recent[i-2], recent[i-1], recent[i]
        if trend=="up":
            if closes[a] < closes[b] and closes[c] > closes[b]: start=b; break
        else:
            if closes[a] > closes[b] and closes[c] < closes[b]: start=b; break
    count = sum(1 for p in recent if p >= start)
    leg_idx = max(1, min(5, count))
    return trend, leg_idx

def enforce_leg_filter(parsed: dict, msg: dict):
    if not LEG_FILTER: return
    tf = find_timeframe_in_msg(msg)
    interval = TF_MAP.get(tf, TF_MAP[LEG_TIMEFRAME_DEFAULT])
    market_base = market_base_for_data(parsed["base"])
    try:
        kl = fetch_klines_binance_spot(market_base, "USDT", interval, min(LEG_MAX_LOOKBACK, 600))
        closes = [c for (_,_,_,c) in kl]
        piv = zigzag_pivots(closes, LEG_ZIGZAG_PCT)
        trend, leg_idx = infer_trend_and_leg(closes, piv)
        if LEG_REQUIRE_TREND_MATCH and trend in ("up","down"):
            if parsed["side"]=="long" and trend!="up":   raise SkipSignal(f"Trend-Mismatch: long vs {trend}")
            if parsed["side"]=="short" and trend!="down":raise SkipSignal(f"Trend-Mismatch: short vs {trend}")
        # nur Leg 1–LEG_MAX erlauben
        if leg_idx > LEG_MAX:
            raise SkipSignal(f"Leg-Filter: aktueller Leg {leg_idx} > {LEG_MAX} ({trend})")
        print(f"[LEG] tf={tf} interval={interval} trend={trend} leg={leg_idx} base={market_base}")
    except SkipSignal: raise
    except Exception as ex:
        msg_txt = f"Leg-Filter Fehler: {ex.__class__.__name__}: {ex}"
        if LEG_FAIL_MODE=="skip": raise SkipSignal(msg_txt)
        else: print(f"[LEG WARN] {msg_txt} → FAIL-OPEN")

# =========================
# Prices (Spot & Futures)
# =========================

def binance_spot_symbol(base: str) -> str:
    return f"{market_base_for_data(base)}USDT"

def binance_futures_symbol(base: str) -> str:
    return f"{market_base_for_data(base)}USDT"

def fetch_last_price_spot(base: str) -> float:
    sym = binance_spot_symbol(base)
    url = "https://api.binance.com/api/v3/ticker/price"
    r = requests.get(url, params={"symbol": sym}, timeout=8)
    r.raise_for_status()
    return float(r.json()["price"])

def fetch_last_price_futures(base: str) -> float:
    sym = binance_futures_symbol(base)
    url = "https://fapi.binance.com/fapi/v1/ticker/price"
    r = requests.get(url, params={"symbol": sym}, timeout=8)
    r.raise_for_status()
    return float(r.json()["price"])

# =========================
# Altrady Payload
# =========================

def build_altrady_payload(parsed: dict, order_type: str = "limit") -> dict:
    payload = {
        "api_key": ALTRADY_API_KEY,
        "api_secret": ALTRADY_API_SECRET,
        "exchange": ALTRADY_EXCHANGE,
        "action": "open",
        "symbol": parsed["symbol"],
        "side": parsed["side"],
        "order_type": order_type,
        "leverage": parsed["leverage"],
        "take_profit": [
            {"price": parsed["tp1"], "position_percentage": TP1_PCT},
            {"price": parsed["tp2"], "position_percentage": TP2_PCT}
        ],
        "stop_loss": {"stop_price": parsed["sl"], "protection_type": "BREAK_EVEN"},
        "entry_expiration": {"time": ENTRY_EXPIRATION_MIN}
    }
    if order_type == "limit":
        payload["signal_price"] = parsed["entry"]
    return payload

# =========================
# Wait-for-Touch (Futures) + Basis-Adjust
# =========================

def clamp_adj_factor(f: float, cap_pct: float) -> float:
    cap = cap_pct / 100.0
    lo, hi = 1.0 - cap, 1.0 + cap
    return max(lo, min(hi, f))

def apply_basis_adjust_once(parsed: dict, spot_last: float, fut_last: float):
    if BASIS_MODE != "adjust": return parsed  # no change
    if spot_last <= 0 or fut_last <= 0: return parsed
    factor = clamp_adj_factor(fut_last/spot_last, BASIS_MAX_PCT)
    adj = dict(parsed)
    adj["entry"] = round_tick(parsed["base"], parsed["entry"] * factor)
    adj["tp1"]   = round_tick(parsed["base"], parsed["tp1"]   * factor)
    adj["tp2"]   = round_tick(parsed["base"], parsed["tp2"]   * factor)
    adj["sl"]    = round_tick(parsed["base"], parsed["sl"]    * factor)
    return adj

def should_wait_for_touch(side: str, fut_last: float, entry: float, tol_abs: float) -> bool:
    if side == "long":
        return fut_last < (entry - tol_abs)
    else:
        return fut_last > (entry + tol_abs)

def wait_for_touch_and_send(parsed_in: dict) -> bool:
    base  = parsed_in["base"]
    side  = parsed_in["side"]

    # Live-Preise
    try:
        spot_last = fetch_last_price_spot(base)
        fut_last  = fetch_last_price_futures(base)
    except Exception as ex:
        print(f"[TOUCH] Preis-Error {base}: {ex} → FAIL-OPEN Limit @ Entry (ohne Adjust)")
        payload = build_altrady_payload(parsed_in, order_type="limit")
        post_to_altrady(payload)
        return True

    # Optional: Basis-Adjust (Spot→Futures)
    parsed = apply_basis_adjust_once(parsed_in, spot_last, fut_last) if BASIS_MODE=="adjust" else dict(parsed_in)

    entry = parsed["entry"]
    tol_abs = entry * (ENTRY_TOL_PCT/100.0)

    # Sofort oder warten?
    if not should_wait_for_touch(side, fut_last, entry, tol_abs):
        print(f"[TOUCH] Kein Warten nötig ({base}) fut={fut_last} entry={entry}")
        payload = build_altrady_payload(parsed, order_type="limit")
        post_to_altrady(payload)
        return True

    # Warten bis Touch (Futures)
    print(f"[TOUCH] Warten auf Futures-Touch ({base}) fut={fut_last} entry={entry} tol={tol_abs:.10f}...")
    t0 = time.time()
    while time.time() - t0 <= ENTRY_WAIT_MAX_SEC:
        time.sleep(max(0.5, ENTRY_POLL_SEC))
        try:
            fut_last = fetch_last_price_futures(base)
        except Exception as ex:
            print(f"[TOUCH] Futures-Preis-Error ({base}): {ex}")
            continue

        if side == "long":
            if fut_last >= (entry - tol_abs):
                print(f"[TOUCH] LONG Touch ({base}) fut={fut_last} ~ entry={entry}")
                otype = ENTRY_TOUCH_ORDER_TYPE if ENTRY_TOUCH_ORDER_TYPE in ("market","limit") else "limit"
                payload = build_altrady_payload(parsed, order_type=otype)
                post_to_altrady(payload)
                return True
        else:
            if fut_last <= (entry + tol_abs):
                print(f"[TOUCH] SHORT Touch ({base}) fut={fut_last} ~ entry={entry}")
                otype = ENTRY_TOUCH_ORDER_TYPE if ENTRY_TOUCH_ORDER_TYPE in ("market","limit") else "limit"
                payload = build_altrady_payload(parsed, order_type=otype)
                post_to_altrady(payload)
                return True

    print(f"[TOUCH] Timeout ({base}) – Entry nicht erreicht.")
    return False

# =========================
# Send to Altrady
# =========================

def post_to_altrady(payload: dict):
    for attempt in range(3):
        try:
            r = requests.post(ALTRADY_WEBHOOK_URL, json=payload, timeout=20)
            if r.status_code == 429:
                delay = 2.0
                try: delay = float(r.json().get("retry_after", 2.0))
                except Exception: pass
                time.sleep(delay + 0.25); continue
            r.raise_for_status()
            return r
        except Exception:
            if attempt == 2: raise
            time.sleep(1.5*(attempt+1))

# =========================
# MAIN
# =========================

def main():
    print(
        "➡️ Exch:{ex} | Quote:{q} | MaxLev:{ml} | Safety%:{s} | TP%:{t1}/{t2} | Exp:{exp}m | "
        "Leg: {lf} max={lmax} zigzag={zz}% lookback={lb} trend={tm} | "
        "Touch: wait={wait}s poll={poll}s tol={tol}% type={otype} | Basis:{basis} cap={bcap}%".format(
            ex=ALTRADY_EXCHANGE, q=QUOTE, ml=MAX_LEVERAGE, s=SAFETY_PCT,
            t1=TP1_PCT, t2=TP2_PCT, exp=ENTRY_EXPIRATION_MIN,
            lf=("ON" if LEG_FILTER else "OFF"), lmax=LEG_MAX, zz=LEG_ZIGZAG_PCT,
            lb=LEG_MAX_LOOKBACK, tm=("REQ" if LEG_REQUIRE_TREND_MATCH else "NO-REQ"),
            wait=ENTRY_WAIT_MAX_SEC, poll=ENTRY_POLL_SEC, tol=ENTRY_TOL_PCT,
            otype=ENTRY_TOUCH_ORDER_TYPE.upper(), basis=BASIS_MODE.upper(), bcap=BASIS_MAX_PCT
        )
    )
    state = load_state(); last_id = state.get("last_id")

    while True:
        try:
            msg = fetch_latest_message(CHANNEL_ID)
            if msg:
                mid = msg.get("id")
                if last_id is None or int(mid) > int(last_id):
                    blocks = extract_signal_blocks(msg)
                    i
