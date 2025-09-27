#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Discord → Altrady Signal-Forwarder (Multi-Block + Leg-Filter + Wait-for-Touch)
- Extrahiert ALLE Signal-Blöcke (BUY/SELL …) aus der neuesten Discord-Message
- Parst: BUY/SELL, on BASE/QUOTE, Price, TP1, TP2, SL
- Nur */USD bzw. */USDT (alles andere, z. B. */BTC, wird übersprungen)
- Mappings: LUNA→LUNA2 (für Altrady), SHIB→1000SHIB, USD→USDT
- Tick-Rundung (Tickmap)
- Leverage: floor(SAFETY_PCT / SL%), capped mit MAX_LEVERAGE + coin-spezifischen Caps (z. B. LUNA2=50)
- TP-Splits aus ENV (TP_SPLITS="x,y", Summe 100)
- Entry-Expiration (Minuten) aus ENV

NEU:
- Optionaler Leg-Filter via Binance-Klines + ZigZag → nur Leg 1–2 (und optional Trend-Match)
- Wait-for-Touch Guard:
  * Wenn Preis auf „falscher Seite“ des Entry ist → warte bis Markt Entry berührt (±Toleranz)
  * bei Touch: Standard MARKET-Order (oder via ENV Limit)
  * Timeout nach ENTRY_WAIT_MAX_SEC
"""

import os
import re
import sys
import time
import json
import math
import traceback
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
    "LUNA":  int(os.getenv("LEV_MAX_LUNA",  "50")),  # falls mal vor Mapping geprüft wird
}

TP_SPLITS_RAW = os.getenv("TP_SPLITS", "20,80").strip()
ENTRY_EXPIRATION_MIN = int(os.getenv("ENTRY_EXPIRATION_MIN", "15"))

POLL_BASE   = int(os.getenv("POLL_BASE_SECONDS", "60"))
POLL_OFFSET = int(os.getenv("POLL_OFFSET_SECONDS", "3"))
STATE_FILE  = Path(os.getenv("STATE_FILE", "state.json"))

# LEG-FILTER ENVs
LEG_FILTER              = os.getenv("LEG_FILTER", "off").lower() == "on"
LEG_TIMEFRAME_DEFAULT   = os.getenv("LEG_TIMEFRAME_DEFAULT", "M5").upper()
LEG_ZIGZAG_PCT          = float(os.getenv("LEG_ZIGZAG_PCT", "1.0"))
LEG_MAX_LOOKBACK        = int(os.getenv("LEG_MAX_LOOKBACK", "400"))
LEG_REQUIRE_TREND_MATCH = os.getenv("LEG_REQUIRE_TREND_MATCH", "on").lower() == "on"
LEG_FAIL_MODE           = os.getenv("LEG_FAIL_MODE", "skip").lower()  # "skip" | "open"

TF_MAP = {"M5": "5m", "M15": "15m", "H1": "1h", "1D": "1d"}

# WAIT-FOR-TOUCH ENVs
ENTRY_WAIT_MAX_SEC      = int(os.getenv("ENTRY_WAIT_MAX_SEC", "900"))   # 15 min
ENTRY_POLL_SEC          = float(os.getenv("ENTRY_POLL_SEC", "3"))
ENTRY_TOL_PCT           = float(os.getenv("ENTRY_TOL_PCT", "0.02"))     # % um Entry
ENTRY_TOUCH_ORDER_TYPE  = os.getenv("ENTRY_TOUCH_ORDER_TYPE", "market").lower()  # "market" | "limit"

# Sanity-Check
if not DISCORD_TOKEN or not CHANNEL_ID or not ALTRADY_WEBHOOK_URL:
    print("Bitte ENV setzen: DISCORD_TOKEN, CHANNEL_ID, ALTRADY_WEBHOOK_URL (und Altrady Keys).")
    sys.exit(1)

HEADERS = {
    "Authorization": DISCORD_TOKEN,   # User-Session
    "User-Agent": "DiscordToAltrady/1.4"
}

# =========================
# Helpers & Exceptions
# =========================

class SkipSignal(Exception):
    """Gezielt überspringen (z. B. Non-USD-Quote, Leg > 2, RR/SL-Filter, Timeout, etc.)."""

def parse_tp_splits(raw: str) -> tuple[int, int]:
    try:
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) != 2:
            raise ValueError
        a, b = int(float(parts[0])), int(float(parts[1]))
        if a <= 0 or b <= 0 or a > 100 or b > 100 or a + b != 100:
            raise ValueError
        return a, b
    except Exception:
        print(f"[WARN] Ungültige TP_SPLITS='{raw}', verwende Fallback 20,80.")
        return 20, 80

TP1_PCT, TP2_PCT = parse_tp_splits(TP_SPLITS_RAW)

# =========================
# Utils: State + Timing
# =========================

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
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
# Discord: neueste Nachricht
# =========================

def fetch_latest_message(channel_id: str):
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    params = {"limit": 1}
    r = requests.get(url, headers=HEADERS, params=params, timeout=15)
    if r.status_code == 429:
        retry = 5
        try:
            retry = float(r.json().get("retry_after", 5))
        except Exception:
            pass
        time.sleep(retry + 0.5)
        r = requests.get(url, headers=HEADERS, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data[0] if data else None  # neueste zuerst

# =========================
# Text-Extraktion: ALLE Signal-Blöcke
# =========================

BUYSELL_LINE = re.compile(r"(?im)^\s*.*\b(BUY|SELL)\b.*$")

def extract_signal_blocks(msg: dict) -> list[str]:
    def source_text() -> str:
        parts = []
        content = (msg.get("content") or "").replace("\r", "")
        parts.append(content)
        embeds = msg.get("embeds") or []
        if embeds and isinstance(embeds, list):
            e0 = embeds[0] or {}
            desc = (e0.get("description") or "").replace("\r", "")
            if desc:
                parts.append(desc)
        return "\n".join([p for p in parts if p]).strip()

    raw = source_text()
    if not raw:
        return []

    starts = [m.start() for m in BUYSELL_LINE.finditer(raw)]
    if not starts:
        return []

    blocks = []
    for i, s in enumerate(starts):
        tail = raw[s:]
        if i + 1 < len(starts):
            nxt = starts[i+1] - s
            chunk = tail[:nxt]
        else:
            chunk = tail

        m_blank = re.search(r"\n\s*\n", chunk)
        if m_blank:
            chunk = chunk[:m_blank.start()]

        chunk = re.sub(r"(?im)^\s*Timeframe:.*$", "", chunk).strip()

        if BUYSELL_LINE.search(chunk):
            blocks.append(chunk)

    return blocks

def find_timeframe_in_msg(msg: dict) -> str:
    parts = []
    content = msg.get("content") or ""
    parts.append(content)
    embeds = msg.get("embeds") or []
    if embeds and isinstance(embeds, list):
        e0 = embeds[0] or {}
        desc = e0.get("description") or ""
        parts.append(desc)
        f = e0.get("footer") or {}
        ft = f.get("text") if isinstance(f, dict) else ""
        if ft: parts.append(ft)
    txt = "\n".join(parts)
    m = re.search(r"Timeframe:\s*(M5|M15|H1|1D)", txt, re.I)
    if m:
        return m.group(1).upper()
    return LEG_TIMEFRAME_DEFAULT

# =========================
# Parser
# =========================

TICK_MAP = {
    "SHIB": 8, "1000SHIB": 8, "DOGE": 5, "XRP": 4, "SOL": 2, "AVAX": 3, "AAVE": 2, "LINK": 3,
    "BTC": 2, "ETH": 2, "BNB": 2, "LTC": 2, "ADA": 5, "MATIC": 5, "EOS": 4, "BCH": 2,
    "ATOM": 3, "ALGO": 5, "LUNA2": 3
}

def round_tick(sym: str, v: float) -> float:
    d = TICK_MAP.get(sym, 4)
    p = 10 ** d
    return round(v * p) / p

SIG_SIDE  = re.compile(r"\b(BUY|SELL)\b", re.I)
SIG_PAIR  = re.compile(r"on\s+([A-Z0-9]+)[/\-]([A-Z0-9]+)", re.I)
NUM       = r"([0-9]*\.?[0-9]+)"
SIG_ENTRY = re.compile(rf"Price:\s*{NUM}", re.I)
SIG_TP1   = re.compile(rf"TP\s*1:\s*{NUM}", re.I)
SIG_TP2   = re.compile(rf"TP\s*2:\s*{NUM}", re.I)
SIG_SL    = re.compile(rf"\bSL\s*:\s*{NUM}", re.I)

def parse_signal_text(text: str) -> dict:
    t = (text or "").replace("\r", "").strip()
    if not t:
        raise AssertionError("Leerer Signaltext.")

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
    if quoted_raw not in ("USD", "USDT"):
        raise SkipSignal(f"Non-USD Quote erkannt: {base}/{quoted_raw}")

    quoted = "USDT"

    if base == "LUNA":
        base = "LUNA2"
    if base == "SHIB":
        base = "1000SHIB"

    entry = float(m_e.group(1))
    tp1   = float(m_tp1.group(1))
    tp2   = float(m_tp2.group(1))
    sl    = float(m_sl.group(1))

    if side == "long" and not (sl < entry and tp1 > entry and tp2 > entry):
        raise ValueError("Long: TP/SL liegen nicht plausibel zum Entry.")
    if side == "short" and not (sl > entry and tp1 < entry and tp2 < entry):
        raise ValueError("Short: TP/SL liegen nicht plausibel zum Entry.")

    sl_pct = ((entry - sl) / entry * 100.0) if side == "long" else ((sl - entry) / entry * 100.0)
    lev = int(SAFETY_PCT // max(sl_pct, 1e-12))
    if lev < 1: lev = 1
    if lev > MAX_LEVERAGE: lev = MAX_LEVERAGE
    coin_cap = COIN_LEV_CAPS.get(base)
    if coin_cap is not None and lev > coin_cap:
        lev = coin_cap

    symbol = f"{ALTRADY_EXCHANGE}_{QUOTE}_{base}"

    entry = round_tick(base, entry)
    tp1   = round_tick(base, tp1)
    tp2   = round_tick(base, tp2)
    sl    = round_tick(base, sl)

    return {
        "side": side,
        "base": base,
        "quote_from_signal": quoted,
        "entry": entry,
        "tp1": tp1,
        "tp2": tp2,
        "sl": sl,
        "sl_pct": float(f"{sl_pct:.6f}"),
        "leverage": lev,
        "symbol": symbol
    }

# =========================
# Leg-Filter (Binance-Klines + ZigZag)
# =========================

def market_base_for_data(base: str) -> str:
    if base == "LUNA2":
        return "LUNA"
    return base

def fetch_klines_binance_spot(base: str, quote: str, interval: str, limit: int):
    sym = f"{base}{quote}"
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": sym, "interval": interval, "limit": limit}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    out = []
    for k in data:
        o,h,l,c = float(k[1]), float(k[2]), float(k[3]), float(k[4])
        out.append((o,h,l,c))
    return out

def zigzag_pivots(closes: list[float], pct: float) -> list[int]:
    if not closes: return []
    thr = pct / 100.0
    piv = []
    last_pivot_i = 0
    last_pivot_val = closes[0]
    direction = 0  # 1=up, -1=down, 0=unknown

    for i in range(1, len(closes)):
        up_change   = (closes[i] - last_pivot_val) / last_pivot_val
        down_change = (last_pivot_val - closes[i]) / last_pivot_val

        if direction >= 0:
            if up_change >= thr:
                piv.append(last_pivot_i); direction = 1
                last_pivot_i = i; last_pivot_val = closes[i]
            elif closes[i] < last_pivot_val:
                last_pivot_i = i; last_pivot_val = closes[i]

        if direction <= 0:
            if down_change >= thr:
                piv.append(last_pivot_i); direction = -1
                last_pivot_i = i; last_pivot_val = closes[i]
            elif closes[i] > last_pivot_val:
                last_pivot_i = i; last_pivot_val = closes[i]

    if last_pivot_i not in piv:
        piv.append(last_pivot_i)
    return sorted(set(piv))

def infer_trend_and_leg(closes: list[float], pivots: list[int]) -> tuple[str,int]:
    if len(pivots) < 3:
        return "unknown", 1
    recent = pivots[-10:]
    last, prev = recent[-1], recent[-2]
    trend = "up" if closes[last] > closes[prev] else "down"
    start = recent[0]
    for i in range(2, len(recent)):
        a,b,c = recent[i-2], recent[i-1], recent[i]
        if trend == "up":
            if closes[a] < closes[b] and closes[c] > closes[b]:
                start = b; break
        else:
            if closes[a] > closes[b] and closes[c] < closes[b]:
                start = b; break
    count = sum(1 for p in recent if p >= start)
    leg_idx = max(1, min(5, count))
    return trend, leg_idx

def enforce_leg_filter(parsed: dict, msg: dict):
    if not LEG_FILTER:
        return
    tf = find_timeframe_in_msg(msg)  # M5/M15/H1/1D
    interval = TF_MAP.get(tf, TF_MAP[LEG_TIMEFRAME_DEFAULT])
    market_base = market_base_for_data(parsed["base"])
    try:
        kl = fetch_klines_binance_spot(market_base, "USDT", interval, min(LEG_MAX_LOOKBACK, 500))
        closes = [c for (_,_,_,c) in kl]
        piv = zigzag_pivots(closes, LEG_ZIGZAG_PCT)
        trend, leg_idx = infer_trend_and_leg(closes, piv)
        if LEG_REQUIRE_TREND_MATCH and trend in ("up","down"):
            if parsed["side"] == "long" and trend != "up":
                raise SkipSignal(f"Trend-Mismatch: side=long, trend={trend}")
            if parsed["side"] == "short" and trend != "down":
                raise SkipSignal(f"Trend-Mismatch: side=short, trend={trend}")
        if leg_idx > 2:
            raise SkipSignal(f"Leg-Filter: aktueller Leg {leg_idx} > 2 ({trend})")
        print(f"[LEG] tf={tf} interval={interval} trend={trend} leg={leg_idx} base={market_base}")
    except SkipSignal:
        raise
    except Exception as ex:
        msg_txt = f"Leg-Filter Fehler: {ex.__class__.__name__}: {ex}"
        if LEG_FAIL_MODE == "skip":
            raise SkipSignal(msg_txt)
        else:
            print(f"[LEG WARN] {msg_txt} → FAIL-OPEN (Signal wird trotzdem ausgeführt)")

# =========================
# Binance Price Helpers (für Wait-for-Touch)
# =========================

def binance_symbol_for_price(base: str) -> str:
    b = market_base_for_data(base)
    return f"{b}USDT"

def fetch_last_price_binance(base: str) -> float:
    sym = binance_symbol_for_price(base)
    url = "https://api.binance.com/api/v3/ticker/price"
    r = requests.get(url, params={"symbol": sym}, timeout=8)
    r.raise_for_status()
    return float(r.json()["price"])

# =========================
# Payload für Altrady
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
        "stop_loss": {
            "stop_price": parsed["sl"],
            "protection_type": "BREAK_EVEN"
        },
        "entry_expiration": {"time": ENTRY_EXPIRATION_MIN}
    }
    # Nur bei Limit darf/muss signal_price mit:
    if order_type == "limit":
        payload["signal_price"] = parsed["entry"]
    return payload

# =========================
# Wait-for-Touch Guard
# =========================

def should_wait_for_touch(side: str, last: float, entry: float, tol_abs: float) -> bool:
    """
    Long: wenn Markt deutlich UNTER Entry ist → warten
    Short: wenn Markt deutlich ÜBER Entry ist → warten
    """
    if side == "long":
        return last < (entry - tol_abs)
    else:
        return last > (entry + tol_abs)

def wait_for_touch_and_send(parsed: dict) -> bool:
    """
    Wartet bis der Markt den Entry „berührt“ (±Toleranz) – max ENTRY_WAIT_MAX_SEC.
    - Wenn Warten NICHT nötig: sendet sofort Limit-Order @ Entry
    - Wenn Warten nötig und Touch erfolgt: sendet Order (Default MARKET, per ENV konfigurierbar)
    - Bei Timeout: False (skip)
    """
    entry = parsed["entry"]
    side  = parsed["side"]
    base  = parsed["base"]

    tol_abs = entry * (ENTRY_TOL_PCT / 100.0)

    try:
        last = fetch_last_price_binance(base)
    except Exception as ex:
        print(f"[TOUCH] Preisabfrage-Fehler ({base}): {ex} → FAIL-OPEN mit Limit")
        # wenn Preis nicht abrufbar, lieber „safe“ sofort Limit @ Entry senden:
        payload = build_altrady_payload(parsed, order_type="limit")
        post_to_altrady(payload)
        return True

    # Entscheiden, ob wir warten müssen
    if not should_wait_for_touch(side, last, entry, tol_abs):
        # Sofort: Limit @ Entry (klassisch, liegt Preis auf „richtiger“ Seite)
        print(f"[TOUCH] Kein Warten nötig ({base}) last={last} entry={entry}")
        payload = build_altrady_payload(parsed, order_type="limit")
        post_to_altrady(payload)
        return True

    # Warten bis Touch
    print(f"[TOUCH] Warten auf Entry-Touch ({base}) last={last} entry={entry} tol={tol_abs:.8f} ...")
    t0 = time.time()
    while time.time() - t0 <= ENTRY_WAIT_MAX_SEC:
        time.sleep(max(0.5, ENTRY_POLL_SEC))
        try:
            last = fetch_last_price_binance(base)
        except Exception as ex:
            print(f"[TOUCH] Preis-Error ({base}): {ex}")
            continue

        if side == "long":
            if last >= (entry - tol_abs):
                print(f"[TOUCH] LONG-Touch erkannt ({base}) last={last} ~ entry={entry}")
                order_type = ENTRY_TOUCH_ORDER_TYPE if ENTRY_TOUCH_ORDER_TYPE in ("market","limit") else "market"
                payload = build_altrady_payload(parsed, order_type=order_type)
                post_to_altrady(payload)
                return True
        else:  # short
            if last <= (entry + tol_abs):
                print(f"[TOUCH] SHORT-Touch erkannt ({base}) last={last} ~ entry={entry}")
                order_type = ENTRY_TOUCH_ORDER_TYPE if ENTRY_TOUCH_ORDER_TYPE in ("market","limit") else "market"
                payload = build_altrady_payload(parsed, order_type=order_type)
                post_to_altrady(payload)
                return True

    # Timeout
    print(f"[TOUCH] Timeout ({base}) – Entry nicht erreicht. Trade verworfen.")
    return False

# =========================
# Senden an Altrady Webhook
# =========================

def post_to_altrady(payload: dict):
    for attempt in range(3):
        try:
            r = requests.post(ALTRADY_WEBHOOK_URL, json=payload, timeout=20)
            if r.status_code == 429:
                delay = 2.0
                try:
                    delay = float(r.json().get("retry_after", 2.0))
                except Exception:
                    pass
                time.sleep(delay + 0.25)
                continue
            r.raise_for_status()
            return r
        except Exception:
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))

# =========================
# MAIN LOOP
# =========================

def main():
    print(f"Getaktet: alle {POLL_BASE}s, jeweils +{POLL_OFFSET}s Offset")
    print(
        "➡️ Exchange: {ex} | Quote: {q} | MaxLev: {gcap} | Safety%: {s} | TP%: {t1}/{t2} | Exp: {exp}m "
        "| CoinCaps: {caps} | LegFilter: {lf}/{pct}%/{req} | Touch: {wait}s/{poll}s/{tol}%/{otype}".format(
            ex=ALTRADY_EXCHANGE, q=QUOTE, gcap=MAX_LEVERAGE, s=SAFETY_PCT,
            t1=TP1_PCT, t2=TP2_PCT, exp=ENTRY_EXPIRATION_MIN, caps=COIN_LEV_CAPS,
            lf=("ON" if LEG_FILTER else "OFF"), pct=LEG_ZIGZAG_PCT, req=("REQ" if LEG_REQUIRE_TREND_MATCH else "NO-REQ"),
            wait=ENTRY_WAIT_MAX_SEC, poll=ENTRY_POLL_SEC, tol=ENTRY_TOL_PCT, otype=ENTRY_TOUCH_ORDER_TYPE.upper()
        )
    )
    state = load_state()
    last_id = state.get("last_id")

    while True:
        try:
            msg = fetch_latest_message(CHANNEL_ID)
            if msg:
                mid = msg.get("id")
                if last_id is None or int(mid) > int(last_id):
                    blocks = extract_signal_blocks(msg)
                    if not blocks:
                        print("[skip] keine erkennbaren Signal-Blöcke.")
                        last_id = mid; state["last_id"] = last_id; save_state(state)
                    else:
                        print(f"[INFO] {len(blocks)} Signal-Block(s) gefunden.")
                        for idx, raw_text in enumerate(blocks, start=1):
                            dbg = re.sub(r"\s+", " ", raw_text)[:140]
                            print(f"[DBG] Block {idx}: {dbg!r}")
                            try:
                                parsed = parse_signal_text(raw_text)
                                enforce_leg_filter(parsed, msg)  # optional aktiv je nach ENV
                            except SkipSignal as sk:
                                ts = datetime.now().strftime("%H:%M:%S")
                                print(f"[{ts}] ⏭️ Block {idx} übersprungen: {sk}")
                                continue
                            except AssertionError as aex:
                                print(f"[PARSE ERROR] Block {idx}: {aex}")
                                continue
                            except Exception:
                                print(f"[ERROR] Block {idx} – unerwarteter Fehler:")
                                traceback.print_exc()
                                continue
                            else:
                                # >>> Wait-for-Touch Guard <<<
                                ok = wait_for_touch_and_send(parsed)
                                ts = datetime.now().strftime("%H:%M:%S")
                                if ok:
                                    print(f"[{ts}] ✅ Order platziert (Block {idx}) | {parsed['symbol']} | {parsed['side']} | entry={parsed['entry']} | lev={parsed['leverage']} | TP%={TP1_PCT}/{TP2_PCT}")
                                else:
                                    print(f"[{ts}] 🚫 Kein Entry (Block {idx}) – Touch nicht erfolgt.")

                        # Nachricht als verarbeitet markieren
                        last_id = mid
                        state["last_id"] = last_id
                        save_state(state)
                else:
                    ts = datetime.now().strftime("%H:%M:%S")
                    print(f"[{ts}] Keine neuere Nachricht.")
            else:
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] Kanal leer.")

        except KeyboardInterrupt:
            print("\nStopped.")
            break
        except requests.HTTPError as http_err:
            body = ""
            try:
                body = http_err.response.text[:200]
            except Exception:
                pass
            print("[HTTP ERROR]", http_err.response.status_code, body or "")
        except Exception:
            print("[ERROR]")
            traceback.print_exc()
            if 'msg' in locals() and msg:
                last_id = msg.get("id"); state["last_id"] = last_id; save_state(state)

        sleep_until_next_tick()

if __name__ == "__main__":
    main()
