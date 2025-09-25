#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, json, sys, traceback, re
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

# Altrady Signal Bot Webhook URL (direkt posten, kein Zapier)
ALTRADY_WEBHOOK_URL = os.getenv("ALTRADY_WEBHOOK_URL", "").strip()

# Altrady API Credentials + Exchange/Quote
ALTRADY_API_KEY    = os.getenv("ALTRADY_API_KEY", "").strip()
ALTRADY_API_SECRET = os.getenv("ALTRADY_API_SECRET", "").strip()
ALTRADY_EXCHANGE   = os.getenv("ALTRADY_EXCHANGE", "BIFU").strip()  # z.B. BIFU, BYBIF
QUOTE              = os.getenv("QUOTE", "USDT").strip().upper()

# Dynamische Leverage-Berechnung (floor(SAFETY_PCT / SL%)), gedeckelt durch MAX_LEVERAGE
MAX_LEVERAGE = int(os.getenv("MAX_LEVERAGE", "75"))
SAFETY_PCT   = float(os.getenv("SAFETY_PCT", "80"))

# Polling (Tick mit Offset, damit alle X Sekunden + Offset geprüft wird)
POLL_BASE   = int(os.getenv("POLL_BASE_SECONDS", "60"))   # Standard 60s
POLL_OFFSET = int(os.getenv("POLL_OFFSET_SECONDS", "3"))  # z.B. :03, :63, ...

STATE_FILE  = Path(os.getenv("STATE_FILE", "state.json"))

if not DISCORD_TOKEN or not CHANNEL_ID or not ALTRADY_WEBHOOK_URL:
    print("Bitte ENV setzen: DISCORD_TOKEN, CHANNEL_ID, ALTRADY_WEBHOOK_URL (und Altrady Keys).")
    sys.exit(1)

HEADERS = {
    # Discord erwartet i.d.R. 'Bot <token>' – falls schon 'Bot ' oder 'Bearer ' vorhanden, nichts doppeln.
    "Authorization": (
        DISCORD_TOKEN if DISCORD_TOKEN.startswith(("Bot ", "Bearer "))
        else f"Bot {DISCORD_TOKEN}"
    ),
    "User-Agent": "DiscordToAltrady/1.0 (+github.com/yourrepo)"
}

# =========================
# Utils: State + Timing
# =========================

def load_state():
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
    sleep_s = max(0, next_tick - now)
    time.sleep(sleep_s)

# =========================
# Discord: nur neueste Message
# =========================

def fetch_latest_message(channel_id: str):
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    params = {"limit": 1}
    r = requests.get(url, headers=HEADERS, params=params, timeout=15)
    if r.status_code == 429:
        retry = 5
        try:
            retry = r.json().get("retry_after", 5)
        except Exception:
            pass
        time.sleep(float(retry) + 0.5)
        r = requests.get(url, headers=HEADERS, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data[0] if data else None  # neueste zuerst

# =========================
# Parsing-Helfer (Text aus Discord-Message ziehen)
# =========================

def extract_text_from_msg(msg: dict) -> str:
    """
    Nimmt – wie bei dir – primär embeds[0].description (nur erster Block),
    sonst content (erster Block). Timeframe wird ignoriert (nicht benötigt).
    """
    def first_block(s: str) -> str:
        parts = re.split(r"\n\s*\n", (s or "").strip())
        return (parts[0] if parts else s or "").strip()

    content = (msg.get("content") or "").strip()
    embeds  = msg.get("embeds") or []
    desc = ""
    if embeds and isinstance(embeds, list):
        e0 = embeds[0] or {}
        desc = (e0.get("description") or "").strip()
    base_text = first_block(desc if desc else content)
    return base_text

# =========================
# Signal-Parser (exakt dein Format)
# Beispiele (aus deiner JS-Logik):
# BUY 📈 on SHIB/USD at Price: 0.00001256
# TP 1: 0.00001264
# TP 2: 0.00001273
# SL :  0.00001238
# =========================

# Ticksize-Map analog deiner JS-Version
TICK_MAP = {
    "SHIB": 8, "1000SHIB": 8, "DOGE": 5, "XRP": 4, "SOL": 2, "AVAX": 3, "AAVE": 2, "LINK": 3,
    "BTC": 2, "ETH": 2, "BNB": 2, "LTC": 2, "ADA": 5, "MATIC": 5, "EOS": 4, "BCH": 2,
    "ATOM": 3, "ALGO": 5,
    "LUNA2": 3
}

def round_tick(sym: str, v: float) -> float:
    d = TICK_MAP.get(sym, 4)
    p = 10 ** d
    return round(v * p) / p

SIG_SIDE = re.compile(r"\b(BUY|SELL)\b", re.I)
SIG_PAIR = re.compile(r"on\s+([A-Z0-9]+)[/\-]([A-Z0-9]+)", re.I)
NUM      = r"([0-9]*\.?[0-9]+)"
SIG_ENTRY= re.compile(rf"Price:\s*{NUM}", re.I)
SIG_TP1  = re.compile(rf"TP\s*1:\s*{NUM}", re.I)
SIG_TP2  = re.compile(rf"TP\s*2:\s*{NUM}", re.I)
SIG_SL   = re.compile(rf"\bSL\s*:\s*{NUM}", re.I)

def parse_signal_text(text: str) -> dict:
    t = text.replace("\r", "").strip()

    m_side = SIG_SIDE.search(t);   assert m_side, "BUY/SELL nicht gefunden."
    m_pair = SIG_PAIR.search(t);   assert m_pair, "Paar (z. B. SOL/USD) nicht gefunden."
    m_e    = SIG_ENTRY.search(t);  assert m_e,    "Entry nicht gefunden."
    m_tp1  = SIG_TP1.search(t);    assert m_tp1,  "TP1 nicht gefunden."
    m_tp2  = SIG_TP2.search(t);    assert m_tp2,  "TP2 nicht gefunden."
    m_sl   = SIG_SL.search(t);     assert m_sl,   "SL nicht gefunden."

    side_raw = m_side.group(1).upper()
    side = "long" if side_raw == "BUY" else "short"

    base = m_pair.group(1).upper()
    quoted = m_pair.group(2).upper()
    if quoted == "USD": quoted = "USDT"

    # Spezielle Mappings
    if base == "LUNA": base = "LUNA2"
    if base == "SHIB": base = "1000SHIB"

    entry = float(m_e.group(1))
    tp1   = float(m_tp1.group(1))
    tp2   = float(m_tp2.group(1))
    sl    = float(m_sl.group(1))

    # Plausibilität
    if side == "long" and not (sl < entry and tp1 > entry and tp2 > entry):
        raise ValueError("Long: TP/SL liegen nicht plausibel zum Entry.")
    if side == "short" and not (sl > entry and tp1 < entry and tp2 < entry):
        raise ValueError("Short: TP/SL liegen nicht plausibel zum Entry.")

    # SL-% & dynamische Leverage
    sl_pct = ((entry - sl) / entry * 100.0) if side == "long" else ((sl - entry) / entry * 100.0)
    lev = int(SAFETY_PCT // max(sl_pct, 1e-12))
    if lev < 1: lev = 1
    if lev > MAX_LEVERAGE: lev = MAX_LEVERAGE

    # Symbol-Format für Altrady: EXCHANGE_QUOTE_BASE (z.B. BIFU_USDT_1000SHIB)
    symbol = f"{ALTRADY_EXCHANGE}_{QUOTE}_{base}"

    # Tick-Rundung
    entry = round_tick(base, entry)
    tp1   = round_tick(base, tp1)
    tp2   = round_tick(base, tp2)
    sl    = round_tick(base, sl)

    return {
        "side": side,
        "base": base,
        "quote_from_signal": quoted,  # reine Info
        "entry": entry,
        "tp1": tp1,
        "tp2": tp2,
        "sl": sl,
        "sl_pct": float(f"{sl_pct:.6f}"),
        "leverage": lev,
        "symbol": symbol
    }

# =========================
# Build Payload (EXAKTES Format wie gewünscht)
# =========================

def build_altrady_payload(parsed: dict) -> dict:
    """
    Gibt das Ziel-JSON exakt in deiner gewünschten Struktur zurück:
    {
      "api_key": "...",
      "api_secret": "...",
      "exchange": "BIFU",
      "action": "open",
      "symbol": "...",
      "side": "long",
      "order_type": "limit",
      "signal_price": <entry>,
      "leverage": <lev>,
      "take_profit": [
        {"price": <tp1>, "position_percentage": 20},
        {"price": <tp2>, "position_percentage": 80}
      ],
      "stop_loss": {"stop_price": <sl>, "protection_type": "BREAK_EVEN"},
      "entry_expiration": {"time": 15}
    }
    """
    return {
        "api_key": ALTRADY_API_KEY,
        "api_secret": ALTRADY_API_SECRET,
        "exchange": ALTRADY_EXCHANGE,
        "action": "open",
        "symbol": parsed["symbol"],
        "side": parsed["side"],                 # "long" | "short"
        "order_type": "limit",
        "signal_price": parsed["entry"],        # Limit-Entry aus Signal
        "leverage": parsed["leverage"],         # dynamisch berechnet
        "take_profit": [
            {"price": parsed["tp1"], "position_percentage": 20},
            {"price": parsed["tp2"], "position_percentage": 80}
        ],
        "stop_loss": {
            "stop_price": parsed["sl"],
            "protection_type": "BREAK_EVEN"
        },
        "entry_expiration": {"time": 15}
    }

# =========================
# Senden an Altrady Webhook
# =========================

def post_to_altrady(payload: dict):
    # 3 einfache Retries (Rate-Limits / temporäre Fehler)
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
        except Exception as ex:
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))

# =========================
# MAIN LOOP
# =========================

def main():
    print(f"Getaktet: alle {POLL_BASE}s, jeweils +{POLL_OFFSET}s Offset (z. B. 10:00:{POLL_OFFSET:02d})")
    print(f"➡️  Exchange: {ALTRADY_EXCHANGE} | Quote: {QUOTE} | MaxLev: {MAX_LEVERAGE} | Safety%: {SAFETY_PCT}")
    state = load_state()
    last_id = state.get("last_id")

    while True:
        try:
            msg = fetch_latest_message(CHANNEL_ID)
            if msg:
                mid = msg.get("id")
                if last_id is None or int(mid) > int(last_id):
                    raw_text = extract_text_from_msg(msg)
                    if not raw_text:
                        print("[skip] leere Nachricht.")
                    else:
                        parsed = parse_signal_text(raw_text)
                        payload = build_altrady_payload(parsed)
                        res = post_to_altrady(payload)
                        ts = datetime.now().strftime("%H:%M:%S")
                        print(f"[{ts}] ✅ gesendet | {parsed['symbol']} | {parsed['side']} | entry={parsed['entry']} lev={parsed['leverage']}")
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
        except AssertionError as aex:
            # Parser-Fehler (fehlende Felder)
            print("[PARSE ERROR]", str(aex))
        except Exception:
            print("[ERROR]")
            traceback.print_exc()

        sleep_until_next_tick()

if __name__ == "__main__":
    main()
