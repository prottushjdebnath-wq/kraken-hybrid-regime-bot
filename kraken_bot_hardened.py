import time
import os
import json
import threading
import traceback
from datetime import datetime, timezone
import ccxt
import pandas as pd
import requests
from flask import Flask, jsonify, render_template_string

# =====================================================================
# 1. ENVIRONMENT CONFIGURATION & VALIDATION
# =====================================================================
PAPER_TRADING = os.environ.get("PAPER_TRADING", "true").strip().lower() in ("true", "1", "yes")
PAPER_INITIAL_BALANCE = float(os.environ.get("PAPER_INITIAL_BALANCE", "1000.0"))

API_KEY = os.environ.get("KRAKEN_API_KEY", "").strip()
SECRET_KEY = os.environ.get("KRAKEN_SECRET_KEY", "").strip()

UPSTASH_REDIS_REST_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip()
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()
REDIS_STATE_KEY = os.environ.get("REDIS_STATE_KEY", "kraken_bot_multi_state").strip()
REDIS_LEDGER_KEY = os.environ.get("REDIS_LEDGER_KEY", "kraken_bot_multi_ledger").strip()

ENABLE_TELEGRAM = os.environ.get("ENABLE_TELEGRAM", "false").strip().lower() in ("true", "1", "yes")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

# Candidate Pool Watchlist
SYMBOLS_RAW = os.environ.get("TRADING_SYMBOLS", "SOL/USDT,BTC/USDT,ETH/USDT").strip()
SYMBOLS = [s.strip() for s in SYMBOLS_RAW.split(",") if s.strip()]

TIMEFRAME = os.environ.get("TRADING_TIMEFRAME", "15m").strip()
ADX_TREND_THRESHOLD = float(os.environ.get("ADX_TREND_THRESHOLD", "25.0"))
ATR_MULTIPLIER = float(os.environ.get("ATR_MULTIPLIER", "1.5"))

# DYNAMIC RISK & PORTFOLIO SIZING PARAMETERS
RISK_PCT_PER_TRADE = float(os.environ.get("RISK_PCT_PER_TRADE", "0.015"))  # 1.5% equity risked per trade
RR_RATIO = float(os.environ.get("RR_RATIO", "2.0"))                       # 1:2 Risk to Reward Take-Profit
MIN_LEVERAGE = float(os.environ.get("MIN_LEVERAGE", "1.0"))
MAX_LEVERAGE = float(os.environ.get("MAX_LEVERAGE", "3.0"))               # Software-side hard cap (<= 5x max)
MAX_CONCURRENT_POSITIONS = int(os.environ.get("MAX_CONCURRENT_POSITIONS", "2"))
MAX_PORTFOLIO_MARGIN_PCT = float(os.environ.get("MAX_PORTFOLIO_MARGIN_PCT", "0.60")) # Max 60% of equity in margin

MAX_FAILURES_ALLOWED = int(os.environ.get("MAX_FAILURES_ALLOWED", "3"))
MIN_BALANCE_USDT = float(os.environ.get("MIN_BALANCE_USDT", "10.0"))

LOOP_INTERVAL_SECONDS = int(os.environ.get("LOOP_INTERVAL_SECONDS", "300"))
SERVER_PORT = int(os.environ.get("PORT", "10000"))

CSV_FILE_PATH = "kraken_bot_trade_ledger.csv"
STATE_FILE_PATH = "kraken_bot_state.json"

def validate_environment():
    """Fail-loud preflight check on mandatory runtime variables."""
    missing_vars = []
    if not PAPER_TRADING:
        if not API_KEY:
            missing_vars.append("KRAKEN_API_KEY")
        if not SECRET_KEY:
            missing_vars.append("KRAKEN_SECRET_KEY")
    if not UPSTASH_REDIS_REST_URL:
        missing_vars.append("UPSTASH_REDIS_REST_URL")
    if not UPSTASH_REDIS_REST_TOKEN:
        missing_vars.append("UPSTASH_REDIS_REST_TOKEN")
    if not SYMBOLS:
        missing_vars.append("TRADING_SYMBOLS")
    if ENABLE_TELEGRAM:
        if not TELEGRAM_BOT_TOKEN:
            missing_vars.append("TELEGRAM_BOT_TOKEN")
        if not TELEGRAM_CHAT_ID:
            missing_vars.append("TELEGRAM_CHAT_ID")

    if missing_vars:
        fault = f"FATAL: Missing mandatory environment variables: {', '.join(missing_vars)}"
        print(f"🛑 {fault}")
        raise RuntimeError(fault)

validate_environment()

# =====================================================================
# 2. SYNCHRONIZED RUNTIME STATE (MULTI-PAIR & MARGIN SCHEMA)
# =====================================================================
state_lock = threading.Lock()

positions = {}
for s in SYMBOLS:
    positions[s] = {
        "position_active": False,
        "entry_price": 0.0,
        "trailing_stop": 0.0,
        "stop_loss_distance": 0.0,
        "take_profit": 0.0,
        "leverage": 1.0,
        "units": 0.0,
        "margin_allocated": 0.0,
        "regime": "NONE",
        "confidence": 0.0
    }

virtual_balance_usdt = PAPER_INITIAL_BALANCE
consecutive_failures = 0

engine_status = {
    "engine_state": "NOT_STARTED",
    "last_loop_timestamp": None,
    "last_successful_loop_timestamp": None,
    "last_error": None,
    "halt_reason": None,
    "thread_alive": False,
    "startup_reconciled": False,
    "startup_timestamp": None,
    "paper_trading": PAPER_TRADING,
    "persistence_backend": "UPSTASH_REDIS_REST",
    "active_news_status": "MONITORING"
}

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def update_engine_status(**kwargs):
    with state_lock:
        engine_status.update(kwargs)

def get_status_snapshot():
    with state_lock:
        total_margin_used = sum(p["margin_allocated"] for p in positions.values() if p["position_active"])
        snapshot = dict(engine_status)
        snapshot.update({
            "mode": "PAPER_TRADING" if PAPER_TRADING else "LIVE_CAPITAL",
            "virtual_balance_usdt": virtual_balance_usdt if PAPER_TRADING else None,
            "total_margin_allocated": round(total_margin_used, 2),
            "tracked_symbols": SYMBOLS,
            "timeframe": TIMEFRAME,
            "positions": {sym: dict(data) for sym, data in positions.items()},
            "consecutive_failures": consecutive_failures,
            "risk_pct_per_trade": RISK_PCT_PER_TRADE,
            "rr_ratio": RR_RATIO,
            "max_leverage": MAX_LEVERAGE
        })
    return snapshot

# =====================================================================
# 3. INITIALIZE KRAKEN EXCHANGE ENGINE
# =====================================================================
exchange_params = {'enableRateLimit': True}
if API_KEY and SECRET_KEY:
    exchange_params['apiKey'] = API_KEY
    exchange_params['secret'] = SECRET_KEY

exchange = ccxt.kraken(exchange_params)

# =====================================================================
# 4. EXTERNAL STATE PERSISTENCE (UPSTASH REST ENGINE)
# =====================================================================
def upstash_command(command_list):
    url = UPSTASH_REDIS_REST_URL.rstrip('/')
    headers = {
        "Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}",
        "Content-Type": "application/json"
    }
    response = requests.post(url, json=command_list, headers=headers, timeout=10)
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(f"Upstash error: {payload['error']}")
    return payload.get("result")

def save_state():
    with state_lock:
        state = {
            'paper_trading': PAPER_TRADING,
            'virtual_balance_usdt': virtual_balance_usdt,
            'positions': positions,
            'consecutive_failures': consecutive_failures,
            'last_updated': utc_now_iso()
        }

    serialized_state = json.dumps(state)

    try:
        upstash_command(["SET", REDIS_STATE_KEY, serialized_state])
    except Exception as e:
        err_msg = f"⚠️ Primary persistence failure (Upstash Redis SET failed): {e}"
        print(err_msg)
        send_telegram_notification(err_msg)

    try:
        tmp_path = STATE_FILE_PATH + '.tmp'
        with open(tmp_path, 'w') as f:
            f.write(serialized_state)
        os.replace(tmp_path, STATE_FILE_PATH)
    except Exception as e:
        print(f"⚠️ Secondary local state save warning: {e}")

def load_state():
    global positions, virtual_balance_usdt, consecutive_failures

    recovered_raw = None

    try:
        recovered_raw = upstash_command(["GET", REDIS_STATE_KEY])
        if recovered_raw:
            print("🌐 Recovered state from Upstash Serverless Redis.")
    except Exception as e:
        msg = f"❌ Failed to reach Upstash Redis during startup recovery: {e}"
        print(msg)
        send_telegram_notification(msg)
        raise RuntimeError(msg) from e

    if not recovered_raw:
        if os.path.isfile(STATE_FILE_PATH):
            print("ℹ️ Upstash state empty. Falling back to local sidecar file.")
            try:
                with open(STATE_FILE_PATH, 'r') as f:
                    recovered_raw = f.read()
            except Exception as e:
                print(f"⚠️ Failed reading local sidecar: {e}")
        else:
            print("ℹ️ No prior state found in Upstash or local disk. Initializing clean registry.")
            return

    try:
        state = json.loads(recovered_raw) if isinstance(recovered_raw, str) else recovered_raw
        with state_lock:
            consecutive_failures = int(state.get('consecutive_failures', 0))
            if PAPER_TRADING and 'virtual_balance_usdt' in state:
                virtual_balance_usdt = float(state['virtual_balance_usdt'])

            saved_positions = state.get('positions', {})
            for sym in SYMBOLS:
                if sym in saved_positions:
                    p = saved_positions[sym]
                    is_active = bool(p.get('position_active', False))
                    entry_p = float(p.get('entry_price', 0.0))
                    t_stop = float(p.get('trailing_stop', 0.0))
                    buf = float(p.get('stop_loss_distance', 0.0))
                    tp = float(p.get('take_profit', 0.0))
                    lev = float(p.get('leverage', 1.0))
                    unt = float(p.get('units', 0.0))
                    marg = float(p.get('margin_allocated', 0.0))
                    reg = p.get('regime', 'NONE')
                    conf = float(p.get('confidence', 0.0))

                    if buf <= 0.0 and is_active and entry_p > t_stop:
                        buf = entry_p - t_stop
                    if tp <= 0.0 and is_active and entry_p > 0:
                        tp = entry_p + (buf * RR_RATIO)

                    positions[sym] = {
                        "position_active": is_active,
                        "entry_price": entry_p,
                        "trailing_stop": t_stop,
                        "stop_loss_distance": buf,
                        "take_profit": tp,
                        "leverage": lev,
                        "units": unt,
                        "margin_allocated": marg,
                        "regime": reg,
                        "confidence": conf
                    }
                else:
                    positions[sym] = {
                        "position_active": False,
                        "entry_price": 0.0,
                        "trailing_stop": 0.0,
                        "stop_loss_distance": 0.0,
                        "take_profit": 0.0,
                        "leverage": 1.0,
                        "units": 0.0,
                        "margin_allocated": 0.0,
                        "regime": "NONE",
                        "confidence": 0.0
                    }

        print(f"♻️ Recovered State (last_updated={state.get('last_updated')})")
        if PAPER_TRADING:
            print(f"   ↳ [PAPER] Virtual Balance: ${virtual_balance_usdt:.2f} USDT")
        for sym in SYMBOLS:
            p = positions[sym]
            if p["position_active"]:
                print(f"   ↳ [{sym}] ACTIVE | Entry: {p['entry_price']:.2f} | Stop: {p['trailing_stop']:.2f} | TP: {p['take_profit']:.2f} | Lev: {p['leverage']}x")

    except Exception as e:
        msg = f"💥 Corrupt state format encountered during recovery: {e}"
        print(msg)
        send_telegram_notification(msg)
        raise RuntimeError(msg) from e

# =====================================================================
# 5. STARTUP RECONCILIATION
# =====================================================================
def reconcile_state_with_exchange():
    global positions

    if PAPER_TRADING:
        print("📄 Paper Trading Mode active: Real exchange balance reconciliation bypassed.")
        with state_lock:
            for sym in SYMBOLS:
                p = positions[sym]
                if p["position_active"]:
                    print(f"   ↳ Active Virtual Position: [{sym}] | Entry: ${p['entry_price']:.2f} | Leverage: {p['leverage']}x")
        return True

    try:
        balances = exchange.fetch_balance()
        dirty = False

        for sym in SYMBOLS:
            base_asset = sym.split('/')[0]
            with state_lock:
                expected_units = positions[sym]["units"]
                current_pos = positions[sym]["position_active"]

            base_balance = (
                balances['free'].get(base_asset, 0.0)
                + balances.get('used', {}).get(base_asset, 0.0)
            )
            threshold = expected_units * 0.9 if expected_units > 0 else 0.0001
            holds_asset = base_balance >= threshold

            print(f"🔎 Startup reconciliation [{sym}] | {base_asset} balance={base_balance:.8f} | local_active={current_pos}")

            if current_pos and not holds_asset:
                msg = (
                    f"⚠️ *STATE MISMATCH* [{sym}]: State says position is OPEN, but exchange shows "
                    f"insufficient {base_asset} balance ({base_balance:.8f}). Flushing stale position."
                )
                print(msg)
                send_telegram_notification(msg)
                with state_lock:
                    positions[sym] = {
                        "position_active": False,
                        "entry_price": 0.0,
                        "trailing_stop": 0.0,
                        "stop_loss_distance": 0.0,
                        "take_profit": 0.0,
                        "leverage": 1.0,
                        "units": 0.0,
                        "margin_allocated": 0.0,
                        "regime": "NONE",
                        "confidence": 0.0
                    }
                dirty = True

            elif not current_pos and holds_asset:
                msg = (
                    f"⚠️ *STATE MISMATCH* [{sym}]: Exchange shows {base_asset} balance ({base_balance:.8f}) "
                    f"but local state says NO position is open. Halting to defend unmanaged capital."
                )
                print(msg)
                send_telegram_notification(msg)
                raise SystemExit(msg)

        if dirty:
            save_state()

        print("✅ Startup reconciliation completed.")
        return True

    except SystemExit:
        raise
    except Exception as e:
        msg = f"❌ Startup reconciliation failed — exchange truth unavailable: {e}"
        print(msg)
        send_telegram_notification(msg)
        raise RuntimeError(msg) from e

# =====================================================================
# 6. TELEGRAM & AUDIT LOGGING UTILITIES
# =====================================================================
def send_telegram_notification(message):
    if not ENABLE_TELEGRAM:
        return
    try:
        prefix = "📝 *[PAPER TRADING]* " if PAPER_TRADING else ""
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": f"{prefix}{message}",
            "parse_mode": "Markdown"
        }
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"⚠️ Telegram Alert Failed: {e}")

def log_trade_to_ledger(timestamp, symbol, order_id, side, regime, price, amount, stop_loss, take_profit, leverage, status):
    trade_record = {
        'Timestamp': timestamp,
        'Mode': 'PAPER' if PAPER_TRADING else 'LIVE',
        'Symbol': symbol,
        'OrderID': str(order_id),
        'Side': side,
        'MarketRegime': regime,
        'ExecutionPrice': float(price),
        'Amount': float(amount),
        'StopLoss': float(stop_loss),
        'TakeProfit': float(take_profit),
        'Leverage': float(leverage),
        'Status': status
    }

    try:
        upstash_command(["RPUSH", REDIS_LEDGER_KEY, json.dumps(trade_record)])
        print(f"🌐 Trade logged to Upstash list '{REDIS_LEDGER_KEY}' [{symbol}]")
    except Exception as e:
        err_msg = f"⚠️ Failed to push trade to Upstash ledger: {e}"
        print(err_msg)
        send_telegram_notification(err_msg)

    try:
        df_new = pd.DataFrame([trade_record])
        if not os.path.isfile(CSV_FILE_PATH):
            df_new.to_csv(CSV_FILE_PATH, index=False)
        else:
            df_new.to_csv(CSV_FILE_PATH, mode='a', header=False, index=False)
        print(f"🗒 Trade logged locally to '{CSV_FILE_PATH}' [{symbol}]")
    except Exception as e:
        print(f"⚠️ Failed to write local trade CSV: {e}")

# =====================================================================
# 7. NEWS & SENTIMENT SAFETY SHIELD (FAIL-OPEN)
# =====================================================================
NEGATIVE_NEWS_KEYWORDS = [
    'hack', 'exploit', 'sec', 'lawsuit', 'ban', 'insolvent', 'scam',
    'fraud', 'investigation', 'crash', 'collapse', 'halt', 'attack', 'subpoena'
]

def check_news_safety(symbol):
    """
    Scans public crypto news feeds for high-severity negative catalysts.
    Fails open (returns True) on network or API timeouts to prevent trade deadlocks.
    """
    base_asset = symbol.split('/')[0].upper()
    try:
        url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN"
        res = requests.get(url, timeout=5)
        if res.status_code != 200:
            return True, "NEWS_API_UNAVAILABLE (PASSED)"

        data = res.json().get("Data", [])
        negative_hits = 0
        matching_articles = 0

        for article in data[:15]:
            title = article.get("title", "").lower()
            body = article.get("body", "").lower()
            text_corpus = f"{title} {body}"

            # Check if article concerns this asset
            if base_asset.lower() in text_corpus or symbol.lower() in text_corpus:
                matching_articles += 1
                for kw in NEGATIVE_NEWS_KEYWORDS:
                    if kw in text_corpus:
                        negative_hits += 1
                        break

        if matching_articles > 0 and negative_hits >= 2:
            warning = f"BLOCKED: {negative_hits} negative news triggers detected for {base_asset}."
            print(f"🛡️ [NEWS SHIELD] {warning}")
            return False, warning

        return True, "NEWS_CLEAR"

    except Exception as e:
        # Fail open
        return True, f"NEWS_CHECK_BYPASS ({e})"

# =====================================================================
# 8. HARDENED SAFETY LAYER & PRE-FLIGHT BALANCES
# =====================================================================
def check_safety_preflight():
    global consecutive_failures, virtual_balance_usdt

    if PAPER_TRADING:
        with state_lock:
            curr_virtual = virtual_balance_usdt
            consecutive_failures = 0

        if curr_virtual < MIN_BALANCE_USDT:
            msg = f"❌ *EMERGENCY SHUTDOWN*: Virtual USDT balance (${curr_virtual:.2f}) fell below floor (${MIN_BALANCE_USDT:.2f})!"
            send_telegram_notification(msg)
            raise SystemExit(msg)
        return True

    try:
        balances = exchange.fetch_balance()
        quote_asset = SYMBOLS[0].split('/')[1]
        quote_balance = balances['free'].get(quote_asset, 0.0)

        if quote_balance < MIN_BALANCE_USDT:
            msg = f"❌ *EMERGENCY SHUTDOWN*: Free {quote_asset} balance ({quote_balance:.2f}) fell below floor ({MIN_BALANCE_USDT:.2f})!"
            send_telegram_notification(msg)
            raise SystemExit(msg)

        with state_lock:
            consecutive_failures = 0
        save_state()
        return True

    except SystemExit:
        raise
    except Exception as e:
        with state_lock:
            consecutive_failures += 1
            failures = consecutive_failures

        print(f"⚠️ Preflight Connection Warning ({failures}/{MAX_FAILURES_ALLOWED}): {e}")
        save_state()

        if failures >= MAX_FAILURES_ALLOWED:
            msg = f"💥 *CRITICAL FAULT*: {MAX_FAILURES_ALLOWED} consecutive API failures. Script halted to defend capital."
            send_telegram_notification(msg)
            raise SystemExit(msg)
        return False

# =====================================================================
# 9. TECHNICAL INDICATOR ENGINE (PURE PANDAS)
# =====================================================================
def analyze_advanced_market(symbol):
    try:
        bars = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=100)
        df = pd.DataFrame(
            bars,
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
        )

        # 1. Bollinger Bands (20, 2)
        rolling_20 = df['close'].rolling(20)
        sma_20 = rolling_20.mean()
        std_20 = rolling_20.std()
        bb_lower = sma_20 - (2.0 * std_20)
        bb_upper = sma_20 + (2.0 * std_20)

        # 2. RSI (14) via Wilder's Smoothing
        delta = df['close'].diff()
        gain = delta.clip(lower=0.0)
        loss = (-delta).clip(lower=0.0)
        avg_gain = gain.ewm(alpha=1.0/14.0, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1.0/14.0, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0.0, float('nan'))
        rsi_series = 100.0 - (100.0 / (1.0 + rs))

        # 3. ATR (14) via Wilder's Smoothing
        prev_close = df['close'].shift(1)
        tr1 = df['high'] - df['low']
        tr2 = (df['high'] - prev_close).abs()
        tr3 = (df['low'] - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr_series = tr.ewm(alpha=1.0/14.0, adjust=False).mean()

        # 4. ADX (14)
        up_move = df['high'].diff()
        down_move = -df['low'].diff()
        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        smooth_plus_dm = plus_dm.ewm(alpha=1.0/14.0, adjust=False).mean()
        smooth_minus_dm = minus_dm.ewm(alpha=1.0/14.0, adjust=False).mean()

        plus_di = 100.0 * (smooth_plus_dm / atr_series.replace(0.0, float('nan')))
        minus_di = 100.0 * (smooth_minus_dm / atr_series.replace(0.0, float('nan')))
        di_sum = plus_di + minus_di
        dx = (100.0 * (plus_di - minus_di).abs() / di_sum.replace(0.0, float('nan'))).fillna(0.0)
        adx_series = dx.ewm(alpha=1.0/14.0, adjust=False).mean()

        return {
            'close': float(df['close'].iloc[-1]),
            'adx': float(adx_series.iloc[-1]),
            'rsi': float(rsi_series.iloc[-1]),
            'atr': float(atr_series.iloc[-1]),
            'bb_lower': float(bb_lower.iloc[-1]),
            'bb_upper': float(bb_upper.iloc[-1])
        }
    except Exception as e:
        print(f"⚠️ Market Data API Failure [{symbol}]: {e}")
        send_telegram_notification(f"⚠️ Market data read failure [{symbol}]: {e}")
        return None

# =====================================================================
# 10. ADAPTIVE LEVERAGE & DYNAMIC POSITION SIZING CALCULATOR
# =====================================================================
def calculate_dynamic_entry(symbol, current_price, atr, regime, metrics):
    """
    Computes:
    1. Signal Confidence Score (0.0 to 1.0)
    2. Adaptive Bounded Leverage (MIN_LEVERAGE to MAX_LEVERAGE)
    3. Risk-Based Position Sizing: (Equity * Risk%) / Stop_Distance
    4. Take-Profit Target (1:2 R:R)
    """
    with state_lock:
        if PAPER_TRADING:
            equity = virtual_balance_usdt
        else:
            try:
                bal = exchange.fetch_balance()
                equity = float(bal['free'].get('USDT', 0.0) + bal.get('used', {}).get('USDT', 0.0))
            except Exception:
                equity = 100.0

    # 1. Signal Confidence Calculation
    if regime == "SIDEWAYS":
        # Deeper penetration below Bollinger Lower Band = higher confidence
        bb_depth = max(0.0, metrics['bb_lower'] - current_price)
        depth_ratio = min(1.0, bb_depth / (atr * 0.75 + 1e-6))
        adx_flatness = max(0.0, (ADX_TREND_THRESHOLD - metrics['adx']) / ADX_TREND_THRESHOLD)
        confidence = round(0.4 * depth_ratio + 0.6 * adx_flatness, 2)
    else:  # SWING
        # Lower RSI and stronger ADX trend = higher breakout momentum confidence
        rsi_extreme = max(0.0, (30.0 - metrics['rsi']) / 20.0)
        adx_strength = min(1.0, (metrics['adx'] - ADX_TREND_THRESHOLD) / 30.0)
        confidence = round(0.5 * rsi_extreme + 0.5 * adx_strength, 2)

    confidence = max(0.1, min(1.0, confidence))

    # 2. Adaptive Leverage (Bounded strictly between MIN_LEVERAGE and MAX_LEVERAGE <= 5.0)
    effective_max_leverage = min(5.0, MAX_LEVERAGE)
    leverage = round(MIN_LEVERAGE + confidence * (effective_max_leverage - MIN_LEVERAGE), 1)

    # 3. Dynamic Position Sizing (Risk-based dollar floor)
    dollar_risk = equity * RISK_PCT_PER_TRADE
    stop_distance = atr * ATR_MULTIPLIER
    calculated_stop = current_price - stop_distance
    take_profit = current_price + (stop_distance * RR_RATIO)

    # Unit sizing so that stop hit exactly equals dollar_risk
    units = dollar_risk / stop_distance
    notional_value = units * current_price
    margin_required = notional_value / leverage

    # Portfolio Guard: Enforce maximum margin exposure limits
    with state_lock:
        current_margin_used = sum(p["margin_allocated"] for p in positions.values() if p["position_active"])

    available_margin_budget = (equity * MAX_PORTFOLIO_MARGIN_PCT) - current_margin_used

    if margin_required > available_margin_budget:
        # Scale back units if budget exceeded
        if available_margin_budget > 10.0:
            margin_required = available_margin_budget
            notional_value = margin_required * leverage
            units = notional_value / current_price
        else:
            return None  # Insufficient margin allowance

    return {
        "confidence": confidence,
        "leverage": leverage,
        "units": round(units, 6),
        "margin_required": round(margin_required, 2),
        "trailing_stop": round(calculated_stop, 2),
        "take_profit": round(take_profit, 2),
        "stop_distance": round(stop_distance, 2)
    }

# =====================================================================
# 11. LIVE POSITION GUARD ENGINE (TRAILING SL & TP)
# =====================================================================
def manage_active_position(symbol, current_price):
    global positions, virtual_balance_usdt

    with state_lock:
        pos = positions[symbol]
        curr_entry = pos["entry_price"]
        curr_stop = pos["trailing_stop"]
        curr_tp = pos["take_profit"]
        risk_dist = pos["stop_loss_distance"]
        units = pos["units"]
        leverage = pos["leverage"]
        margin_allocated = pos["margin_allocated"]
        regime = pos["regime"]

    mode_label = "PAPER" if PAPER_TRADING else "LIVE"
    print(
        f"🛡️ [{mode_label} | {symbol}] {regime} | Entry: {curr_entry:.2f} | Current: {current_price:.2f} | "
        f"SL: {curr_stop:.2f} | TP: {curr_tp:.2f} | Lev: {leverage}x"
    )

    exit_triggered = False
    exit_type = ""

    if current_price <= curr_stop:
        exit_triggered = True
        exit_type = "TRAILING_STOP_LOSS"
    elif current_price >= curr_tp:
        exit_triggered = True
        exit_type = "TAKE_PROFIT_LIMIT"

    if exit_triggered:
        print(f"🚨 [{symbol}] {exit_type} TRIGGERED at {current_price:.2f}!")
        try:
            timestamp = utc_now_iso()
            pnl = (current_price - curr_entry) * units

            if PAPER_TRADING:
                order_id = f"SIM_{exit_type[:4]}_{int(time.time() * 1000)}"
                with state_lock:
                    virtual_balance_usdt += (margin_allocated + pnl)
                print(f"📝 [PAPER] Exit filled at ${current_price:.2f}. PnL: ${pnl:+.2f} USDT. Balance: ${virtual_balance_usdt:.2f}")
            else:
                params = {}
                if leverage > 1.0:
                    params['leverage'] = int(leverage)

                order = exchange.create_market_sell_order(symbol, units, params)
                order_id = order.get('id', 'UNKNOWN_ID')

            msg = (
                f"🚨 *POSITION CLOSED* [{symbol}]\n"
                f"Reason: `{exit_type}`\n"
                f"Exit Price: `{current_price:.2f}`\n"
                f"PnL: `{pnl:+.2f} USDT`\n"
                f"Leverage: `{leverage}x`"
            )
            send_telegram_notification(msg)
            log_trade_to_ledger(
                timestamp, symbol, order_id, 'SELL', regime,
                current_price, units, curr_stop, curr_tp, leverage, exit_type
            )

            with state_lock:
                positions[symbol] = {
                    "position_active": False,
                    "entry_price": 0.0,
                    "trailing_stop": 0.0,
                    "stop_loss_distance": 0.0,
                    "take_profit": 0.0,
                    "leverage": 1.0,
                    "units": 0.0,
                    "margin_allocated": 0.0,
                    "regime": "NONE",
                    "confidence": 0.0
                }
            save_state()

        except Exception as e:
            print(f"❌ [{symbol}] Exit order failure: {e}")
            send_telegram_notification(f"❌ CRITICAL [{symbol}]: Exit order failed: {e}")

    elif current_price > curr_entry:
        # Dynamic Ratchet: Trail stop upward point-for-point preserving risk distance
        new_stop = current_price - risk_dist
        if new_stop > curr_stop:
            with state_lock:
                positions[symbol]["trailing_stop"] = new_stop
            print(f"📈 [{symbol}] Trailing Stop ratcheted to: {new_stop:.2f}")
            save_state()

# =====================================================================
# 12. MASTER SIGNAL & MULTI-ASSET DISPATCHER
# =====================================================================
def execution_orchestrator():
    global positions, virtual_balance_usdt

    safety_ok = check_safety_preflight()

    # Step 1: Manage all active positions
    for symbol in SYMBOLS:
        with state_lock:
            is_active = positions[symbol]["position_active"]

        if is_active:
            metrics = analyze_advanced_market(symbol)
            if metrics:
                manage_active_position(symbol, metrics['close'])

    if not safety_ok:
        return

    # Step 2: Enforce Maximum Concurrent Positions Gate
    with state_lock:
        active_count = sum(1 for p in positions.values() if p["position_active"])

    if active_count >= MAX_CONCURRENT_POSITIONS:
        print(f"⏸️ Max concurrent positions reached ({active_count}/{MAX_CONCURRENT_POSITIONS}). Scanning paused.")
        return

    # Step 3: Scan candidate watchlist pool for optimal setups
    for symbol in SYMBOLS:
        with state_lock:
            if positions[symbol]["position_active"]:
                continue

        metrics = analyze_advanced_market(symbol)
        if not metrics:
            continue

        current_price = metrics['close']
        timestamp = utc_now_iso()

        regime_signal = None

        # Sideways Regime: ADX < 25 and price touching/piercing lower Bollinger Band
        if metrics['adx'] < ADX_TREND_THRESHOLD:
            if current_price <= metrics['bb_lower']:
                regime_signal = "SIDEWAYS"
        # Swing Breakout Regime: ADX >= 25 and extreme oversold momentum
        else:
            if metrics['rsi'] <= 30.0:
                regime_signal = "SWING"

        if not regime_signal:
            continue

        print(f"🔍 [{symbol}] Potential {regime_signal} Setup detected! Evaluating news & risk sizing...")

        # Step 4: News Safety Shield Gate
        news_ok, news_msg = check_news_safety(symbol)
        update_engine_status(active_news_status=f"[{symbol}]: {news_msg}")
        if not news_ok:
            print(f"🛑 Trade entry blocked by News Shield: {news_msg}")
            continue

        # Step 5: Dynamic Position Sizing & Adaptive Leverage Calculation
        plan = calculate_dynamic_entry(symbol, current_price, metrics['atr'], regime_signal, metrics)
        if not plan:
            print(f"⚠️ Sizing rejected: Insufficient portfolio margin allocation budget.")
            continue

        print(
            f"⚡ Executing {regime_signal} Entry [{symbol}] | Price: {current_price:.2f} | Units: {plan['units']} | "
            f"Margin: ${plan['margin_required']} | Lev: {plan['leverage']}x | Conf: {plan['confidence']}"
        )

        try:
            if PAPER_TRADING:
                order_id = f"SIM_BUY_{int(time.time() * 1000)}"
                with state_lock:
                    virtual_balance_usdt -= plan['margin_required']
                print(f"📝 [PAPER] Virtual position opened. Margin reserved: ${plan['margin_required']:.2f}")
            else:
                params = {}
                if plan['leverage'] > 1.0:
                    params['leverage'] = int(plan['leverage'])
                order = exchange.create_market_buy_order(symbol, plan['units'], params)
                order_id = order.get('id', 'UNKNOWN_ID')

            with state_lock:
                positions[symbol] = {
                    "position_active": True,
                    "entry_price": current_price,
                    "trailing_stop": plan['trailing_stop'],
                    "stop_loss_distance": plan['stop_distance'],
                    "take_profit": plan['take_profit'],
                    "leverage": plan['leverage'],
                    "units": plan['units'],
                    "margin_allocated": plan['margin_required'],
                    "regime": regime_signal,
                    "confidence": plan['confidence']
                }
            save_state()

            msg = (
                f"🟢 *TRADE OPENED ({regime_signal})*\n"
                f"Symbol: `{symbol}`\n"
                f"Entry Price: `{current_price:.2f}`\n"
                f"Size: `{plan['units']}` (${plan['margin_required'] * plan['leverage']:.2f} Notional)\n"
                f"Adaptive Leverage: `{plan['leverage']}x` (Confidence: {plan['confidence']})\n"
                f"Initial SL: `{plan['trailing_stop']:.2f}`\n"
                f"Target TP (1:{RR_RATIO}): `{plan['take_profit']:.2f}`"
            )
            send_telegram_notification(msg)
            log_trade_to_ledger(
                timestamp, symbol, order_id, 'BUY', regime_signal,
                current_price, plan['units'], plan['trailing_stop'], plan['take_profit'], plan['leverage'], 'POSITION_OPEN'
            )

            # Check if concurrent limits reached after this entry
            with state_lock:
                active_count = sum(1 for p in positions.values() if p["position_active"])
            if active_count >= MAX_CONCURRENT_POSITIONS:
                break

        except Exception as e:
            print(f"❌ Entry order failed [{symbol}]: {e}")
            send_telegram_notification(f"❌ Entry blocked [{symbol}]: {e}")

# =====================================================================
# 13. BACKGROUND TRADING ENGINE DAEMON
# =====================================================================
def trading_engine_loop():
    update_engine_status(
        engine_state="STARTING",
        thread_alive=True,
        startup_timestamp=utc_now_iso(),
        last_error=None,
        halt_reason=None,
        startup_reconciled=False
    )

    try:
        print("♻️ Loading multi-pair margin state from Upstash...")
        load_state()

        print("🔎 Performing mandatory startup reconciliation...")
        reconcile_state_with_exchange()

        update_engine_status(
            startup_reconciled=True,
            engine_state="RUNNING"
        )

        mode_str = "PAPER TRADING" if PAPER_TRADING else "LIVE CAPITAL"
        print(f"🚀 Quantum Shield Engaged [{mode_str}]. Candidate Pool: {', '.join(SYMBOLS)}")
        send_telegram_notification(f"🚀 Kraken Bot Engine Active [{mode_str}]! Watching: {', '.join(SYMBOLS)}")

        while True:
            update_engine_status(
                last_loop_timestamp=utc_now_iso(),
                engine_state="RUNNING",
                thread_alive=True
            )

            try:
                execution_orchestrator()
                update_engine_status(
                    last_successful_loop_timestamp=utc_now_iso(),
                    last_error=None
                )
            except SystemExit as e:
                msg = f"Hard safety shutdown activated: {e}"
                print(f"🛑 {msg}")
                update_engine_status(
                    engine_state="HALTED",
                    halt_reason=str(e),
                    last_error=None,
                    thread_alive=False
                )
                send_telegram_notification(f"🛑 *ENGINE HALTED*: {e}")
                break
            except Exception as e:
                error_details = f"{type(e).__name__}: {e}"
                print(f"💥 Global trading loop fault: {error_details}")
                traceback.print_exc()
                update_engine_status(
                    engine_state="RUNNING_WITH_ERRORS",
                    last_error=error_details
                )
                send_telegram_notification(f"⚠️ Loop exception: {error_details}")

            time.sleep(LOOP_INTERVAL_SECONDS)

    except SystemExit as e:
        msg = f"Startup safety halt: {e}"
        print(f"🛑 {msg}")
        update_engine_status(
            engine_state="HALTED",
            halt_reason=str(e),
            last_error=None,
            thread_alive=False
        )
        send_telegram_notification(f"🛑 *STARTUP HALTED*: {e}")
    except Exception as e:
        error_details = f"{type(e).__name__}: {e}"
        print(f"💥 Engine failed during startup: {error_details}")
        traceback.print_exc()
        update_engine_status(
            engine_state="STARTUP_FAILED",
            last_error=error_details,
            halt_reason="Startup failure.",
            thread_alive=False,
            startup_reconciled=False
        )
        send_telegram_notification(f"💥 *ENGINE STARTUP FAILED*: {error_details}")
    finally:
        with state_lock:
            engine_status["thread_alive"] = False
        print("ℹ️ Trading thread stopped. Web server remains active for diagnostics.")

# =====================================================================
# 14. OPERATOR TERMINAL UI (DARK QUANT DASHBOARD)
# =====================================================================
app = Flask(__name__)

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>Kraken Dynamic Regime Terminal</title>
  <style>
    :root {
      --bg: #090d16;
      --card: #111726;
      --card-alt: #161f33;
      --border: #1f2b45;
      --text: #e6edf3;
      --muted: #8b949e;
      --green: #2ea043;
      --green-glow: rgba(46, 160, 67, 0.15);
      --red: #f85149;
      --red-glow: rgba(248, 81, 73, 0.15);
      --blue: #58a6ff;
      --amber: #d29922;
      --amber-glow: rgba(210, 153, 34, 0.15);
      --font-mono: ui-monospace, SFMono-Regular, "SF Pro Text", Menlo, Monaco, Consolas, monospace;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: var(--font-mono);
      padding: 16px;
      line-height: 1.4;
      -webkit-font-smoothing: antialiased;
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 14px 18px;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 10px;
      margin-bottom: 16px;
    }
    .title {
      font-size: 15px;
      font-weight: 700;
      letter-spacing: 0.5px;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    .badge-running { background: var(--green-glow); color: var(--green); border: 1px solid var(--green); }
    .badge-halted { background: var(--red-glow); color: var(--red); border: 1px solid var(--red); }
    .badge-paper { background: var(--amber-glow); color: var(--amber); border: 1px solid var(--amber); }
    .badge-live { background: var(--green-glow); color: var(--green); border: 1px solid var(--green); }
    .dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: currentColor;
      animation: pulse 2s infinite ease-in-out;
    }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 14px;
    }
    .card-label {
      font-size: 11px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.5px;
      margin-bottom: 6px;
    }
    .card-value {
      font-size: 20px;
      font-weight: 700;
      color: var(--text);
    }
    .section-title {
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      color: var(--muted);
      margin-bottom: 10px;
      font-weight: 600;
    }
    .table-container {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow-x: auto;
      margin-bottom: 16px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
      font-size: 13px;
    }
    th, td {
      padding: 12px 14px;
      border-bottom: 1px solid var(--border);
      white-space: nowrap;
    }
    th {
      color: var(--muted);
      font-weight: 600;
      font-size: 11px;
      text-transform: uppercase;
    }
    tr:last-child td { border-bottom: none; }
    .pos-open { color: var(--green); font-weight: 700; }
    .pos-flat { color: var(--muted); }
    .raw-box {
      background: #05080f;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 12px;
      font-size: 11px;
      color: var(--muted);
      overflow-x: auto;
      white-space: pre-wrap;
      word-break: break-all;
    }
    .footer {
      display: flex;
      justify-content: space-between;
      font-size: 11px;
      color: var(--muted);
      margin-top: 16px;
      padding: 0 4px;
    }
  </style>
</head>
<body>
  <div class="header">
    <div class="title">
      <span>KRAKEN QUANT ENGINE</span>
      <span id="badge-mode" class="badge badge-paper"><span class="dot"></span>PAPER</span>
      <span id="badge-state" class="badge badge-running"><span class="dot"></span>RUNNING</span>
    </div>
    <div style="font-size: 12px; color: var(--muted);" id="last-updated">--:--:--</div>
  </div>

  <div class="grid">
    <div class="card">
      <div class="card-label">Virtual Capital</div>
      <div class="card-value" id="val-balance">$0.00</div>
    </div>
    <div class="card">
      <div class="card-label">Margin In Use</div>
      <div class="card-value" id="val-margin">$0.00</div>
    </div>
    <div class="card">
      <div class="card-label">Risk Per Trade</div>
      <div class="card-value" id="val-risk">1.5%</div>
    </div>
    <div class="card">
      <div class="card-label">News Safety Shield</div>
      <div class="card-value" id="val-news" style="font-size: 14px; margin-top: 4px;">MONITORING</div>
    </div>
  </div>

  <div class="section-title">Dynamic Candidate Watchlist & Active Positions</div>
  <div class="table-container">
    <table>
      <thead>
        <tr>
          <th>Symbol</th>
          <th>State</th>
          <th>Regime</th>
          <th>Leverage</th>
          <th>Units</th>
          <th>Margin</th>
          <th>Entry</th>
          <th>Trailing SL</th>
          <th>Target TP (1:2)</th>
        </tr>
      </thead>
      <tbody id="positions-tbody">
        <tr><td colspan="9" style="color: var(--muted); text-align: center;">Scanning candidate pool...</td></tr>
      </tbody>
    </table>
  </div>

  <div class="section-title">Telemetry Snapshot</div>
  <pre class="raw-box" id="raw-state">Fetching telemetrics...</pre>

  <div class="footer">
    <span>API: /status</span>
    <span>Auto-refresh: 3s</span>
  </div>

  <script>
    async function updateDashboard() {
      try {
        const res = await fetch('/status');
        const data = await res.json();

        const badgeMode = document.getElementById('badge-mode');
        badgeMode.textContent = data.mode === 'PAPER_TRADING' ? 'PAPER SIMULATION' : 'LIVE CAPITAL';
        badgeMode.className = 'badge ' + (data.mode === 'PAPER_TRADING' ? 'badge-paper' : 'badge-live');

        const badgeState = document.getElementById('badge-state');
        badgeState.textContent = data.engine_state;
        badgeState.className = 'badge ' + (data.engine_state === 'RUNNING' ? 'badge-running' : 'badge-halted');

        document.getElementById('last-updated').textContent = new Date().toLocaleTimeString();

        const bal = data.virtual_balance_usdt !== null ? '$' + Number(data.virtual_balance_usdt).toFixed(2) : 'LIVE ACCOUNT';
        document.getElementById('val-balance').textContent = bal;
        document.getElementById('val-margin').textContent = '$' + Number(data.total_margin_allocated || 0).toFixed(2);
        document.getElementById('val-risk').textContent = (data.risk_pct_per_trade * 100).toFixed(1) + '% / 1:' + (data.rr_ratio || 2);
        document.getElementById('val-news').textContent = data.active_news_status || 'MONITORING';

        const tbody = document.getElementById('positions-tbody');
        tbody.innerHTML = '';
        for (const [sym, pos] of Object.entries(data.positions || {})) {
          const row = document.createElement('tr');
          const isAct = pos.position_active;
          const posCell = isAct ? '<span class="pos-open">OPEN</span>' : '<span class="pos-flat">SCANNING</span>';
          const reg = isAct ? pos.regime : '--';
          const lev = isAct ? pos.leverage + 'x' : '--';
          const units = isAct ? pos.units : '--';
          const marg = isAct ? '$' + Number(pos.margin_allocated).toFixed(2) : '--';
          const entry = isAct ? '$' + Number(pos.entry_price).toFixed(2) : '--';
          const stop = isAct ? '$' + Number(pos.trailing_stop).toFixed(2) : '--';
          const tp = isAct ? '$' + Number(pos.take_profit).toFixed(2) : '--';

          row.innerHTML = `
            <td><strong>${sym}</strong></td>
            <td>${posCell}</td>
            <td>${reg}</td>
            <td>${lev}</td>
            <td>${units}</td>
            <td>${marg}</td>
            <td>${entry}</td>
            <td>${stop}</td>
            <td>${tp}</td>
          `;
          tbody.appendChild(row);
        }

        document.getElementById('raw-state').textContent = JSON.stringify(data, null, 2);
      } catch (err) {
        document.getElementById('badge-state').textContent = 'DISCONNECTED';
        document.getElementById('badge-state').className = 'badge badge-halted';
      }
    }

    updateDashboard();
    setInterval(updateDashboard, 3000);
  </script>
</body>
</html>
"""

@app.route("/", methods=["GET"])
def dashboard():
    return render_template_string(DASHBOARD_HTML)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "kraken-hybrid-regime-bot-multipair",
        "paper_trading": PAPER_TRADING,
        "timestamp": utc_now_iso()
    }), 200

@app.route("/status", methods=["GET"])
def status():
    return jsonify(get_status_snapshot()), 200

# =====================================================================
# 15. APPLICATION BOOTSTRAP
# =====================================================================
def start_trading_thread():
    active_threads = [
        t for t in threading.enumerate()
        if t.name == "KrakenTradingEngine" and t.is_alive()
    ]
    if active_threads:
        print("⚠️ Trading engine thread already running. Skipping duplicate spawn.")
        return

    engine_thread = threading.Thread(
        target=trading_engine_loop,
        name="KrakenTradingEngine",
        daemon=True
    )
    engine_thread.start()
    print("🧵 Dynamic Multi-Asset Risk Engine thread started.")

if __name__ == "__main__":
    print(f"🌐 Starting Kraken Multi-Asset Web Service on 0.0.0.0:{SERVER_PORT}")
    print(f"⚙️ Mode: {'PAPER TRADING (Simulated)' if PAPER_TRADING else 'LIVE CAPITAL'}")
    print("💻 Terminal UI: /")
    print("📡 Health Check: /health")
    print("📊 Telemetry API: /status")

    start_trading_thread()

    app.run(
        host="0.0.0.0",
        port=SERVER_PORT,
        debug=False,
        use_reloader=False,
        threaded=True
    )
