import time
import os
import json
import threading
import traceback
from datetime import datetime, timezone
import ccxt
import pandas as pd
import requests
from flask import Flask, jsonify

# =====================================================================
# 1. ENVIRONMENT CONFIGURATION & VALIDATION
# =====================================================================
# Paper Trading Toggle: Default to True if unspecified or 'true'/'1'
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

SYMBOLS_RAW = os.environ.get("TRADING_SYMBOLS", "BTC/USDT").strip()
SYMBOLS = [s.strip() for s in SYMBOLS_RAW.split(",") if s.strip()]

TRADE_AMOUNTS_RAW = os.environ.get("TRADE_AMOUNTS", '{"BTC/USDT": 0.0005}').strip()
try:
    TRADE_AMOUNTS = json.loads(TRADE_AMOUNTS_RAW)
except Exception:
    TRADE_AMOUNTS = {"BTC/USDT": 0.0005}

TIMEFRAME = os.environ.get("TRADING_TIMEFRAME", "15m").strip()
ADX_TREND_THRESHOLD = float(os.environ.get("ADX_TREND_THRESHOLD", "25.0"))
ATR_MULTIPLIER = float(os.environ.get("ATR_MULTIPLIER", "1.5"))

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
# 2. SYNCHRONIZED RUNTIME STATE (MULTI-PAIR SCHEMA)
# =====================================================================
state_lock = threading.Lock()

positions = {}
for s in SYMBOLS:
    positions[s] = {
        "position_active": False,
        "entry_price": 0.0,
        "trailing_stop": 0.0,
        "stop_loss_distance": 0.0
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
    "persistence_backend": "UPSTASH_REDIS_REST"
}

def utc_now_iso():
    """Return a timezone-aware UTC timestamp formatted in ISO 8601."""
    return datetime.now(timezone.utc).isoformat()

def update_engine_status(**kwargs):
    """Thread-safe status updates for HTTP monitoring endpoints."""
    with state_lock:
        engine_status.update(kwargs)

def get_status_snapshot():
    """Return an in-memory thread-safe diagnostic snapshot."""
    with state_lock:
        snapshot = dict(engine_status)
        snapshot.update({
            "mode": "PAPER_TRADING" if PAPER_TRADING else "LIVE_CAPITAL",
            "virtual_balance_usdt": virtual_balance_usdt if PAPER_TRADING else None,
            "tracked_symbols": SYMBOLS,
            "timeframe": TIMEFRAME,
            "positions": {sym: dict(data) for sym, data in positions.items()},
            "consecutive_failures": consecutive_failures,
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
    """Execute a raw Redis command array via Upstash HTTPS REST API."""
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
    """Persist all positions and virtual balances to Upstash Redis REST and local sidecar."""
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
    """Recover multi-symbol state on startup from Upstash Redis REST or local disk."""
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
            print("ℹ️ No prior state found in Upstash or local disk. Initializing fresh registry.")
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

                    if buf <= 0.0 and is_active and entry_p > t_stop:
                        buf = entry_p - t_stop

                    positions[sym] = {
                        "position_active": is_active,
                        "entry_price": entry_p,
                        "trailing_stop": t_stop,
                        "stop_loss_distance": buf
                    }
                else:
                    positions[sym] = {
                        "position_active": False,
                        "entry_price": 0.0,
                        "trailing_stop": 0.0,
                        "stop_loss_distance": 0.0
                    }

        print(f"♻️ Recovered State (last_updated={state.get('last_updated')})")
        if PAPER_TRADING:
            print(f"   ↳ [MODE: PAPER] Virtual Balance: ${virtual_balance_usdt:.2f} USDT")
        for sym in SYMBOLS:
            p = positions[sym]
            print(f"   ↳ [{sym}] Active: {p['position_active']} | Entry: {p['entry_price']:.2f} | Stop: {p['trailing_stop']:.2f}")

    except Exception as e:
        msg = f"💥 Corrupt state format encountered during recovery: {e}"
        print(msg)
        send_telegram_notification(msg)
        raise RuntimeError(msg) from e

# =====================================================================
# 5. STARTUP RECONCILIATION
# =====================================================================
def reconcile_state_with_exchange():
    """Cross-check state against live Kraken balances if live, or validate virtual holdings if paper."""
    global positions

    if PAPER_TRADING:
        print("📄 Paper Trading Mode active: Live balance reconciliation bypassed.")
        with state_lock:
            for sym in SYMBOLS:
                p = positions[sym]
                if p["position_active"]:
                    print(f"   ↳ Retaining virtual position for [{sym}] at entry ${p['entry_price']:.2f}")
        return True

    try:
        balances = exchange.fetch_balance()
        dirty = False

        for sym in SYMBOLS:
            base_asset = sym.split('/')[0]
            trade_amt = TRADE_AMOUNTS.get(sym, 0.0005)

            base_balance = (
                balances['free'].get(base_asset, 0.0)
                + balances.get('used', {}).get(base_asset, 0.0)
            )
            holds_asset = base_balance >= (trade_amt * 0.9)

            with state_lock:
                current_pos = positions[sym]["position_active"]

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
                        "stop_loss_distance": 0.0
                    }
                dirty = True

            elif not current_pos and holds_asset:
                msg = (
                    f"⚠️ *STATE MISMATCH* [{sym}]: Exchange shows {base_asset} balance ({base_balance:.8f}) "
                    f"but local state says NO position is open. Halting to prevent untracked risk."
                )
                print(msg)
                send_telegram_notification(msg)
                raise SystemExit(msg)

        if dirty:
            save_state()

        print("✅ Startup live balance reconciliation completed successfully.")
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
    """Sends real-time push alerts via Telegram."""
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

def log_trade_to_ledger(timestamp, symbol, order_id, side, regime, price, amount, stop_loss, status):
    """Appends trade records to durable Upstash List and local CSV."""
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
# 7. HARDENED SAFETY LAYER & PRE-FLIGHT BALANCES
# =====================================================================
def check_safety_preflight():
    """Verifies cash reserves exist and API authentication remains intact."""
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
# 8. TECHNICAL INDICATOR ENGINE (VECTORIZED PANDAS)
# =====================================================================
def analyze_advanced_market(symbol):
    """Calculates ADX, Bollinger Bands, RSI, and ATR natively using Pandas."""
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
# 9. LIVE POSITION GUARD ENGINE (PER-PAIR)
# =====================================================================
def manage_active_position(symbol, current_price):
    global positions, virtual_balance_usdt

    with state_lock:
        curr_entry = positions[symbol]["entry_price"]
        curr_stop = positions[symbol]["trailing_stop"]
        risk_dist = positions[symbol]["stop_loss_distance"]

    mode_label = "PAPER" if PAPER_TRADING else "LIVE"
    print(
        f"🛡️ [{mode_label} | {symbol}] Active Position | Entry: {curr_entry:.2f} | "
        f"Current: {current_price:.2f} | Trailing Stop: {curr_stop:.2f}"
    )

    if current_price <= curr_stop:
        print(f"🚨 [{symbol}] TRAILING STOP TRIGGERED! Executing market exit.")
        try:
            trade_amt = TRADE_AMOUNTS.get(symbol, 0.0005)
            timestamp = utc_now_iso()

            if PAPER_TRADING:
                order_id = f"SIM_SELL_{int(time.time() * 1000)}"
                sell_amount = trade_amt
                proceeds = current_price * sell_amount
                with state_lock:
                    virtual_balance_usdt += proceeds
                print(f"📝 [PAPER] Virtual exit filled at ${current_price:.2f}. New balance: ${virtual_balance_usdt:.2f} USDT")
            else:
                base_asset = symbol.split('/')[0]
                sell_amount = trade_amt
                try:
                    balances = exchange.fetch_balance()
                    available_base = balances['free'].get(base_asset, 0.0)
                    if available_base > 0:
                        sell_amount = min(trade_amt, available_base)
                except Exception as bal_err:
                    print(f"⚠️ [{symbol}] Balance check error before exit: {bal_err}")

                order = exchange.create_market_sell_order(symbol, sell_amount)
                order_id = order.get('id', 'UNKNOWN_ID')

            msg = f"🚨 *EXIT EXECUTED* [{symbol}]: Trailing stop hit at `{current_price:.2f}`. Position liquidated."
            send_telegram_notification(msg)
            log_trade_to_ledger(
                timestamp, symbol, order_id, 'SELL', 'EXIT',
                current_price, sell_amount, curr_stop, 'STOP_LOSS_CLOSED'
            )

            with state_lock:
                positions[symbol] = {
                    "position_active": False,
                    "entry_price": 0.0,
                    "trailing_stop": 0.0,
                    "stop_loss_distance": 0.0
                }
            save_state()

        except Exception as e:
            print(f"❌ [{symbol}] Critical failure on exit order: {e}")
            send_telegram_notification(f"❌ CRITICAL [{symbol}]: Exit order failed: {e}")

    elif current_price > curr_entry:
        new_stop = current_price - risk_dist
        if new_stop > curr_stop:
            with state_lock:
                positions[symbol]["trailing_stop"] = new_stop
            print(f"📈 [{symbol}] Trailing stop ratcheted to: {new_stop:.2f}")
            save_state()

# =====================================================================
# 10. MASTER SIGNAL PROCESSOR (PER-PAIR ORCHESTRATION)
# =====================================================================
def execution_orchestrator():
    global positions, virtual_balance_usdt

    safety_ok = check_safety_preflight()

    for symbol in SYMBOLS:
        with state_lock:
            is_active = positions[symbol]["position_active"]

        if not safety_ok:
            if is_active:
                metrics = analyze_advanced_market(symbol)
                if metrics:
                    manage_active_position(symbol, metrics['close'])
            continue

        metrics = analyze_advanced_market(symbol)
        if not metrics:
            continue

        current_price = metrics['close']
        timestamp = utc_now_iso()

        if is_active:
            manage_active_position(symbol, current_price)
            continue

        mode_label = "PAPER" if PAPER_TRADING else "LIVE"
        print(f"🔍 [{mode_label} | {symbol}] Scanning | Price: {current_price:.2f} | ADX: {metrics['adx']:.1f}")

        trade_amt = TRADE_AMOUNTS.get(symbol, 0.0005)
        trade_cost = current_price * trade_amt

        # ------ SIDEWAYS REGIME ENTRY LOGIC ------
        if metrics['adx'] < ADX_TREND_THRESHOLD:
            if current_price <= metrics['bb_lower']:
                print(f"🟢 [{symbol}] Sideways Floor Signal Found! Entering position.")
                try:
                    if PAPER_TRADING:
                        with state_lock:
                            if virtual_balance_usdt < trade_cost:
                                print(f"⚠️ [PAPER] Insufficient virtual balance ({virtual_balance_usdt:.2f}) for trade cost ({trade_cost:.2f})")
                                continue
                            virtual_balance_usdt -= trade_cost
                        order_id = f"SIM_BUY_{int(time.time() * 1000)}"
                        print(f"📝 [PAPER] Virtual entry filled at ${current_price:.2f}. Balance remaining: ${virtual_balance_usdt:.2f} USDT")
                    else:
                        order = exchange.create_market_buy_order(symbol, trade_amt)
                        order_id = order.get('id', 'UNKNOWN_ID')

                    calculated_dist = metrics['atr'] * ATR_MULTIPLIER
                    calculated_stop = current_price - calculated_dist

                    with state_lock:
                        positions[symbol] = {
                            "position_active": True,
                            "entry_price": current_price,
                            "trailing_stop": calculated_stop,
                            "stop_loss_distance": calculated_dist
                        }
                    save_state()

                    msg = (
                        f"🟢 *TRADE OPENED (SIDEWAYS)*\n"
                        f"Pair: {symbol}\n"
                        f"Price: {current_price:.2f}\n"
                        f"Initial Stop: {calculated_stop:.2f}\n"
                        f"Buffer: {calculated_dist:.2f}"
                    )
                    send_telegram_notification(msg)
                    log_trade_to_ledger(
                        timestamp, symbol, order_id, 'BUY', 'SIDEWAYS',
                        current_price, trade_amt, calculated_stop, 'POSITION_OPEN'
                    )
                except Exception as e:
                    print(f"❌ [{symbol}] Entry order blocked: {e}")
                    send_telegram_notification(f"❌ [{symbol}] Entry order failure: {e}")

        # ------ SWING BREAKOUT REGIME ENTRY LOGIC ------
        else:
            if metrics['rsi'] <= 30:
                print(f"⚡ [{symbol}] Breakout Momentum Entry Signal! Entering position.")
                try:
                    if PAPER_TRADING:
                        with state_lock:
                            if virtual_balance_usdt < trade_cost:
                                print(f"⚠️ [PAPER] Insufficient virtual balance ({virtual_balance_usdt:.2f}) for trade cost ({trade_cost:.2f})")
                                continue
                            virtual_balance_usdt -= trade_cost
                        order_id = f"SIM_BUY_{int(time.time() * 1000)}"
                        print(f"📝 [PAPER] Virtual entry filled at ${current_price:.2f}. Balance remaining: ${virtual_balance_usdt:.2f} USDT")
                    else:
                        order = exchange.create_market_buy_order(symbol, trade_amt)
                        order_id = order.get('id', 'UNKNOWN_ID')

                    calculated_dist = metrics['atr'] * ATR_MULTIPLIER
                    calculated_stop = current_price - calculated_dist

                    with state_lock:
                        positions[symbol] = {
                            "position_active": True,
                            "entry_price": current_price,
                            "trailing_stop": calculated_stop,
                            "stop_loss_distance": calculated_dist
                        }
                    save_state()

                    msg = (
                        f"⚡ *TRADE OPENED (SWING TREND)*\n"
                        f"Pair: {symbol}\n"
                        f"Price: {current_price:.2f}\n"
                        f"Initial Stop: {calculated_stop:.2f}\n"
                        f"Buffer: {calculated_dist:.2f}"
                    )
                    send_telegram_notification(msg)
                    log_trade_to_ledger(
                        timestamp, symbol, order_id, 'BUY', 'SWING',
                        current_price, trade_amt, calculated_stop, 'POSITION_OPEN'
                    )
                except Exception as e:
                    print(f"❌ [{symbol}] Entry order blocked: {e}")
                    send_telegram_notification(f"❌ [{symbol}] Entry order failure: {e}")

# =====================================================================
# 11. BACKGROUND TRADING ENGINE
# =====================================================================
def trading_engine_loop():
    """Dedicated worker thread decoupled from Flask HTTP lifecycles."""
    update_engine_status(
        engine_state="STARTING",
        thread_alive=True,
        startup_timestamp=utc_now_iso(),
        last_error=None,
        halt_reason=None,
        startup_reconciled=False
    )

    try:
        print("♻️ Loading persistent multi-pair state from Upstash...")
        load_state()

        print("🔎 Performing startup reconciliation...")
        reconcile_state_with_exchange()

        update_engine_status(
            startup_reconciled=True,
            engine_state="RUNNING"
        )

        mode_str = "PAPER TRADING" if PAPER_TRADING else "LIVE CAPITAL"
        print(f"🚀 Engine Active [{mode_str}]. Monitoring {len(SYMBOLS)} symbols.")
        send_telegram_notification(f"🚀 Kraken Engine Active [{mode_str}]! Tracking: {', '.join(SYMBOLS)}")

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
                print(f"💥 Global trading loop fault encountered: {error_details}")
                traceback.print_exc()
                update_engine_status(
                    engine_state="RUNNING_WITH_ERRORS",
                    last_error=error_details
                )
                send_telegram_notification(f"⚠️ Trading loop exception: {error_details}")

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
        send_telegram_notification(f"🛑 *ENGINE STARTUP HALTED*: {e}")
    except Exception as e:
        error_details = f"{type(e).__name__}: {e}"
        print(f"💥 Trading engine failed during startup: {error_details}")
        traceback.print_exc()
        update_engine_status(
            engine_state="STARTUP_FAILED",
            last_error=error_details,
            halt_reason="Trading engine startup failed.",
            thread_alive=False,
            startup_reconciled=False
        )
        send_telegram_notification(f"💥 *ENGINE STARTUP FAILED*: {error_details}")
    finally:
        with state_lock:
            engine_status["thread_alive"] = False
        print("ℹ️ Trading engine thread stopped. Web service remains online.")

# =====================================================================
# 12. RENDER WEB SERVICE
# =====================================================================
app = Flask(__name__)

@app.route("/", methods=["GET"])
@app.route("/health", methods=["GET"])
def health():
    """Zero-dependency keep-alive endpoint for external uptime monitors."""
    return jsonify({
        "status": "ok",
        "service": "kraken-hybrid-regime-bot-multipair",
        "paper_trading": PAPER_TRADING,
        "timestamp": utc_now_iso()
    }), 200

@app.route("/status", methods=["GET"])
def status():
    """Diagnostic telemetry endpoint exposing thread-safe state cache."""
    return jsonify(get_status_snapshot()), 200

# =====================================================================
# 13. APPLICATION BOOTSTRAP
# =====================================================================
def start_trading_thread():
    """Spawns the background trading daemon."""
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
    print("🧵 Background multi-pair trading engine thread started.")

if __name__ == "__main__":
    print(f"🌐 Starting Kraken Multi-Pair Web Service on 0.0.0.0:{SERVER_PORT}")
    print(f"⚙️ Execution Mode: {'PAPER TRADING (Simulated)' if PAPER_TRADING else 'LIVE (Real Capital)'}")
    print("📡 Health endpoint: /health")
    print("📊 Status endpoint: /status")

    start_trading_thread()

    app.run(
        host="0.0.0.0",
        port=SERVER_PORT,
        debug=False,
        use_reloader=False,
        threaded=True
    )
