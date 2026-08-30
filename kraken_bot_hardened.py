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

SYMBOLS_RAW = os.environ.get("TRADING_SYMBOLS", "BTC/USDT,ETH/USDT").strip()
SYMBOLS = [s.strip() for s in SYMBOLS_RAW.split(",") if s.strip()]

TRADE_AMOUNTS_RAW = os.environ.get("TRADE_AMOUNTS", '{"BTC/USDT": 0.0005, "ETH/USDT": 0.01}').strip()
try:
    TRADE_AMOUNTS = json.loads(TRADE_AMOUNTS_RAW)
except Exception:
    TRADE_AMOUNTS = {"BTC/USDT": 0.0005, "ETH/USDT": 0.01}

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
# 2. SYNCHRONIZED RUNTIME STATE
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
            "trade_amounts": TRADE_AMOUNTS,
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
    """Dedicated worker thread running decoupled from Flask HTTP lifecycles."""
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
# 12. RENDER WEB SERVICE & OPERATOR TERMINAL UI
# =====================================================================
app = Flask(__name__)

DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, viewport-fit=cover, user-scalable=no">
<title>REGIME // Kraken Execution Terminal</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:ital,wght@0,400;0,500;0,600;0,700;0,800;1,500&display=swap" rel="stylesheet">
<style>
  :root{
    --bg:#07090f;
    --bg-elevated:#0b0f18;
    --card:#0f1420;
    --card-hi:#131a29;
    --border:#1c2536;
    --border-hi:#2b3752;
    --text:#e7edf6;
    --dim:#8792a6;
    --faint:#4c5568;
    --mint:#2dd4a8;
    --mint-dim:rgba(45,212,168,0.12);
    --rose:#ff5d78;
    --rose-dim:rgba(255,93,120,0.12);
    --amber:#f5b93d;
    --amber-dim:rgba(245,185,61,0.12);
    --sky:#4fb0ff;
    --sky-dim:rgba(79,176,255,0.12);
    --violet:#9a8cfb;
    --mono:'JetBrains Mono',ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
    --radius:10px;
  }
  *{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent;}
  html{background:var(--bg);}
  body{
    background:
      radial-gradient(1200px 500px at 15% -10%, rgba(79,176,255,0.06), transparent 60%),
      radial-gradient(1000px 600px at 100% 0%, rgba(45,212,168,0.05), transparent 55%),
      var(--bg);
    color:var(--text);
    font-family:var(--mono);
    -webkit-font-smoothing:antialiased;
    padding:12px 12px 32px;
    min-height:100vh;
  }
  @media(min-width:720px){ body{padding:20px 24px 40px;} }

  ::selection{background:var(--sky-dim);color:var(--sky);}

  /* ---------- Top bar ---------- */
  .topbar{
    display:flex;align-items:center;justify-content:space-between;gap:10px;
    padding:12px 14px;background:var(--bg-elevated);border:1px solid var(--border);
    border-radius:var(--radius);margin-bottom:10px;flex-wrap:wrap;
  }
  .brand{display:flex;align-items:center;gap:10px;min-width:0;}
  .brand-mark{
    width:26px;height:26px;flex:none;border-radius:6px;
    background:conic-gradient(from 220deg, var(--mint), var(--sky) 45%, var(--violet) 75%, var(--mint));
    display:flex;align-items:center;justify-content:center;
    box-shadow:0 0 14px rgba(45,212,168,0.35);
  }
  .brand-mark::after{content:'◆';font-size:11px;color:#04070c;font-weight:800;}
  .brand-text{display:flex;flex-direction:column;line-height:1.15;min-width:0;}
  .brand-title{font-size:13px;font-weight:800;letter-spacing:1.5px;white-space:nowrap;}
  .brand-sub{font-size:10px;color:var(--dim);letter-spacing:1px;white-space:nowrap;}
  .top-right{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
  .clock{font-size:12px;color:var(--dim);letter-spacing:0.5px;min-width:76px;text-align:right;}

  .pill{
    display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:999px;
    font-size:10px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;
    border:1px solid transparent;white-space:nowrap;
  }
  .pill-dot{width:6px;height:6px;border-radius:50%;background:currentColor;animation:blink 1.8s ease-in-out infinite;}
  @keyframes blink{0%,100%{opacity:1;box-shadow:0 0 0 0 currentColor;}50%{opacity:.35;}}
  .pill-mint{background:var(--mint-dim);color:var(--mint);border-color:rgba(45,212,168,0.35);}
  .pill-rose{background:var(--rose-dim);color:var(--rose);border-color:rgba(255,93,120,0.35);}
  .pill-amber{background:var(--amber-dim);color:var(--amber);border-color:rgba(245,185,61,0.35);}
  .pill-sky{background:var(--sky-dim);color:var(--sky);border-color:rgba(79,176,255,0.35);}
  .pill-dim{background:rgba(255,255,255,0.03);color:var(--faint);border-color:var(--border);}

  /* ---------- Ticker tape ---------- */
  .tape-wrap{
    background:var(--bg-elevated);border:1px solid var(--border);border-radius:var(--radius);
    margin-bottom:10px;overflow:hidden;position:relative;
  }
  .tape-wrap::before,.tape-wrap::after{
    content:'';position:absolute;top:0;bottom:0;width:26px;z-index:2;pointer-events:none;
  }
  .tape-wrap::before{left:0;background:linear-gradient(90deg,var(--bg-elevated),transparent);}
  .tape-wrap::after{right:0;background:linear-gradient(-90deg,var(--bg-elevated),transparent);}
  .tape-track{display:flex;width:max-content;animation:scroll-tape 26s linear infinite;}
  .tape-track:hover{animation-play-state:paused;}
  @keyframes scroll-tape{from{transform:translateX(0);}to{transform:translateX(-50%);}}
  .tape-item{
    display:flex;align-items:center;gap:8px;padding:9px 18px;font-size:11px;
    border-right:1px solid var(--border);white-space:nowrap;letter-spacing:0.3px;
  }
  .tape-sym{font-weight:700;color:var(--text);}
  .tape-state{font-weight:700;}
  .tape-state.on{color:var(--mint);}
  .tape-state.off{color:var(--faint);}
  .tape-alloc{color:var(--dim);}

  /* ---------- Stat grid ---------- */
  .grid{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;margin-bottom:10px;}
  @media(min-width:640px){.grid{grid-template-columns:repeat(3,1fr);}}
  @media(min-width:980px){.grid{grid-template-columns:repeat(6,1fr);}}
  .stat{
    background:var(--card);border:1px solid var(--border);border-radius:var(--radius);
    padding:12px 13px;position:relative;overflow:hidden;
  }
  .stat-label{font-size:9.5px;color:var(--dim);text-transform:uppercase;letter-spacing:1px;margin-bottom:7px;}
  .stat-value{font-size:18px;font-weight:700;letter-spacing:0.2px;font-variant-numeric:tabular-nums;}
  .stat-sub{font-size:10px;color:var(--faint);margin-top:4px;}
  .stat-value.mint{color:var(--mint);}
  .stat-value.rose{color:var(--rose);}
  .stat-value.amber{color:var(--amber);}
  .stat-value.sky{color:var(--sky);}

  .breaker-bar{display:flex;gap:3px;margin-top:8px;}
  .breaker-seg{height:4px;flex:1;border-radius:2px;background:var(--border);}
  .breaker-seg.lit{background:var(--rose);}

  /* ---------- Panels ---------- */
  .panel{
    background:var(--card);border:1px solid var(--border);border-radius:var(--radius);
    margin-bottom:10px;overflow:hidden;
  }
  .panel-head{
    display:flex;align-items:center;justify-content:space-between;
    padding:11px 14px;border-bottom:1px solid var(--border);
  }
  .panel-title{font-size:10.5px;text-transform:uppercase;letter-spacing:1.2px;color:var(--dim);font-weight:700;}
  .panel-meta{font-size:10px;color:var(--faint);}

  .alert-box{
    margin:12px 14px;padding:10px 12px;border-radius:8px;font-size:11.5px;line-height:1.5;
    background:var(--rose-dim);border:1px solid rgba(255,93,120,0.35);color:#ffc3ce;
  }
  .alert-box b{color:var(--rose);}

  /* ---------- Blotter table ---------- */
  .table-scroll{overflow-x:auto;}
  table{width:100%;border-collapse:collapse;font-size:12px;min-width:640px;}
  th{
    text-align:left;padding:9px 14px;font-size:9.5px;text-transform:uppercase;
    letter-spacing:0.8px;color:var(--faint);border-bottom:1px solid var(--border);
    background:var(--bg-elevated);white-space:nowrap;
  }
  td{padding:11px 14px;border-bottom:1px solid var(--border);white-space:nowrap;font-variant-numeric:tabular-nums;}
  tr:last-child td{border-bottom:none;}
  .sym-cell{display:flex;align-items:center;gap:8px;font-weight:700;}
  .sym-dot{width:7px;height:7px;border-radius:50%;flex:none;}
  .sym-dot.on{background:var(--mint);box-shadow:0 0 8px var(--mint);}
  .sym-dot.off{background:var(--faint);}
  .state-open{color:var(--mint);font-weight:700;}
  .state-flat{color:var(--faint);}
  .risk-cell{display:flex;align-items:center;gap:8px;}
  .risk-track{width:64px;height:5px;border-radius:3px;background:var(--border);overflow:hidden;flex:none;}
  .risk-fill{height:100%;background:linear-gradient(90deg,var(--amber),var(--rose));}
  .risk-pct{color:var(--dim);font-size:11px;min-width:38px;}
  .empty-row td{text-align:center;color:var(--faint);padding:20px;}

  /* ---------- Event log ---------- */
  .log-body{max-height:220px;overflow-y:auto;padding:6px 0;}
  .log-row{
    display:flex;gap:10px;padding:6px 14px;font-size:11px;border-bottom:1px solid rgba(255,255,255,0.02);
  }
  .log-time{color:var(--faint);flex:none;width:64px;}
  .log-msg{color:var(--dim);word-break:break-word;}
  .log-msg.mint{color:var(--mint);}
  .log-msg.rose{color:var(--rose);}
  .log-msg.amber{color:var(--amber);}
  .log-cursor{display:inline-block;width:7px;height:12px;background:var(--mint);margin-left:14px;vertical-align:-2px;animation:blink 1s steps(2) infinite;}

  /* ---------- Raw JSON ---------- */
  .raw-toggle{
    background:none;border:none;color:var(--sky);font-family:var(--mono);font-size:10.5px;
    cursor:pointer;padding:11px 14px;text-transform:uppercase;letter-spacing:0.8px;font-weight:700;
    display:flex;align-items:center;gap:6px;width:100%;justify-content:space-between;
  }
  .raw-toggle .chev{transition:transform .2s ease;color:var(--faint);}
  .raw-toggle.open .chev{transform:rotate(90deg);}
  .raw-box{
    display:none;background:#04060a;border-top:1px solid var(--border);
    padding:12px 14px;font-size:10.5px;color:var(--dim);overflow-x:auto;
    white-space:pre-wrap;word-break:break-all;max-height:280px;overflow-y:auto;
  }
  .raw-box.open{display:block;}

  .footer{
    display:flex;justify-content:space-between;flex-wrap:wrap;gap:6px;
    font-size:10px;color:var(--faint);padding:6px 4px 0;letter-spacing:0.3px;
  }
</style>
</head>
<body>

  <div class="topbar">
    <div class="brand">
      <div class="brand-mark"></div>
      <div class="brand-text">
        <span class="brand-title">REGIME ENGINE</span>
        <span class="brand-sub">KRAKEN &middot; MULTI-PAIR EXECUTION</span>
      </div>
    </div>
    <div class="top-right">
      <span id="badge-mode" class="pill pill-amber"><span class="pill-dot"></span>PAPER</span>
      <span id="badge-state" class="pill pill-mint"><span class="pill-dot"></span>SYNCING</span>
      <span class="clock" id="local-clock">--:--:--</span>
    </div>
  </div>

  <div class="tape-wrap">
    <div class="tape-track" id="tape-track">
      <div class="tape-item"><span class="tape-sym">CONNECTING&hellip;</span></div>
    </div>
  </div>

  <div class="grid">
    <div class="stat">
      <div class="stat-label">Capital</div>
      <div class="stat-value mint" id="val-balance">&mdash;</div>
      <div class="stat-sub" id="val-balance-sub">&nbsp;</div>
    </div>
    <div class="stat">
      <div class="stat-label">Uptime</div>
      <div class="stat-value" id="val-uptime">00:00:00</div>
      <div class="stat-sub">since boot</div>
    </div>
    <div class="stat">
      <div class="stat-label">Last Loop</div>
      <div class="stat-value sky" id="val-loopage">&mdash;</div>
      <div class="stat-sub" id="val-loop-abs">&nbsp;</div>
    </div>
    <div class="stat">
      <div class="stat-label">Circuit Breaker</div>
      <div class="stat-value" id="val-failures">0 / 3</div>
      <div class="breaker-bar" id="breaker-bar"></div>
    </div>
    <div class="stat">
      <div class="stat-label">Timeframe</div>
      <div class="stat-value" id="val-timeframe">&mdash;</div>
      <div class="stat-sub">ADX &middot; ATR regime split</div>
    </div>
    <div class="stat">
      <div class="stat-label">Persistence</div>
      <div class="stat-value amber" id="val-persist" style="font-size:13px;">&mdash;</div>
      <div class="stat-sub" id="val-reconciled">&nbsp;</div>
    </div>
  </div>

  <div class="panel" id="alert-panel" style="display:none;">
    <div class="panel-head"><span class="panel-title">Fault Report</span></div>
    <div class="alert-box" id="alert-box"></div>
  </div>

  <div class="panel">
    <div class="panel-head">
      <span class="panel-title">Position Blotter</span>
      <span class="panel-meta" id="blotter-meta">&mdash;</span>
    </div>
    <div class="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Symbol</th><th>State</th><th>Alloc</th><th>Entry</th><th>Stop</th><th>Buffer</th><th>Risk</th>
          </tr>
        </thead>
        <tbody id="positions-tbody">
          <tr class="empty-row"><td colspan="7">Awaiting telemetry&hellip;</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <div class="panel">
    <div class="panel-head">
      <span class="panel-title">Event Log</span>
      <span class="panel-meta">local &middot; session</span>
    </div>
    <div class="log-body" id="log-body">
      <div class="log-row"><span class="log-time">--:--:--</span><span class="log-msg">Awaiting first poll<span class="log-cursor"></span></span></div>
    </div>
  </div>

  <div class="panel">
    <button class="raw-toggle" id="raw-toggle">
      <span>Raw Snapshot &middot; /status</span>
      <span class="chev">&rsaquo;</span>
    </button>
    <pre class="raw-box" id="raw-state"></pre>
  </div>

  <div class="footer">
    <span>GET /status &middot; GET /health</span>
    <span>poll 3s &middot; local clocks 1s</span>
  </div>

<script>
(function(){
  var lastData = null;
  var lastFetchOk = false;
  var prevState = null, prevError = null, prevHalt = null;
  var logMax = 40;

  function pad(n){ return String(n).padStart(2,'0'); }

  function fmtClock(d){
    return pad(d.getHours())+':'+pad(d.getMinutes())+':'+pad(d.getSeconds());
  }

  function fmtDuration(ms){
    if (ms < 0) ms = 0;
    var s = Math.floor(ms/1000);
    var h = Math.floor(s/3600); s -= h*3600;
    var m = Math.floor(s/60); s -= m*60;
    return pad(h)+':'+pad(m)+':'+pad(s);
  }

  function fmtAgo(ms){
    if (ms < 0) ms = 0;
    var s = Math.floor(ms/1000);
    if (s < 60) return s + 's ago';
    var m = Math.floor(s/60);
    if (m < 60) return m + 'm ' + (s%60) + 's ago';
    var h = Math.floor(m/60);
    return h + 'h ' + (m%60) + 'm ago';
  }

  function addLog(msg, cls){
    var body = document.getElementById('log-body');
    var row = document.createElement('div');
    row.className = 'log-row';
    row.innerHTML = '<span class="log-time">'+fmtClock(new Date())+'</span><span class="log-msg '+(cls||'')+'">'+msg+'</span>';
    body.insertBefore(row, body.firstChild);
    while (body.children.length > logMax) body.removeChild(body.lastChild);
  }

  function setPill(el, text, variant){
    el.className = 'pill pill-' + variant;
    el.innerHTML = '<span class="pill-dot"></span>' + text;
  }

  function renderTape(data){
    var track = document.getElementById('tape-track');
    var syms = data.tracked_symbols || [];
    var positions = data.positions || {};
    var amounts = data.trade_amounts || {};
    if (!syms.length){ track.innerHTML = '<div class="tape-item"><span class="tape-sym">NO SYMBOLS TRACKED</span></div>'; return; }

    function buildItems(){
      return syms.map(function(sym){
        var p = positions[sym] || {};
        var active = !!p.position_active;
        var alloc = amounts[sym] !== undefined ? amounts[sym] : '--';
        return '<div class="tape-item">'
          + '<span class="tape-sym">'+sym+'</span>'
          + '<span class="tape-state '+(active?'on':'off')+'">'+(active?'OPEN':'FLAT')+'</span>'
          + '<span class="tape-alloc">'+alloc+' sz</span>'
          + '</div>';
      }).join('');
    }
    // duplicate the sequence so the CSS 50%-translate loop is seamless
    track.innerHTML = buildItems() + buildItems();
  }

  function renderPositions(data){
    var tbody = document.getElementById('positions-tbody');
    var positions = data.positions || {};
    var amounts = data.trade_amounts || {};
    var syms = data.tracked_symbols || Object.keys(positions);
    var meta = document.getElementById('blotter-meta');
    var openCount = syms.filter(function(s){ return positions[s] && positions[s].position_active; }).length;
    meta.textContent = openCount + ' open / ' + syms.length + ' tracked';

    if (!syms.length){
      tbody.innerHTML = '<tr class="empty-row"><td colspan="7">No symbols tracked</td></tr>';
      return;
    }

    tbody.innerHTML = syms.map(function(sym){
      var p = positions[sym] || {position_active:false, entry_price:0, trailing_stop:0, stop_loss_distance:0};
      var active = !!p.position_active;
      var alloc = amounts[sym] !== undefined ? amounts[sym] : '--';
      var entry = active ? '$'+Number(p.entry_price).toFixed(2) : '&mdash;';
      var stop = active ? '$'+Number(p.trailing_stop).toFixed(2) : '&mdash;';
      var buf = active ? '$'+Number(p.stop_loss_distance).toFixed(2) : '&mdash;';
      var riskPct = (active && p.entry_price) ? (p.stop_loss_distance / p.entry_price * 100) : 0;
      var riskPctClamped = Math.max(0, Math.min(100, riskPct * 10)); // amplify for visibility on a small bar
      var riskCell = active
        ? '<div class="risk-cell"><div class="risk-track"><div class="risk-fill" style="width:'+riskPctClamped.toFixed(0)+'%"></div></div><span class="risk-pct">'+riskPct.toFixed(2)+'%</span></div>'
        : '<span class="risk-pct">&mdash;</span>';

      return '<tr>'
        + '<td><div class="sym-cell"><span class="sym-dot '+(active?'on':'off')+'"></span>'+sym+'</div></td>'
        + '<td><span class="'+(active?'state-open':'state-flat')+'">'+(active?'OPEN':'FLAT')+'</span></td>'
        + '<td>'+alloc+'</td>'
        + '<td>'+entry+'</td>'
        + '<td>'+stop+'</td>'
        + '<td>'+buf+'</td>'
        + '<td>'+riskCell+'</td>'
        + '</tr>';
    }).join('');
  }

  function renderBreaker(fails){
    fails = fails || 0;
    var bar = document.getElementById('breaker-bar');
    var html = '';
    for (var i=0;i<3;i++){
      html += '<div class="breaker-seg '+(i<fails?'lit':'')+'"></div>';
    }
    bar.innerHTML = html;
    var valEl = document.getElementById('val-failures');
    valEl.textContent = fails + ' / 3';
    valEl.className = 'stat-value ' + (fails >= 3 ? 'rose' : fails > 0 ? 'amber' : '');
  }

  function renderAlert(data){
    var panel = document.getElementById('alert-panel');
    var box = document.getElementById('alert-box');
    var msg = data.halt_reason || data.last_error;
    if (msg){
      panel.style.display = 'block';
      var label = data.halt_reason ? 'HALTED' : 'LAST ERROR';
      box.innerHTML = '<b>'+label+':</b>&nbsp;' + String(msg).replace(/</g,'&lt;');
    } else {
      panel.style.display = 'none';
    }
  }

  function render(data){
    lastData = data;
    lastFetchOk = true;

    var isPaper = data.mode === 'PAPER_TRADING';
    setPill(document.getElementById('badge-mode'), isPaper ? 'PAPER' : 'LIVE', isPaper ? 'amber' : 'mint');

    var state = data.engine_state || 'UNKNOWN';
    var stateVariant = 'mint';
    if (state === 'HALTED' || state === 'STARTUP_FAILED') stateVariant = 'rose';
    else if (state === 'RUNNING_WITH_ERRORS') stateVariant = 'amber';
    else if (state === 'STARTING' || state === 'NOT_STARTED') stateVariant = 'sky';
    setPill(document.getElementById('badge-state'), state.replace(/_/g,' '), stateVariant);

    var balEl = document.getElementById('val-balance');
    var balSub = document.getElementById('val-balance-sub');
    if (data.virtual_balance_usdt !== null && data.virtual_balance_usdt !== undefined){
      balEl.textContent = '$' + Number(data.virtual_balance_usdt).toFixed(2);
      balSub.textContent = 'virtual USDT';
    } else {
      balEl.textContent = 'LIVE';
      balSub.textContent = 'exchange balance';
    }

    document.getElementById('val-timeframe').textContent = data.timeframe || '--';
    document.getElementById('val-persist').textContent = data.persistence_backend || '--';
    document.getElementById('val-reconciled').textContent = data.startup_reconciled ? 'reconciled ✓' : 'not reconciled';

    renderBreaker(data.consecutive_failures);
    renderAlert(data);
    renderTape(data);
    renderPositions(data);

    document.getElementById('raw-state').textContent = JSON.stringify(data, null, 2);

    // event log on meaningful transitions
    if (prevState !== null && prevState !== state){
      addLog('engine_state ' + prevState + ' &rarr; ' + state, stateVariant === 'rose' ? 'rose' : stateVariant === 'amber' ? 'amber' : 'mint');
    } else if (prevState === null){
      addLog('telemetry link established &middot; state=' + state, 'mint');
    }
    if (data.last_error && data.last_error !== prevError){
      addLog('error: ' + String(data.last_error), 'rose');
    }
    if (data.halt_reason && data.halt_reason !== prevHalt){
      addLog('halt: ' + String(data.halt_reason), 'rose');
    }
    prevState = state; prevError = data.last_error; prevHalt = data.halt_reason;
  }

  function tickLocalClocks(){
    document.getElementById('local-clock').textContent = fmtClock(new Date());

    if (lastData && lastData.startup_timestamp){
      var boot = new Date(lastData.startup_timestamp).getTime();
      document.getElementById('val-uptime').textContent = fmtDuration(Date.now() - boot);
    }
    if (lastData && lastData.last_loop_timestamp){
      var loop = new Date(lastData.last_loop_timestamp).getTime();
      document.getElementById('val-loopage').textContent = fmtAgo(Date.now() - loop);
      document.getElementById('val-loop-abs').textContent = new Date(lastData.last_loop_timestamp).toLocaleTimeString();
    }
  }

  async function poll(){
    try{
      var res = await fetch('/status', {cache:'no-store'});
      var data = await res.json();
      render(data);
    } catch(err){
      if (lastFetchOk){
        addLog('telemetry link lost', 'rose');
      }
      lastFetchOk = false;
      setPill(document.getElementById('badge-state'), 'DISCONNECTED', 'rose');
    }
  }

  document.getElementById('raw-toggle').addEventListener('click', function(){
    var box = document.getElementById('raw-state');
    var open = box.classList.toggle('open');
    this.classList.toggle('open', open);
  });

  poll();
  setInterval(poll, 3000);
  setInterval(tickLocalClocks, 1000);
  tickLocalClocks();
})();
</script>
</body>
</html>
"""

@app.route("/", methods=["GET"])
def dashboard():
    """Operator terminal dashboard rendered as clean dark-mode HTML."""
    return render_template_string(DASHBOARD_HTML)

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
    print("💻 Operator Dashboard: /")
    print("📡 Health endpoint: /health")
    print("📊 Status API: /status")

    start_trading_thread()

    app.run(
        host="0.0.0.0",
        port=SERVER_PORT,
        debug=False,
        use_reloader=False,
        threaded=True
    )
