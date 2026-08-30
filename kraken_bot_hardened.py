import time
import os
import json
import threading
import traceback
from datetime import datetime, timezone
import ccxt
import pandas as pd
import requests
from flask import Flask, jsonify, render_template_string, request

# =====================================================================
# 1. ENVIRONMENT CONFIGURATION & VALIDATION
# =====================================================================
PAPER_TRADING = os.environ.get("PAPER_TRADING", "false").strip().lower() in ("true", "1", "yes")
PAPER_INITIAL_BALANCE = float(os.environ.get("PAPER_INITIAL_BALANCE", "1000.0"))

# Dual Exchange Configuration
ENABLED_EXCHANGES_RAW = os.environ.get("ENABLED_EXCHANGES", "binance,kraken").strip().lower()
ENABLED_EXCHANGES = [e.strip() for e in ENABLED_EXCHANGES_RAW.split(",") if e.strip()]

BINANCE_TESTNET = os.environ.get("BINANCE_TESTNET", "true").strip().lower() in ("true", "1", "yes")
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "").strip()
BINANCE_SECRET_KEY = os.environ.get("BINANCE_SECRET_KEY", "").strip()

KRAKEN_API_KEY = os.environ.get("KRAKEN_API_KEY", "").strip()
KRAKEN_SECRET_KEY = os.environ.get("KRAKEN_SECRET_KEY", "").strip()

UPSTASH_REDIS_REST_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip()
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()
REDIS_STATE_KEY = os.environ.get("REDIS_STATE_KEY", "dual_exchange_bot_state").strip()
REDIS_LEDGER_KEY = os.environ.get("REDIS_LEDGER_KEY", "dual_exchange_bot_ledger").strip()

ENABLE_TELEGRAM = os.environ.get("ENABLE_TELEGRAM", "false").strip().lower() in ("true", "1", "yes")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

# Discovery & Regime Parameters
TRADING_SYMBOLS_MODE = os.environ.get("TRADING_SYMBOLS", "AUTO").strip()
SCAN_QUOTE_CURRENCY = os.environ.get("SCAN_QUOTE_CURRENCY", "USDT").strip().upper()
SCAN_TOP_LIQUID_PAIRS = int(os.environ.get("SCAN_TOP_LIQUID_PAIRS", "15"))

TIMEFRAME = os.environ.get("TRADING_TIMEFRAME", "15m").strip()
ADX_TREND_THRESHOLD = float(os.environ.get("ADX_TREND_THRESHOLD", "25.0"))
ATR_MULTIPLIER = float(os.environ.get("ATR_MULTIPLIER", "1.5"))

# Risk & Portfolio Parameters
RISK_PCT_PER_TRADE = float(os.environ.get("RISK_PCT_PER_TRADE", "0.015"))
RR_RATIO = float(os.environ.get("RR_RATIO", "2.0"))
MIN_LEVERAGE = float(os.environ.get("MIN_LEVERAGE", "1.0"))
MAX_LEVERAGE = float(os.environ.get("MAX_LEVERAGE", "3.0"))
MAX_CONCURRENT_POSITIONS = int(os.environ.get("MAX_CONCURRENT_POSITIONS", "3"))
MAX_PORTFOLIO_MARGIN_PCT = float(os.environ.get("MAX_PORTFOLIO_MARGIN_PCT", "0.60"))

MAX_FAILURES_ALLOWED = int(os.environ.get("MAX_FAILURES_ALLOWED", "3"))
MIN_BALANCE_USDT = float(os.environ.get("MIN_BALANCE_USDT", "10.0"))

LOOP_INTERVAL_SECONDS = int(os.environ.get("LOOP_INTERVAL_SECONDS", "300"))
LIVE_PRICE_REFRESH_SECONDS = int(os.environ.get("LIVE_PRICE_REFRESH_SECONDS", "5"))
SERVER_PORT = int(os.environ.get("PORT", "10000"))

CSV_FILE_PATH = "dual_exchange_trade_ledger.csv"
STATE_FILE_PATH = "dual_exchange_state.json"

def validate_environment():
    missing_vars = []
    if not UPSTASH_REDIS_REST_URL:
        missing_vars.append("UPSTASH_REDIS_REST_URL")
    if not UPSTASH_REDIS_REST_TOKEN:
        missing_vars.append("UPSTASH_REDIS_REST_TOKEN")
    if not PAPER_TRADING:
        if "binance" in ENABLED_EXCHANGES and (not BINANCE_API_KEY or not BINANCE_SECRET_KEY):
            missing_vars.append("BINANCE_API_KEY / BINANCE_SECRET_KEY")
        if "kraken" in ENABLED_EXCHANGES and (not KRAKEN_API_KEY or not KRAKEN_SECRET_KEY):
            missing_vars.append("KRAKEN_API_KEY / KRAKEN_SECRET_KEY")
    if ENABLE_TELEGRAM:
        if not TELEGRAM_BOT_TOKEN:
            missing_vars.append("TELEGRAM_BOT_TOKEN")
        if not TELEGRAM_CHAT_ID:
            missing_vars.append("TELEGRAM_CHAT_ID")

    if missing_vars:
        fault = f"FATAL: Missing required environment settings: {', '.join(missing_vars)}"
        print(f"🛑 {fault}")
        raise RuntimeError(fault)

validate_environment()

# =====================================================================
# 2. MULTI-EXCHANGE CLIENT INITIALIZATION
# =====================================================================
exchanges = {}

if "binance" in ENABLED_EXCHANGES:
    b_params = {'enableRateLimit': True, 'options': {'defaultType': 'spot'}}
    if BINANCE_API_KEY and BINANCE_SECRET_KEY:
        b_params['apiKey'] = BINANCE_API_KEY
        b_params['secret'] = BINANCE_SECRET_KEY
    b_client = ccxt.binance(b_params)
    if BINANCE_TESTNET:
        b_client.set_sandbox_mode(True)
        print("🧪 [BINANCE] Sandbox/Testnet routing engaged.")
    exchanges['binance'] = b_client

if "kraken" in ENABLED_EXCHANGES:
    k_params = {'enableRateLimit': True}
    if KRAKEN_API_KEY and KRAKEN_SECRET_KEY:
        k_params['apiKey'] = KRAKEN_API_KEY
        k_params['secret'] = KRAKEN_SECRET_KEY
    k_client = ccxt.kraken(k_params)
    exchanges['kraken'] = k_client

print(f"🏛️ Initialized {len(exchanges)} execution venues: {list(exchanges.keys())}")

# =====================================================================
# 3. SYNCHRONIZED MULTI-VENUE RUNTIME STATE
# =====================================================================
state_lock = threading.Lock()

# positions schema: { "BINANCE:BTC/USDT": { exchange, symbol, position_active, entry_price, ... } }
positions = {}
active_candidate_universe = {}
top_scanned_opportunities = []

# manual_watchlist schema: { "binance": ["BTC/USDT", ...], "kraken": [...] }
manual_watchlist = {ex: [] for ex in exchanges.keys()}

# live_prices schema: { "BINANCE:BTC/USDT": {"price": 64213.5, "ts": iso_string} }
live_prices = {}

virtual_balance_usdt = PAPER_INITIAL_BALANCE
consecutive_failures = 0

realized_pnl_total = 0.0
realized_pnl_by_exchange = {ex: 0.0 for ex in exchanges.keys()}

engine_status = {
    "venues": [e.upper() for e in exchanges.keys()],
    "binance_testnet": BINANCE_TESTNET,
    "engine_state": "NOT_STARTED",
    "last_loop_timestamp": None,
    "last_successful_loop_timestamp": None,
    "last_error": None,
    "halt_reason": None,
    "thread_alive": False,
    "live_feed_alive": False,
    "startup_reconciled": False,
    "startup_timestamp": None,
    "paper_trading": PAPER_TRADING,
    "persistence_backend": "UPSTASH_REDIS_REST",
    "scanner_status": "IDLE",
    "active_news_status": "MONITORING"
}

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def update_engine_status(**kwargs):
    with state_lock:
        engine_status.update(kwargs)

def get_status_snapshot():
    with state_lock:
        total_margin_used = sum(p["margin_allocated"] for p in positions.values() if p.get("position_active", False))

        active_positions_out = {}
        for key, data in positions.items():
            if not data.get("position_active", False):
                continue
            pos_copy = dict(data)
            live = live_prices.get(key)
            current_price = live["price"] if live else pos_copy.get("entry_price", 0.0)
            entry_price = pos_copy.get("entry_price", 0.0)
            leverage = pos_copy.get("leverage", 1.0) or 1.0
            units = pos_copy.get("units", 0.0)

            pos_copy["current_price"] = current_price
            pos_copy["price_updated_at"] = live["ts"] if live else None
            pos_copy["unrealized_pnl"] = round((current_price - entry_price) * units, 2)
            pos_copy["unrealized_pnl_pct"] = round(
                ((current_price / entry_price) - 1.0) * leverage * 100.0, 2
            ) if entry_price else 0.0
            active_positions_out[key] = pos_copy

        opportunities_out = []
        for item in top_scanned_opportunities:
            item_copy = {k: v for k, v in item.items() if k != "metrics"}
            live = live_prices.get(item.get("composite_key", ""))
            if live:
                item_copy["live_price"] = live["price"]
                item_copy["live_updated_at"] = live["ts"]
            opportunities_out.append(item_copy)

        total_unrealized = round(sum(p["unrealized_pnl"] for p in active_positions_out.values()), 2)

        snapshot = dict(engine_status)
        snapshot.update({
            "mode": "PAPER_TRADING" if PAPER_TRADING else "LIVE_OR_TESTNET",
            "virtual_balance_usdt": virtual_balance_usdt if PAPER_TRADING else None,
            "total_margin_allocated": round(total_margin_used, 2),
            "scanner_mode": TRADING_SYMBOLS_MODE,
            "candidate_universe_by_venue": active_candidate_universe,
            "manual_watchlist": {k: list(v) for k, v in manual_watchlist.items()},
            "timeframe": TIMEFRAME,
            "positions": active_positions_out,
            "top_opportunities": opportunities_out,
            "consecutive_failures": consecutive_failures,
            "risk_pct_per_trade": RISK_PCT_PER_TRADE,
            "rr_ratio": RR_RATIO,
            "max_leverage": MAX_LEVERAGE,
            "total_unrealized_pnl": total_unrealized,
            "realized_pnl_total": round(realized_pnl_total, 2),
            "realized_pnl_by_exchange": {k: round(v, 2) for k, v in realized_pnl_by_exchange.items()},
            "bot_parameters": {
                "risk_pct_per_trade": RISK_PCT_PER_TRADE,
                "rr_ratio": RR_RATIO,
                "min_leverage": MIN_LEVERAGE,
                "max_leverage": MAX_LEVERAGE,
                "max_concurrent_positions": MAX_CONCURRENT_POSITIONS,
                "max_portfolio_margin_pct": MAX_PORTFOLIO_MARGIN_PCT,
                "adx_trend_threshold": ADX_TREND_THRESHOLD,
                "atr_multiplier": ATR_MULTIPLIER,
                "timeframe": TIMEFRAME,
                "scan_quote_currency": SCAN_QUOTE_CURRENCY,
                "scan_top_liquid_pairs": SCAN_TOP_LIQUID_PAIRS,
                "loop_interval_seconds": LOOP_INTERVAL_SECONDS,
                "live_price_refresh_seconds": LIVE_PRICE_REFRESH_SECONDS,
                "trading_symbols_mode": TRADING_SYMBOLS_MODE
            }
        })
    return snapshot

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
        active_pos_only = {k: data for k, data in positions.items() if data.get("position_active", False)}
        state = {
            'venues': list(exchanges.keys()),
            'paper_trading': PAPER_TRADING,
            'virtual_balance_usdt': virtual_balance_usdt,
            'positions': active_pos_only,
            'consecutive_failures': consecutive_failures,
            'manual_watchlist': {k: list(v) for k, v in manual_watchlist.items()},
            'realized_pnl_total': realized_pnl_total,
            'realized_pnl_by_exchange': dict(realized_pnl_by_exchange),
            'last_updated': utc_now_iso()
        }

    serialized_state = json.dumps(state)

    try:
        upstash_command(["SET", REDIS_STATE_KEY, serialized_state])
    except Exception as e:
        err_msg = f"⚠️ Persistence failure (Upstash SET failed): {e}"
        print(err_msg)
        send_telegram_notification(err_msg)

    try:
        tmp_path = STATE_FILE_PATH + '.tmp'
        with open(tmp_path, 'w') as f:
            f.write(serialized_state)
        os.replace(tmp_path, STATE_FILE_PATH)
    except Exception as e:
        print(f"⚠️ Secondary local state write warning: {e}")

def load_state():
    global positions, virtual_balance_usdt, consecutive_failures
    global manual_watchlist, realized_pnl_total, realized_pnl_by_exchange

    recovered_raw = None

    try:
        recovered_raw = upstash_command(["GET", REDIS_STATE_KEY])
        if recovered_raw:
            print("🌐 Recovered multi-exchange state from Upstash.")
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
            print("ℹ️ No prior state found. Initializing fresh multi-venue registry.")
            return

    try:
        state = json.loads(recovered_raw) if isinstance(recovered_raw, str) else recovered_raw
        with state_lock:
            consecutive_failures = int(state.get('consecutive_failures', 0))
            if PAPER_TRADING and 'virtual_balance_usdt' in state:
                virtual_balance_usdt = float(state['virtual_balance_usdt'])

            recovered_watchlist = state.get('manual_watchlist', {})
            for ex_name in exchanges.keys():
                manual_watchlist.setdefault(ex_name, [])
                if ex_name in recovered_watchlist:
                    manual_watchlist[ex_name] = list(dict.fromkeys(recovered_watchlist[ex_name]))

            realized_pnl_total = float(state.get('realized_pnl_total', 0.0))
            recovered_realized_by_ex = state.get('realized_pnl_by_exchange', {})
            for ex_name in exchanges.keys():
                realized_pnl_by_exchange.setdefault(ex_name, 0.0)
                if ex_name in recovered_realized_by_ex:
                    realized_pnl_by_exchange[ex_name] = float(recovered_realized_by_ex[ex_name])

            saved_positions = state.get('positions', {})
            for key, p in saved_positions.items():
                is_active = bool(p.get('position_active', False))
                ex_name = p.get('exchange', key.split(':')[0].lower() if ':' in key else 'binance')
                sym = p.get('symbol', key.split(':')[1] if ':' in key else key)
                entry_p = float(p.get('entry_price', 0.0))
                t_stop = float(p.get('trailing_stop', 0.0))
                buf = float(p.get('stop_loss_distance', 0.0))
                tp = float(p.get('take_profit', 0.0))
                lev = float(p.get('leverage', 1.0))
                unt = float(p.get('units', 0.0))
                marg = float(p.get('margin_allocated', 0.0))
                reg = p.get('regime', 'NONE')
                conf = float(p.get('confidence', 0.0))
                opened = p.get('opened_at', utc_now_iso())

                if buf <= 0.0 and is_active and entry_p > t_stop:
                    buf = entry_p - t_stop
                if tp <= 0.0 and is_active and entry_p > 0:
                    tp = entry_p + (buf * RR_RATIO)

                composite_key = f"{ex_name.upper()}:{sym}"
                positions[composite_key] = {
                    "exchange": ex_name.lower(),
                    "symbol": sym,
                    "position_active": is_active,
                    "entry_price": entry_p,
                    "trailing_stop": t_stop,
                    "stop_loss_distance": buf,
                    "take_profit": tp,
                    "leverage": lev,
                    "units": unt,
                    "margin_allocated": marg,
                    "regime": reg,
                    "confidence": conf,
                    "opened_at": opened
                }

        print(f"♻️ Recovered State (last_updated={state.get('last_updated')})")
        if PAPER_TRADING:
            print(f"   ↳ [PAPER] Virtual Balance: ${virtual_balance_usdt:.2f} USDT")
        for k, p in positions.items():
            if p["position_active"]:
                print(f"   ↳ [{k}] ACTIVE | Entry: {p['entry_price']:.2f} | Stop: {p['trailing_stop']:.2f} | TP: {p['take_profit']:.2f} | Lev: {p['leverage']}x")

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
        print("📄 Paper Trading Mode: Real exchange balance reconciliation bypassed.")
        return True

    try:
        dirty = False
        with state_lock:
            active_keys = [k for k, p in positions.items() if p.get("position_active", False)]

        for key in active_keys:
            pos = positions[key]
            ex_name = pos["exchange"]
            sym = pos["symbol"]
            expected_units = pos["units"]
            base_asset = sym.split('/')[0]

            if ex_name not in exchanges:
                continue

            client = exchanges[ex_name]
            balances = client.fetch_balance()

            base_balance = (
                balances['free'].get(base_asset, 0.0)
                + balances.get('used', {}).get(base_asset, 0.0)
            )
            threshold = expected_units * 0.8 if expected_units > 0 else 0.0001
            holds_asset = base_balance >= threshold

            print(f"🔎 Startup reconciliation [{key}] on {ex_name.upper()} | Balance: {base_balance:.8f}")

            if not holds_asset:
                msg = f"⚠️ *STATE MISMATCH* [{key}]: Position recorded open but {base_asset} balance missing. Flushing stale record."
                print(msg)
                send_telegram_notification(msg)
                with state_lock:
                    positions[key]["position_active"] = False
                dirty = True

        if dirty:
            save_state()

        print("✅ Startup reconciliation completed across all active venues.")
        return True

    except Exception as e:
        msg = f"❌ Multi-exchange startup reconciliation failed: {e}"
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
        mode_tag = "PAPER" if PAPER_TRADING else ("BINANCE TESTNET / KRAKEN" if BINANCE_TESTNET else "LIVE CAPITAL")
        prefix = f"⚡ *[DUAL-EXCHANGE // {mode_tag}]* "
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

def log_trade_to_ledger(timestamp, exchange_name, symbol, order_id, side, regime, price, amount, stop_loss, take_profit, leverage, status):
    trade_record = {
        'Timestamp': timestamp,
        'Exchange': exchange_name.upper(),
        'Mode': 'PAPER' if PAPER_TRADING else 'LIVE_TESTNET',
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
        print(f"🌐 Trade logged to Upstash list '{REDIS_LEDGER_KEY}' [{exchange_name.upper()}:{symbol}]")
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
        print(f"🗒 Trade logged locally: '{CSV_FILE_PATH}' [{exchange_name.upper()}:{symbol}]")
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

            if base_asset.lower() in text_corpus or symbol.lower() in text_corpus:
                matching_articles += 1
                for kw in NEGATIVE_NEWS_KEYWORDS:
                    if kw in text_corpus:
                        negative_hits += 1
                        break

        if matching_articles > 0 and negative_hits >= 2:
            warning = f"BLOCKED: {negative_hits} negative headlines detected for {base_asset}."
            print(f"🛡️ [NEWS SHIELD] {warning}")
            return False, warning

        return True, "NEWS_CLEAR"

    except Exception as e:
        return True, f"NEWS_CHECK_BYPASS ({e})"

# =====================================================================
# 8. PRE-FLIGHT BALANCES & SAFETY LAYER
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

    # Multi-exchange real/testnet balance checks
    try:
        for ex_name, client in exchanges.items():
            bal = client.fetch_balance()
            quote_balance = bal['free'].get(SCAN_QUOTE_CURRENCY, 0.0)
            if quote_balance < MIN_BALANCE_USDT:
                print(f"⚠️ Low {SCAN_QUOTE_CURRENCY} balance on {ex_name.upper()}: {quote_balance:.2f} (Floor: {MIN_BALANCE_USDT:.2f})")

        with state_lock:
            consecutive_failures = 0
        save_state()
        return True

    except Exception as e:
        with state_lock:
            consecutive_failures += 1
            failures = consecutive_failures

        print(f"⚠️ Preflight Connection Warning ({failures}/{MAX_FAILURES_ALLOWED}): {e}")
        save_state()

        if failures >= MAX_FAILURES_ALLOWED:
            msg = f"💥 *CRITICAL FAULT*: {MAX_FAILURES_ALLOWED} consecutive API failures across venues. Script halted."
            send_telegram_notification(msg)
            raise SystemExit(msg)
        return False

# =====================================================================
# 9. DUAL-VENUE MARKET DISCOVERY SCANNER
# =====================================================================
def discover_all_markets():
    global active_candidate_universe

    universe = {}

    for ex_name, client in exchanges.items():
        with state_lock:
            manual_syms = list(manual_watchlist.get(ex_name, []))

        if TRADING_SYMBOLS_MODE.upper() != "AUTO":
            pairs = [s.strip() for s in TRADING_SYMBOLS_MODE.split(",") if s.strip()]
            universe[ex_name] = list(dict.fromkeys(pairs + manual_syms))
            continue

        try:
            is_testnet = (ex_name == 'binance' and BINANCE_TESTNET)
            min_volume_threshold = 0.0 if is_testnet else 50000.0

            tickers = client.fetch_tickers()
            valid_pairs = []

            for symbol, ticker in tickers.items():
                if not symbol.endswith(f"/{SCAN_QUOTE_CURRENCY}"):
                    continue

                base = symbol.split('/')[0]
                if base in ['USDC', 'DAI', 'FDUSD', 'EURT', 'PYUSD', 'TUSD', 'UST', 'BUSD', 'EUR', 'GBP']:
                    continue
                if 'UP/' in symbol or 'DOWN/' in symbol or 'BEAR/' in symbol or 'BULL/' in symbol:
                    continue

                quote_vol = ticker.get('quoteVolume')
                if quote_vol is None:
                    quote_vol = (ticker.get('baseVolume') or 0.0) * (ticker.get('last') or 0.0)

                if quote_vol is not None and quote_vol >= min_volume_threshold:
                    valid_pairs.append({
                        'symbol': symbol,
                        'volume': quote_vol,
                        'price': ticker.get('last') or 0.0
                    })

            valid_pairs.sort(key=lambda x: x['volume'], reverse=True)
            top_pairs = [item['symbol'] for item in valid_pairs[:SCAN_TOP_LIQUID_PAIRS]]

            if not top_pairs:
                top_pairs = [f'BTC/{SCAN_QUOTE_CURRENCY}', f'ETH/{SCAN_QUOTE_CURRENCY}', f'SOL/{SCAN_QUOTE_CURRENCY}']

            universe[ex_name] = list(dict.fromkeys(top_pairs + manual_syms))

        except Exception as e:
            print(f"⚠️ Discovery warning for {ex_name.upper()}: {e}")
            fallback = [f'BTC/{SCAN_QUOTE_CURRENCY}', f'ETH/{SCAN_QUOTE_CURRENCY}']
            universe[ex_name] = list(dict.fromkeys(fallback + manual_syms))

    with state_lock:
        active_candidate_universe = universe

    total_markets = sum(len(v) for v in universe.values())
    print(f"🎯 [DUAL SCANNER] Discovered {total_markets} active candidates across {list(universe.keys())}")
    return universe

# =====================================================================
# 10. TECHNICAL INDICATOR ENGINE & OPPORTUNITY SCORER
# =====================================================================
def analyze_market_pair(client, symbol):
    try:
        bars = client.fetch_ohlcv(symbol, TIMEFRAME, limit=100)
        df = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        rolling_20 = df['close'].rolling(20)
        sma_20 = rolling_20.mean()
        std_20 = rolling_20.std()
        bb_lower = sma_20 - (2.0 * std_20)
        bb_upper = sma_20 + (2.0 * std_20)

        delta = df['close'].diff()
        gain = delta.clip(lower=0.0)
        loss = (-delta).clip(lower=0.0)
        avg_gain = gain.ewm(alpha=1.0/14.0, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1.0/14.0, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0.0, float('nan'))
        rsi_series = 100.0 - (100.0 / (1.0 + rs))

        prev_close = df['close'].shift(1)
        tr1 = df['high'] - df['low']
        tr2 = (df['high'] - prev_close).abs()
        tr3 = (df['low'] - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr_series = tr.ewm(alpha=1.0/14.0, adjust=False).mean()

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

        vol_sma_20 = df['volume'].rolling(20).mean()
        volume_surge = float(df['volume'].iloc[-1] / (vol_sma_20.iloc[-1] + 1e-6))

        return {
            'close': float(df['close'].iloc[-1]),
            'adx': float(adx_series.iloc[-1]),
            'rsi': float(rsi_series.iloc[-1]),
            'atr': float(atr_series.iloc[-1]),
            'bb_lower': float(bb_lower.iloc[-1]),
            'bb_upper': float(bb_upper.iloc[-1]),
            'vol_surge': volume_surge
        }
    except Exception:
        return None

def score_trading_opportunity(symbol, metrics):
    current_price = metrics['close']
    atr = metrics['atr']
    adx = metrics['adx']
    rsi = metrics['rsi']
    bb_lower = metrics['bb_lower']

    # 1. Sideways Mean-Reversion Evaluation
    if adx < ADX_TREND_THRESHOLD:
        if current_price <= bb_lower:
            bb_depth = max(0.0, bb_lower - current_price)
            depth_score = min(1.0, bb_depth / (atr * 0.75 + 1e-6))
            adx_calm_score = max(0.0, (ADX_TREND_THRESHOLD - adx) / ADX_TREND_THRESHOLD)
            rsi_rebound_score = max(0.0, (50.0 - abs(rsi - 35.0)) / 50.0)

            score = round(0.40 * depth_score + 0.35 * adx_calm_score + 0.25 * rsi_rebound_score, 3)
            return "SIDEWAYS", max(0.1, min(1.0, score))

    # 2. Swing Breakout Evaluation
    else:
        if rsi <= 32.0:
            rsi_extreme = max(0.0, (32.0 - rsi) / 20.0)
            trend_power = min(1.0, (adx - ADX_TREND_THRESHOLD) / 30.0)
            vol_bonus = min(1.0, (metrics['vol_surge'] - 1.0) / 2.0) if metrics['vol_surge'] > 1.0 else 0.0

            score = round(0.45 * rsi_extreme + 0.35 * trend_power + 0.20 * vol_bonus, 3)
            return "SWING", max(0.1, min(1.0, score))

    return "NONE", 0.0

# =====================================================================
# 11. ADAPTIVE LEVERAGE & DYNAMIC POSITION SIZING
# =====================================================================
def calculate_dynamic_entry(client, ex_name, symbol, current_price, atr, regime, score):
    with state_lock:
        if PAPER_TRADING:
            equity = virtual_balance_usdt
        else:
            try:
                bal = client.fetch_balance()
                equity = float(bal['free'].get(SCAN_QUOTE_CURRENCY, 0.0) + bal.get('used', {}).get(SCAN_QUOTE_CURRENCY, 0.0))
            except Exception:
                equity = 100.0

    effective_max_leverage = min(5.0, MAX_LEVERAGE)
    leverage = round(MIN_LEVERAGE + score * (effective_max_leverage - MIN_LEVERAGE), 1)

    dollar_risk = equity * RISK_PCT_PER_TRADE
    stop_distance = atr * ATR_MULTIPLIER
    calculated_stop = current_price - stop_distance
    take_profit = current_price + (stop_distance * RR_RATIO)

    raw_units = dollar_risk / stop_distance

    # Binance Min-Notional Rule ($10.5 minimum notional)
    if ex_name == "binance" and (raw_units * current_price) < 10.5:
        raw_units = 11.0 / current_price

    # Precision formatting via CCXT
    try:
        units = float(client.amount_to_precision(symbol, raw_units))
    except Exception:
        units = round(raw_units, 4)

    notional_value = units * current_price
    margin_required = notional_value / leverage

    with state_lock:
        current_margin_used = sum(p["margin_allocated"] for p in positions.values() if p.get("position_active", False))

    available_margin_budget = (equity * MAX_PORTFOLIO_MARGIN_PCT) - current_margin_used

    if margin_required > available_margin_budget:
        if available_margin_budget > 10.0:
            margin_required = available_margin_budget
            notional_value = margin_required * leverage
            try:
                units = float(client.amount_to_precision(symbol, notional_value / current_price))
            except Exception:
                units = round(notional_value / current_price, 4)
        else:
            return None

    return {
        "confidence": score,
        "leverage": leverage,
        "units": units,
        "margin_required": round(margin_required, 2),
        "trailing_stop": round(calculated_stop, 2),
        "take_profit": round(take_profit, 2),
        "stop_distance": round(stop_distance, 2)
    }

# =====================================================================
# 12. ACTIVE POSITION GUARD ENGINE
# =====================================================================
def manage_active_position(key, current_price):
    global positions, virtual_balance_usdt, realized_pnl_total, realized_pnl_by_exchange

    with state_lock:
        pos = positions[key]
        ex_name = pos["exchange"]
        symbol = pos["symbol"]
        curr_entry = pos["entry_price"]
        curr_stop = pos["trailing_stop"]
        curr_tp = pos["take_profit"]
        risk_dist = pos["stop_loss_distance"]
        units = pos["units"]
        leverage = pos["leverage"]
        margin_allocated = pos["margin_allocated"]
        regime = pos["regime"]

    mode_label = "PAPER" if PAPER_TRADING else ("TESTNET" if (ex_name == 'binance' and BINANCE_TESTNET) else "LIVE")
    print(
        f"🛡️ [{key} | {mode_label}] {regime} | Entry: {curr_entry:.2f} | Current: {current_price:.2f} | "
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
        print(f"🚨 [{key}] {exit_type} TRIGGERED at {current_price:.2f}!")
        try:
            timestamp = utc_now_iso()
            pnl = (current_price - curr_entry) * units

            if PAPER_TRADING:
                order_id = f"SIM_{exit_type[:4]}_{int(time.time() * 1000)}"
                with state_lock:
                    virtual_balance_usdt += (margin_allocated + pnl)
                print(f"📝 [{key} PAPER] Exit filled at ${current_price:.2f}. PnL: ${pnl:+.2f}. New Balance: ${virtual_balance_usdt:.2f}")
            else:
                client = exchanges[ex_name]
                params = {}
                if leverage > 1.0 and ex_name == "kraken":
                    params['leverage'] = int(leverage)
                order = client.create_market_sell_order(symbol, units, params)
                order_id = order.get('id', 'UNKNOWN_ID')

            msg = (
                f"🚨 *POSITION CLOSED* [{key}]\n"
                f"Venue: `{ex_name.upper()}`\n"
                f"Reason: `{exit_type}`\n"
                f"Exit Price: `{current_price:.2f}`\n"
                f"PnL: `{pnl:+.2f} {SCAN_QUOTE_CURRENCY}`\n"
                f"Leverage: `{leverage}x`"
            )
            send_telegram_notification(msg)
            log_trade_to_ledger(
                timestamp, ex_name, symbol, order_id, 'SELL', regime,
                current_price, units, curr_stop, curr_tp, leverage, exit_type
            )

            with state_lock:
                positions[key]["position_active"] = False
                realized_pnl_total += pnl
                realized_pnl_by_exchange[ex_name] = realized_pnl_by_exchange.get(ex_name, 0.0) + pnl
            save_state()

        except Exception as e:
            print(f"❌ [{key}] Exit order failure: {e}")
            send_telegram_notification(f"❌ CRITICAL [{key}]: Exit order failed: {e}")

    elif current_price > curr_entry:
        new_stop = current_price - risk_dist
        if new_stop > curr_stop:
            with state_lock:
                positions[key]["trailing_stop"] = new_stop
            print(f"📈 [{key}] Trailing Stop ratcheted to: {new_stop:.2f}")
            save_state()

# =====================================================================
# 13. MASTER DUAL-EXCHANGE EXECUTION ORCHESTRATOR
# =====================================================================
def execution_orchestrator():
    global positions, virtual_balance_usdt, top_scanned_opportunities

    safety_ok = check_safety_preflight()

    # Step 1: Manage Active Positions across all exchanges
    with state_lock:
        active_keys = [k for k, p in positions.items() if p.get("position_active", False)]

    for key in active_keys:
        pos = positions[key]
        ex_name = pos["exchange"]
        symbol = pos["symbol"]
        if ex_name in exchanges:
            metrics = analyze_market_pair(exchanges[ex_name], symbol)
            if metrics:
                manage_active_position(key, metrics['close'])

    if not safety_ok:
        return

    # Step 2: Enforce Maximum Concurrent Positions Gate
    with state_lock:
        current_open_count = sum(1 for p in positions.values() if p.get("position_active", False))

    free_slots = MAX_CONCURRENT_POSITIONS - current_open_count
    if free_slots <= 0:
        print(f"⏸️ Max capacity engaged ({current_open_count}/{MAX_CONCURRENT_POSITIONS}). Scanning paused.")
        return

    # Step 3: Run Discovery and Scanning across both venues
    universe_by_exchange = discover_all_markets()
    total_pairs = sum(len(pairs) for pairs in universe_by_exchange.values())
    update_engine_status(scanner_status=f"SCANNING {total_pairs} PAIRS ACROSS VENUES")

    scored_candidates = []

    for ex_name, pair_list in universe_by_exchange.items():
        client = exchanges[ex_name]

        for symbol in pair_list:
            composite_key = f"{ex_name.upper()}:{symbol}"

            with state_lock:
                if positions.get(composite_key, {}).get("position_active", False):
                    continue

            metrics = analyze_market_pair(client, symbol)
            if not metrics:
                continue

            regime, score = score_trading_opportunity(symbol, metrics)

            if score > 0.0:
                scored_candidates.append({
                    'exchange': ex_name,
                    'symbol': symbol,
                    'composite_key': composite_key,
                    'regime': regime,
                    'score': score,
                    'price': metrics['close'],
                    'atr': metrics['atr'],
                    'adx': metrics['adx'],
                    'rsi': metrics['rsi'],
                    'metrics': metrics
                })

    # Sort descending by score across BOTH exchanges
    scored_candidates.sort(key=lambda x: x['score'], reverse=True)

    with state_lock:
        top_scanned_opportunities = scored_candidates[:10]

    update_engine_status(scanner_status=f"FOUND {len(scored_candidates)} SIGNALS")

    if not scored_candidates:
        print(f"🔍 [DUAL SCANNER] Scanned {total_pairs} markets across Binance and Kraken. No valid setups met threshold.")
        return

    # Step 4: Execute on the Best Cross-Venue Opportunities
    for candidate in scored_candidates:
        if free_slots <= 0:
            break

        ex_name = candidate['exchange']
        sym = candidate['symbol']
        key = candidate['composite_key']
        regime = candidate['regime']
        score = candidate['score']
        price = candidate['price']
        client = exchanges[ex_name]

        print(f"🎯 Top Venue Setup Selected: [{key}] | Regime: {regime} | Score: {score:.3f}")

        news_ok, news_msg = check_news_safety(sym)
        update_engine_status(active_news_status=f"[{key}]: {news_msg}")
        if not news_ok:
            print(f"🛑 Trade entry blocked by News Shield for {key}: {news_msg}")
            continue

        plan = calculate_dynamic_entry(client, ex_name, sym, price, candidate['atr'], regime, score)
        if not plan:
            print(f"⚠️ Sizing rejected: Insufficient portfolio margin allowance for {key}.")
            continue

        print(
            f"⚡ Executing {regime} Entry [{key}] | Price: {price:.2f} | Units: {plan['units']} | "
            f"Margin: ${plan['margin_required']} | Lev: {plan['leverage']}x | Score: {score:.3f}"
        )

        try:
            timestamp = utc_now_iso()
            if PAPER_TRADING:
                order_id = f"SIM_BUY_{int(time.time() * 1000)}"
                with state_lock:
                    virtual_balance_usdt -= plan['margin_required']
                print(f"📝 [{key} PAPER] Virtual entry filled. Margin: ${plan['margin_required']:.2f}")
            else:
                params = {}
                if plan['leverage'] > 1.0 and ex_name == "kraken":
                    params['leverage'] = int(plan['leverage'])
                order = client.create_market_buy_order(sym, plan['units'], params)
                order_id = order.get('id', 'UNKNOWN_ID')

            with state_lock:
                positions[key] = {
                    "exchange": ex_name,
                    "symbol": sym,
                    "position_active": True,
                    "entry_price": price,
                    "trailing_stop": plan['trailing_stop'],
                    "stop_loss_distance": plan['stop_distance'],
                    "take_profit": plan['take_profit'],
                    "leverage": plan['leverage'],
                    "units": plan['units'],
                    "margin_allocated": plan['margin_required'],
                    "regime": regime,
                    "confidence": score,
                    "opened_at": timestamp
                }
            save_state()

            msg = (
                f"🟢 *{ex_name.upper()} SCANNER ENTRY ({regime})*\n"
                f"Market: `{key}`\n"
                f"Opportunity Score: `{score:.3f}`\n"
                f"Entry Price: `{price:.2f}`\n"
                f"Size: `{plan['units']}` (${plan['margin_required'] * plan['leverage']:.2f} Notional)\n"
                f"Adaptive Leverage: `{plan['leverage']}x`\n"
                f"Initial Trailing SL: `{plan['trailing_stop']:.2f}`\n"
                f"Target TP (1:{RR_RATIO}): `{plan['take_profit']:.2f}`"
            )
            send_telegram_notification(msg)
            log_trade_to_ledger(
                timestamp, ex_name, sym, order_id, 'BUY', regime,
                price, plan['units'], plan['trailing_stop'], plan['take_profit'], plan['leverage'], 'POSITION_OPEN'
            )

            free_slots -= 1

        except Exception as e:
            print(f"❌ Entry order failed [{key}]: {e}")
            send_telegram_notification(f"❌ Entry blocked [{key}]: {e}")

# =====================================================================
# 14. BACKGROUND TRADING DAEMON
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
        # Load exchange markets for precision calculations
        for ex_name, client in exchanges.items():
            try:
                client.load_markets()
                print(f"📦 Loaded {len(client.markets)} markets from {ex_name.upper()}.")
            except Exception as mex:
                print(f"⚠️ Market loading warning on {ex_name.upper()}: {mex}")

        print("♻️ Loading multi-venue state from Upstash...")
        load_state()

        print("🔎 Performing startup reconciliation...")
        reconcile_state_with_exchange()

        update_engine_status(
            startup_reconciled=True,
            engine_state="RUNNING"
        )

        mode_str = "PAPER TRADING" if PAPER_TRADING else ("BINANCE TESTNET // KRAKEN" if BINANCE_TESTNET else "LIVE CAPITAL")
        print(f"🚀 Dual-Exchange Quantum Scanner Active [{mode_str}]. Venues: {list(exchanges.keys())}")
        send_telegram_notification(f"🚀 Dual-Exchange Engine Engaged [{mode_str}] on Binance & Kraken!")

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
# 14b. LIVE PRICE FEED DAEMON (fast-poll ticker refresh, independent of strategy loop)
# =====================================================================
def live_price_feed_loop():
    update_engine_status(live_feed_alive=True)
    print(f"📡 Live price feed thread starting (refresh every {LIVE_PRICE_REFRESH_SECONDS}s).")

    while True:
        try:
            with state_lock:
                watch_targets = {}
                for key, p in positions.items():
                    if p.get("position_active", False):
                        watch_targets[key] = (p["exchange"], p["symbol"])
                for item in top_scanned_opportunities:
                    watch_targets[item["composite_key"]] = (item["exchange"], item["symbol"])

            fresh_prices = {}
            for key, (ex_name, symbol) in watch_targets.items():
                client = exchanges.get(ex_name)
                if not client:
                    continue
                try:
                    ticker = client.fetch_ticker(symbol)
                    price = ticker.get("last") or ticker.get("close") or 0.0
                    fresh_prices[key] = {"price": float(price), "ts": utc_now_iso()}
                except Exception as tick_err:
                    print(f"⚠️ Live feed ticker fetch failed [{key}]: {tick_err}")

            if fresh_prices:
                with state_lock:
                    live_prices.update(fresh_prices)

            update_engine_status(live_feed_alive=True)

        except Exception as e:
            print(f"⚠️ Live price feed loop error: {e}")

        time.sleep(max(2, LIVE_PRICE_REFRESH_SECONDS))

# =====================================================================
# 15. OPERATOR TERMINAL UI (MULTI-VENUE DASHBOARD)
# =====================================================================
app = Flask(__name__)

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
  <title>Institutional Multi-Exchange Terminal</title>
  <style>
    :root {
      --bg: #090d16;
      --card: #111726;
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
      padding-top: calc(16px + env(safe-area-inset-top));
      padding-left: calc(16px + env(safe-area-inset-left));
      padding-right: calc(16px + env(safe-area-inset-right));
      padding-bottom: calc(16px + env(safe-area-inset-bottom));
      line-height: 1.4;
      -webkit-font-smoothing: antialiased;
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 10px;
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
      flex-wrap: wrap;
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
    .badge-testnet { background: rgba(88, 166, 255, 0.15); color: var(--blue); border: 1px solid var(--blue); }
    .badge-live { background: var(--green-glow); color: var(--green); border: 1px solid var(--green); }
    .badge-venue { background: #1a2233; color: var(--text); border: 1px solid var(--border); }
    .dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: currentColor;
      animation: pulse 2s infinite ease-in-out;
    }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }

    .tabs {
      display: flex;
      gap: 8px;
      margin-bottom: 14px;
      flex-wrap: wrap;
    }
    .tab-btn {
      background: var(--card);
      border: 1px solid var(--border);
      color: var(--muted);
      padding: 8px 16px;
      border-radius: 8px;
      font-family: var(--font-mono);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      cursor: pointer;
    }
    .tab-btn.active { color: var(--text); border-color: var(--blue); background: rgba(88,166,255,0.1); }

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
      margin-top: 6px;
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
    .score-high { color: var(--green); font-weight: 700; }
    .venue-tag {
      font-size: 10px;
      padding: 2px 6px;
      border-radius: 4px;
      font-weight: 700;
      text-transform: uppercase;
    }
    .venue-binance { background: rgba(240, 185, 11, 0.15); color: #f0b90b; border: 1px solid #f0b90b; }
    .venue-kraken { background: rgba(87, 65, 217, 0.15); color: #9c88ff; border: 1px solid #9c88ff; }
    .pnl-pos { color: var(--green); font-weight: 700; }
    .pnl-neg { color: var(--red); font-weight: 700; }
    .live-dot {
      display: inline-block;
      width: 6px; height: 6px;
      border-radius: 50%;
      background: var(--green);
      margin-right: 5px;
      animation: pulse 1.4s infinite ease-in-out;
    }

    .live-trade-panel {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 16px;
      margin-bottom: 16px;
    }
    .live-trade-empty { color: var(--muted); text-align: center; padding: 18px 0; }
    .live-trade-card {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
      gap: 10px;
      background: #0c1220;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 14px;
      margin-bottom: 10px;
    }
    .live-trade-card:last-child { margin-bottom: 0; }
    .ltc-field-label { font-size: 10px; color: var(--muted); text-transform: uppercase; margin-bottom: 3px; }
    .ltc-field-value { font-size: 15px; font-weight: 700; }

    .param-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 10px;
    }
    .param-item {
      background: #0c1220;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 10px 12px;
    }
    .param-label { font-size: 10px; color: var(--muted); text-transform: uppercase; margin-bottom: 4px; }
    .param-value { font-size: 14px; font-weight: 700; color: var(--blue); }

    .manual-panel {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 14px;
    }
    @media (max-width: 700px) { .manual-panel { grid-template-columns: 1fr; } }
    .manual-col {
      background: #0c1220;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 12px;
    }
    .manual-col h4 {
      font-size: 12px; text-transform: uppercase; color: var(--muted); margin-bottom: 10px; letter-spacing: 0.5px;
    }
    .manual-form { display: flex; gap: 6px; margin-bottom: 10px; flex-wrap: wrap; }
    .manual-form input, .manual-form select {
      background: var(--bg);
      border: 1px solid var(--border);
      color: var(--text);
      padding: 7px 9px;
      border-radius: 6px;
      font-family: var(--font-mono);
      font-size: 12px;
      flex: 1;
      min-width: 100px;
    }
    .manual-form button {
      background: var(--blue);
      color: #05080f;
      border: none;
      padding: 7px 14px;
      border-radius: 6px;
      font-family: var(--font-mono);
      font-weight: 700;
      font-size: 12px;
      cursor: pointer;
    }
    .watchlist-chips { display: flex; flex-wrap: wrap; gap: 6px; }
    .chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: #1a2233;
      border: 1px solid var(--border);
      color: var(--text);
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 11px;
    }
    .chip button {
      background: none;
      border: none;
      color: var(--red);
      cursor: pointer;
      font-weight: 700;
      font-size: 12px;
      line-height: 1;
      padding: 0;
    }
    .manual-status { font-size: 11px; color: var(--muted); margin-top: 6px; min-height: 14px; }

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

    /* =================================================================
       IPHONE / MOBILE LAYOUT (<= 480px, also applies up to 600px)
       ================================================================= */
    @media (max-width: 600px) {
      body {
        padding: 10px;
        padding-top: calc(10px + env(safe-area-inset-top));
        padding-left: calc(10px + env(safe-area-inset-left));
        padding-right: calc(10px + env(safe-area-inset-right));
        padding-bottom: calc(10px + env(safe-area-inset-bottom));
      }

      .header {
        flex-direction: column;
        align-items: flex-start;
        padding: 12px 14px;
      }
      .title { font-size: 13px; gap: 6px; }
      .badge { font-size: 10px; padding: 4px 8px; }
      #last-updated { font-size: 11px; align-self: flex-end; }

      .tabs { gap: 6px; }
      .tab-btn {
        flex: 1;
        min-width: 0;
        padding: 12px 6px;
        font-size: 11px;
        text-align: center;
      }

      .grid { grid-template-columns: repeat(2, 1fr); gap: 8px; }
      .card { padding: 10px; }
      .card-label { font-size: 9px; }
      .card-value { font-size: 16px; }

      .section-title { font-size: 12px; margin-top: 14px; }

      /* Manual market columns already stack via the 700px rule above */
      .manual-form input, .manual-form select { min-width: 0; font-size: 13px; padding: 10px; }
      .manual-form button { padding: 10px 14px; font-size: 13px; }
      .chip { font-size: 12px; padding: 6px 10px; }
      .chip button { font-size: 15px; }

      .param-grid { grid-template-columns: repeat(2, 1fr); }

      .live-trade-card { grid-template-columns: repeat(2, 1fr); padding: 12px; gap: 8px; }
      .ltc-field-value { font-size: 13px; }

      /* Convert wide data tables into stacked cards on phones */
      .table-container.responsive-table table thead { display: none; }
      .table-container.responsive-table table,
      .table-container.responsive-table table tbody,
      .table-container.responsive-table table tr,
      .table-container.responsive-table table td {
        display: block;
        width: 100%;
      }
      .table-container.responsive-table table { overflow-x: visible; }
      .table-container.responsive-table tr {
        border-bottom: 1px solid var(--border);
        padding: 10px 12px;
      }
      .table-container.responsive-table tr:last-child { border-bottom: none; }
      .table-container.responsive-table tr.empty-row {
        text-align: center;
        color: var(--muted);
      }
      .table-container.responsive-table td {
        border-bottom: none;
        padding: 4px 0;
        white-space: normal;
        display: flex;
        justify-content: space-between;
        gap: 10px;
        font-size: 13px;
      }
      .table-container.responsive-table td[data-label]::before {
        content: attr(data-label);
        font-size: 10px;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.4px;
        flex-shrink: 0;
      }
      .table-container.responsive-table td.empty-cell::before { content: ""; }

      .raw-box { font-size: 10px; max-height: 260px; overflow-y: auto; }
      .footer { flex-direction: column; gap: 4px; }
    }
  </style>
</head>
<body>
  <div class="header">
    <div class="title">
      <span>DUAL QUANT TERMINAL</span>
      <span class="badge badge-venue">BINANCE + KRAKEN</span>
      <span id="badge-mode" class="badge badge-testnet"><span class="dot"></span>MODE</span>
      <span id="badge-state" class="badge badge-running"><span class="dot"></span>RUNNING</span>
      <span id="badge-feed" class="badge badge-testnet"><span class="live-dot"></span>LIVE FEED</span>
    </div>
    <div style="font-size: 12px; color: var(--muted);" id="last-updated">--:--:--</div>
  </div>

  <div class="tabs">
    <button class="tab-btn active" data-venue="all" onclick="setVenueFilter('all')">All Venues</button>
    <button class="tab-btn" data-venue="binance" onclick="setVenueFilter('binance')">Binance</button>
    <button class="tab-btn" data-venue="kraken" onclick="setVenueFilter('kraken')">Kraken</button>
  </div>

  <div class="grid">
    <div class="card">
      <div class="card-label">Execution Mode</div>
      <div class="card-value" id="val-mode" style="font-size: 16px; margin-top: 4px;">SYNCING</div>
    </div>
    <div class="card">
      <div class="card-label">Scanner Status</div>
      <div class="card-value" id="val-scanner" style="font-size: 15px; margin-top: 4px;">SCANNING</div>
    </div>
    <div class="card">
      <div class="card-label">Active Positions</div>
      <div class="card-value" id="val-positions">0 / 3</div>
    </div>
    <div class="card">
      <div class="card-label">News Safety Shield</div>
      <div class="card-value" id="val-news" style="font-size: 14px; margin-top: 4px;">MONITORING</div>
    </div>
    <div class="card">
      <div class="card-label">Unrealized PnL (open)</div>
      <div class="card-value" id="val-unrealized">$0.00</div>
    </div>
    <div class="card">
      <div class="card-label">Realized PnL (closed)</div>
      <div class="card-value" id="val-realized">$0.00</div>
    </div>
  </div>

  <div class="section-title">🔴 Current Live Trade(s)</div>
  <div class="live-trade-panel" id="live-trade-panel">
    <div class="live-trade-empty">No open positions right now. The spotlight panel will populate the instant a trade fires.</div>
  </div>

  <div class="section-title">Active Positions Across Venues</div>
  <div class="table-container responsive-table">
    <table>
      <thead>
        <tr>
          <th>Venue</th>
          <th>Symbol</th>
          <th>Regime</th>
          <th>Leverage</th>
          <th>Size</th>
          <th>Margin</th>
          <th>Entry</th>
          <th>Live Price</th>
          <th>Unrealized PnL</th>
          <th>Trailing SL</th>
          <th>Target TP</th>
        </tr>
      </thead>
      <tbody id="active-positions-tbody">
        <tr><td colspan="11" style="color: var(--muted); text-align: center;">No open positions. Dual scanner evaluating candidate markets.</td></tr>
      </tbody>
    </table>
  </div>

  <div class="section-title">Cross-Exchange Top Opportunities (Ranked by Score)</div>
  <div class="table-container responsive-table">
    <table>
      <thead>
        <tr>
          <th>Venue</th>
          <th>Market</th>
          <th>Regime</th>
          <th>Opportunity Score</th>
          <th>Last Scanned Price</th>
          <th>Live Price</th>
          <th>ADX (14)</th>
          <th>RSI (14)</th>
        </tr>
      </thead>
      <tbody id="opportunities-tbody">
        <tr><td colspan="8" style="color: var(--muted); text-align: center;">Scanning candidate markets across Binance & Kraken...</td></tr>
      </tbody>
    </table>
  </div>

  <div class="section-title">Manual Market Selection</div>
  <div class="table-container" style="padding: 14px;">
    <div class="manual-panel">
      <div class="manual-col">
        <h4>Binance Watchlist</h4>
        <div class="manual-form">
          <input type="text" id="binance-symbol-input" placeholder="e.g. BTC/USDT" />
          <button onclick="addManualSymbol('binance')">Add</button>
        </div>
        <div class="watchlist-chips" id="binance-watchlist-chips"></div>
        <div class="manual-status" id="binance-manual-status"></div>
      </div>
      <div class="manual-col">
        <h4>Kraken Watchlist</h4>
        <div class="manual-form">
          <input type="text" id="kraken-symbol-input" placeholder="e.g. ETH/USDT" />
          <button onclick="addManualSymbol('kraken')">Add</button>
        </div>
        <div class="watchlist-chips" id="kraken-watchlist-chips"></div>
        <div class="manual-status" id="kraken-manual-status"></div>
      </div>
    </div>
  </div>

  <div class="section-title">Bot Entry Parameters</div>
  <div class="table-container" style="padding: 14px;">
    <div class="param-grid" id="param-grid">
      <div class="param-item"><div class="param-label">Loading...</div><div class="param-value">--</div></div>
    </div>
  </div>

  <div class="section-title">Raw Engine Telemetry Diagnostics</div>
  <pre class="raw-box" id="raw-state">Fetching telemetrics...</pre>

  <div class="footer">
    <span>Endpoint: /status</span>
    <span>Auto-refresh: 3s &middot; Live feed: fast poll</span>
  </div>

  <script>
    let venueFilter = 'all';
    let lastSnapshot = null;

    function setVenueFilter(v) {
      venueFilter = v;
      document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.venue === v);
      });
      if (lastSnapshot) renderDashboard(lastSnapshot);
    }

    function fmtMoney(n) {
      const v = Number(n || 0);
      return (v >= 0 ? '$' : '-$') + Math.abs(v).toFixed(2);
    }

    function pnlClass(n) {
      return Number(n) >= 0 ? 'pnl-pos' : 'pnl-neg';
    }

    function venueClass(ex) {
      return ex === 'binance' ? 'venue-binance' : 'venue-kraken';
    }

    function matchesFilter(ex) {
      return venueFilter === 'all' || ex === venueFilter;
    }

    async function addManualSymbol(exchange) {
      const inputEl = document.getElementById(exchange + '-symbol-input');
      const statusEl = document.getElementById(exchange + '-manual-status');
      const symbol = (inputEl.value || '').trim().toUpperCase();
      if (!symbol.includes('/')) {
        statusEl.textContent = 'Enter symbol as BASE/QUOTE, e.g. BTC/USDT';
        return;
      }
      statusEl.textContent = 'Adding ' + symbol + '...';
      try {
        const res = await fetch('/manual-market', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ exchange: exchange, symbol: symbol, action: 'add' })
        });
        const data = await res.json();
        if (!res.ok) {
          statusEl.textContent = data.error || 'Failed to add symbol.';
          return;
        }
        inputEl.value = '';
        statusEl.textContent = symbol + ' added to ' + exchange + ' watchlist.';
        renderWatchlist(exchange, data[exchange] || []);
      } catch (err) {
        statusEl.textContent = 'Request failed: ' + err;
      }
    }

    async function removeManualSymbol(exchange, symbol) {
      try {
        const res = await fetch('/manual-market', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ exchange: exchange, symbol: symbol, action: 'remove' })
        });
        const data = await res.json();
        if (res.ok) renderWatchlist(exchange, data[exchange] || []);
      } catch (err) {
        console.error(err);
      }
    }

    function renderWatchlist(exchange, symbols) {
      const container = document.getElementById(exchange + '-watchlist-chips');
      if (!container) return;
      container.innerHTML = '';
      if (!symbols.length) {
        container.innerHTML = '<span style="color: var(--muted); font-size: 11px;">No manual symbols added yet.</span>';
        return;
      }
      for (const sym of symbols) {
        const chip = document.createElement('span');
        chip.className = 'chip';
        chip.innerHTML = '<span>' + sym + '</span>';
        const btn = document.createElement('button');
        btn.textContent = '×';
        btn.onclick = () => removeManualSymbol(exchange, sym);
        chip.appendChild(btn);
        container.appendChild(chip);
      }
    }

    function renderParams(params) {
      const grid = document.getElementById('param-grid');
      if (!params) return;
      const entries = [
        ['Risk / Trade', (params.risk_pct_per_trade * 100).toFixed(2) + '%'],
        ['Reward:Risk', '1 : ' + params.rr_ratio],
        ['Leverage Range', params.min_leverage + 'x – ' + params.max_leverage + 'x'],
        ['Max Concurrent Positions', params.max_concurrent_positions],
        ['Max Portfolio Margin', (params.max_portfolio_margin_pct * 100).toFixed(0) + '%'],
        ['ADX Trend Threshold', params.adx_trend_threshold],
        ['ATR Multiplier', params.atr_multiplier],
        ['Timeframe', params.timeframe],
        ['Scan Quote Currency', params.scan_quote_currency],
        ['Top Liquid Pairs Scanned', params.scan_top_liquid_pairs],
        ['Strategy Loop Interval', params.loop_interval_seconds + 's'],
        ['Live Feed Refresh', params.live_price_refresh_seconds + 's'],
        ['Symbol Mode', params.trading_symbols_mode]
      ];
      grid.innerHTML = '';
      for (const [label, value] of entries) {
        const item = document.createElement('div');
        item.className = 'param-item';
        item.innerHTML = '<div class="param-label">' + label + '</div><div class="param-value">' + value + '</div>';
        grid.appendChild(item);
      }
    }

    function renderLiveTradePanel(positions) {
      const panel = document.getElementById('live-trade-panel');
      const entries = Object.entries(positions).filter(([k, p]) => matchesFilter(p.exchange));
      if (entries.length === 0) {
        panel.innerHTML = '<div class="live-trade-empty">No open positions right now. The spotlight panel will populate the instant a trade fires.</div>';
        return;
      }
      panel.innerHTML = '';
      for (const [key, pos] of entries) {
        const card = document.createElement('div');
        card.className = 'live-trade-card';
        const pnlCls = pnlClass(pos.unrealized_pnl);
        let heldFor = '--';
        if (pos.opened_at) {
          const openedMs = new Date(pos.opened_at).getTime();
          if (!isNaN(openedMs)) {
            const diffMin = Math.floor((Date.now() - openedMs) / 60000);
            heldFor = diffMin < 60 ? (diffMin + 'm') : (Math.floor(diffMin / 60) + 'h ' + (diffMin % 60) + 'm');
          }
        }
        card.innerHTML = `
          <div><div class="ltc-field-label">Venue / Symbol</div><div class="ltc-field-value"><span class="venue-tag ${venueClass(pos.exchange)}">${pos.exchange}</span> ${pos.symbol}</div></div>
          <div><div class="ltc-field-label">Regime</div><div class="ltc-field-value">${pos.regime}</div></div>
          <div><div class="ltc-field-label">Entry Price</div><div class="ltc-field-value">$${Number(pos.entry_price).toFixed(2)}</div></div>
          <div><div class="ltc-field-label"><span class="live-dot"></span>Live Price</div><div class="ltc-field-value">$${Number(pos.current_price).toFixed(2)}</div></div>
          <div><div class="ltc-field-label">Unrealized PnL</div><div class="ltc-field-value ${pnlCls}">${fmtMoney(pos.unrealized_pnl)} (${Number(pos.unrealized_pnl_pct).toFixed(2)}%)</div></div>
          <div><div class="ltc-field-label">Leverage</div><div class="ltc-field-value">${pos.leverage}x</div></div>
          <div><div class="ltc-field-label">Trailing SL / TP</div><div class="ltc-field-value">$${Number(pos.trailing_stop).toFixed(2)} / $${Number(pos.take_profit).toFixed(2)}</div></div>
          <div><div class="ltc-field-label">Held For</div><div class="ltc-field-value">${heldFor}</div></div>
        `;
        panel.appendChild(card);
      }
    }

    function renderDashboard(data) {
      const badgeMode = document.getElementById('badge-mode');
      if (data.mode === 'PAPER_TRADING') {
        badgeMode.textContent = 'PAPER SIMULATION';
        badgeMode.className = 'badge badge-paper';
        document.getElementById('val-mode').textContent = 'PAPER SIMULATION';
      } else {
        badgeMode.textContent = 'BINANCE TESTNET / KRAKEN';
        badgeMode.className = 'badge badge-testnet';
        document.getElementById('val-mode').textContent = 'DEMO TESTNET & LIVE';
      }

      const badgeState = document.getElementById('badge-state');
      badgeState.textContent = data.engine_state;
      badgeState.className = 'badge ' + (data.engine_state === 'RUNNING' ? 'badge-running' : 'badge-halted');

      const badgeFeed = document.getElementById('badge-feed');
      badgeFeed.textContent = data.live_feed_alive ? 'LIVE FEED ACTIVE' : 'LIVE FEED IDLE';
      badgeFeed.className = 'badge ' + (data.live_feed_alive ? 'badge-running' : 'badge-halted');

      document.getElementById('last-updated').textContent = new Date().toLocaleTimeString();
      document.getElementById('val-scanner').textContent = data.scanner_status || 'SCANNING';

      const allPositions = data.positions || {};
      const filteredPositionsCount = Object.values(allPositions).filter(p => matchesFilter(p.exchange)).length;
      document.getElementById('val-positions').textContent = filteredPositionsCount + ' / ' + (data.bot_parameters ? data.bot_parameters.max_concurrent_positions : 3) + ' Cap';
      document.getElementById('val-news').textContent = data.active_news_status || 'MONITORING';
      document.getElementById('val-unrealized').textContent = fmtMoney(data.total_unrealized_pnl);
      document.getElementById('val-unrealized').className = 'card-value ' + pnlClass(data.total_unrealized_pnl);

      const realizedForFilter = venueFilter === 'all'
        ? data.realized_pnl_total
        : (data.realized_pnl_by_exchange ? data.realized_pnl_by_exchange[venueFilter] : 0) || 0;
      document.getElementById('val-realized').textContent = fmtMoney(realizedForFilter);
      document.getElementById('val-realized').className = 'card-value ' + pnlClass(realizedForFilter);

      renderLiveTradePanel(allPositions);

      // 1. Active Positions Table
      const actBody = document.getElementById('active-positions-tbody');
      actBody.innerHTML = '';
      const posEntries = Object.entries(allPositions).filter(([k, p]) => matchesFilter(p.exchange));
      if (posEntries.length === 0) {
        actBody.innerHTML = '<tr class="empty-row"><td colspan="11" class="empty-cell" style="color: var(--muted); text-align: center;">No open positions for this venue filter.</td></tr>';
      } else {
        for (const [key, pos] of posEntries) {
          const exClass = venueClass(pos.exchange);
          const row = document.createElement('tr');
          row.innerHTML = `
            <td data-label="Venue"><span class="venue-tag ${exClass}">${pos.exchange}</span></td>
            <td data-label="Symbol"><strong>${pos.symbol}</strong></td>
            <td data-label="Regime">${pos.regime}</td>
            <td data-label="Leverage">${pos.leverage}x</td>
            <td data-label="Size">${pos.units}</td>
            <td data-label="Margin">$${Number(pos.margin_allocated).toFixed(2)}</td>
            <td data-label="Entry">$${Number(pos.entry_price).toFixed(2)}</td>
            <td data-label="Live Price">$${Number(pos.current_price).toFixed(2)}</td>
            <td data-label="Unrealized PnL" class="${pnlClass(pos.unrealized_pnl)}">${fmtMoney(pos.unrealized_pnl)}</td>
            <td data-label="Trailing SL">$${Number(pos.trailing_stop).toFixed(2)}</td>
            <td data-label="Target TP">$${Number(pos.take_profit).toFixed(2)}</td>
          `;
          actBody.appendChild(row);
        }
      }

      // 2. Top Scanned Opportunities Table
      const oppBody = document.getElementById('opportunities-tbody');
      oppBody.innerHTML = '';
      const opps = (data.top_opportunities || []).filter(item => matchesFilter(item.exchange));
      if (opps.length === 0) {
        oppBody.innerHTML = '<tr class="empty-row"><td colspan="8" class="empty-cell" style="color: var(--muted); text-align: center;">Evaluating candidate markets across venues...</td></tr>';
      } else {
        for (const item of opps) {
          const exClass = venueClass(item.exchange);
          const row = document.createElement('tr');
          const livePrice = (item.live_price !== undefined) ? '$' + Number(item.live_price).toFixed(2) : '--';
          row.innerHTML = `
            <td data-label="Venue"><span class="venue-tag ${exClass}">${item.exchange}</span></td>
            <td data-label="Market"><strong>${item.symbol}</strong></td>
            <td data-label="Regime">${item.regime}</td>
            <td data-label="Score" class="score-high">${Number(item.score).toFixed(3)}</td>
            <td data-label="Last Scanned">$${Number(item.price).toFixed(2)}</td>
            <td data-label="Live Price">${livePrice}</td>
            <td data-label="ADX">${Number(item.adx).toFixed(1)}</td>
            <td data-label="RSI">${Number(item.rsi).toFixed(1)}</td>
          `;
          oppBody.appendChild(row);
        }
      }

      renderParams(data.bot_parameters);

      const manualWatchlist = data.manual_watchlist || {};
      renderWatchlist('binance', manualWatchlist.binance || []);
      renderWatchlist('kraken', manualWatchlist.kraken || []);

      document.getElementById('raw-state').textContent = JSON.stringify(data, null, 2);
    }

    async function updateDashboard() {
      try {
        const res = await fetch('/status');
        const data = await res.json();
        lastSnapshot = data;
        renderDashboard(data);
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

# =====================================================================
# 15b. TELEGRAM MINI APP LAYOUT
# =====================================================================
# Register this exact URL (…/miniapp) as your bot's Web App URL in
# BotFather. Uses the Telegram Web App JS SDK for native theme colors,
# the platform MainButton (instead of a custom button), and haptics.
MINIAPP_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
  <title>Dual-Exchange Bot</title>
  <script src="https://telegram.org/js/telegram-web-app.js"></script>
  <style>
    :root {
      --bg: #090d16;
      --card: #111726;
      --border: #1f2b45;
      --text: #e6edf3;
      --hint: #8b949e;
      --link: #58a6ff;
      --button: #58a6ff;
      --button-text: #05080f;
      --green: #2ea043;
      --green-glow: rgba(46, 160, 67, 0.15);
      --red: #f85149;
      --red-glow: rgba(248, 81, 73, 0.15);
      --amber: #d29922;
      --font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: var(--font);
      padding: 12px;
      padding-top: calc(12px + env(safe-area-inset-top));
      padding-left: calc(12px + env(safe-area-inset-left));
      padding-right: calc(12px + env(safe-area-inset-right));
      padding-bottom: calc(90px + env(safe-area-inset-bottom));
      line-height: 1.4;
      -webkit-font-smoothing: antialiased;
    }
    .tg-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
    }
    .tg-title { font-size: 17px; font-weight: 700; display: flex; align-items: center; gap: 8px; }
    .dot {
      width: 8px; height: 8px; border-radius: 50%;
      background: var(--green);
      animation: pulse 2s infinite ease-in-out;
      flex-shrink: 0;
    }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.35; } }
    .tg-mode-chip {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.3px;
      padding: 4px 9px;
      border-radius: 999px;
      background: var(--card);
      border: 1px solid var(--border);
      color: var(--hint);
    }
    .tg-mode-chip.paper { color: var(--amber); border-color: var(--amber); background: rgba(210,153,34,0.12); }
    .tg-mode-chip.live { color: var(--green); border-color: var(--green); background: var(--green-glow); }

    .tg-segment {
      display: flex;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 3px;
      margin-bottom: 12px;
    }
    .tg-segment button {
      flex: 1;
      background: none;
      border: none;
      color: var(--hint);
      font-family: var(--font);
      font-size: 13px;
      font-weight: 600;
      padding: 8px 4px;
      border-radius: 8px;
      cursor: pointer;
    }
    .tg-segment button.active { background: var(--button); color: var(--button-text); }

    .tg-stats-row {
      display: flex;
      gap: 8px;
      overflow-x: auto;
      padding-bottom: 4px;
      margin-bottom: 16px;
      -webkit-overflow-scrolling: touch;
      scrollbar-width: none;
    }
    .tg-stats-row::-webkit-scrollbar { display: none; }
    .tg-stat-chip {
      flex: 0 0 auto;
      min-width: 118px;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 10px 12px;
    }
    .tg-stat-label { font-size: 10px; color: var(--hint); text-transform: uppercase; letter-spacing: 0.3px; margin-bottom: 4px; }
    .tg-stat-value { font-size: 16px; font-weight: 700; }
    .pnl-pos { color: var(--green); }
    .pnl-neg { color: var(--red); }

    .tg-section-title {
      font-size: 13px;
      font-weight: 700;
      color: var(--hint);
      text-transform: uppercase;
      letter-spacing: 0.3px;
      margin: 18px 0 8px 2px;
      display: flex;
      align-items: center;
      gap: 6px;
    }

    .tg-empty {
      background: var(--card);
      border: 1px dashed var(--border);
      border-radius: 12px;
      padding: 16px;
      text-align: center;
      color: var(--hint);
      font-size: 13px;
    }

    .tg-pos-card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px 14px;
      margin-bottom: 8px;
    }
    .tg-pos-top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
    .tg-pos-symbol { font-size: 15px; font-weight: 700; display: flex; align-items: center; gap: 6px; }
    .venue-pill {
      font-size: 10px; font-weight: 700; text-transform: uppercase;
      padding: 2px 7px; border-radius: 6px;
    }
    .venue-binance { background: rgba(240, 185, 11, 0.15); color: #f0b90b; }
    .venue-kraken { background: rgba(87, 65, 217, 0.18); color: #9c88ff; }
    .tg-pos-pnl { font-size: 15px; font-weight: 700; }
    .tg-pos-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 6px 10px;
      font-size: 12px;
    }
    .tg-pos-grid div span { display: block; color: var(--hint); font-size: 10px; text-transform: uppercase; margin-bottom: 1px; }

    .tg-opp-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px 12px;
      margin-bottom: 6px;
      font-size: 13px;
    }
    .tg-opp-left { display: flex; align-items: center; gap: 8px; }
    .tg-opp-score { font-weight: 700; color: var(--green); }

    .tg-watchlist-toggle { display: flex; gap: 8px; margin-bottom: 8px; }
    .tg-watchlist-toggle button {
      flex: 1;
      background: var(--card);
      border: 1px solid var(--border);
      color: var(--hint);
      font-family: var(--font);
      font-weight: 600;
      font-size: 13px;
      padding: 8px;
      border-radius: 8px;
    }
    .tg-watchlist-toggle button.active { color: var(--text); border-color: var(--link); }
    .tg-add-form { display: flex; gap: 6px; margin-bottom: 10px; }
    .tg-add-form input {
      flex: 1;
      background: var(--card);
      border: 1px solid var(--border);
      color: var(--text);
      padding: 10px 12px;
      border-radius: 8px;
      font-family: var(--font);
      font-size: 13px;
    }
    .tg-add-form button {
      background: var(--button);
      color: var(--button-text);
      border: none;
      padding: 0 16px;
      border-radius: 8px;
      font-family: var(--font);
      font-weight: 700;
      font-size: 13px;
    }
    .tg-chip-row { display: flex; flex-wrap: wrap; gap: 6px; }
    .tg-chip {
      display: inline-flex; align-items: center; gap: 6px;
      background: var(--card); border: 1px solid var(--border);
      padding: 5px 9px; border-radius: 999px; font-size: 12px;
    }
    .tg-chip button { background: none; border: none; color: var(--red); font-weight: 700; font-size: 14px; padding: 0; }
    .tg-status-note { font-size: 11px; color: var(--hint); margin-top: 6px; min-height: 14px; }

    details.tg-params {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 10px 14px;
    }
    details.tg-params summary {
      font-size: 13px; font-weight: 700; cursor: pointer; list-style: none;
      display: flex; justify-content: space-between; align-items: center;
    }
    details.tg-params summary::-webkit-details-marker { display: none; }
    .tg-param-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 12px; margin-top: 10px; }
    .tg-param-grid div span { display: block; font-size: 10px; color: var(--hint); text-transform: uppercase; }
    .tg-param-grid div b { font-size: 13px; }

    .tg-footer-note { text-align: center; font-size: 11px; color: var(--hint); margin-top: 20px; }
  </style>
</head>
<body>
  <div class="tg-header">
    <div class="tg-title"><span class="dot"></span>Dual-Exchange Bot</div>
    <span class="tg-mode-chip" id="tg-mode-chip">SYNCING</span>
  </div>

  <div class="tg-segment">
    <button class="active" data-venue="all" onclick="setVenue('all')">All</button>
    <button data-venue="binance" onclick="setVenue('binance')">Binance</button>
    <button data-venue="kraken" onclick="setVenue('kraken')">Kraken</button>
  </div>

  <div class="tg-stats-row" id="tg-stats-row">
    <div class="tg-stat-chip"><div class="tg-stat-label">Loading</div><div class="tg-stat-value">--</div></div>
  </div>

  <div class="tg-section-title">🔴 Live Trades</div>
  <div id="tg-positions">
    <div class="tg-empty">No open positions right now.</div>
  </div>

  <div class="tg-section-title">🎯 Top Signals</div>
  <div id="tg-opportunities">
    <div class="tg-empty">Scanning markets...</div>
  </div>

  <div class="tg-section-title">➕ Manual Watchlist</div>
  <div class="tg-watchlist-toggle">
    <button class="active" data-ex="binance" onclick="setWatchExchange('binance')">Binance</button>
    <button data-ex="kraken" onclick="setWatchExchange('kraken')">Kraken</button>
  </div>
  <div class="tg-add-form">
    <input type="text" id="tg-symbol-input" placeholder="e.g. BTC/USDT" />
    <button onclick="tgAddSymbol()">Add</button>
  </div>
  <div class="tg-chip-row" id="tg-watchlist-chips"></div>
  <div class="tg-status-note" id="tg-watch-status"></div>

  <div class="tg-section-title">⚙️ Bot Parameters</div>
  <details class="tg-params">
    <summary>View entry parameters <span id="tg-params-caret">▾</span></summary>
    <div class="tg-param-grid" id="tg-param-grid"></div>
  </details>

  <div class="tg-footer-note">Full terminal available at /</div>

  <script>
    const tg = (window.Telegram && window.Telegram.WebApp) ? window.Telegram.WebApp : null;
    let venueFilter = 'all';
    let watchExchange = 'binance';
    let lastSnapshot = null;

    function applyTelegramTheme() {
      if (!tg) return;
      const p = tg.themeParams || {};
      const root = document.documentElement.style;
      if (p.bg_color) root.setProperty('--bg', p.bg_color);
      if (p.text_color) root.setProperty('--text', p.text_color);
      if (p.hint_color) root.setProperty('--hint', p.hint_color);
      if (p.link_color) root.setProperty('--link', p.link_color);
      if (p.button_color) root.setProperty('--button', p.button_color);
      if (p.button_text_color) root.setProperty('--button-text', p.button_text_color);
      if (p.secondary_bg_color) root.setProperty('--card', p.secondary_bg_color);
      try {
        tg.setHeaderColor(p.bg_color || '#090d16');
        tg.setBackgroundColor(p.bg_color || '#090d16');
      } catch (e) { /* older client versions may not support this */ }
    }

    if (tg) {
      tg.ready();
      tg.expand();
      applyTelegramTheme();
      tg.onEvent('themeChanged', applyTelegramTheme);
      tg.MainButton.setText('REFRESH');
      tg.MainButton.show();
      tg.MainButton.onClick(() => {
        if (tg.HapticFeedback) tg.HapticFeedback.impactOccurred('light');
        updateDashboard();
      });
    }

    function setVenue(v) {
      venueFilter = v;
      document.querySelectorAll('.tg-segment button').forEach(b => b.classList.toggle('active', b.dataset.venue === v));
      if (tg && tg.HapticFeedback) tg.HapticFeedback.selectionChanged();
      if (lastSnapshot) render(lastSnapshot);
    }

    function setWatchExchange(ex) {
      watchExchange = ex;
      document.querySelectorAll('.tg-watchlist-toggle button').forEach(b => b.classList.toggle('active', b.dataset.ex === ex));
      if (tg && tg.HapticFeedback) tg.HapticFeedback.selectionChanged();
      if (lastSnapshot) renderWatchlist(lastSnapshot.manual_watchlist || {});
    }

    function fmtMoney(n) {
      const v = Number(n || 0);
      return (v >= 0 ? '$' : '-$') + Math.abs(v).toFixed(2);
    }
    function pnlClass(n) { return Number(n) >= 0 ? 'pnl-pos' : 'pnl-neg'; }
    function matchesFilter(ex) { return venueFilter === 'all' || ex === venueFilter; }
    function venueClass(ex) { return ex === 'binance' ? 'venue-binance' : 'venue-kraken'; }

    async function tgAddSymbol() {
      const input = document.getElementById('tg-symbol-input');
      const status = document.getElementById('tg-watch-status');
      const symbol = (input.value || '').trim().toUpperCase();
      if (!symbol.includes('/')) {
        status.textContent = 'Enter symbol as BASE/QUOTE, e.g. BTC/USDT';
        return;
      }
      status.textContent = 'Adding ' + symbol + '...';
      try {
        const res = await fetch('/manual-market', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ exchange: watchExchange, symbol: symbol, action: 'add' })
        });
        const data = await res.json();
        if (!res.ok) { status.textContent = data.error || 'Failed to add.'; return; }
        input.value = '';
        status.textContent = symbol + ' added.';
        if (tg && tg.HapticFeedback) tg.HapticFeedback.notificationOccurred('success');
        renderWatchlist(data);
      } catch (err) {
        status.textContent = 'Request failed.';
      }
    }

    async function tgRemoveSymbol(exchange, symbol) {
      try {
        const res = await fetch('/manual-market', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ exchange: exchange, symbol: symbol, action: 'remove' })
        });
        const data = await res.json();
        if (res.ok) renderWatchlist(data);
      } catch (err) { /* ignore */ }
    }

    function renderWatchlist(watchlist) {
      const container = document.getElementById('tg-watchlist-chips');
      const symbols = watchlist[watchExchange] || [];
      container.innerHTML = '';
      if (!symbols.length) {
        container.innerHTML = '<span style="color: var(--hint); font-size: 12px;">No manual symbols yet.</span>';
        return;
      }
      for (const sym of symbols) {
        const chip = document.createElement('span');
        chip.className = 'tg-chip';
        chip.innerHTML = '<span>' + sym + '</span>';
        const btn = document.createElement('button');
        btn.textContent = '×';
        btn.onclick = () => tgRemoveSymbol(watchExchange, sym);
        chip.appendChild(btn);
        container.appendChild(chip);
      }
    }

    function renderParams(params) {
      const grid = document.getElementById('tg-param-grid');
      if (!params) return;
      const entries = [
        ['Risk / Trade', (params.risk_pct_per_trade * 100).toFixed(2) + '%'],
        ['Reward:Risk', '1:' + params.rr_ratio],
        ['Leverage', params.min_leverage + 'x–' + params.max_leverage + 'x'],
        ['Max Positions', params.max_concurrent_positions],
        ['Max Margin Use', (params.max_portfolio_margin_pct * 100).toFixed(0) + '%'],
        ['ADX Threshold', params.adx_trend_threshold],
        ['ATR Multiplier', params.atr_multiplier],
        ['Timeframe', params.timeframe],
        ['Loop Interval', params.loop_interval_seconds + 's'],
        ['Live Refresh', params.live_price_refresh_seconds + 's']
      ];
      grid.innerHTML = '';
      for (const [label, value] of entries) {
        const item = document.createElement('div');
        item.innerHTML = '<span>' + label + '</span><b>' + value + '</b>';
        grid.appendChild(item);
      }
    }

    function render(data) {
      const modeChip = document.getElementById('tg-mode-chip');
      if (data.mode === 'PAPER_TRADING') {
        modeChip.textContent = 'PAPER';
        modeChip.className = 'tg-mode-chip paper';
      } else {
        modeChip.textContent = data.binance_testnet ? 'TESTNET' : 'LIVE';
        modeChip.className = 'tg-mode-chip ' + (data.binance_testnet ? '' : 'live');
      }

      const positions = data.positions || {};
      const posEntries = Object.entries(positions).filter(([k, p]) => matchesFilter(p.exchange));

      const realizedForFilter = venueFilter === 'all'
        ? data.realized_pnl_total
        : (data.realized_pnl_by_exchange ? data.realized_pnl_by_exchange[venueFilter] : 0) || 0;

      const statsRow = document.getElementById('tg-stats-row');
      statsRow.innerHTML = '';
      const stats = [];
      if (data.mode === 'PAPER_TRADING') {
        stats.push(['Virtual Balance', '$' + Number(data.virtual_balance_usdt || 0).toFixed(2), '']);
      }
      stats.push(['Open Positions', posEntries.length + ' / ' + (data.bot_parameters ? data.bot_parameters.max_concurrent_positions : 3), '']);
      stats.push(['Unrealized PnL', fmtMoney(data.total_unrealized_pnl), pnlClass(data.total_unrealized_pnl)]);
      stats.push(['Realized PnL', fmtMoney(realizedForFilter), pnlClass(realizedForFilter)]);
      stats.push(['Margin Used', '$' + Number(data.total_margin_allocated || 0).toFixed(2), '']);
      for (const [label, value, cls] of stats) {
        const chip = document.createElement('div');
        chip.className = 'tg-stat-chip';
        chip.innerHTML = '<div class="tg-stat-label">' + label + '</div><div class="tg-stat-value ' + cls + '">' + value + '</div>';
        statsRow.appendChild(chip);
      }

      const posContainer = document.getElementById('tg-positions');
      posContainer.innerHTML = '';
      if (posEntries.length === 0) {
        posContainer.innerHTML = '<div class="tg-empty">No open positions right now.</div>';
      } else {
        for (const [key, pos] of posEntries) {
          const card = document.createElement('div');
          card.className = 'tg-pos-card';
          card.innerHTML = `
            <div class="tg-pos-top">
              <div class="tg-pos-symbol"><span class="venue-pill ${venueClass(pos.exchange)}">${pos.exchange}</span> ${pos.symbol}</div>
              <div class="tg-pos-pnl ${pnlClass(pos.unrealized_pnl)}">${fmtMoney(pos.unrealized_pnl)}</div>
            </div>
            <div class="tg-pos-grid">
              <div><span>Entry</span>$${Number(pos.entry_price).toFixed(2)}</div>
              <div><span>Live</span>$${Number(pos.current_price).toFixed(2)}</div>
              <div><span>Lev</span>${pos.leverage}x</div>
              <div><span>SL</span>$${Number(pos.trailing_stop).toFixed(2)}</div>
              <div><span>TP</span>$${Number(pos.take_profit).toFixed(2)}</div>
              <div><span>Regime</span>${pos.regime}</div>
            </div>
          `;
          posContainer.appendChild(card);
        }
      }

      const oppContainer = document.getElementById('tg-opportunities');
      oppContainer.innerHTML = '';
      const opps = (data.top_opportunities || []).filter(item => matchesFilter(item.exchange));
      if (opps.length === 0) {
        oppContainer.innerHTML = '<div class="tg-empty">Scanning markets...</div>';
      } else {
        for (const item of opps.slice(0, 8)) {
          const row = document.createElement('div');
          row.className = 'tg-opp-row';
          row.innerHTML = `
            <div class="tg-opp-left">
              <span class="venue-pill ${venueClass(item.exchange)}">${item.exchange}</span>
              <span>${item.symbol}</span>
            </div>
            <div class="tg-opp-score">${Number(item.score).toFixed(3)}</div>
          `;
          oppContainer.appendChild(row);
        }
      }

      renderParams(data.bot_parameters);
      renderWatchlist(data.manual_watchlist || {});
    }

    async function updateDashboard() {
      try {
        const res = await fetch('/status');
        const data = await res.json();
        lastSnapshot = data;
        render(data);
      } catch (err) {
        console.error('Status fetch failed', err);
      }
    }

    updateDashboard();
    setInterval(updateDashboard, 4000);
  </script>
</body>
</html>
"""

@app.route("/miniapp", methods=["GET"])
def miniapp():
    return render_template_string(MINIAPP_HTML)

@app.route("/", methods=["GET"])
def dashboard():
    return render_template_string(DASHBOARD_HTML)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "dual-exchange-scanner",
        "venues": [e.upper() for e in exchanges.keys()],
        "paper_trading": PAPER_TRADING,
        "timestamp": utc_now_iso()
    }), 200

@app.route("/status", methods=["GET"])
def status():
    return jsonify(get_status_snapshot()), 200

@app.route("/config", methods=["GET"])
def config():
    return jsonify({
        "risk_pct_per_trade": RISK_PCT_PER_TRADE,
        "rr_ratio": RR_RATIO,
        "min_leverage": MIN_LEVERAGE,
        "max_leverage": MAX_LEVERAGE,
        "max_concurrent_positions": MAX_CONCURRENT_POSITIONS,
        "max_portfolio_margin_pct": MAX_PORTFOLIO_MARGIN_PCT,
        "adx_trend_threshold": ADX_TREND_THRESHOLD,
        "atr_multiplier": ATR_MULTIPLIER,
        "timeframe": TIMEFRAME,
        "scan_quote_currency": SCAN_QUOTE_CURRENCY,
        "scan_top_liquid_pairs": SCAN_TOP_LIQUID_PAIRS,
        "loop_interval_seconds": LOOP_INTERVAL_SECONDS,
        "live_price_refresh_seconds": LIVE_PRICE_REFRESH_SECONDS,
        "trading_symbols_mode": TRADING_SYMBOLS_MODE,
        "paper_trading": PAPER_TRADING,
        "paper_initial_balance": PAPER_INITIAL_BALANCE if PAPER_TRADING else None
    }), 200

@app.route("/symbols/<exchange>", methods=["GET"])
def list_symbols(exchange):
    ex_name = exchange.strip().lower()
    if ex_name not in exchanges:
        return jsonify({"error": f"Exchange '{exchange}' is not enabled on this instance."}), 404
    client = exchanges[ex_name]
    try:
        if not client.markets:
            client.load_markets()
        symbols = sorted([s for s in client.markets.keys() if s.endswith(f"/{SCAN_QUOTE_CURRENCY}")])
        return jsonify({"exchange": ex_name, "quote": SCAN_QUOTE_CURRENCY, "symbols": symbols}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/manual-market", methods=["GET", "POST"])
def manual_market():
    global manual_watchlist

    if request.method == "GET":
        with state_lock:
            return jsonify({k: list(v) for k, v in manual_watchlist.items()}), 200

    payload = request.get_json(force=True, silent=True) or {}
    ex_name = str(payload.get("exchange", "")).strip().lower()
    symbol = str(payload.get("symbol", "")).strip().upper()
    action = str(payload.get("action", "add")).strip().lower()

    if ex_name not in exchanges:
        return jsonify({"error": f"Exchange '{ex_name}' is not enabled on this instance."}), 400
    if "/" not in symbol:
        return jsonify({"error": "Symbol must be in BASE/QUOTE format, e.g. BTC/USDT"}), 400
    if action not in ("add", "remove"):
        return jsonify({"error": "action must be 'add' or 'remove'"}), 400

    with state_lock:
        watchlist = manual_watchlist.setdefault(ex_name, [])
        if action == "add":
            if symbol not in watchlist:
                watchlist.append(symbol)
        else:
            if symbol in watchlist:
                watchlist.remove(symbol)
        snapshot = {k: list(v) for k, v in manual_watchlist.items()}

    save_state()
    return jsonify(snapshot), 200

# =====================================================================
# 16. APPLICATION BOOTSTRAP
# =====================================================================
def start_trading_thread():
    active_threads = [
        t for t in threading.enumerate()
        if t.name == "TradingEngine" and t.is_alive()
    ]
    if active_threads:
        print("⚠️ Trading engine thread already running. Skipping duplicate spawn.")
        return

    engine_thread = threading.Thread(
        target=trading_engine_loop,
        name="TradingEngine",
        daemon=True
    )
    engine_thread.start()
    print("🧵 Dual-Exchange Quantum Engine thread started.")

def start_live_feed_thread():
    active_threads = [
        t for t in threading.enumerate()
        if t.name == "LivePriceFeed" and t.is_alive()
    ]
    if active_threads:
        print("⚠️ Live price feed thread already running. Skipping duplicate spawn.")
        return

    feed_thread = threading.Thread(
        target=live_price_feed_loop,
        name="LivePriceFeed",
        daemon=True
    )
    feed_thread.start()
    print("🧵 Live price feed thread started.")

if __name__ == "__main__":
    print(f"🌐 Starting Dual-Exchange Web Service on 0.0.0.0:{SERVER_PORT}")
    print(f"🏛️ Enabled Venues: {list(exchanges.keys())}")
    print("💻 Dashboard: /")
    print("📱 Telegram Mini App: /miniapp")
    print("📡 Health Check: /health")
    print("📊 Telemetry API: /status")
    print("⚙️  Config API: /config")
    print("🎯 Manual Market API: /manual-market")

    start_trading_thread()
    start_live_feed_thread()

    app.run(
        host="0.0.0.0",
        port=SERVER_PORT,
        debug=False,
        use_reloader=False,
        threaded=True
    )
