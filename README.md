# Dual-Exchange Quant Terminal — Setup & Activation Guide

This build adds to the original bot: real futures venues (Binance + Kraken),
trade-quality grading with anti-overtrading gates, a live CSV trade-history
view, and a dashboard tuned for iPad Pro (M4) and iPhone 17 Pro Max, plus
Telegram alerts + a Telegram Mini App.

## 1. Install & run

```bash
pip install ccxt flask pandas requests --break-system-packages
python trading_bot.py
```

Endpoints once running:
- `/` — main dashboard
- `/miniapp` — Telegram Mini App view
- `/status` — full JSON telemetry
- `/history` — recent trades as JSON
- `/history.csv` — full trade ledger download
- `/health`, `/config`, `/symbols/<exchange>`, `/manual-market`

## 2. Required environment variables

| Var | Purpose |
|---|---|
| `UPSTASH_REDIS_REST_URL` / `UPSTASH_REDIS_REST_TOKEN` | State persistence (required) |
| `PAPER_TRADING` | `true` while testing (default `false` — set this explicitly) |
| `PAPER_INITIAL_BALANCE` | Starting virtual USDT balance |
| `ENABLED_EXCHANGES` | `binance,kraken` |
| `BINANCE_API_KEY` / `BINANCE_SECRET_KEY` | Required if not paper trading |
| `KRAKEN_API_KEY` / `KRAKEN_SECRET_KEY` | Required if not paper trading |

## 3. Activating Telegram notifications

1. Talk to **@BotFather** on Telegram → `/newbot` → get your `TELEGRAM_BOT_TOKEN`.
2. Send your new bot any message, then hit `https://api.telegram.org/bot<TOKEN>/getUpdates`
   in a browser to find your `chat.id` → that's `TELEGRAM_CHAT_ID`.
3. Set:
   ```
   ENABLE_TELEGRAM=true
   TELEGRAM_BOT_TOKEN=<your token>
   TELEGRAM_CHAT_ID=<your chat id>
   ```
4. Restart the bot. You'll get alerts on every entry, exit, and critical fault.

## 4. Activating the Telegram Mini App

1. In BotFather: `/mybots` → select your bot → **Bot Settings → Menu Button**
   (or **Mini Apps**) → set the URL to `https://<your-deployed-host>/miniapp`.
2. The bot must be reachable over HTTPS (Telegram requires it) — deploy behind
   a host that terminates TLS (Render, Railway, Fly.io, your own reverse proxy).
3. Open your bot in Telegram → tap the menu button → the mini app dashboard loads.

## 5. Futures — new environment variables

```
ENABLE_FUTURES=true              # turns on binance_futures + kraken_futures venues
FUTURES_MAX_LEVERAGE=5.0         # hard cap, independent of spot MAX_LEVERAGE
FUTURES_MAX_PORTFOLIO_MARGIN_PCT=0.35   # separate margin budget from spot
```

Futures venues appear as `binance_futures` / `kraken_futures` alongside
`binance` / `kraken` in `/status`. Spot and futures margin budgets are
tracked and enforced **separately** so one sleeve can never eat the other's
allocation. The bot is long-only in both spot and futures — leverage adds
capital efficiency, it does not enable shorting, keeping the risk model
consistent across both sleeves.

**Before flipping this on live**, confirm on your exchange accounts that
futures/derivatives trading is actually enabled for your API keys — this is
a separate permission from spot trading on both Binance and Kraken.

> Note: `ccxt.krakenfutures` requires a reasonably recent `ccxt` version. If
> your installed version doesn't have it, the bot logs a warning and simply
> skips the Kraken Futures venue rather than crashing — check your logs on
> first boot with `ENABLE_FUTURES=true` to confirm both futures venues came up.

## 6. Trade-quality grading & anti-overtrading — new environment variables

```
MIN_GRADE_TO_TRADE=B             # A (best) .. D (worst); only >= this grade trades
MIN_SIGNAL_CONFIRMATIONS=2       # setup must persist this many scan cycles
SYMBOL_COOLDOWN_MINUTES=30       # freeze re-entry on a symbol after closing it
```

How this stops "to-and-fro" trading:
- **Grading** — every opportunity score (0.0–1.0) is bucketed into A/B/C/D.
  Trades below `MIN_GRADE_TO_TRADE` are scanned and shown on the dashboard,
  but never executed.
- **Confirmation gate** — a signal must reappear with the same regime/grade
  across `MIN_SIGNAL_CONFIRMATIONS` consecutive scan loops before it fires.
  One noisy candle can't trigger a trade.
- **Cooldown** — after any position closes (win or loss), that exact
  market is frozen from re-entry for `SYMBOL_COOLDOWN_MINUTES`. This is
  what stops the bot immediately re-chasing the same symbol back and forth.

All three gates are visible live under `risk_manager` in `/status`
(`signals_building_conviction`, `symbols_in_cooldown`).

## 7. Trade documentation

Every open and close is written to `dual_exchange_trade_ledger.csv` (also
pushed to Upstash as a durable list) with `MarketType`, `Grade`, and
realized `PnL` on closes. View live in the dashboard's "Trade History"
table, or download the full ledger any time from `/history.csv`.

## 8. Mobile / tablet dashboard

- `/` is responsive at three tiers: phone (≤600px, stacked cards),
  iPad Pro (768–1400px, multi-column grid), and desktop (>1400px).
- Add to Home Screen on iPhone 17 Pro Max or iPad Pro (M4) from Safari's
  share sheet — the `apple-mobile-web-app-capable` meta tags make it launch
  full-screen without Safari's UI, like a native app.

## 9. What still needs real-device / real-account verification

This was built and syntax/logic-checked in a sandbox without network access
to `ccxt`, so it has **not** been runtime-tested against live Binance/Kraken
futures endpoints. Before trusting it with real capital:

1. Run with `PAPER_TRADING=true, ENABLE_FUTURES=true` first and confirm both
   futures venues show up in `/status` → `venues`.
2. Confirm `client.set_leverage()` succeeds against Binance USDM futures
   testnet and Kraken Futures demo — the call is wrapped in try/except but
   silent leverage-setting failures should be checked manually at least once.
3. Verify order sizing/precision on futures contracts (`amount_to_precision`)
   matches each venue's actual contract size rules — futures often round
   differently than spot.
4. Load the dashboard on an actual iPad Pro (M4) and iPhone 17 Pro Max in
   Safari to confirm the breakpoints look right (I sized them off the
   devices' known logical resolutions, not a live render).
5. Confirm Kraken Futures order params (`create_market_buy_order`) don't
   need venue-specific params beyond what's currently passed — Kraken
   Futures' unified ccxt surface has some quirks around `reduceOnly` /
   position mode that weren't exercised here.
