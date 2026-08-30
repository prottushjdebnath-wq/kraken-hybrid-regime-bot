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
BLOCK_TRADE_ON_LEVERAGE_FAIL=false      # see section 9 — opt-in, not default-changed
```

Futures venues appear as `binance_futures` / `kraken_futures` alongside
`binance` / `kraken` in `/status`. Spot and futures margin budgets are
tracked and enforced **separately** so one sleeve can never eat the other's
allocation. The bot is long-only in both spot and futures — leverage adds
capital efficiency, it does not enable shorting, keeping the risk model
consistent across both sleeves.

**Before flipping this on live**, confirm on your exchange accounts that
futures/derivatives trading is actually enabled for your API keys — this is
a separate permission from spot trading on both Binance and Kraken. Also
confirm your Binance futures account is in **one-way position mode** — see
section 9, item 1, for why hedge mode is not currently supported.

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
- On iPad Pro **landscape** (≥1180px), the "Active Positions" and
  "Cross-Exchange Top Opportunities" tables now sit side-by-side via
  `.tablet-split`; below that width (portrait, or phone) they stack as
  before. See section 9, item 3.

## 9. Second-pass hardening notes (this round)

This round of work was also done in a sandbox **with no outbound network
access** (`pip install ccxt` fails — no PyPI reachability, let alone Binance/
Kraken testnet endpoints) and no physical iPad/iPhone. So exactly like the
prior pass, this could only go as far as static/logic review — nothing below
was verified by actually running against an exchange or a real device.
What changed:

1. **Fixed a real bug: futures exits now pass `reduceOnly: true`.**
   `manage_active_position()`'s exit order previously called
   `create_market_sell_order` with no params on futures venues. On a Binance
   USDM account in *hedge mode* (or if the bot's local position record ever
   drifts from the account's actual position), a plain sell can open a new
   short instead of closing the tracked long — doubling exposure instead of
   flattening it. `reduceOnly` is now set whenever the position's market
   type is `FUTURES`. This is a one-way-mode assumption (matches the entry
   side, which also never sets `positionSide`) — **if you run Binance in
   hedge mode, entries and exits both need an explicit `positionSide` param,
   which is not implemented.** Confirm your account is in one-way mode, or
   treat hedge-mode support as still open.

2. **`set_leverage()` failure handling is now configurable, not silently
   changed.** New env var:
   ```
   BLOCK_TRADE_ON_LEVERAGE_FAIL=false   # default: unchanged behavior
   ```
   Default `false` preserves the original behavior (log the failure, enter
   the trade anyway at whatever leverage the venue account currently
   defaults to — which will not match the leverage the risk manager sized
   the trade for). Set to `true` to abort the entry instead if leverage
   can't be confirmed. **This is a real trade-off, not something I picked
   for you:** blocking is safer but means a transient API hiccup can stall
   entries; not blocking means a sized position can occasionally execute at
   the wrong leverage. Left as opt-in rather than flipped by default.

3. **`.tablet-split` is now wired into the markup.** The "Active Positions"
   and "Cross-Exchange Top Opportunities" tables are wrapped in
   `.tablet-split`, which goes two-column at `≥1180px` (iPad Pro landscape,
   both 11" and 13") and stays stacked below that — so phone and iPad
   portrait layouts are visually unchanged. Each table keeps its own
   horizontal scroll (`overflow-x: auto`) since two 10–13 column tables at
   half width will still need it even on a 13" iPad. **Not visually
   confirmed on a real device** — only reasoned from the CSS.

4. **Everything below is still open, same as the prior pass** — none of it
   could be exercised without real network/API access or real hardware:
   - Booting the bot and confirming `binance_futures` + `kraken_futures`
     both appear in `/status` → `venues` with real or testnet keys.
   - Whether `ccxt.krakenfutures` is present and initializes cleanly on
     whatever `ccxt` version actually gets installed at deploy time.
   - `discover_all_markets()` against live tickers — the filtering logic
     (quote-currency match via market metadata, `linear` contracts only,
     leveraged-token symbol exclusion) reads correctly but was never run
     against a real market list.
   - Whether `client.set_leverage()` actually applies pre-trade on Binance
     USDM testnet and Kraken Futures demo, now that failures can optionally
     block the trade (see item 2 above).
   - `amount_to_precision` / contract sizing correctness on real futures
     contract specs (multipliers can differ from spot) — the code always
     calls it through the specific venue's `ccxt` client instance, which is
     the correct pattern, but the actual rounding was never checked against
     live market precision data.
   - `GRADE_THRESHOLDS` (A ≥0.75, B ≥0.55, C ≥0.40), `MIN_SIGNAL_CONFIRMATIONS=2`,
     `SYMBOL_COOLDOWN_MINUTES=30` are still untuned defaults — reasonable
     starting points, not validated against real signal frequency or paper
     PnL. No change made here; needs a paper-trading run with logging to
     tune properly.
   - Cross-venue correlation control: right now `MAX_CONCURRENT_POSITIONS`
     and separate spot/futures margin budgets are the only diversification
     guard — nothing stops the bot from opening several correlated majors
     (e.g. BTC, ETH, SOL) at once if they all score independently. A
     correlation gate would need a rolling price-correlation matrix across
     open + candidate symbols; that's a real feature but non-trivial and
     risk-relevant enough that it wasn't added speculatively without data to
     validate it against. Flagging as a design recommendation rather than
     shipping unverified.
   - Full Add-to-Home-Screen full-screen confirmation on real iPadOS/iOS
     Safari.

## 10. Miniapp redesign — wallet-app style (this round)

`/miniapp` was rebuilt to match the layout pattern you showed me (a
Telegram wallet-style mini app): avatar + title header, a big tappable
hero number, four circular quick-action buttons, two info cards, and a
fixed bottom tab bar. The main browser dashboard at `/` was **not**
touched — only `/miniapp`.

Since your bot doesn't have wallet actions (deposit/send/withdraw), the
four quick-action circles map to real bot capabilities instead:
- **Stop All** — a new manual kill-switch (see below).
- **Market** — jumps to the live scanned Signals list.
- **Setup** — jumps to Manual Watchlist + Bot Parameters.
- **History** — jumps to a new trade-history list, pulled live from `/history`.

"Positions" isn't a fifth circle — like the reference app's own "Assets"
list, it's the main content below the actions, not a quick action.

**New: manual "Stop All" kill-switch.** This didn't exist before — new
routes `POST /engine/pause` and `POST /engine/resume`. Tapping the red
circle asks for confirmation, then pauses the bot: **no new positions will
open**, but any already-open position keeps being monitored and will still
hit its stop-loss or take-profit normally. It does **not** force-close open
positions at the current price — flattening everything immediately is a
much more aggressive action with its own risk (forced exit at a possibly
bad price), so I didn't build that in without you asking for it explicitly.
If you want a true "flatten everything now" button in addition to this,
that's a separate, riskier feature — say so and I'll add it deliberately,
not bundle it in here.

**Known gap — these routes have no auth**, same as every other route in
this app (`/manual-market`, etc.). Pausing is low-risk if someone else
hits the URL; *resuming* trading you didn't intend to resume is the
direction that actually matters. If this is deployed somewhere reachable
by more than just you, put it behind at minimum a shared-secret header or
IP allowlist at the proxy level before relying on Stop All as a real
safety control.

**Hero number honesty note:** the toggle shows "Total Equity" (paper mode
only — for live/testnet mode there's no real balance in `/status` today,
so it shows `—` rather than a fabricated number) and "Realized PnL,
all-time" — this is cumulative PnL since the bot started, **not** a
calendar-day figure, since the bot doesn't track day boundaries anywhere.
I labeled it "all-time" rather than "today's" for that reason. Let me know
if you actually want daily PnL — that needs a UTC-midnight reset added to
the state, which isn't there yet.

**Not verified:** same caveat as everything else in this thread — no
network access to actually open this in Telegram's WebView, so the
bottom nav / Telegram native `MainButton` interaction, `tg.showConfirm` /
`tg.showPopup` availability across Telegram client versions, and general
layout have only been reasoned through, not seen rendered.

> **Heads up:** the code in this repo does not currently contain the
> `/engine/pause` / `/engine/resume` kill-switch or the wallet-style
> hero/quick-action/bottom-tab redesign this section describes — the
> `/miniapp` template in `trading_bot.py` is the earlier segmented-list
> layout. If that round of work exists, it wasn't in the file this pass
> worked from; treat this section as aspirational until it's reconciled
> with the actual code.

## 11. Grid trading bot + live chart (this round)

**New: a real grid trading strategy**, independent of the regime scanner,
with its own env vars:

```
ENABLE_GRID_BOT=true
GRID_EXCHANGE=binance          # one venue at a time
GRID_SYMBOL=BTC/USDT
GRID_LOWER_PRICE=58000
GRID_UPPER_PRICE=68000
GRID_LEVELS=10                 # rungs, evenly spaced between lower/upper
GRID_ORDER_SIZE_USDT=25
GRID_MAX_ALLOCATION_PCT=0.20   # this sleeve's own budget, separate from spot/futures
```

**How it trades:** the range is split into `GRID_LEVELS` evenly-spaced
rungs. Every rung starts as a resting BUY (this deliberately mirrors how a
real limit order sits until price reaches it, so rungs above the current
price don't need special-casing — they just wait). When a rung's BUY
fills, the rung directly above it is armed as the matching SELL for that
inventory. When that SELL fills, PnL is realized and the original rung
re-arms as a BUY. The bot stays long-only the whole time — same as the
rest of the strategy — it never shorts. In paper mode, fills are simulated
the moment the live ticker price crosses a rung; in live/testnet mode,
real limit orders are placed and polled for fills once per main loop
cycle (not on the faster live-price cadence, to avoid hammering the
exchange with status polls).

`GRID_MAX_ALLOCATION_PCT` is this sleeve's own budget cap, tracked
completely separately from the spot/futures margin split in section 5 —
the grid bot can never eat into scanner-driven position sizing, or vice
versa.

**New endpoints:**
- `/candles?exchange=binance&symbol=BTC/USDT&timeframe=5m&limit=300` —
  OHLCV candles (unix-second timestamps) for charting.
- `/grid` — current ladder (`levels`, each an order line: price, side,
  status), realized PnL, allocation used, and the persisted
  `order_size_timeline` behind the chart's markers.

**New: live candlestick chart on `/miniapp`**, under a "📐 Grid Bot"
section, built with `lightweight-charts` (loaded from a CDN in the
browser — no build step). It shows:
- Real candles for the grid symbol, refreshed on its own 15s interval
  (separate from the 4s dashboard poll, to keep exchange API load down).
- **Grid order lines** — one horizontal price line per rung, drawn with
  `createPriceLine()`: green dashed for a pending BUY, red dashed for a
  pending SELL, solid grey once a rung has filled and moved on. Lines are
  cleared and redrawn from `/grid` on every refresh rather than mutated,
  since the charting library has no "update a price line" call.
- **Order size timeline, persisted** — every fill (and every new order
  placed as a rung re-arms) is appended to a durable Upstash list
  (`REDIS_GRID_TIMELINE_KEY`), not just kept in memory. The chart re-reads
  the full timeline from `/grid` on every refresh and redraws it as
  up/down arrow markers sized by order value. Because the markers are
  rebuilt from server-persisted data each time rather than accumulated in
  the browser, they survive a page reload, a pan, or a zoom — panning or
  zooming is just a viewport change in the chart library, it never
  touches the underlying marker data.
- **Candle interaction** — hovering or dragging across the chart
  (`subscribeCrosshairMove`) shows that candle's timestamp and OHLC values
  in a small readout strip above the chart, without disturbing the grid
  lines or markers.

**Not verified (same caveat as every other round):** this was built with
no outbound network access, so `lightweight-charts` loading from its CDN,
real order fills/polling against Binance/Kraken testnet, and the chart
rendering inside Telegram's WebView have only been reasoned through, not
seen running. Two things worth testing deliberately before relying on
this live:
1. **Sizing at the top rung** — if the highest rung's BUY fills, there's
   no rung above it to sell into (see `_grid_handle_buy_fill`), so that
   inventory just sits held. This is logged, not auto-handled — decide if
   you want a "sell above the range" fallback before running near the top
   of a real range.
2. **`GRID_LEVELS` vs. exchange minimum notional** — `GRID_ORDER_SIZE_USDT`
   is applied per rung with no minimum-notional check (unlike the
   scanner's `calculate_dynamic_entry`, which bumps under-sized Binance
   orders up to $10.5). A small size on a wide range with many levels
   could produce orders the exchange rejects; worth checking against your
   actual `GRID_LEVELS` / `GRID_ORDER_SIZE_USDT` combination before going
   live.
