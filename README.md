# Prediction Market Arbitrage Bot

A 24/7 bot that scans **Polymarket**, **Kalshi**, and **Manifold Markets** for arbitrage opportunities — price discrepancies of 10%+ on the same events across platforms.

## Features

- **Multi-platform scanning**: Polymarket, Kalshi, and Manifold Markets
- **Smart event matching**: Fuzzy string matching to identify the same event across platforms
- **Confidence scoring**: 1-10 score based on spread size, liquidity, and time to event
- **Success probability estimates**: Based on historical spread closure rates (73% base rate for 10%+ spreads)
- **ROI calculation**: Estimated return on investment for each opportunity
- **Alert-only mode**: No automatic trading — human decision required
- **Spam prevention**: Tracks already-alerted opportunities to avoid duplicate notifications
- **15-minute scan interval**: Configurable via environment variables

## Telegram Alert Format

```
🚨 ARBITRAGE OPPORTUNITY DETECTED 🚨
━━━━━━━━━━━━━━━━━━━━━━
📌 Event: Will Bitcoin reach $100,000 in March?
📅 Closes: Mar 31, 2026

💰 Price Discrepancy:
  • Polymarket: 45.0¢ (SELL YES / BUY NO)
  • Kalshi: 30.0¢ (BUY YES)

📊 Analysis:
  • Spread: 15.0%
  • ROI Potential: 50.0%
  • Success Probability: 78%
  • Confidence: ⭐⭐⭐⭐⭐⭐⭐☆☆☆ (7/10)

🔗 Links:
  • [Polymarket](https://...)
  • [Kalshi](https://...)
━━━━━━━━━━━━━━━━━━━━━━
⚠️ Alert-only mode. No automatic trading.
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | — | Telegram bot token |
| `TELEGRAM_CHAT_ID` | — | Telegram chat ID for alerts |
| `SPREAD_THRESHOLD` | `0.10` | Minimum spread to alert (10%) |
| `SIMILARITY_THRESHOLD` | `85` | Fuzzy match threshold (0-100) |
| `SCAN_INTERVAL_SECONDS` | `900` | Seconds between scans (15 min) |
| `MARKETS_PER_PLATFORM` | `200` | Markets to fetch per platform |

## Deployment (Railway)

1. Push to GitHub
2. Create a new Railway project from the GitHub repo
3. Set environment variables in Railway dashboard
4. The `Procfile` configures Railway to run `python bot.py` as a worker

## How It Works

1. Every 15 minutes, the bot fetches the top markets from each platform
2. It compares every pair of markets across different platforms using fuzzy string matching
3. If two markets match (≥85% similarity) and have a price spread ≥10%, it's flagged as an opportunity
4. A confidence score (1-10) is calculated based on:
   - **Spread size**: Larger spread = higher score
   - **Liquidity**: More liquidity = higher score (easier to execute)
   - **Time to event**: More time remaining = higher score (more time for spread to close)
5. Success probability is estimated based on historical data (73% base rate for 10%+ spreads)
6. A Telegram alert is sent with all details

## Strategy Notes

- **Best opportunities**: Spreads between 10-30% with confidence ≥ 6
- **Execution**: Buy YES on the cheaper platform, sell YES (or buy NO) on the more expensive platform
- **Risk**: Platforms may have different resolution criteria — always verify before trading
- **Liquidity**: Ensure sufficient liquidity on both sides before entering a position
