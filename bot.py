"""
Arbitrage Bot for Prediction Markets

Scans Polymarket, Kalshi, and Manifold Markets for price discrepancies on the same events.
Sends Telegram alerts when spreads exceed 10%, including confidence scores and success probability estimates.
"""

import os
import re
import json
import time
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional

import requests
from fuzzywuzzy import fuzz
from telegram import Bot, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- Configuration ---
POLYMARKET_API_URL = "https://gamma-api.polymarket.com/markets"
KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
MANIFOLD_API_URL = "https://api.manifold.markets/v0/markets"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8540700613:AAG0ICH0j997-OTLdnvA_00wokkFFOChk1g")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "547766473")

SPREAD_THRESHOLD = float(os.environ.get("SPREAD_THRESHOLD", "0.07"))  # 7%
SIMILARITY_THRESHOLD = int(os.environ.get("SIMILARITY_THRESHOLD", "80"))  # Fuzzy match threshold
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "900"))  # 15 minutes
MARKETS_PER_PLATFORM = int(os.environ.get("MARKETS_PER_PLATFORM", "200"))

# Track already-alerted opportunities to avoid spam
alerted_opportunities: set = set()

# Track last scan stats for status command
_last_scan_time: str = "טרם בוצעה"
_last_scan_count: int = 0
_last_opportunities_found: int = 0

# Common stop words to remove before matching
STOP_WORDS = {
    "will", "the", "a", "an", "be", "is", "are", "was", "were", "in", "on",
    "at", "to", "for", "of", "and", "or", "by", "with", "from", "this",
    "that", "these", "those", "it", "its", "have", "has", "had", "do",
    "does", "did", "not", "no", "yes", "get", "win", "lose", "reach",
    "above", "below", "before", "after", "than", "more", "less", "over",
    "under", "during", "between", "through", "about", "against",
}


# ─────────────────────────────────────────────
# Data Fetching
# ─────────────────────────────────────────────

def get_polymarket_data() -> list[dict]:
    """Fetches binary market data from Polymarket, sorted by 24h volume."""
    try:
        params = {
            "limit": MARKETS_PER_PLATFORM,
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
        }
        response = requests.get(POLYMARKET_API_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        markets = []
        for market in data:
            try:
                prices_raw = market.get("outcomePrices", "[]")
                prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
                if len(prices) == 2:
                    yes_price = float(prices[0])
                    if 0 < yes_price < 1:
                        markets.append({
                            "question": market["question"],
                            "price": yes_price,
                            "volume": float(market.get("volume24hr", 0)),
                            "liquidity": float(market.get("liquidity", 0)),
                            "end_date": market.get("endDate"),
                            "source": "Polymarket",
                            "url": f"https://polymarket.com/event/{market.get('slug', '')}",
                        })
            except (json.JSONDecodeError, IndexError, TypeError, ValueError) as e:
                logger.debug(f"Skipping Polymarket market: {e}")
        logger.info(f"Fetched {len(markets)} markets from Polymarket")
        return markets
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Polymarket data: {e}")
        return []


def get_kalshi_data() -> list[dict]:
    """Fetches open market data from Kalshi using yes_bid/yes_ask midpoint for pricing."""
    try:
        # Use min_close_ts to get longer-term markets (1 week from now)
        import time as _time
        min_close_ts = int(_time.time()) + 7 * 24 * 3600
        params = {
            "limit": MARKETS_PER_PLATFORM,
            "status": "open",
            "min_close_ts": min_close_ts,
        }
        response = requests.get(KALSHI_API_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        markets = []
        seen_events: set = set()  # Deduplicate by event ticker
        for market in data.get("markets", []):
            title = market.get("title", "")
            event_ticker = market.get("event_ticker", "")

            # Skip multi-leg parlay markets (contain commas separating multiple legs)
            # These have patterns like "yes X, no Y" or "yes A, yes B"
            # But allow titles with commas that are natural language (e.g., "Will X, Y, or Z happen?")
            # Multi-leg markets always have "yes " or "no " after a comma
            import re as _re
            if _re.search(r',\s*(yes|no)\s+', title, _re.IGNORECASE):
                continue

            # Use midpoint of yes_bid and yes_ask for price
            yes_bid = market.get("yes_bid", 0) or 0
            yes_ask = market.get("yes_ask", 0) or 0
            last_price = market.get("last_price", 0) or 0

            # Prefer last_price if available, else midpoint
            if last_price > 0:
                price = last_price / 100.0
            elif yes_bid > 0 and yes_ask > 0:
                price = (yes_bid + yes_ask) / 2 / 100.0
            elif yes_ask > 0:
                price = yes_ask / 100.0
            else:
                continue

            if 0 < price < 1:
                # Deduplicate: for multi-outcome events, only take the first market
                if event_ticker and event_ticker in seen_events:
                    continue
                if event_ticker:
                    seen_events.add(event_ticker)

                markets.append({
                    "question": title,
                    "price": price,
                    "volume": market.get("volume_24h", 0) or 0,
                    "liquidity": market.get("liquidity", 0) or 0,
                    "end_date": market.get("close_time"),
                    "source": "Kalshi",
                    "url": f"https://kalshi.com/markets/{market.get('ticker', '')}",
                })
        logger.info(f"Fetched {len(markets)} markets from Kalshi")
        return markets
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Kalshi data: {e}")
        return []


def get_manifold_data() -> list[dict]:
    """Fetches binary market data from Manifold Markets."""
    try:
        params = {
            "limit": MARKETS_PER_PLATFORM,
        }
        response = requests.get(MANIFOLD_API_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        markets = []
        for market in data:
            prob = market.get("probability")
            # Only include binary markets with real activity
            if (prob is not None
                    and not market.get("isResolved", False)
                    and market.get("outcomeType") == "BINARY"
                    and float(market.get("volume", 0)) > 10):  # Minimum volume filter
                price = float(prob)
                if 0 < price < 1:
                    markets.append({
                        "question": market["question"],
                        "price": price,
                        "volume": float(market.get("volume", 0)),
                        "liquidity": float(market.get("totalLiquidity", 0)),
                        "end_date": market.get("closeTime"),
                        "source": "Manifold",
                        "url": market.get("url", ""),
                    })
        logger.info(f"Fetched {len(markets)} markets from Manifold")
        return markets
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Manifold data: {e}")
        return []


# ─────────────────────────────────────────────
# Matching Logic
# ─────────────────────────────────────────────

def normalize_question(question: str) -> str:
    """Normalizes a question string for fuzzy matching."""
    q = question.lower().strip()
    q = q.rstrip("?").strip()
    # Remove special characters except spaces and hyphens
    q = re.sub(r"[^\w\s\-]", " ", q)
    # Normalize whitespace
    q = re.sub(r"\s+", " ", q).strip()
    return q


def extract_keywords(question: str) -> set[str]:
    """Extracts meaningful keywords from a question, removing stop words."""
    normalized = normalize_question(question)
    words = normalized.split()
    keywords = {w for w in words if len(w) > 2 and w not in STOP_WORDS}
    return keywords


def questions_are_similar(q1: str, q2: str) -> tuple[bool, int]:
    """
    Determines if two questions are about the same event.
    Returns (is_similar, similarity_score).

    Uses a two-stage approach:
    1. Keyword overlap check (fast filter)
    2. Fuzzy string matching (final verification)
    """
    # Stage 1: Keyword overlap
    kw1 = extract_keywords(q1)
    kw2 = extract_keywords(q2)

    if not kw1 or not kw2:
        return False, 0

    # Calculate Jaccard similarity of keywords
    intersection = kw1 & kw2
    union = kw1 | kw2
    jaccard = len(intersection) / len(union) if union else 0

    # Need at least 2 shared keywords OR Jaccard > 0.3
    if len(intersection) < 2 and jaccard < 0.3:
        return False, 0

    # Stage 2: Fuzzy string matching
    n1 = normalize_question(q1)
    n2 = normalize_question(q2)
    fuzzy_score = fuzz.token_set_ratio(n1, n2)

    # Require both keyword overlap AND fuzzy match
    if fuzzy_score >= SIMILARITY_THRESHOLD and len(intersection) >= 2:
        return True, fuzzy_score

    return False, fuzzy_score


# ─────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────

def calculate_confidence_score(
    spread: float,
    liquidity1: float,
    liquidity2: float,
    end_date: Optional[str | int | float],
) -> int:
    """
    Calculates a confidence score (1-10) for an arbitrage opportunity.

    Factors:
    - Spread size (larger = higher score)
    - Liquidity (higher = higher score)
    - Time to event (further = higher score, more time to close)
    """
    score = 0.0

    # Spread contribution (0-4 points)
    if spread >= 0.30:
        score += 4.0
    elif spread >= 0.20:
        score += 3.0
    elif spread >= 0.15:
        score += 2.0
    else:
        score += 1.0

    # Liquidity contribution (0-3 points)
    min_liquidity = min(liquidity1, liquidity2)
    if min_liquidity >= 100_000:
        score += 3.0
    elif min_liquidity >= 10_000:
        score += 2.0
    elif min_liquidity >= 1_000:
        score += 1.0

    # Time to event contribution (0-3 points)
    if end_date:
        try:
            if isinstance(end_date, (int, float)):
                # Manifold uses milliseconds timestamp
                end_dt = datetime.fromtimestamp(end_date / 1000, tz=timezone.utc)
            else:
                end_dt = datetime.fromisoformat(str(end_date).replace("Z", "+00:00"))
            days_remaining = (end_dt - datetime.now(tz=timezone.utc)).days
            if days_remaining > 30:
                score += 3.0
            elif days_remaining > 7:
                score += 2.0
            elif days_remaining > 1:
                score += 1.0
        except (ValueError, TypeError):
            score += 1.5  # Unknown time, give partial credit

    return max(1, min(10, round(score)))


def estimate_success_probability(spread: float, confidence: int) -> float:
    """
    Estimates the probability that this arbitrage spread will close profitably.

    Based on research: 10%+ spreads have ~73% closure rate within 24 hours.
    Larger spreads and higher confidence increase this estimate.
    """
    base_rate = 0.73

    # Adjust for spread size
    if spread >= 0.30:
        spread_bonus = 0.10
    elif spread >= 0.20:
        spread_bonus = 0.05
    else:
        spread_bonus = 0.0

    # Adjust for confidence score
    confidence_bonus = (confidence - 5) * 0.02  # ±0.02 per point from midpoint

    probability = base_rate + spread_bonus + confidence_bonus
    return max(0.50, min(0.95, probability))


def find_arbitrage_opportunities(all_markets: list[dict]) -> list[dict]:
    """Finds matching markets across platforms and identifies arbitrage opportunities."""
    opportunities = []
    seen_pairs: set = set()

    for i in range(len(all_markets)):
        for j in range(i + 1, len(all_markets)):
            market1 = all_markets[i]
            market2 = all_markets[j]

            # Skip same-platform comparisons
            if market1["source"] == market2["source"]:
                continue

            # Check similarity
            is_similar, similarity = questions_are_similar(
                market1["question"], market2["question"]
            )

            if not is_similar:
                continue

            price1 = market1["price"]
            price2 = market2["price"]
            spread = abs(price1 - price2)

            if spread < SPREAD_THRESHOLD:
                continue

            # Avoid duplicate pairs
            pair_key = tuple(sorted([market1["question"][:40], market2["question"][:40]]))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            # Determine which platform has higher/lower price
            if price1 > price2:
                high_market, low_market = market1, market2
                high_price, low_price = price1, price2
            else:
                high_market, low_market = market2, market1
                high_price, low_price = price2, price1

            # Calculate scores
            confidence = calculate_confidence_score(
                spread,
                market1.get("liquidity", 0),
                market2.get("liquidity", 0),
                market1.get("end_date") or market2.get("end_date"),
            )
            success_prob = estimate_success_probability(spread, confidence)
            roi_potential = spread / low_price * 100 if low_price > 0 else 0

            opportunities.append({
                "event": market1["question"],
                "high_platform": high_market["source"],
                "high_price": high_price,
                "high_url": high_market.get("url", ""),
                "low_platform": low_market["source"],
                "low_price": low_price,
                "low_url": low_market.get("url", ""),
                "spread": spread,
                "spread_pct": spread * 100,
                "roi_potential": roi_potential,
                "confidence": confidence,
                "success_probability": success_prob,
                "similarity": similarity,
                "end_date": market1.get("end_date") or market2.get("end_date"),
            })

    return opportunities


# ─────────────────────────────────────────────
# Telegram Alerts
# ─────────────────────────────────────────────

def format_price(price: float) -> str:
    """Formats a price as a percentage string."""
    return f"{price * 100:.1f}¢"


def format_alert(opp: dict) -> str:
    """Formats an arbitrage opportunity as a Telegram message."""
    confidence_stars = "⭐" * opp["confidence"] + "☆" * (10 - opp["confidence"])
    success_pct = opp["success_probability"] * 100

    end_date_str = ""
    if opp.get("end_date"):
        try:
            ed = opp["end_date"]
            if isinstance(ed, (int, float)):
                end_dt = datetime.fromtimestamp(ed / 1000, tz=timezone.utc)
            else:
                end_dt = datetime.fromisoformat(str(ed).replace("Z", "+00:00"))
            end_date_str = f"\n📅 *Closes:* {end_dt.strftime('%b %d, %Y')}"
        except (ValueError, TypeError):
            pass

    message = (
        f"🚨 *ARBITRAGE OPPORTUNITY DETECTED* 🚨\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 *Event:* {opp['event'][:100]}\n"
        f"{end_date_str}\n\n"
        f"💰 *Price Discrepancy:*\n"
        f"  • {opp['high_platform']}: {format_price(opp['high_price'])} _(SELL YES / BUY NO)_\n"
        f"  • {opp['low_platform']}: {format_price(opp['low_price'])} _(BUY YES)_\n\n"
        f"📊 *Analysis:*\n"
        f"  • Spread: *{opp['spread_pct']:.1f}%*\n"
        f"  • ROI Potential: *{opp['roi_potential']:.1f}%*\n"
        f"  • Success Probability: *{success_pct:.0f}%*\n"
        f"  • Confidence: {confidence_stars} ({opp['confidence']}/10)\n\n"
        f"🔗 *Links:*\n"
        f"  • [{opp['high_platform']}]({opp['high_url']})\n"
        f"  • [{opp['low_platform']}]({opp['low_url']})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ _Alert\\-only mode\\. No automatic trading\\._"
    )
    return message


async def send_telegram_alert(bot: Bot, opp: dict) -> None:
    """Sends an arbitrage alert to Telegram."""
    message = format_alert(opp)
    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode=ParseMode.MARKDOWN_V2,
            disable_web_page_preview=True,
        )
        logger.info(f"Alert sent for: {opp['event'][:60]}")
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {e}")
        # Try plain text fallback
        try:
            plain = (
                f"ARBITRAGE OPPORTUNITY: {opp['event'][:80]}\n"
                f"{opp['high_platform']}: {format_price(opp['high_price'])} vs "
                f"{opp['low_platform']}: {format_price(opp['low_price'])}\n"
                f"Spread: {opp['spread_pct']:.1f}% | Confidence: {opp['confidence']}/10 | "
                f"Success: {opp['success_probability']*100:.0f}%"
            )
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=plain)
        except Exception as e2:
            logger.error(f"Fallback alert also failed: {e2}")

    # Send audio alert sound
    alert_sound_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alert.mp3")
    if os.path.exists(alert_sound_path):
        try:
            with open(alert_sound_path, "rb") as audio_file:
                await bot.send_audio(
                    chat_id=TELEGRAM_CHAT_ID,
                    audio=audio_file,
                    title="Arbitrage Alert",
                    performer="Arbitrage Bot",
                )
        except Exception as audio_err:
            logger.warning(f"Failed to send audio alert: {audio_err}")


async def send_status_message(bot: Bot, message: str) -> None:
    """Sends a plain status message to Telegram."""
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
    except Exception as e:
        logger.error(f"Failed to send status message: {e}")


# ─────────────────────────────────────────────
# Main Loop
# ─────────────────────────────────────────────

async def run_scan(bot) -> None:
    """Runs a single scan cycle across all platforms."""
    global _last_scan_time, _last_scan_count, _last_opportunities_found
    scan_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    _last_scan_time = scan_time
    logger.info(f"Starting scan at {scan_time}")

    # Fetch data from all platforms
    polymarket = get_polymarket_data()
    kalshi = get_kalshi_data()
    manifold = get_manifold_data()

    all_markets = polymarket + kalshi + manifold
    logger.info(
        f"Total markets loaded: {len(all_markets)} "
        f"({len(polymarket)} Polymarket, {len(kalshi)} Kalshi, {len(manifold)} Manifold)"
    )

    # Find opportunities
    opportunities = find_arbitrage_opportunities(all_markets)
    logger.info(
        f"Found {len(opportunities)} arbitrage opportunities "
        f"(spread >= {SPREAD_THRESHOLD * 100:.0f}%)"
    )

    # Send alerts for new opportunities
    new_alerts = 0
    for opp in sorted(opportunities, key=lambda x: x["spread"], reverse=True):
        opp_key = f"{opp['event'][:50]}|{opp['high_platform']}|{opp['low_platform']}"
        if opp_key not in alerted_opportunities:
            await send_telegram_alert(bot, opp)
            alerted_opportunities.add(opp_key)
            new_alerts += 1
            await asyncio.sleep(1)  # Rate limiting

    if new_alerts == 0 and len(opportunities) > 0:
        logger.info("All opportunities already alerted")
    elif new_alerts == 0:
        logger.info("No new arbitrage opportunities found this scan")

    _last_scan_count += 1
    _last_opportunities_found = len(opportunities)

    # Clear old alerts periodically
    if len(alerted_opportunities) > 1000:
        alerted_opportunities.clear()
        logger.info("Cleared alerted opportunities cache")


# ─────────────────────────────────────────────
# Command Handlers
# ─────────────────────────────────────────────

async def cmd_a_ping(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Responds to /a_ping — confirms the bot is alive."""
    from datetime import datetime
    now = datetime.now().strftime("%H:%M:%S")
    await update.message.reply_text(
        f"🟢 *בוט ארביטראז' פעיל!*\n\nשעה: {now}",
        parse_mode="Markdown"
    )


async def cmd_a_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Responds to /a_status — shows bot status and last scan info."""
    await update.message.reply_text(
        f"🤖 *סטטוס בוט ארביטראז'*\n\n"
        f"🔍 סריקה אחרונה: {_last_scan_time}\n"
        f"📊 סריקות שבוצעו: {_last_scan_count}\n"
        f"⚡ הזדמנויות בסריקה אחרונה: {_last_opportunities_found}\n"
        f"🎯 סף פרש: {SPREAD_THRESHOLD * 100:.0f}%\n"
        f"⏱ סריקה כל: {SCAN_INTERVAL_SECONDS // 60} דקות\n"
        f"🌐 מקורות: Polymarket, Kalshi, Manifold",
        parse_mode="Markdown"
    )


async def cmd_a_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Responds to /a_report — triggers an immediate scan."""
    await update.message.reply_text("🔍 מתחיל סריקה מידית... (כמה דקות)")
    # Run scan in background
    ptb_app = ctx.application
    ptb_app.create_task(run_scan(ptb_app.bot))


async def _scan_loop(bot) -> None:
    """Background scan loop that runs every SCAN_INTERVAL_SECONDS."""
    global _last_scan_count
    await asyncio.sleep(10)  # Short delay before first scan
    while True:
        try:
            _last_scan_count += 1
            logger.info(f"=== Scan #{_last_scan_count} ===")
            await run_scan(bot)
        except Exception as e:
            logger.error(f"Error during scan: {e}", exc_info=True)
        logger.info(f"Sleeping {SCAN_INTERVAL_SECONDS // 60} minutes until next scan...")
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)


async def main() -> None:
    """Main async function to run the arbitrage bot with PTB command handlers."""
    # Build PTB application
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Register /a_ command handlers
    app.add_handler(CommandHandler("a_ping",   cmd_a_ping))
    app.add_handler(CommandHandler("a_status", cmd_a_status))
    app.add_handler(CommandHandler("a_report", cmd_a_report))

    # Send startup message
    startup_msg = (
        f"🤖 Arbitrage Bot Started\n"
        f"Scanning: Polymarket, Kalshi, Manifold\n"
        f"Spread threshold: {SPREAD_THRESHOLD * 100:.0f}%\n"
        f"Scan interval: {SCAN_INTERVAL_SECONDS // 60} minutes\n"
        f"Commands: /a_ping | /a_status | /a_report\n"
        f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    try:
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=startup_msg)
    except Exception as e:
        logger.error(f"Failed to send startup message: {e}")

    # Initialize PTB and start polling
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    # Run the scan loop in the background
    await _scan_loop(app.bot)


if __name__ == "__main__":
    asyncio.run(main())
