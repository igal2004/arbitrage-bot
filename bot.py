"""
Arbitrage Bot for Prediction Markets

Scans Polymarket vs Kalshi for price discrepancies on the same events.
Uses Manifold, PredictIt, and Metaculus as confidence indicators (domain-specific).

Domain logic:
- Manifold: confidence indicator for ALL markets
- PredictIt: confidence indicator for US POLITICAL markets only
- Metaculus: confidence indicator for SCIENCE/TECH/GEOPOLITICS/ECONOMICS markets only
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
PREDICTIT_API_URL = "https://www.predictit.org/api/marketdata/all/"
METACULUS_API_URL = "https://www.metaculus.com/api/posts/"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8540700613:AAG0ICH0j997-OTLdnvA_00wokkFFOChk1g")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "547766473")
METACULUS_TOKEN = os.environ.get("METACULUS_TOKEN", "47b57944d86da8ca4870eef7be2859aded32a7a4")

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
# Domain Detection
# ─────────────────────────────────────────────

# Keywords that indicate US political markets (PredictIt domain)
US_POLITICAL_KEYWORDS = {
    "trump", "biden", "harris", "democrat", "republican", "senate", "house",
    "congress", "president", "election", "vote", "ballot", "gop", "dnc",
    "white house", "oval office", "speaker", "majority", "minority",
    "midterm", "primary", "caucus", "filibuster", "impeach", "veto",
    "supreme court", "scotus", "doj", "fbi", "cia", "nsa", "dhs",
    "secretary", "cabinet", "governor", "mayor", "senator", "representative",
    "roe", "wade", "abortion", "gun", "second amendment", "border",
    "immigration", "tariff", "nafta", "usmca", "fed", "powell",
    "schumer", "mcconnell", "pelosi", "johnson", "desantis", "newsom",
    "rubio", "vance", "bannon", "maga", "america", "usa", "us ", " us ",
    "united states", "washington dc", "capitol",
}

# Keywords that indicate science/tech/geopolitics/economics markets (Metaculus domain)
METACULUS_DOMAIN_KEYWORDS = {
    # Science & Tech
    "ai", "artificial intelligence", "gpt", "openai", "anthropic", "google",
    "microsoft", "nvidia", "chip", "semiconductor", "quantum", "nuclear",
    "fusion", "climate", "temperature", "co2", "carbon", "vaccine", "virus",
    "pandemic", "covid", "cancer", "drug", "fda", "approval", "space",
    "nasa", "spacex", "mars", "moon", "satellite", "rocket", "launch",
    "bitcoin", "crypto", "ethereum", "blockchain", "defi",
    # Geopolitics
    "ukraine", "russia", "china", "taiwan", "nato", "un ", "united nations",
    "iran", "north korea", "israel", "gaza", "war", "ceasefire", "peace",
    "sanctions", "treaty", "invasion", "military", "troops", "missile",
    "nuclear weapon", "arms", "conflict",
    # Economics
    "gdp", "inflation", "recession", "interest rate", "unemployment",
    "stock market", "s&p", "nasdaq", "dow", "economy", "imf", "world bank",
    "trade", "export", "import", "deficit", "debt", "budget",
}


def detect_market_domain(question: str) -> dict:
    """
    Detects the domain of a market question.
    Returns dict with 'is_us_political' and 'is_metaculus_domain' flags.
    """
    q_lower = question.lower()

    is_us_political = any(kw in q_lower for kw in US_POLITICAL_KEYWORDS)
    is_metaculus_domain = any(kw in q_lower for kw in METACULUS_DOMAIN_KEYWORDS)

    return {
        "is_us_political": is_us_political,
        "is_metaculus_domain": is_metaculus_domain,
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
        seen_events: set = set()
        for market in data.get("markets", []):
            title = market.get("title", "")
            event_ticker = market.get("event_ticker", "")

            # Skip multi-leg parlay markets
            import re as _re
            if _re.search(r',\s*(yes|no)\s+', title, _re.IGNORECASE):
                continue

            # Kalshi API uses _dollars suffix (string values, already in dollar format)
            # Fallback to old integer fields (cents) for backward compatibility
            def _parse_price(val):
                """Parse price from string or int, returns float 0-1 or None."""
                if val is None:
                    return None
                try:
                    f = float(val)
                    # If value > 1, it's in cents (old API format)
                    if f > 1:
                        f = f / 100.0
                    return f if 0 < f < 1 else None
                except (ValueError, TypeError):
                    return None

            last_price = (
                _parse_price(market.get("last_price_dollars"))
                or _parse_price(market.get("last_price"))
            )
            yes_bid = (
                _parse_price(market.get("yes_bid_dollars"))
                or _parse_price(market.get("yes_bid"))
            )
            yes_ask = (
                _parse_price(market.get("yes_ask_dollars"))
                or _parse_price(market.get("yes_ask"))
            )

            if last_price:
                price = last_price
            elif yes_bid and yes_ask:
                price = (yes_bid + yes_ask) / 2
            elif yes_ask:
                price = yes_ask
            else:
                continue

            if 0 < price < 1:
                if event_ticker and event_ticker in seen_events:
                    continue
                if event_ticker:
                    seen_events.add(event_ticker)

                # Parse volume and liquidity (new API uses _fp and _dollars suffixes)
                try:
                    vol = float(market.get("volume_24h_fp") or market.get("volume_24h") or 0)
                except (TypeError, ValueError):
                    vol = 0
                try:
                    liq = float(market.get("liquidity_dollars") or market.get("liquidity") or 0)
                except (TypeError, ValueError):
                    liq = 0

                markets.append({
                    "question": title,
                    "price": price,
                    "volume": vol,
                    "liquidity": liq,
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
        params = {"limit": MARKETS_PER_PLATFORM}
        response = requests.get(MANIFOLD_API_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        markets = []
        for market in data:
            prob = market.get("probability")
            if (prob is not None
                    and not market.get("isResolved", False)
                    and market.get("outcomeType") == "BINARY"
                    and float(market.get("volume", 0)) > 10):
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


def get_predictit_data() -> list[dict]:
    """
    Fetches binary market data from PredictIt.
    Only returns single-contract (binary yes/no) markets.
    PredictIt focuses on US political markets.
    """
    try:
        response = requests.get(PREDICTIT_API_URL, timeout=15)
        response.raise_for_status()
        data = response.json()
        markets = []
        for market in data.get("markets", []):
            contracts = market.get("contracts", [])
            # Only include binary markets (single contract = yes/no)
            if len(contracts) == 1:
                c = contracts[0]
                # Use lastTradePrice or bestBuyYesCost as price
                price = c.get("lastTradePrice") or c.get("bestBuyYesCost")
                if price and 0 < float(price) < 1:
                    markets.append({
                        "question": market.get("name", ""),
                        "price": float(price),
                        "volume": 0,  # PredictIt doesn't expose volume easily
                        "liquidity": 0,
                        "end_date": market.get("timeStamp"),
                        "source": "PredictIt",
                        "url": f"https://www.predictit.org/markets/detail/{market.get('id', '')}",
                    })
        logger.info(f"Fetched {len(markets)} binary markets from PredictIt")
        return markets
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching PredictIt data: {e}")
        return []


def search_metaculus(question: str, max_results: int = 3) -> list[dict]:
    """
    Searches Metaculus for questions matching the given question text.
    Returns a list of matching questions with their titles.
    Note: With restricted API tier, we can only check if a question EXISTS,
    not get the community prediction probability.
    """
    if not METACULUS_TOKEN:
        return []
    try:
        # Extract key terms for search (first 5 meaningful words)
        words = [w for w in question.split() if len(w) > 3 and w.lower() not in STOP_WORDS]
        search_query = " ".join(words[:5])
        if not search_query:
            return []

        headers = {"Authorization": f"Token {METACULUS_TOKEN}"}
        params = {
            "limit": max_results,
            "search": search_query,
            "forecast_type": "binary",
        }
        response = requests.get(METACULUS_API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        results = []
        for post in data.get("results", []):
            results.append({
                "title": post.get("title", ""),
                "status": post.get("question", {}).get("status", ""),
                "url": f"https://www.metaculus.com/questions/{post.get('id', '')}/",
            })
        return results
    except Exception as e:
        logger.debug(f"Metaculus search failed: {e}")
        return []


# ─────────────────────────────────────────────
# Matching Logic
# ─────────────────────────────────────────────

def normalize_question(question: str) -> str:
    """Normalizes a question string for fuzzy matching."""
    q = question.lower().strip()
    q = q.rstrip("?").strip()
    q = re.sub(r"[^\w\s\-]", " ", q)
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
    """
    kw1 = extract_keywords(q1)
    kw2 = extract_keywords(q2)

    if not kw1 or not kw2:
        return False, 0

    intersection = kw1 & kw2
    union = kw1 | kw2
    jaccard = len(intersection) / len(union) if union else 0

    if len(intersection) < 2 and jaccard < 0.3:
        return False, 0

    n1 = normalize_question(q1)
    n2 = normalize_question(q2)
    fuzzy_score = fuzz.token_set_ratio(n1, n2)

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
    Calculates a base confidence score (1-10) for an arbitrage opportunity.

    Factors:
    - Spread size (larger = higher score)
    - Liquidity (higher = higher score)
    - Time to event (further = higher score)
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
            score += 1.5

    return max(1, min(10, round(score)))


def estimate_success_probability(spread: float, confidence: int) -> float:
    """
    Estimates the probability that this arbitrage spread will close profitably.
    """
    base_rate = 0.73

    if spread >= 0.30:
        spread_bonus = 0.10
    elif spread >= 0.20:
        spread_bonus = 0.05
    else:
        spread_bonus = 0.0

    confidence_bonus = (confidence - 5) * 0.02
    probability = base_rate + spread_bonus + confidence_bonus
    return max(0.50, min(0.95, probability))


def find_arbitrage_opportunities(
    all_markets: list[dict],
    predictit_markets: list[dict],
) -> list[dict]:
    """
    Finds matching markets across platforms and identifies arbitrage opportunities.
    Only alerts on Polymarket vs Kalshi (real money) pairs.

    Confidence indicators (domain-specific):
    - Manifold: ALL markets
    - PredictIt: US political markets only
    - Metaculus: science/tech/geopolitics/economics markets only
    """
    opportunities = []
    seen_pairs: set = set()

    polymarket_markets = [m for m in all_markets if m["source"] == "Polymarket"]
    kalshi_markets = [m for m in all_markets if m["source"] == "Kalshi"]
    manifold_markets = [m for m in all_markets if m["source"] == "Manifold"]

    for market1 in polymarket_markets:
        for market2 in kalshi_markets:
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

            pair_key = tuple(sorted([market1["question"][:40], market2["question"][:40]]))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            if price1 > price2:
                high_market, low_market = market1, market2
                high_price, low_price = price1, price2
            else:
                high_market, low_market = market2, market1
                high_price, low_price = price2, price1

            # Max price threshold for profitability
            min_profit_margin = 0.02
            max_buy_price = round(high_price - min_profit_margin, 3)

            # Detect market domain
            domain = detect_market_domain(market1["question"])

            # ── Manifold confidence (ALL markets) ──
            manifold_confirms = False
            manifold_price = None
            for mf in manifold_markets:
                is_sim, _ = questions_are_similar(market1["question"], mf["question"])
                if is_sim:
                    manifold_price = mf["price"]
                    if abs(manifold_price - low_price) < abs(manifold_price - high_price):
                        manifold_confirms = True
                    break

            # ── PredictIt confidence (US political markets only) ──
            predictit_confirms = False
            predictit_price = None
            if domain["is_us_political"]:
                for pi in predictit_markets:
                    is_sim, _ = questions_are_similar(market1["question"], pi["question"])
                    if is_sim:
                        predictit_price = pi["price"]
                        if abs(predictit_price - low_price) < abs(predictit_price - high_price):
                            predictit_confirms = True
                        break

            # ── Metaculus confidence (science/tech/geopolitics/economics only) ──
            metaculus_found = False
            metaculus_url = None
            if domain["is_metaculus_domain"]:
                metaculus_results = search_metaculus(market1["question"])
                if metaculus_results:
                    # Check if any result is similar enough
                    for mc_q in metaculus_results:
                        is_sim, _ = questions_are_similar(market1["question"], mc_q["title"])
                        if is_sim:
                            metaculus_found = True
                            metaculus_url = mc_q["url"]
                            break

            # Calculate base confidence score
            confidence = calculate_confidence_score(
                spread,
                market1.get("liquidity", 0),
                market2.get("liquidity", 0),
                market1.get("end_date") or market2.get("end_date"),
            )

            # Boost confidence based on domain-appropriate confirmations
            if manifold_confirms:
                confidence = min(10, confidence + 1)
            if predictit_confirms and domain["is_us_political"]:
                confidence = min(10, confidence + 1)
            if metaculus_found and domain["is_metaculus_domain"]:
                confidence = min(10, confidence + 1)

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
                "max_buy_price": max_buy_price,
                # Confidence indicators
                "manifold_confirms": manifold_confirms,
                "manifold_price": manifold_price,
                "predictit_confirms": predictit_confirms,
                "predictit_price": predictit_price,
                "metaculus_found": metaculus_found,
                "metaculus_url": metaculus_url,
                # Domain info
                "is_us_political": domain["is_us_political"],
                "is_metaculus_domain": domain["is_metaculus_domain"],
            })

    return opportunities


# ─────────────────────────────────────────────
# Telegram Alerts
# ─────────────────────────────────────────────

def format_price(price: float) -> str:
    """Formats a price as a percentage string."""
    return f"{price * 100:.1f}¢"


# Hebrew platform name mapping
PLATFORM_HE = {
    "Polymarket": "פולימרקט",
    "Kalshi": "קלשי",
    "Manifold": "מניפולד",
    "PredictIt": "פרדיקטאיט",
}


def escape_md(text: str) -> str:
    """Escapes special characters for Telegram MarkdownV2."""
    special_chars = r'_*[]()~`>#+-=|{}.!'
    for ch in special_chars:
        text = text.replace(ch, f'\\{ch}')
    return text


def format_alert(opp: dict) -> str:
    """Formats an arbitrage opportunity as a Telegram MarkdownV2 message."""
    confidence_stars = "⭐" * opp["confidence"] + "☆" * (10 - opp["confidence"])
    success_pct = opp["success_probability"] * 100

    high_he = PLATFORM_HE.get(opp["high_platform"], opp["high_platform"])
    low_he = PLATFORM_HE.get(opp["low_platform"], opp["low_platform"])

    end_date_str = ""
    if opp.get("end_date"):
        try:
            ed = opp["end_date"]
            if isinstance(ed, (int, float)):
                end_dt = datetime.fromtimestamp(ed / 1000, tz=timezone.utc)
            else:
                end_dt = datetime.fromisoformat(str(ed).replace("Z", "+00:00"))
            end_date_str = f"\n📅 *סגירה:* {end_dt.strftime('%d/%m/%Y')}"
        except (ValueError, TypeError):
            pass

    # Build confidence indicators section
    confidence_lines = []

    # Manifold (always shown if available)
    if opp.get("manifold_price") is not None:
        mf_emoji = "✅" if opp.get("manifold_confirms") else "⚠️"
        mf_status = "מאשר" if opp.get("manifold_confirms") else "לא מאשר"
        confidence_lines.append(
            f"  • מניפולד: {format_price(opp['manifold_price'])} {mf_emoji} _{mf_status}_"
        )

    # PredictIt (US political only)
    if opp.get("is_us_political") and opp.get("predictit_price") is not None:
        pi_emoji = "✅" if opp.get("predictit_confirms") else "⚠️"
        pi_status = "מאשר" if opp.get("predictit_confirms") else "לא מאשר"
        confidence_lines.append(
            f"  • פרדיקטאיט: {format_price(opp['predictit_price'])} {pi_emoji} _{pi_status}_"
        )
    elif opp.get("is_us_political") and opp.get("predictit_price") is None:
        confidence_lines.append("  • פרדיקטאיט: לא נמצא שוק מתאים")

    # Metaculus (science/tech/geopolitics/economics only)
    if opp.get("is_metaculus_domain"):
        if opp.get("metaculus_found"):
            mc_url = opp.get("metaculus_url", "")
            confidence_lines.append(
                f"  • מטקולוס: ✅ _שאלה קיימת_ \\([קישור]({mc_url})\\)"
            )
        else:
            confidence_lines.append("  • מטקולוס: לא נמצאה שאלה מתאימה")

    confidence_section = "\n".join(confidence_lines) if confidence_lines else "  • אין מדדי ביטחון נוספים"

    # Max buy price line
    max_price = opp.get("max_buy_price", 0)
    max_price_line = f"  • 🛑 *מחיר מקסימלי לקנייה:* {format_price(max_price)} \\(מעל זה — לא כדאי\\)"

    # Escape event name for MarkdownV2
    event_escaped = escape_md(opp['event'][:100])

    # Build the message - handle end_date_str to avoid triple newlines
    date_section = f"{end_date_str}\n" if end_date_str else ""

    message = (
        f"🚨 *הזדמנות ארביטראז'* 🚨\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 *אירוע:* {event_escaped}\n"
        f"{date_section}\n"
        f"💰 *פערי מחיר:*\n"
        f"  • {high_he}: {format_price(opp['high_price'])} _\\(מחיר גבוה — מכור כן / קנה לא\\)_\n"
        f"  • {low_he}: {format_price(opp['low_price'])} _\\(מחיר נמוך — קנה כן כאן\\)_\n"
        f"{max_price_line}\n\n"
        f"🔍 *מדדי ביטחון:*\n"
        f"{confidence_section}\n\n"
        f"📊 *ניתוח:*\n"
        f"  • פער: *{opp['spread_pct']:.1f}%*\n"
        f"  • פוטנציאל רווח: *{opp['roi_potential']:.1f}%*\n"
        f"  • הסתברות הצלחה: *{success_pct:.0f}%*\n"
        f"  • ביטחון: {confidence_stars} \\({opp['confidence']}/10\\)\n\n"
        f"🔗 *קישורים:*\n"
        f"  • [{high_he}]({opp['high_url']})\n"
        f"  • [{low_he}]({opp['low_url']})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ _התראה בלבד\\. בדוק מחיר עדכני לפני ביצוע\\._"
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
                f"ARBITRAGE: {opp['event'][:80]}\n"
                f"{opp['high_platform']}: {format_price(opp['high_price'])} vs "
                f"{opp['low_platform']}: {format_price(opp['low_price'])}\n"
                f"Spread: {opp['spread_pct']:.1f}% | Confidence: {opp['confidence']}/10 | "
                f"Success: {opp['success_probability']*100:.0f}%"
            )
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=plain)
        except Exception as e2:
            logger.error(f"Fallback alert also failed: {e2}")

    # Send audio alert
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
    predictit = get_predictit_data()

    all_markets = polymarket + kalshi + manifold
    logger.info(
        f"Total markets loaded: {len(all_markets)} "
        f"({len(polymarket)} Polymarket, {len(kalshi)} Kalshi, "
        f"{len(manifold)} Manifold) + {len(predictit)} PredictIt"
    )

    # Find opportunities
    opportunities = find_arbitrage_opportunities(all_markets, predictit)
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
            await asyncio.sleep(1)

    if new_alerts == 0 and len(opportunities) > 0:
        logger.info("All opportunities already alerted")
    elif new_alerts == 0:
        logger.info("No new arbitrage opportunities found this scan")

    _last_scan_count += 1
    _last_opportunities_found = len(opportunities)

    if len(alerted_opportunities) > 1000:
        alerted_opportunities.clear()
        logger.info("Cleared alerted opportunities cache")


# ─────────────────────────────────────────────
# Command Handlers
# ─────────────────────────────────────────────

async def cmd_a_ping(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Responds to /a_ping — confirms the bot is alive."""
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
        f"🌐 מקורות: Polymarket, Kalshi\n"
        f"🔍 מדדי ביטחון: Manifold, PredictIt \\(פוליטי\\), Metaculus \\(מדע/טק/גאו\\)",
        parse_mode="Markdown"
    )


async def cmd_a_report(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Responds to /a_report — triggers an immediate scan."""
    await update.message.reply_text("🔍 מתחיל סריקה מידית... (כמה דקות)")
    ptb_app = ctx.application
    ptb_app.create_task(run_scan(ptb_app.bot))


async def send_daily_backup(bot: Bot) -> None:
    """Sends a silent daily status backup to Telegram for recovery purposes."""
    try:
        backup_data = {
            "_arb_backup": True,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "scans_completed": _last_scan_count,
            "last_scan_time": _last_scan_time,
            "last_opportunities_found": _last_opportunities_found,
            "spread_threshold": SPREAD_THRESHOLD,
            "alerted_count": len(alerted_opportunities),
        }
        import json as _json
        backup_text = f"💾 *Arbitrage Bot Daily Backup*\n`{_json.dumps(backup_data, ensure_ascii=False)}`"
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=backup_text,
            parse_mode="Markdown",
            disable_notification=True,
        )
        logger.info("Daily backup sent to Telegram")
    except Exception as e:
        logger.warning(f"Failed to send daily backup: {e}")


async def _daily_backup_loop(bot: Bot) -> None:
    """Sends a daily backup every 24 hours."""
    await asyncio.sleep(3600)  # Wait 1 hour before first backup
    while True:
        await send_daily_backup(bot)
        await asyncio.sleep(86400)


async def _scan_loop(bot) -> None:
    """Background scan loop that runs every SCAN_INTERVAL_SECONDS."""
    global _last_scan_count
    await asyncio.sleep(10)
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
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("a_ping",   cmd_a_ping))
    app.add_handler(CommandHandler("a_status", cmd_a_status))
    app.add_handler(CommandHandler("a_report", cmd_a_report))

    startup_msg = (
        f"🤖 בוט ארביטראז' נדלק!\n"
        f"🌐 סריקה: Polymarket vs Kalshi בלבד\n"
        f"🔍 מדדי ביטחון:\n"
        f"  • מניפולד — כל השווקים\n"
        f"  • פרדיקטאיט — שווקים פוליטיים אמריקאים\n"
        f"  • מטקולוס — מדע/טק/גיאופוליטיקה/כלכלה\n"
        f"🎯 סף Spread: {SPREAD_THRESHOLD * 100:.0f}%\n"
        f"⏱ סריקה כל: {SCAN_INTERVAL_SECONDS // 60} דקות\n"
        f"💾 גיבוי יומי אוטומטי\n"
        f"💬 פקודות: /a_ping | /a_status | /a_report\n"
        f"⏰ התחלת פעילה: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    try:
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=startup_msg)
    except Exception as e:
        logger.error(f"Failed to send startup message: {e}")

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    asyncio.ensure_future(_daily_backup_loop(app.bot))
    await _scan_loop(app.bot)


if __name__ == "__main__":
    asyncio.run(main())
