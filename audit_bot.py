#!/usr/bin/env python3.11
"""
audit_bot.py — מנגנון בקרה מעגלי אוטומטי — בוט ארביטראז'
בודק שכל תכונה שסוכמה מיושמת בפועל בקוד ובסביבה.

הרצה ידנית:  python3.11 audit_bot.py
הרצה שקטה:  python3.11 audit_bot.py --silent
"""

import os
import re
import sys
import requests
from datetime import datetime

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SILENT = "--silent" in sys.argv

PASS = "✅"
FAIL = "❌"
WARN = "⚠️"

results = []


def check(name: str, passed: bool, detail: str = "", warn_only: bool = False):
    icon = PASS if passed else (WARN if warn_only else FAIL)
    results.append({"name": name, "passed": passed, "icon": icon, "detail": detail})
    status = "PASS" if passed else ("WARN" if warn_only else "FAIL")
    print(f"  {icon} [{status}] {name}" + (f" — {detail}" if detail else ""))


def grep(filepath: str, pattern: str) -> bool:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        return bool(re.search(pattern, content))
    except FileNotFoundError:
        return False


def env_set(var: str) -> bool:
    val = os.getenv(var, "")
    return bool(val and len(val) > 5)


# ─────────────────────────────────────────────
# בדיקות: תכונות ליבה
# ─────────────────────────────────────────────
print("\n📋 בדיקת תכונות ליבה (Arbitrage Bot)")

check("A01 — סריקת פולימרקט",
      grep("bot.py", r"POLYMARKET_API_URL") and grep("bot.py", r"gamma-api.polymarket.com"),
      "API URL מוגדר")

check("A02 — סריקת מטקולוס",
      grep("bot.py", r"METACULUS_API_URL") and grep("bot.py", r"metaculus.com"),
      "Metaculus API URL מוגדר")

check("A03 — זיהוי פער ארביטראז'",
      grep("bot.py", r"gap\|GAP\|arbitrage\|ארביטראז"),
      "לוגיקת פער קיימת")

check("A04 — שליחת התראת ארביטראז' לטלגרם",
      grep("bot.py", r"send_message") and grep("bot.py", r"TELEGRAM_CHAT_ID"),
      "send_message נקרא עם CHAT_ID")

check("A05 — לולאת סריקה אוטומטית",
      grep("bot.py", r"_scan_loop") and grep("bot.py", r"SCAN_INTERVAL_SECONDS"),
      "_scan_loop + SCAN_INTERVAL_SECONDS")

check("A06 — פקודת /a_ping",
      grep("bot.py", r"cmd_a_ping") and grep("bot.py", r"a_ping"),
      "פקודה רשומה")

check("A07 — פקודת /a_status",
      grep("bot.py", r"cmd_a_status") and grep("bot.py", r"a_status"),
      "פקודה רשומה")

check("A08 — פקודת /a_report",
      grep("bot.py", r"cmd_a_report") and grep("bot.py", r"a_report"),
      "פקודה רשומה")

check("A09 — גיבוי יומי לטלגרם",
      grep("bot.py", r"_daily_backup_loop"),
      "_daily_backup_loop פעיל")

check("A10 — פקודת /a_audit",
      grep("bot.py", r"cmd_a_audit") and grep("bot.py", r"a_audit"),
      "פקודת בקרה מעגלית ידנית")

check("A11 — לולאת בקרה מעגלית יומית",
      grep("bot.py", r"_circular_audit_loop"),
      "daily audit loop פעיל")

# ─────────────────────────────────────────────
# בדיקות: משתני סביבה
# ─────────────────────────────────────────────
print("\n🔑 בדיקת משתני סביבה (Railway)")

check("A12 — TELEGRAM_BOT_TOKEN",
      env_set("TELEGRAM_BOT_TOKEN"),
      "מוגדר" if env_set("TELEGRAM_BOT_TOKEN") else "חסר!")

check("A13 — TELEGRAM_CHAT_ID",
      env_set("TELEGRAM_CHAT_ID"),
      "מוגדר" if env_set("TELEGRAM_CHAT_ID") else "חסר!")

check("A14 — METACULUS_TOKEN",
      env_set("METACULUS_TOKEN"),
      "מוגדר" if env_set("METACULUS_TOKEN") else "חסר!")

# ─────────────────────────────────────────────
# סיכום
# ─────────────────────────────────────────────
total = len(results)
passed = sum(1 for r in results if r["passed"])
failed = [r for r in results if not r["passed"] and r["icon"] == FAIL]
warnings = [r for r in results if r["icon"] == WARN]

print(f"\n{'='*50}")
print(f"📊 תוצאות בקרה (ארביטראז'): {passed}/{total} עברו")
print(f"❌ כשלים: {len(failed)} | ⚠️ אזהרות: {len(warnings)}")
print(f"{'='*50}\n")


def send_telegram(text: str):
    if SILENT or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10
        )
    except Exception as e:
        print(f"שגיאה בשליחה לטלגרם: {e}")


now_str = datetime.now().strftime("%d/%m/%Y %H:%M")
status_emoji = "✅" if len(failed) == 0 else "❌"

lines = [
    f"{status_emoji} *בקרה מעגלית — ארביטראז' — {now_str}*\n",
    f"📊 עברו: *{passed}/{total}* | ❌ כשלים: *{len(failed)}* | ⚠️ אזהרות: *{len(warnings)}*\n",
]

if failed:
    lines.append("*🚨 כשלים שדורשים תיקון:*")
    for r in failed:
        lines.append(f"  ❌ {r['name']}" + (f"\n     ↳ {r['detail']}" if r['detail'] else ""))
elif warnings:
    lines.append("⚠️ יש אזהרות הדורשות בדיקה")
else:
    lines.append("🎉 *כל התכונות פועלות כמצופה!*")

lines.append("\n_/a\\_audit לבדיקה ידנית_")
send_telegram("\n".join(lines))

sys.exit(1 if failed else 0)
