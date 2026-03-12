import requests
import json

TOKEN = "8612471675:AAG22kCF2tTsADFW74BtrdjYaxINdFnz7lE"
CHAT_ID = "547766473"

msg = (
    "\U0001f916 *בוט ארביטראז' - בדיקת חיבור*\n\n"
    "הבוט מחובר בהצלחה!\n\n"
    "הבוט יסרוק כל 5 דקות את:\n"
    "\u2022 Polymarket\n"
    "\u2022 Kalshi\n"
    "\u2022 Manifold Markets\n\n"
    "וישלח התראה כשיימצא פער מחירים של 10%+ על אותו אירוע."
)

r = requests.post(
    f"https://api.telegram.org/bot{TOKEN}/sendMessage",
    json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}
)
data = r.json()
if data.get("ok"):
    print("SUCCESS - Message sent!")
else:
    print(f"ERROR: {data}")
