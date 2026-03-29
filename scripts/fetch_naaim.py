import requests, json, re
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path

url = "https://naaim.org/programs/naaim-exposure-index/"
headers = {"User-Agent": "Mozilla/5.0 (compatible; dashboard-bot/1.0)"}

try:
    res = requests.get(url, headers=headers, timeout=15)
    soup = BeautifulSoup(res.text, "html.parser")

    rows = []
    table = soup.find("table")
    if table:
        for tr in table.find_all("tr")[1:11]:  # Latest 10 rows
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) >= 2:
                rows.append({"date": cells[0], "value": cells[1]})

    latest = float(rows[0]["value"]) if rows else None
    avg4w  = round(sum(float(r["value"]) for r in rows[:4]) / min(4, len(rows)), 2) if rows else None

    output = {
        "updated": datetime.utcnow().strftime("%Y-%m-%d"),
        "latest": latest,
        "latest_date": rows[0]["date"] if rows else None,
        "avg_4week": avg4w,
        "history": rows[:9]
    }

    Path("data").mkdir(exist_ok=True)
    with open("data/naaim.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"✅ NAAIM saved: {latest} ({rows[0]['date'] if rows else 'N/A'})")

except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)
