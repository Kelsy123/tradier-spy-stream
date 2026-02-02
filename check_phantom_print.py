import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# === USER SETTINGS ===
API_KEY = "YOUR_MASSIVE_API_KEY"  # <-- Put your API key here
SYMBOL = "SPY"
TARGET_PRICE = 69.00
TARGET_TIME = datetime(2026, 2, 2, 4, 5, 27, tzinfo=ZoneInfo("America/New_York"))  # <-- Change as needed
WINDOW_SECONDS = 30  # +/- seconds around target time

def main():
    start_dt = (TARGET_TIME - timedelta(seconds=WINDOW_SECONDS)).isoformat()
    end_dt = (TARGET_TIME + timedelta(seconds=WINDOW_SECONDS)).isoformat()
    url = (
        f"https://api.massive.io/v1/stocks/{SYMBOL}/trades"
        f"?start={start_dt}&end={end_dt}"
    )
    headers = {"Authorization": f"Bearer {API_KEY}"}
    print(f"Checking for {SYMBOL} trades at ${TARGET_PRICE} near {TARGET_TIME}...")
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        trades = response.json().get("results", [])
        matches = [
            t for t in trades
            if t.get("p") == TARGET_PRICE
        ]
        if matches:
            print(f"Found {len(matches)} trade(s) at ${TARGET_PRICE}:")
            for t in matches:
                print(t)
        else:
            print(f"No trades at ${TARGET_PRICE} found in the time window.")
    else:
        print("Error:", response.status_code, response.text)

if __name__ == "__main__":
    main()
