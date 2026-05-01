from pathlib import Path
import json

def main():
    raw_data_dir = Path("../raw_data").resolve()
    files = raw_data_dir.glob("*.json")
    res: list[dict[str, str]] = []
    for file in files:
        data = json.loads(file.read_text())
        symbol = data["Meta Data"]["2. Symbol"]
        timeseries = data["Time Series (Daily)"]
        for time, bar in timeseries.items():
            flattened_bar = {
                "date": time,
                "symbol": symbol,
                "open": bar["1. open"],
                "high": bar["2. high"],
                "low": bar["3. low"],
                "close": bar["4. close"],
                "volume": bar["5. volume"],
            }
            res.append(flattened_bar) 
    with open(raw_data_dir / "raw_data.json", "w") as f:
        json.dump(res, f, indent=4)

if __name__ == "__main__":
    main()
