import os
import json
import random
from datetime import datetime

def extract_market_data(logical_date: str) -> str:
    print(f"Starting high-volume extraction pipeline for date: {logical_date}")
    
    tickers = [
        "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOT", "DOGE", "AVAX", "LINK",
        "MATIC", "SHIB", "TON", "LTC", "ATOM", "UNI", "LINK", "ETC", "XLM", "BCH",
        "NEAR", "FIL", "LDO", "TIA", "SUI", "APT", "OP", "ARB", "INJ", "RNDR",
        "GRT", "STX", "EGLD", "IMX", "FET", "AGIX", "GALA", "FLOW", "MKR", "CRV",
        "AAVE", "ALGO", "QNT", "MINA", "THETA", "EGLD", "SAND", "MANA", "AXS", "CHZ"
    ]
    
    records = []
    for ticker in tickers:
        base_price = random.uniform(10, 50000) if ticker in ["BTC", "ETH"] else random.uniform(0.5, 150)
        
        for minute in range(1440):
            pct_change = random.uniform(-0.002, 0.002)
            open_p = base_price * (1 + pct_change)
            close_p = open_p * (1 + random.uniform(-0.001, 0.001))
            high_p = max(open_p, close_p) * (1 + random.uniform(0, 0.001))
            low_p = min(open_p, close_p) * (1 - random.uniform(0, 0.001))
            volume = random.uniform(1000, 500000)
            
            records.append({
                "timestamp": f"{logical_date} {str(minute//60).zfill(2)}:{str(minute%60).zfill(2)}:00",
                "asset_ticker": ticker,
                "price_open": round(open_p, 4),
                "price_high": round(high_p, 4),
                "price_low": round(low_p, 4),
                "price_close": round(close_p, 4),
                "volume_24h": round(volume, 2),
                "engine_metadata_load_id": random.randint(100000, 999999)
            })
            base_price = close_p
            
    target_dir = "/tmp/crypto_raw"
    os.makedirs(target_dir, exist_ok=True)
    
    file_path = f"{target_dir}/snapshot_{logical_date.replace('-', '')}.json"
    
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)
        
    print(f"Successfully generated and dumped {len(records)} golden market entries to {file_path}")
    return file_path