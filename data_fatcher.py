"""
Market Sentiment Model – Day 2
-----------------------------
Fetch raw data for VIX, CNN Fear & Greed Index, and SPY and store as CSVs
in `data/raw/` so that later modules (signal_generator, backtester, etc.) can
operate on clean, local files.

Run from project root:
    $ python -m src.data_fetcher  # or `python src/data_fetcher.py`

Dependencies (add to requirements.txt if not present):
    pandas
    yfinance
    requests

© 2025 Your Name – MIT License
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sys
import time
from functools import wraps
from pathlib import Path
from typing import Final, Dict, Any, Optional

import pandas as pd
import requests
import yfinance as yf

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent
RAW_DIR: Final[Path] = PROJECT_ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

START_DATE: Final[str] = "2000-01-01"  # earliest date for VIX/SPY historical

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------
def retry_on_error(max_retries: int = 3, delay: int = 1):
    """重試裝飾器，用於處理暫時性錯誤"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"第 {attempt + 1} 次嘗試失敗：{str(e)}，等待 {delay} 秒後重試...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

# ---------------------------------------------------------------------------
# Fetch functions
# ---------------------------------------------------------------------------
@retry_on_error(max_retries=3, delay=1)
def fetch_yfinance_series(ticker: str, column: str = "Adj Close", start: str = START_DATE) -> pd.Series:
    """從 Yahoo Finance 下載單一價格序列"""
    logger.info("Downloading %s from Yahoo Finance…", ticker)
    try:
        df = yf.download(ticker, start=start, progress=False, auto_adjust=False)
        if df.empty:
            raise ValueError(f"No data returned for {ticker}. Check ticker and internet connection.")
        series = df[column].copy()
        series.index = pd.to_datetime(series.index.date)  # strip intraday tz info
        series.name = ticker.replace("^", "")  # pretty series name (VIX instead of ^VIX)
        logger.info("%s: %d rows", ticker, len(series))
        return series
    except Exception as e:
        logger.error(f"下載 {ticker} 時發生錯誤：{str(e)}")
        raise

@retry_on_error(max_retries=3, delay=1)
def fetch_cnn_fear_greed() -> pd.Series:
    """從 CNN 的 JSON 端點獲取恐懼與貪婪指數"""
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.cnn.com/markets/fear-and-greed",
        "Origin": "https://www.cnn.com"
    }
    
    logger.info("Fetching CNN Fear & Greed Index…")
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        
        json_data = resp.json()
        logger.info("Received JSON keys: %s", list(json_data.keys()))
        
        # 首先嘗試獲取歷史數據
        if "fear_and_greed_historical" in json_data:
            historical_data = json_data["fear_and_greed_historical"]
            logger.info("Found historical data with keys: %s", list(historical_data.keys()))
            
            if "data" in historical_data:
                data = historical_data["data"]
                logger.info("Found historical data points: %d", len(data))
            else:
                logger.error("No data field in historical data. Available keys: %s", list(historical_data.keys()))
                raise ValueError("Could not find data in fear_and_greed_historical")
        else:
            # 如果沒有歷史數據，則使用當前數據
            logger.warning("No historical data found, using current data")
            if "fear_and_greed" in json_data:
                fear_greed = json_data["fear_and_greed"]
                logger.info("fear_and_greed keys: %s", list(fear_greed.keys()))
                
                if "score" in fear_greed:
                    data = [{"x": int(dt.datetime.now(dt.UTC).timestamp() * 1000), "y": fear_greed["score"]}]
                    logger.info("Using current score: %s", fear_greed["score"])
                else:
                    logger.error("No score found in current data. Available keys: %s", list(fear_greed.keys()))
                    raise ValueError("Could not find score in current data")
            else:
                logger.error("No fear_and_greed data found. Available keys: %s", list(json_data.keys()))
                raise ValueError("Could not find fear_and_greed data")

        if not data:
            raise ValueError("No data found in the response")

        # 使用 timezone-aware 的時間戳轉換
        records = {
            dt.datetime.fromtimestamp(item["x"] / 1000, dt.UTC).date(): item["y"]
            for item in data
        }
        series = pd.Series(records, name="CNN_FG")
        series.index = pd.to_datetime(series.index)
        logger.info("CNN_FG: %d rows", len(series))
        return series.sort_index()
        
    except requests.exceptions.RequestException as e:
        logger.error("Network error while fetching CNN data: %s", str(e))
        raise
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON response: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error while processing CNN data: %s", str(e))
        raise

@retry_on_error(max_retries=3, delay=1)
def fetch_alternative_fear_greed() -> pd.Series:
    """從 Alternative.me API 獲取歷史恐懼與貪婪指數"""
    url = "https://api.alternative.me/fng/"
    params = {
        "limit": 0,  # 0 表示獲取所有可用數據
        "format": "json",
        "date_format": "world"  # 使用世界日期格式 (DD-MM-YYYY)
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    logger.info("Fetching historical Fear & Greed Index from Alternative.me…")
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        if "data" not in data:
            raise ValueError("No data field in response")
            
        records = {}
        for item in data["data"]:
            try:
                # 解析 DD-MM-YYYY 格式的日期
                date_str = item["timestamp"]
                day, month, year = map(int, date_str.split("-"))
                date = dt.date(year, month, day)
                records[date] = int(item["value"])
            except (ValueError, KeyError) as e:
                logger.warning("Skipping invalid date entry: %s", str(e))
                continue
                
        if not records:
            raise ValueError("No valid data records found")
            
        series = pd.Series(records, name="CNN_FG")
        series.index = pd.to_datetime(series.index)
        logger.info("Alternative.me Fear & Greed: %d rows", len(series))
        return series.sort_index()
        
    except requests.exceptions.RequestException as e:
        logger.error("Network error while fetching Alternative.me data: %s", str(e))
        raise
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON response from Alternative.me: %s", str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error while processing Alternative.me data: %s", str(e))
        raise

# ---------------------------------------------------------------------------
# Saving helper
# ---------------------------------------------------------------------------
@retry_on_error(max_retries=3, delay=1)
def save_series(series: pd.Series, filename: str) -> Path:
    """將 Series 儲存為 CSV 檔案"""
    path = RAW_DIR / filename
    try:
        series.to_csv(path, header=True)
        logger.info("Saved %s (%d rows)", path.name, len(series))
        return path
    except PermissionError:
        logger.warning(f"檔案 {path} 被鎖定，等待 1 秒後重試...")
        time.sleep(1)
        series.to_csv(path, header=True)
        logger.info("Saved %s (%d rows)", path.name, len(series))
        return path
    except Exception as e:
        logger.error(f"儲存檔案 {path} 時發生錯誤：{str(e)}")
        raise

# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------
def main() -> None:
    try:
        logger.info("Saving data to: %s", RAW_DIR)

        # 1. 下載 VIX 和 SPY 數據
        vix = fetch_yfinance_series("^VIX")
        spy = fetch_yfinance_series("SPY")
        
        # 2. 儲存 VIX 和 SPY 數據
        vix_path = save_series(vix, "vix.csv")
        spy_path = save_series(spy, "spy.csv")
        
        # 3. 嘗試獲取恐懼與貪婪指數
        try:
            # 首先嘗試從 Alternative.me 獲取更長期的數據
            cnn = fetch_alternative_fear_greed()
            cnn_path = save_series(cnn, "fear_greed_historical.csv")
            logger.info("Successfully saved historical Fear & Greed data to fear_greed_historical.csv")
        except Exception as e:
            logger.error("Failed to fetch Alternative.me data: %s", str(e))
            logger.info("Trying CNN API as fallback...")
            try:
                cnn = fetch_cnn_fear_greed()
                cnn_path = save_series(cnn, "cnn_fear_greed_current.csv")
                logger.info("Successfully saved current CNN Fear & Greed data to cnn_fear_greed_current.csv")
            except Exception as e:
                logger.error("Failed to fetch CNN data: %s", str(e))
                logger.info("Continuing with VIX and SPY data only")

    except Exception as exc:
        logger.exception("Data fetch failed: %s", exc)
        raise SystemExit(1) from exc

if __name__ == "__main__":
    main()
