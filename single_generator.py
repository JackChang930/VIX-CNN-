"""
Market Sentiment Model – Day 3
-----------------------------
Merge raw data (VIX, SPY, CNN Fear & Greed), engineer simple sentiment metrics,
and output a signal table for back‑testing.

Strategy (v0):
    • BUY  when CNN_FG <= 20  *and* VIX >= 30       (Extreme fear contrarian)
    • SELL when CNN_FG >= 80  *and* VIX <= 15       (Euphoria risk‑off)
    • HOLD otherwise

Generated CSV:
    data/processed/signals.csv  – columns: date, spy_price, vix, cnn_fg, signal

CLI Usage:
    $ python src.signal_generator.py

© 2025 Your Name – MIT License
"""
from __future__ import annotations

import logging
import sys
import time
from functools import wraps
from pathlib import Path
from typing import Final, Dict, Any

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT: Final[Path] = Path(r"C:\Jack\投資管理\市場情緒分析\VIX跟CNN恐慌指數")  # 改為實際的專案根目錄
RAW_DIR: Final[Path] = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR: Final[Path] = PROJECT_ROOT / "data" / "processed"

# 確保目錄存在
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

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
# Signal rules
# ---------------------------------------------------------------------------
class SignalGenerator:
    def __init__(self, 
                 buy_fg_threshold: int = 20,
                 buy_vix_threshold: float = 30,
                 sell_fg_threshold: int = 80,
                 sell_vix_threshold: float = 15):
        self.buy_fg_threshold = buy_fg_threshold
        self.buy_vix_threshold = buy_vix_threshold
        self.sell_fg_threshold = sell_fg_threshold
        self.sell_vix_threshold = sell_vix_threshold
    
    def generate_signal(self, row: pd.Series) -> str:
        """根據規則生成交易信號"""
        fg = row["cnn_fg"]
        vix = row["vix"]
        if pd.isna(fg) or pd.isna(vix):
            return "HOLD"
        if fg <= self.buy_fg_threshold and vix >= self.buy_vix_threshold:
            return "BUY"
        if fg >= self.sell_fg_threshold and vix <= self.sell_vix_threshold:
            return "SELL"
        return "HOLD"

# ---------------------------------------------------------------------------
# Data validation
# ---------------------------------------------------------------------------
def _validate_data(df: pd.DataFrame) -> None:
    """驗證數據的品質和完整性"""
    # 檢查是否有缺失值
    missing_data = df.isnull().sum()
    if missing_data.any():
        logger.warning("發現缺失值：\n%s", missing_data[missing_data > 0])
    
    # 檢查數據範圍
    if (df["vix"] < 0).any():
        logger.error("VIX 數據包含負值")
        raise ValueError("Invalid VIX data")
    
    if (df["cnn_fg"] < 0).any() or (df["cnn_fg"] > 100).any():
        logger.error("CNN Fear & Greed 指數超出有效範圍 (0-100)")
        raise ValueError("Invalid CNN Fear & Greed data")
    
    # 檢查日期連續性
    date_diff = df.index.to_series().diff()
    if date_diff.max().days > 5:  # 假設最大允許間隔為5天
        logger.warning("數據中存在較大的日期間隔")

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@retry_on_error(max_retries=3, delay=1)
def _load_series(path: Path, value_col: str | None = None) -> pd.Series:
    """載入 CSV 檔案並轉換為 Series"""
    try:
        # 確保檔案存在
        if not path.exists():
            raise FileNotFoundError(f"找不到檔案：{path}")
            
        # 嘗試讀取檔案
        try:
            df = pd.read_csv(path, index_col=0, parse_dates=True)
        except PermissionError:
            logger.warning(f"檔案 {path} 被鎖定，等待 1 秒後重試...")
            time.sleep(1)
            df = pd.read_csv(path, index_col=0, parse_dates=True)
            
        if value_col and value_col in df.columns:
            df.rename(columns={value_col: value_col}, inplace=True)
        if len(df.columns) != 1:
            raise ValueError(f"Expected exactly 1 data column in {path.name}")
        series = df.iloc[:, 0].copy()
        series.name = value_col or series.name
        logger.info("Loaded %s (%d rows)", path.name, len(series))
        return series
    except Exception as e:
        logger.error(f"讀取檔案 {path} 時發生錯誤：{str(e)}")
        raise

# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------
def _print_signal_stats(df: pd.DataFrame) -> None:
    """輸出信號統計資訊"""
    signal_counts = df["signal"].value_counts()
    logger.info("信號統計：\n%s", signal_counts)
    
    # 計算平均持倉時間
    hold_periods = []
    current_period = 0
    for signal in df["signal"]:
        if signal == "HOLD":
            current_period += 1
        else:
            if current_period > 0:
                hold_periods.append(current_period)
            current_period = 0
    
    if hold_periods:
        avg_hold = sum(hold_periods) / len(hold_periods)
        logger.info("平均持倉時間：%.2f 天", avg_hold)

# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------
def main() -> None:
    try:
        # 1. 定義數據源配置
        data_sources = {
            "spy": {
                "file": "spy.csv",
                "col": "spy_price",
                "required": True
            },
            "vix": {
                "file": "vix.csv",
                "col": "vix",
                "required": True
            },
            "cnn_fg": {
                "files": ["fear_greed_historical.csv", "cnn_fear_greed_current.csv", "cnn_fear_greed.csv"],
                "col": "cnn_fg",
                "required": True
            }
        }
        
        # 2. 載入所有數據
        series_dict = {}
        for key, config in data_sources.items():
            if "files" in config:  # 多個可能的檔案
                for file in config["files"]:
                    try:
                        series_dict[key] = _load_series(RAW_DIR / file, config["col"])
                        logger.info(f"成功載入 {file}")
                        break
                    except FileNotFoundError:
                        continue
                if key not in series_dict and config["required"]:
                    raise FileNotFoundError(f"找不到 {key} 的數據檔案")
            else:  # 單一檔案
                try:
                    series_dict[key] = _load_series(RAW_DIR / config["file"], config["col"])
                except FileNotFoundError as e:
                    if config["required"]:
                        raise
                    logger.warning(f"跳過非必要數據 {key}: {str(e)}")

        # 3. 合併數據
        df = pd.concat(series_dict.values(), axis=1, join="inner").sort_index()
        logger.info("合併後的數據形狀：%s", df.shape)

        # 4. 驗證數據
        _validate_data(df)

        # 5. 生成信號
        generator = SignalGenerator()
        df["signal"] = df.apply(generator.generate_signal, axis=1)

        # 6. 輸出統計資訊
        _print_signal_stats(df)

        # 7. 儲存結果
        out_path = PROCESSED_DIR / "signals.csv"
        df.to_csv(out_path, index_label="date")
        logger.info("信號已儲存：%s (%d 行)", out_path.name, len(df))

    except Exception as e:
        logger.error(f"程式執行錯誤：{str(e)}")
        raise

if __name__ == "__main__":
    main()
