# 市場情緒交易策略回測系統

這是一個基於市場情緒指標（VIX 和 CNN 恐慌指數）的交易策略回測系統。該系統通過分析市場情緒指標來產生交易信號，並對 SPY（標普500 ETF）進行回測。

## 專案結構

```
market-sentiment-strategy/
├── data/
│   ├── raw/          # 原始數據
│   └── processed/    # 處理後的數據
├── src/
│   ├── data_fetcher.py    # 數據獲取模組
│   ├── signal_generator.py # 信號生成模組
│   └── backtester.py      # 回測模組
├── requirements.txt   # 專案依賴
└── README.md         # 專案說明
```

## 功能特點

- 自動獲取 VIX 和 SPY 歷史數據
- 整合 CNN 恐慌指數數據
- 基於市場情緒指標生成交易信號
- 向量化回測系統
- 完整的績效分析報告

## 回測結果

### 策略表現
- 年化報酬率 (CAGR)：1.19%
- 總報酬率：9.07%
- 年化波動率：10.18%
- 夏普比率：16.74%
- 最大回撤：-22.25%
- 交易次數：54次
- 勝率：53.75%

### 交易特徵
- 平均持倉時間：63.96天
- 信號分布：
  - HOLD：1772次
  - BUY：52次
  - SELL：27次

## 安裝說明

1. 克隆專案：
```bash
git clone https://github.com/your-username/market-sentiment-strategy.git
cd market-sentiment-strategy
```

2. 建立虛擬環境：
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. 安裝依賴：
```bash
pip install -r requirements.txt
```

## 使用說明

1. 獲取數據：
```bash
python src/data_fetcher.py
```

2. 生成交易信號：
```bash
python src/signal_generator.py
```

3. 執行回測：
```bash
python src/backtester.py
```

## 注意事項

- 本策略僅供研究使用，不構成投資建議
- 回測結果不代表未來表現
- 建議在實盤交易前進行更詳細的風險評估

## 未來改進方向

1. 加入更多市場情緒指標
2. 優化交易頻率
3. 加入交易成本考慮
4. 改進風險管理機制
5. 增加更多績效指標

## 授權說明

MIT License

## 作者

[您的名字]

## 貢獻指南

歡迎提交 Issue 和 Pull Request 來改進這個專案。 