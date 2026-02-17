# Market Analyser — 项目记忆

## 项目目标
股票市场分析工具，当前已实现 QQQ ETF 成分股分析。后续计划：市场动态追踪与总结。

## 技术栈
- **后端**：Python + Flask（`app.py`）
- **数据**：yfinance（已安装版本 1.2.0，非 0.2.x，API 有差异）
- **前端**：原生 HTML/CSS/JS，无框架

## 启动方式
```bash
conda activate kagg   # 含所有依赖的 conda 环境
cd C:\Users\li\Desktop\market_analyser
python app.py
# 浏览器访问 http://localhost:5000
```

## 文件结构
```
market_analyser/
├── app.py              # Flask 路由：GET /, GET /api/qqq, POST /api/refresh
├── data_fetcher.py     # 核心数据逻辑
├── requirements.txt
├── templates/index.html
└── static/
    ├── css/style.css   # 浅色主题，背景 #f6f7e8
    └── js/main.js
```

## data_fetcher.py 关键设计

### 持仓数据（4 层降级）
1. Invesco 官方 CSV（当前返回 406，自动跳过）
2. **Slickcharts 网页解析**（当前主力，101 支成分股，需 lxml 库）
3. yfinance `funds_data.top_holdings`（约 10 支）
4. 静态硬编码列表（30 支，保底）

### 价格数据（批量，避免限流）
- 两次 `yf.download()` 批量请求：intraday (1d/2m) + daily (5d/1d)
- **yfinance 1.2.0 列顺序是 `(ticker, field)` 而非旧版的 `(field, ticker)`**，`_extract_price()` 已适配
- 盘前/盘后：intraday 为空时自动回退到 daily 最后一根 bar

### 市值数据
- `get_market_caps()`：ThreadPoolExecutor 8 线程并行拉取 `fast_info.market_cap`
- 缓存 24 小时（holdings 同周期）

### 缓存 TTL
```python
PRICE_CACHE_TTL   = 60      # 秒，价格数据
HOLDINGS_CACHE_TTL = 86400  # 24 小时，持仓 + 市值
```

### 贡献度计算
```
贡献度 = 权重(0~1) × 涨跌幅(%)   单位：百分点
```

## API 返回结构
```json
{
  "qqq": { "price", "change_dollar", "change_pct", "total_contribution" },
  "holdings": [
    { "ticker", "name", "market_cap", "weight", "price",
      "change_dollar", "change_pct", "contribution", "valid" }
  ],
  "market_status": { "session", "label", "time_et", "refresh_interval" },
  "fetched_at": "ISO8601Z",
  "cache_age_seconds": 0
}
```

## 前端设计
- **三区布局**：顶部 Hero 横幅 → 热力图（前 30 大持仓）→ 完整表格（101 支）
- **实时 ET 时钟**：`startLiveClock()` 每秒独立 tick，与数据刷新解耦
- **自动刷新频率**：由 API 返回的 `refresh_interval` 控制（开盘 75s / 盘前后 120s / 休市 300s）
- **表格列**：代码 | 公司名称 | 市值 | 权重% | 当前价 | 涨跌额 | 涨跌% | 对QQQ贡献
- **颜色主题**：浅色，`--bg-base: #f6f7e8`，深绿/深红文字

## 已知注意事项
- yfinance 1.2.0 的 `download()` 列为 `(ticker, field)` 顺序，旧版是 `(field, ticker)`
- Invesco CSV 端点返回 406，Slickcharts 是目前实际使用的持仓来源
- 市场关闭时（如美国联邦假日）intraday 数据为空，显示最近收盘价
- 市值拉取首次约 10 秒（101 个并行请求），之后 24 小时内走缓存
- 依赖 `lxml` 和 `html5lib` 用于 Slickcharts 的 `pd.read_html()` 解析

## 待开发功能
- 市场动态追踪与总结（第二个功能模块）
