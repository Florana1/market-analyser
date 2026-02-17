# QQQ Market Analyser

A real-time web dashboard for QQQ ETF component stock analysis, showing each holding's price change and weighted contribution to QQQ's daily move.

## Features

- **Real-time prices** via yfinance (15s delayed)
- **Heatmap** — top 30 holdings by weight, colored by % change
- **Full holdings table** — all 101 QQQ components, sortable by any column
- **Contribution calc** — `weight × change_pct` shows each stock's impact on QQQ
- **Auto-refresh** — 75s during market hours, 120s pre/after, 300s when closed
- **Live ET clock** — ticks every second, independent of data refresh

## Tech Stack

- Backend: Python + Flask
- Data: yfinance 1.2.0, Slickcharts (holdings scrape)
- Frontend: vanilla HTML / CSS / JS (no framework)

## Setup

```bash
# Requires conda environment with dependencies
conda activate kagg

# Install dependencies (first time)
pip install -r requirements.txt

# Run
python app.py
# Open http://localhost:5000
```

Or double-click `启动.bat` to launch automatically.

## Project Structure

```
market_analyser/
├── app.py              # Flask routes: GET /, GET /api/qqq, POST /api/refresh
├── data_fetcher.py     # Holdings fetch (Slickcharts), price fetch (yfinance batch), contribution calc
├── requirements.txt
├── 启动.bat            # One-click Windows launcher
├── templates/
│   └── index.html
└── static/
    ├── css/style.css
    └── js/main.js
```

## Holdings Data Source

1. Invesco official CSV (returns 406, skipped)
2. **Slickcharts HTML scrape** (active, 101 holdings)
3. yfinance `funds_data.top_holdings` (~10 holdings, fallback)
4. Hardcoded top-30 list (last resort)

## Notes

- yfinance 1.2.0 `download()` returns `(ticker, field)` column order (reversed from older versions)
- Market cap is fetched in parallel (8 threads) and cached for 24 hours
- Price data cached for 60 seconds
