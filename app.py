"""
app.py
Flask web application for the QQQ ETF analyzer.
"""

import time
import logging

from flask import Flask, jsonify, render_template

import data_fetcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/qqq")
def api_qqq():
    try:
        data = data_fetcher.get_qqq_data()
        # Annotate how old the cached data is
        data = dict(data)
        data["cache_age_seconds"] = round(time.time() - data_fetcher._cache["timestamp"])
        return jsonify(data)
    except Exception as e:
        logger.error(f"API error: {e}", exc_info=True)
        return jsonify({
            "error": str(e),
            "qqq": {},
            "holdings": [],
            "market_status": data_fetcher.get_market_status(),
        }), 500


@app.route("/api/refresh", methods=["POST"])
def api_force_refresh():
    """Expire the price cache so the next /api/qqq call fetches fresh data."""
    data_fetcher._cache["timestamp"] = 0.0
    return jsonify({"status": "cache_cleared"})


if __name__ == "__main__":
    app.run(debug=True, port=5000, threaded=True)
