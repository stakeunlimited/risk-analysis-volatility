> Submodule of [d-analysis](https://github.com/stakeunlimited/d-analysis)

# Stablecoin Volatility Tracker

A comprehensive data collection and analysis system for monitoring stablecoin volatility in DeFi protocols. This project fetches real-time and historical price data from various sources, calculates volatility metrics, and stores them in a PostgreSQL database for dashboard visualization.

## Overview

The Stablecoin Volatility Tracker consists of two main components:

1. **Volatility Tracker**: Fetches OHLC (Open, High, Low, Close) data from CoinGecko API for multiple stablecoins, calculates volatility metrics using the Rogers and Satchell formula, and stores the results in PostgreSQL.

2. **Price Scraper**: A dedicated service that monitors USDT price movements at regular intervals using the CoinMarketCap API, providing real-time price tracking capabilities.

### Features

- Real-time and historical price data collection
- Volatility calculation using Rogers and Satchell formula
- MSE (Mean Squared Error) calculation for price deviation analysis
- Support for multiple stablecoins including DAI, USDC, USDT, USDD, and more
- Automated hourly data updates
- Rate limiting and error handling for API requests
- Comprehensive logging system

## Prerequisites

1. **CoinGecko API Key**: Obtain an API key from [CoinGecko](https://www.coingecko.com/en/api) and add it to `constants.py` as `COINGECKO_API_KEY`.
2. **CoinMarketCap API Key**: Required for USDT price scraping as `API_URL`.
3. **PostgreSQL Credentials**: Add your PostgreSQL login details in `constants.py` for database connectivity as `DB_URL`.

## Installation

1. **Clone the repository**:

    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2. **Install dependencies**:

    ```bash
    pip install -r requirements.txt
    ```

## Configuration

Ensure that your `constants.py` file includes:

```python
COINGECKO_API_KEY = 'your_api_key_here'
DB_URL = 'your_api_key_here'
```

## Running the Scripts

Start each script in a separate `tmux` session on your server to keep them running in the background:

```bash
tmux new -s volatility_tracker -d 'python volatility_tracker.py'
tmux new -s usdt_scraper -d 'python usdt_scraper.py'
```

You can reattach to these sessions anytime to monitor their status:
```bash
tmux attach -t volatility_tracker  # For volatility tracker
tmux attach -t usdt_scraper        # For USDT scraper
```

To detach from a `tmux` session and leave it running, press `Ctrl+B`, then `D`.
