## Prerequisites

1. **CoinGecko API Key**: Obtain an API key from [CoinGecko](https://www.coingecko.com/en/api) and add it to `constants.py` as `COINGECKO_API_KEY`.
2. **PostgreSQL Credentials**: Add your PostgreSQL login details in `constants.py` for database connectivity as `DB_URL`.

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
