import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import logging
from constants import COINGECKO_API_KEY, DB_URL
import time
import random
import schedule

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('volatility_tracker.log'),
        logging.StreamHandler()
    ]
)

# CoinGecko ID mappings
# Cannot use API because multiple matches for different tokens
# So, from Google sheet instead: https://docs.google.com/spreadsheets/d/1wTTuxXt8n9q7C4NDXqQpI3wpKu1_5bGVmP9Xz0XGSyU/edit?gid=0#gid=0
COIN_ID_MAP = {
    'DAI': 'dai',
    'USDC': 'usd-coin',
    'USDT': 'tether',
    'USDD': 'usdd',
    'FDUSD': 'first-digital-usd',
    'USDC.e': 'bridged-usdc-polygon-pos-bridge',
    'USDe': 'ethena-usde',
    'USDJ': 'just-stablecoin',
}

class VolatilityTracker:
    def __init__(self):
        self.conn = None
        self.connect_db()

    def connect_db(self):
        """Establish database connection"""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(DB_URL)

    def has_today_data(self, asset_id):
        """Check if we already have today's data for the given asset"""
        self.connect_db()
        today = datetime.now().date()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM \"VolatilityData\"
                    WHERE \"assetId\"::text = %s AND \"date\" = %s
                )
            """, (str(asset_id), today))
            return cur.fetchone()[0]
        
    def fetch_coingecko_data(self, symbol: str, days: str) -> pd.DataFrame:
        """Fetch OHLC data from CoinGecko API with rate limit handling"""
        symbol = symbol
        if symbol not in COIN_ID_MAP:
            logging.error(f"No mapping found for symbol {symbol}")
            return None
            
        coin_id = COIN_ID_MAP[symbol]
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc"
        
        params = {
            'vs_currency': 'usd',
            'days': days
        }
        
        headers = {
            "accept": "application/json",
            "x-cg-api-key": COINGECKO_API_KEY
        }
        
        max_retries = 3
        current_retry = 0
        
        while current_retry < max_retries:
            try:
                response = requests.get(url, params=params, headers=headers)

                if response.status_code == 200:
                    data = pd.DataFrame(response.json(), columns=['timestamp', 'open', 'high', 'low', 'close'])
                    data['date'] = pd.to_datetime(data['timestamp'], unit='ms')
                    data = data.drop('timestamp', axis=1)
                    return data[['date', 'open', 'high', 'low', 'close']]
                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    logging.warning(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    current_retry += 1
                    continue
                else:
                    logging.error(f"API request failed with status code: {response.status_code}")
                    return None
                    
            except Exception as e:
                logging.error(f"Error fetching data for {symbol}: {str(e)}")
                return None
        
        logging.error(f"Max retries reached for {symbol}")
        return None

    def calculate_volatility(self, df):
        """Calculate realized volatility using Rogers and Satchell formula"""
        df_copy = df.copy()
        term1 = np.log(df_copy['high'] / df_copy['close']) * np.log(df_copy['high'] / df_copy['open'])
        term2 = np.log(df_copy['low'] / df_copy['close']) * np.log(df_copy['low'] / df_copy['open'])
        T = 365
        daily_vol = np.sqrt(T * (term1 + term2))
        return daily_vol

    def process_asset(self, asset_id, symbol):
        """Process a single asset's volatility data"""
        try:
            # First check if we already have today's data
            if self.has_today_data(asset_id):
                logging.info(f"Already have today's data for {symbol}, skipping")
                return
                
            # Get the date range we need to fill
            last_update = self.get_last_update_date(asset_id)
            today = datetime.now().date()
            
            # Fetch both 30-day and 365-day data
            dfs = []
            
            # First get recent data (30 days)
            df_recent = self.fetch_coingecko_data(symbol, "30")
            if df_recent is not None:
                dfs.append(df_recent)
                time.sleep(1)
            
            # Then get historical data (365 days)
            df_historical = self.fetch_coingecko_data(symbol, "365")
            if df_historical is not None:
                dfs.append(df_historical)
                time.sleep(1)
            
            if not dfs:
                logging.error(f"No data fetched for {symbol}")
                return
                
            # Combine and deduplicate data
            df = pd.concat(dfs).drop_duplicates(subset=['date'])
            
            # Get missing dates from our database
            missing_dates = self.get_missing_dates(
                asset_id,
                max(last_update, today - timedelta(days=365)),
                today
            )
            
            if missing_dates:
                # Filter for only the missing dates
                df_missing = df[df['date'].dt.date.isin(missing_dates)].copy()
                if not df_missing.empty:
                    self.store_volatility_data(asset_id, symbol, df_missing)
                    logging.info(f"Added {len(df_missing)} new records for {symbol}")
            else:
                logging.info(f"No missing dates for {symbol}")
                
        except Exception as e:
            logging.error(f"Error processing {symbol}: {str(e)}")
            self.conn.rollback()

    def get_last_update_date(self, asset_id):
        """Get the most recent date for which we have data for an asset"""
        self.connect_db()
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(\"date\") FROM \"VolatilityData\" WHERE \"assetId\" = %s",
                (asset_id,)
            )
            result = cur.fetchone()[0]
            return result if result else datetime.now().date() - timedelta(days=365)

    def get_missing_dates(self, asset_id, start_date, end_date):
        """Get dates that are missing from the database for this asset"""
        self.connect_db()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT d::date
                FROM generate_series(%s::date, %s::date, '1 day'::interval) d
                WHERE d::date NOT IN (
                    SELECT \"date\"
                    FROM \"VolatilityData\"
                    WHERE \"assetId\" = %s
                    AND \"date\" BETWEEN %s AND %s
                )
            """, (start_date, end_date, asset_id, start_date, end_date))
            return [row[0] for row in cur.fetchall()]

    def calculate_daily_mse(self, row):
        """Calculate MSE for a single day using average of OHLC as the actual price"""
        # Calculate the actual price as average of OHLC
        actual_price = (row['open'] + row['high'] + row['low'] + row['close']) / 4
        predicted_price = 1
        return (actual_price - predicted_price) ** 2

    def update_missing_mse(self):
        """Update MSE for records where it's missing"""
        self.connect_db()
        try:
            # First, get all unique asset IDs and symbols where MSE is NULL
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT \"assetId\", symbol 
                    FROM \"VolatilityData\"
                    WHERE mse IS NULL
                """)
                assets_to_update = cur.fetchall()

            for asset_id, symbol in assets_to_update:
                # Get all records for this asset
                with self.conn.cursor() as cur:
                    cur.execute("""
                        SELECT \"date\", open, high, low, close 
                        FROM \"VolatilityData\"
                        WHERE \"assetId\" = %s
                        AND mse IS NULL
                        ORDER BY \"date\"
                    """, (asset_id,))
                    records = cur.fetchall()

                if records:
                    df = pd.DataFrame(records, columns=['date', 'open', 'high', 'low', 'close'])
                    
                    # Calculate MSE for each day
                    df['mse'] = df.apply(self.calculate_daily_mse, axis=1)

                    # Update records with calculated MSE values
                    data_to_update = [(float(row['mse']), asset_id, row['date']) for _, row in df.iterrows()]
                    
                    with self.conn.cursor() as cur:
                        execute_values(
                            cur,
                            """
                            UPDATE \"VolatilityData\" AS v SET
                                mse = d.mse
                            FROM (VALUES %s) AS d (mse, asset_id, date)
                            WHERE v.\"assetId\" = d.asset_id::uuid
                            AND v.\"date\" = d.date::date
                            """,
                            data_to_update,
                            template='(%(0)s, %(1)s, %(2)s)'
                        )

                    self.conn.commit()
                    logging.info(f"Updated MSE for {symbol} (asset_id: {asset_id})")

        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error updating MSE values: {str(e)}")
            raise
    
    def force_update_all_mse(self):
        """Force update MSE for all records regardless of current value"""
        self.connect_db()
        try:
            # First, get all unique asset IDs and symbols
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT \"assetId\", symbol 
                    FROM \"VolatilityData\"
                """)
                assets_to_update = cur.fetchall()

            for asset_id, symbol in assets_to_update:
                # Get all records for this asset
                with self.conn.cursor() as cur:
                    cur.execute("""
                        SELECT \"date\", open, high, low, close 
                        FROM \"VolatilityData\"
                        WHERE \"assetId\" = %s
                        ORDER BY \"date\"
                    """, (asset_id,))
                    records = cur.fetchall()

                if records:
                    # Convert to DataFrame
                    df = pd.DataFrame(records, columns=['date', 'open', 'high', 'low', 'close'])
                    
                    # Calculate MSE for each day
                    df['mse'] = df.apply(self.calculate_daily_mse, axis=1)

                    # Update records with calculated MSE values using individual updates
                    for _, row in df.iterrows():
                        with self.conn.cursor() as cur:
                            cur.execute("""
                                UPDATE \"VolatilityData\"
                                SET mse = %s
                                WHERE \"assetId\" = %s
                                AND \"date\" = %s
                            """, (float(row['mse']), asset_id, row['date']))

                    self.conn.commit()
                    logging.info(f"Force updated MSE for {symbol} (asset_id: {asset_id})")

        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error force updating MSE values: {str(e)}")
            raise

    def store_volatility_data(self, asset_id, symbol, df):
        """Store volatility data in the database"""
        if df is None or df.empty:
            return

        self.connect_db()
        df = df.copy()
        
        # Calculate volatility, kurtosis, and MSE
        df['volatility'] = self.calculate_volatility(df)
        kurtosis = df['volatility'].kurtosis()
        df['mse'] = df.apply(self.calculate_daily_mse, axis=1)
        
        # Prepare data for insertion
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                asset_id,
                symbol,
                row['date'].date(),
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                float(row['volatility']),
                float(kurtosis),
                float(row['mse']),
            ))

        try:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO \"VolatilityData\" 
                    (\"assetId\", \"symbol\", \"date\", \"open\", \"high\", \"low\", \"close\", \"volatility\", \"kurtosis\", \"mse\")
                    VALUES %s
                    ON CONFLICT ON CONSTRAINT \"VolatilityData_pkey\" DO NOTHING
                    """,
                    data_to_insert
                )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error storing data: {str(e)}")
            raise

    def fetch_assets(self):
        """Fetch all assets from the Asset table"""
        self.connect_db()
        with self.conn.cursor() as cur:
            cur.execute("SELECT \"id\", \"symbol\" FROM \"Asset\"")
            return cur.fetchall()

    def run(self):
        """Main execution method"""
        try:
            logging.info("Starting volatility tracking run")
            self.connect_db()
            
            self.update_missing_mse()

            assets = self.fetch_assets()
            random.shuffle(assets)
            
            for asset_id, symbol in assets:
                try:
                    if symbol in COIN_ID_MAP:
                        self.process_asset(asset_id, symbol)
                        time.sleep(2)  # Base rate limiting between assets
                    else:
                        logging.warning(f"No CoinGecko mapping for {symbol}, skipping")
                except Exception as e:
                    logging.error(f"Error processing {symbol}: {str(e)}")
                    continue
                    
            logging.info("Completed volatility tracking run")
            
        except Exception as e:
            logging.error(f"Fatal error in run(): {str(e)}")
        finally:
            if self.conn:
                self.conn.close()

def run_tracker():
    """Wrapper function for scheduler"""
    tracker = VolatilityTracker()
    tracker.run()

if __name__ == "__main__":
    # Run once immediately
    
    # If MSE values are broken, uncomment and run this:
    # tracker = VolatilityTracker()
    # tracker.force_update_all_mse()
    # assert False

    logging.info("Running initial volatility tracking...")
    run_tracker()
    
    # Schedule the job to run every hour
    schedule.every().hour.do(run_tracker)
    
    logging.info("Starting scheduler. Will run every hour.")
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute for pending jobs
        except Exception as e:
            logging.error(f"Scheduler error: {str(e)}")
            time.sleep(60)  # Wait a minute before trying again