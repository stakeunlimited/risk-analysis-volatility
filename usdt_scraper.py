import requests
import time
from datetime import datetime
import logging
import psycopg2
from psycopg2.extras import DictCursor
from constants import API_KEY, DB_URL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class CryptoScraper:
    def __init__(self, api_key: str, db_url: str):
        self.api_key = api_key
        self.db_url = db_url
        self.headers = {
            'X-CMC_PRO_API_KEY': api_key,
            'Accept': 'application/json'
        }
        self.base_url = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest'
        
    def connect_to_db(self):
        """Establish connection to PostgreSQL database."""
        try:
            conn = psycopg2.connect(self.db_url)
            logging.info("Successfully connected to PostgreSQL")
            return conn
        except Exception as e:
            logging.error(f"Failed to connect to PostgreSQL: {e}")
            return None

    def fetch_usdt_price(self):
        """Fetch USDT price data from CoinMarketCap."""
        params = {'symbol': 'USDT'}
        
        try:
            response = requests.get(
                self.base_url,
                headers=self.headers,
                params=params
            )
            response.raise_for_status()
            
            data = response.json()
            usdt_data = data['data']['USDT'][0]
            
            return {
                'price': usdt_data['quote']['USD']['price'],
                'timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            logging.error(f"Error fetching price data: {e}")
            return None

    def run(self, interval=60):
        """Run the scraper at specified interval."""
        conn = self.connect_to_db()
        if not conn:
            return
        
        logging.info(f"Starting price scraper with {interval} second interval")
        
        try:
            while True:
                try:
                    price_data = self.fetch_usdt_price()
                    if price_data:
                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO "PriceData" ("id", "assetId", "priceUSD", "priceDate")
                                VALUES (
                                    gen_random_uuid(),
                                    (SELECT "id" FROM "Asset" WHERE "symbol" = 'USDT'),
                                    %(price)s,
                                    %(timestamp)s
                                )
                            """, price_data)
                            conn.commit()
                            logging.info(f"Stored price: {price_data['price']} at {price_data['timestamp']}")
                    
                    time.sleep(interval)
                    
                except KeyboardInterrupt:
                    logging.info("Scraper stopped by user")
                    break
                except Exception as e:
                    logging.error(f"Unexpected error: {e}")
                    time.sleep(interval)
                    
        finally:
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    scraper = CryptoScraper(API_KEY, DB_URL)
    scraper.run()
