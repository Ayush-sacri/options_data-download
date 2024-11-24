import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
import os
from maticalgos.historical import historical
import logging
from pathlib import Path
import concurrent.futures

class MarketDataDownloader:
    def __init__(self, email, password):
        self.setup_logging()
        self.ma = self.initialize_connection(email, password)
        self.base_path = Path.cwd() / 'market_data'
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('market_downloader.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def initialize_connection(self, email, password):
        """Initialize connection to maticalgos"""
        try:
            ma = historical(email)
            ma.login(password)
            if ma.check_login:
                self.logger.info("Login Successful")
                return ma
            else:
                raise ConnectionError("Login failed")
        except Exception as e:
            self.logger.error(f"Error in MA login: {str(e)}")
            raise

    def create_directory_structure(self, date, instrument):
        """Create directory structure for the given date and instrument"""
        directories = {
            'fut': self.base_path / f'{instrument.lower()}_fut' / str(date.year) / str(date.month),
            'spot': self.base_path / f'{instrument.lower()}_spot' / str(date.year) / str(date.month),
            'options': self.base_path / f'{instrument.lower()}_options' / str(date.year) / str(date.month)
        }
        
        for path in directories.values():
            path.mkdir(parents=True, exist_ok=True)
            
        return directories

    def process_data(self, data, instrument):
        """Process the downloaded data into different components"""
        base_columns = ['date', 'time', 'symbol', 'open', 'high', 'low', 'close', 'oi', 'volume']
        data = data[base_columns]
        
        # Define instrument-specific symbols
        spot_symbol = instrument
        futures_symbol = f"{instrument}-I"
        
        return {
            'futures': data[data['symbol'] == futures_symbol],
            'spot': data[data['symbol'] == spot_symbol][['date', 'time', 'symbol', 'open', 'high', 'low', 'close']],
            'options': data[(data['symbol'] != spot_symbol) & (data['symbol'] != futures_symbol)]
        }

    def save_data(self, data_dict, directories, date, instrument):
        """Save processed data to respective directories"""
        date_str = date.strftime('%d_%m_%Y')
        
        # Save data only if it's not empty
        if not data_dict['futures'].empty:
            data_dict['futures'].to_csv(directories['fut'] / f"{instrument.lower()}_fut_{date_str}.csv", index=False)
        if not data_dict['spot'].empty:
            data_dict['spot'].to_csv(directories['spot'] / f"{instrument.lower()}_spot_{date_str}.csv", index=False)
        if not data_dict['options'].empty:
            data_dict['options'].to_csv(directories['options'] / f"{instrument.lower()}_options_{date_str}.csv", index=False)

    def download_single_date_instrument(self, date, instrument):
        """Download and process data for a single date and instrument"""
        try:
            if date.weekday() in [5, 6]:  # Skip weekends
                self.logger.info(f"Skipping weekend: {date.strftime('%d/%m/%Y')}")
                return None

            self.logger.info(f"Processing {instrument} for date: {date.strftime('%d/%m/%Y')}")
            
            directories = self.create_directory_structure(date, instrument)
            raw_data = self.ma.get_data(instrument.lower(), date)
            processed_data = self.process_data(raw_data, instrument)
            self.save_data(processed_data, directories, date, instrument)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error processing {instrument} for {date.strftime('%d/%m/%Y')}: {str(e)}")
            return (date, instrument, str(e))

    def download_data(self, start_date, end_date, instruments=['NIFTY', 'BANKNIFTY'], max_workers=4):
        """Download data for date range and instruments with parallel processing"""
        dates = []
        current_date = start_date
        while current_date < end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)

        tasks = [(date, instrument) for date in dates for instrument in instruments]
        errors = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.download_single_date_instrument, date, instrument) 
                      for date, instrument in tasks]
            
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    errors.append(result)

        if errors:
            self.logger.error("Errors occurred during download:")
            for date, instrument, error in errors:
                self.logger.error(f"{instrument} - {date.strftime('%d/%m/%Y')}: {error}")
            
        return errors

def main():
    # Configuration         
    EMAIL = "abc@gmail.com" ########## enter your registered mail ############
    PASSWORD = "856544"               ########## enter your registered password ############
    START_DATE = dt.date(2020, 1, 1)  ########## enter start date ############
    END_DATE = dt.date(2024, 11, 1)   ########## enter end  date, end date data can't be current month ############
    INSTRUMENTS = ['NIFTY', 'BANKNIFTY']
    
    # Initialize and run downloader
    try:
        downloader = MarketDataDownloader(EMAIL, PASSWORD)
        errors = downloader.download_data(START_DATE, END_DATE, INSTRUMENTS)
        
        if not errors:
            logging.info("Data download completed successfully")
        else:
            logging.warning(f"Download completed with {len(errors)} errors")
            
    except Exception as e:
        logging.error(f"Critical error: {str(e)}")
        raise

if __name__ == "__main__":
    main()