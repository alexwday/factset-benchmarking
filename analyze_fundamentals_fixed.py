"""
FactSet Fundamentals API Analysis - Fixed version with correct model types
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
import time

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# Correct FactSet SDK imports
import fds.sdk.FactSetFundamentals as ff
from fds.sdk.FactSetFundamentals.api.fact_set_fundamentals_api import FactSetFundamentalsApi
from fds.sdk.FactSetFundamentals.api.metrics_api import MetricsApi

# Import the correct model types
from fds.sdk.FactSetFundamentals.model.fundamentals_request import FundamentalsRequest
from fds.sdk.FactSetFundamentals.model.fundamental_request_body import FundamentalRequestBody
from fds.sdk.FactSetFundamentals.model.ids_batch_max30000 import IdsBatchMax30000
from fds.sdk.FactSetFundamentals.model.metrics import Metrics as MetricsModel
from fds.sdk.FactSetFundamentals.model.fiscal_period import FiscalPeriod
from fds.sdk.FactSetFundamentals.model.periodicity import Periodicity

from config.banks_config import monitored_institutions

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fundamentals_analysis_fixed.log')
    ]
)
logger = logging.getLogger(__name__)


class FundamentalsAnalyzer:
    """Fixed version using correct FactSet model types"""
    
    def __init__(self):
        self.configuration = None
        self.api_client = None
        self.metrics_api = None
        self.fundamentals_api = None
        
        # Results storage
        self.all_metrics = {}
        self.bank_results = {}
        
    def setup_api_client(self):
        """Setup FactSet API client"""
        try:
            # Get API credentials
            api_username = os.getenv('API_USERNAME')
            api_password = os.getenv('API_PASSWORD')
            
            if not api_username or not api_password:
                raise ValueError("API_USERNAME and API_PASSWORD must be set in .env file")
            
            # Configure FactSet API
            self.configuration = ff.Configuration(
                username=api_username,
                password=api_password
            )
            
            # Setup proxy if configured
            proxy_url = os.getenv('PROXY_URL')
            if proxy_url:
                proxy_user = os.getenv('PROXY_USER')
                proxy_password = os.getenv('PROXY_PASSWORD')
                if proxy_user and proxy_password:
                    self.configuration.proxy = f"http://{proxy_user}:{proxy_password}@{proxy_url}"
            
            # Create API client
            self.api_client = ff.ApiClient(self.configuration)
            
            # Initialize API instances
            self.metrics_api = MetricsApi(self.api_client)
            self.fundamentals_api = FactSetFundamentalsApi(self.api_client)
            
            logger.info("FactSet API client configured successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup API client: {e}")
            raise
    
    def test_single_ticker(self):
        """Test with a single known ticker to verify API connection"""
        logger.info("Testing API with known ticker (Apple)...")
        
        test_formats = ['AAPL', 'AAPL-US', 'AAPL-USA', 'FDS', 'FDS-US']
        
        for ticker in test_formats:
            try:
                # Create the IDs model object
                ids_obj = IdsBatchMax30000([ticker])
                
                # Create metrics list - try without model wrapper first
                metrics_list = ['FF_SALES']
                
                # Create request body
                request_body = FundamentalRequestBody(
                    ids=ids_obj,
                    metrics=metrics_list
                )
                
                # Create request
                request = FundamentalsRequest(data=request_body)
                
                # Make API call
                response = self.fundamentals_api.get_fds_fundamentals_for_list(request)
                
                if response and hasattr(response, 'data') and response.data:
                    logger.info(f"✅ {ticker} works! Got {len(response.data)} data points")
                    return ticker  # Return the working format
                else:
                    logger.info(f"❌ {ticker} - No data returned")
                    
            except Exception as e:
                logger.error(f"❌ {ticker} - Error: {e}")
        
        return None
    
    def fetch_all_metrics(self):
        """Fetch all available metrics"""
        logger.info("Fetching available metrics...")
        
        try:
            # Get metrics without category filter first
            response = self.metrics_api.get_fds_fundamentals_metrics()
            
            if response and response.data:
                for metric in response.data:
                    metric_id = getattr(metric, 'metric', None)
                    if metric_id:
                        self.all_metrics[metric_id] = {
                            'metric_id': metric_id,
                            'name': getattr(metric, 'name', ''),
                            'description': getattr(metric, 'description', ''),
                            'category': getattr(metric, 'category', ''),
                            'data_type': getattr(metric, 'data_type', '')
                        }
                
                logger.info(f"Fetched {len(self.all_metrics)} metrics")
            else:
                logger.warning("No metrics returned from API")
                
        except Exception as e:
            logger.error(f"Error fetching metrics: {e}")
    
    def analyze_banks(self):
        """Analyze banks with correct model types"""
        logger.info(f"Analyzing {len(monitored_institutions)} banks...")
        
        # Key metrics to test
        test_metrics = ['FF_SALES', 'FF_NET_INC', 'FF_ASSETS', 'FF_EQUITY']
        
        for ticker, bank_info in tqdm(monitored_institutions.items(), desc="Processing banks"):
            try:
                logger.info(f"Analyzing {ticker}: {bank_info['name']}")
                
                # Try different ticker formats
                ticker_formats = [
                    ticker,  # As-is from config
                    f"{ticker}-US" if not '-' in ticker else ticker,  # Add -US if not present
                    ticker.split('-')[0] if '-' in ticker else ticker,  # Remove suffix
                ]
                
                data_found = False
                for test_ticker in ticker_formats:
                    try:
                        # Create IDs model
                        ids_obj = IdsBatchMax30000([test_ticker])
                        
                        # Create fiscal period
                        fiscal_period = FiscalPeriod(
                            start='2022-01-01',
                            end='2023-12-31'
                        )
                        
                        # Create request
                        request_body = FundamentalRequestBody(
                            ids=ids_obj,
                            metrics=test_metrics,
                            fiscal_period=fiscal_period,
                            periodicity='QTR'
                        )
                        
                        request = FundamentalsRequest(data=request_body)
                        response = self.fundamentals_api.get_fds_fundamentals_for_list(request)
                        
                        if response and response.data and len(response.data) > 0:
                            logger.info(f"  ✅ Found data for {test_ticker}: {len(response.data)} points")
                            self.bank_results[ticker] = {
                                'working_ticker': test_ticker,
                                'data_points': len(response.data),
                                'has_data': True
                            }
                            data_found = True
                            break
                            
                    except Exception as e:
                        continue
                
                if not data_found:
                    logger.warning(f"  ❌ No data found for {ticker}")
                    self.bank_results[ticker] = {
                        'working_ticker': None,
                        'data_points': 0,
                        'has_data': False
                    }
                    
                # Rate limiting
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Error analyzing {ticker}: {e}")
    
    def save_results(self):
        """Save analysis results"""
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save results
        results = {
            'timestamp': timestamp,
            'banks_analyzed': len(self.bank_results),
            'banks_with_data': sum(1 for b in self.bank_results.values() if b['has_data']),
            'metrics_available': len(self.all_metrics),
            'bank_results': self.bank_results,
            'metrics': self.all_metrics
        }
        
        output_file = output_dir / f'analysis_results_{timestamp}.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Results saved to {output_file}")
        
        # Print summary
        print("\n" + "=" * 60)
        print("ANALYSIS SUMMARY")
        print("=" * 60)
        print(f"Banks analyzed: {results['banks_analyzed']}")
        print(f"Banks with data: {results['banks_with_data']}")
        print(f"Metrics available: {results['metrics_available']}")
        print("\nBanks with working tickers:")
        for ticker, result in self.bank_results.items():
            if result['has_data']:
                print(f"  {ticker}: {result['working_ticker']} ({result['data_points']} data points)")
    
    def run(self):
        """Main execution"""
        try:
            logger.info("Starting FactSet Fundamentals Analysis...")
            
            # Setup API
            self.setup_api_client()
            
            # Test connection
            working_format = self.test_single_ticker()
            if not working_format:
                logger.error("Could not connect to API with any ticker format")
                return
            
            logger.info(f"API connection successful with format: {working_format}")
            
            # Fetch metrics
            self.fetch_all_metrics()
            
            # Analyze banks
            self.analyze_banks()
            
            # Save results
            self.save_results()
            
            logger.info("Analysis completed!")
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            raise


def main():
    analyzer = FundamentalsAnalyzer()
    analyzer.run()


if __name__ == "__main__":
    main()