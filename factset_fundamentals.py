#!/usr/bin/env python3
"""
FactSet Fundamentals API Client
A consolidated script to fetch and analyze financial fundamentals data
"""

import os
import json
import sys
import logging
import tempfile
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import time
from pathlib import Path
import urllib.parse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('factset_fundamentals.log')
    ]
)
logger = logging.getLogger(__name__)

# Try to load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    logger.warning("python-dotenv not installed. Using system environment variables.")

# Import FactSet SDK components
try:
    import fds.sdk.FactSetFundamentals as ff
    from fds.sdk.FactSetFundamentals.api.fact_set_fundamentals_api import FactSetFundamentalsApi
    from fds.sdk.FactSetFundamentals.api.metrics_api import MetricsApi
    from fds.sdk.FactSetFundamentals.model.fundamentals_request import FundamentalsRequest
    from fds.sdk.FactSetFundamentals.model.fundamental_request_body import FundamentalRequestBody
    from fds.sdk.FactSetFundamentals.model.ids_batch_max30000 import IdsBatchMax30000
    from fds.sdk.FactSetFundamentals.model.metrics import Metrics
    from fds.sdk.FactSetFundamentals.model.fiscal_period import FiscalPeriod
    from fds.sdk.FactSetFundamentals.model.periodicity import Periodicity
except ImportError as e:
    logger.error(f"Failed to import FactSet SDK: {e}")
    logger.error("Please ensure the FactSet SDK is installed: pip install fds.sdk.FactSetFundamentals")
    sys.exit(1)

# Import Pandas for data analysis
try:
    import pandas as pd
except ImportError:
    logger.warning("pandas not installed. Data analysis features will be limited.")
    pd = None


class FactSetFundamentalsClient:
    """Client for interacting with FactSet Fundamentals API"""
    
    # Key financial metrics for analysis
    DEFAULT_METRICS = [
        'FF_SALES',          # Revenue
        'FF_NET_INC',        # Net Income
        'FF_EPS_BASIC',      # Basic Earnings per Share
        'FF_ASSETS',         # Total Assets
        'FF_LIAB',           # Total Liabilities
        'FF_EQUITY',         # Total Equity
        'FF_OPER_INC',       # Operating Income
        'FF_EBIT',           # EBIT
        'FF_EBITDA',         # EBITDA
        'FF_CASH',           # Cash and Cash Equivalents
        'FF_DIV_PER_SHR',    # Dividends per Share
        'FF_BK_VAL_PER_SHR', # Book Value per Share
        'FF_OPER_CASH_FLOW', # Operating Cash Flow
        'FF_FREE_CASH_FLOW', # Free Cash Flow
    ]
    
    # Bank-specific metrics
    BANK_METRICS = [
        'FF_INT_INCOME',     # Interest Income
        'FF_INT_EXP',        # Interest Expense
        'FF_NET_INT_INC',    # Net Interest Income
        'FF_LOAN_LOSS_PROV', # Loan Loss Provision
        'FF_LOANS',          # Total Loans
        'FF_DEPOSITS',       # Total Deposits
        'FF_TIER1_CAP_RATIO',# Tier 1 Capital Ratio
        'FF_TOT_CAP_RATIO',  # Total Capital Ratio
    ]
    
    # Test tickers with known valid formats
    TEST_TICKERS = {
        'US_Banks': ['JPM-US', 'BAC-US', 'WFC-US', 'C-US', 'GS-US'],
        'Tech': ['AAPL-US', 'MSFT-US', 'GOOGL-US'],
        'Canadian_Banks': ['RY-CA', 'TD-CA', 'BMO-CA', 'BNS-CA'],
    }
    
    def __init__(self):
        """Initialize the FactSet client"""
        self.configuration = None
        self.api_client = None
        self.fundamentals_api = None
        self.metrics_api = None
        self.ssl_cert_path = None
        self._setup_authentication()
        
    def _setup_ssl_certificate(self):
        """Setup SSL certificate from file or environment"""
        try:
            # Check for SSL certificate path in environment
            ssl_cert_path = os.getenv('SSL_CERT_PATH')
            
            if ssl_cert_path and os.path.exists(ssl_cert_path):
                # Read certificate file
                with open(ssl_cert_path, 'rb') as cert_file:
                    cert_data = cert_file.read()
                    
                # Create temporary certificate file
                temp_cert = tempfile.NamedTemporaryFile(mode='wb', suffix='.cer', delete=False)
                temp_cert.write(cert_data)
                temp_cert.close()
                
                # Set environment variables for SSL
                os.environ['REQUESTS_CA_BUNDLE'] = temp_cert.name
                os.environ['SSL_CERT_FILE'] = temp_cert.name
                
                logger.info(f"SSL certificate configured from: {ssl_cert_path}")
                return temp_cert.name
            else:
                # If no custom certificate, return None (will use system defaults)
                logger.info("No custom SSL certificate configured, using system defaults")
                return None
                
        except Exception as e:
            logger.warning(f"Error setting up SSL certificate: {e}")
            return None
        
    def _setup_authentication(self):
        """Setup API authentication and create API instances"""
        try:
            # Setup SSL certificate first
            self.ssl_cert_path = self._setup_ssl_certificate()
            
            # Get credentials from environment
            username = os.getenv('API_USERNAME')
            password = os.getenv('API_PASSWORD')
            
            if not username or not password:
                logger.error("API credentials not found. Please set API_USERNAME and API_PASSWORD environment variables.")
                return False
            
            # Create configuration
            self.configuration = ff.Configuration(
                username=username,
                password=password
            )
            
            # Add SSL certificate if available
            if self.ssl_cert_path:
                self.configuration.ssl_ca_cert = self.ssl_cert_path
            
            # Setup proxy configuration
            self._configure_proxy()
            
            # Create API client and instances
            self.api_client = ff.ApiClient(self.configuration)
            self.fundamentals_api = FactSetFundamentalsApi(self.api_client)
            self.metrics_api = MetricsApi(self.api_client)
            
            logger.info("API client initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup authentication: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _configure_proxy(self):
        """Configure proxy settings"""
        try:
            proxy_url = os.getenv('PROXY_URL')
            
            # Only configure proxy if PROXY_URL is set
            if not proxy_url:
                logger.info("No proxy configured")
                return
            
            # Check if proxy should be used
            use_proxy = os.getenv('USE_PROXY', 'true').lower() == 'true'
            if not use_proxy:
                logger.info("Proxy disabled via USE_PROXY=false")
                return
            
            proxy_user = os.getenv('PROXY_USER')
            proxy_password = os.getenv('PROXY_PASSWORD')
            
            if proxy_user and proxy_password:
                proxy_domain = os.getenv('PROXY_DOMAIN', 'MAPLE')
                # Format: http://DOMAIN\\user:pass@proxy:port
                escaped_user = urllib.parse.quote(f"{proxy_domain}\\{proxy_user}")
                escaped_pass = urllib.parse.quote(proxy_password)
                full_proxy_url = f"http://{escaped_user}:{escaped_pass}@{proxy_url}"
            else:
                full_proxy_url = f"http://{proxy_url}"
            
            self.configuration.proxy = full_proxy_url
            logger.info(f"Proxy configured: {proxy_url}")
            
        except Exception as e:
            logger.warning(f"Failed to configure proxy: {e}")
    
    def get_available_metrics(self, category=None):
        """Get list of available metrics from the API"""
        try:
            if not self.metrics_api:
                logger.error("Metrics API not initialized")
                return []
            
            # Get metrics for a specific category or all
            response = self.metrics_api.get_fds_fundamentals_metrics(category=category)
            
            if response and response.data:
                metrics = []
                for metric in response.data:
                    metrics.append({
                        'metric': getattr(metric, 'metric', 'N/A'),
                        'name': getattr(metric, 'name', 'N/A'),
                        'category': getattr(metric, 'category', 'N/A')
                    })
                logger.info(f"Retrieved {len(metrics)} metrics")
                return metrics
            else:
                logger.warning("No metrics returned from API")
                return []
                
        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_fundamentals(self, tickers, metrics=None, fiscal_period=None, periodicity=None):
        """
        Get fundamentals data for specified tickers and metrics
        
        Args:
            tickers: List of ticker symbols (e.g., ['AAPL-US', 'MSFT-US'])
            metrics: List of metric codes (e.g., ['FF_SALES', 'FF_NET_INC'])
            fiscal_period: Fiscal period object
            periodicity: Periodicity object (e.g., 'QTR', 'ANN')
        """
        try:
            if not self.fundamentals_api:
                logger.error("Fundamentals API not initialized")
                return None
            
            # Use default metrics if none provided
            if not metrics:
                metrics = self.DEFAULT_METRICS
            
            # Validate tickers
            if not tickers:
                logger.error("No tickers provided")
                return None
            
            logger.info(f"Fetching fundamentals for {len(tickers)} tickers and {len(metrics)} metrics")
            
            # Create IDs object
            ids = IdsBatchMax30000(tickers)
            
            # Create Metrics object (not a plain list!)
            metrics_obj = Metrics(metrics)
            
            # Create request body
            request_body = FundamentalRequestBody(
                ids=ids,
                metrics=metrics_obj
            )
            
            # Add optional parameters
            if fiscal_period:
                request_body.fiscal_period = fiscal_period
            if periodicity:
                request_body.periodicity = periodicity
            
            # Create request
            request = FundamentalsRequest(data=request_body)
            
            # Make API call
            response = self.fundamentals_api.get_fds_fundamentals_for_list(request)
            
            if response and hasattr(response, 'data'):
                results = []
                for item in response.data:
                    result = {
                        'ticker': getattr(item, 'request_id', 'N/A'),
                        'metric': getattr(item, 'metric', 'N/A'),
                        'value': getattr(item, 'value', None),
                        'date': getattr(item, 'date', None),
                        'fiscal_period': getattr(item, 'fiscal_period', None),
                        'fiscal_year': getattr(item, 'fiscal_year', None)
                    }
                    results.append(result)
                
                logger.info(f"Retrieved {len(results)} data points")
                return results
            else:
                logger.warning("No data in response")
                return []
                
        except Exception as e:
            logger.error(f"Failed to get fundamentals: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def test_connection(self):
        """Test API connection with a simple request"""
        logger.info("Testing API connection...")
        
        # Test 1: Get metrics
        logger.info("Test 1: Getting available metrics...")
        metrics = self.get_available_metrics(category='INCOME_STATEMENT')
        if metrics:
            logger.info(f"✅ Metrics API working - found {len(metrics)} income statement metrics")
            if len(metrics) > 0:
                logger.info(f"   Sample metric: {metrics[0]['metric']} - {metrics[0]['name']}")
        else:
            logger.warning("❌ Metrics API test failed")
        
        # Test 2: Get fundamentals for a known ticker
        logger.info("\nTest 2: Getting fundamentals for Apple (AAPL-US)...")
        results = self.get_fundamentals(
            tickers=['AAPL-US'],
            metrics=['FF_SALES', 'FF_NET_INC']
        )
        
        if results:
            logger.info(f"✅ Fundamentals API working - retrieved {len(results)} data points")
            if pd and len(results) > 0:
                df = pd.DataFrame(results)
                logger.info(f"\nSample data:\n{df.head()}")
        else:
            logger.warning("❌ Fundamentals API test failed")
        
        return metrics is not None or results is not None
    
    def analyze_banks(self, output_file='bank_fundamentals.json'):
        """Analyze fundamentals for banks from config"""
        logger.info("Starting bank fundamentals analysis...")
        
        # Import bank configuration
        try:
            from config.banks_config import monitored_institutions
        except ImportError:
            logger.warning("Could not import banks config, using test tickers")
            monitored_institutions = {
                'JPM': {'name': 'JPMorgan Chase', 'type': 'US_Banks'},
                'BAC': {'name': 'Bank of America', 'type': 'US_Banks'},
                'RY': {'name': 'Royal Bank of Canada', 'type': 'Canadian_Banks'},
                'TD': {'name': 'Toronto-Dominion Bank', 'type': 'Canadian_Banks'},
            }
        
        all_results = {}
        
        # Process each bank
        for ticker, info in monitored_institutions.items():
            logger.info(f"\nProcessing {info['name']} ({ticker})...")
            
            # Try different ticker formats
            ticker_formats = [
                ticker,           # Already formatted ticker from config
            ]
            
            # If ticker doesn't have a suffix, try common ones
            if '-' not in ticker:
                ticker_formats.extend([
                    f"{ticker}-US",   # US format
                    f"{ticker}-USA",  # USA format
                    f"{ticker}-CA",   # Canada format
                    f"{ticker}-NYSE", # NYSE format
                    f"{ticker}-TSE",  # Toronto format
                ])
            
            success = False
            for test_ticker in ticker_formats:
                try:
                    # Get fundamentals
                    results = self.get_fundamentals(
                        tickers=[test_ticker],
                        metrics=self.DEFAULT_METRICS + self.BANK_METRICS
                    )
                    
                    if results and len(results) > 0:
                        logger.info(f"  ✅ Success with format: {test_ticker}")
                        all_results[ticker] = {
                            'name': info['name'],
                            'type': info['type'],
                            'working_ticker': test_ticker,
                            'data': results,
                            'timestamp': datetime.now().isoformat()
                        }
                        success = True
                        break
                    
                except Exception as e:
                    logger.debug(f"  Failed with {test_ticker}: {e}")
                    continue
            
            if not success:
                logger.warning(f"  ❌ Could not retrieve data for {ticker}")
                all_results[ticker] = {
                    'name': info['name'],
                    'type': info['type'],
                    'error': 'No valid ticker format found',
                    'timestamp': datetime.now().isoformat()
                }
            
            # Rate limiting
            time.sleep(0.5)
        
        # Save results
        output_path = Path(output_file)
        with open(output_path, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        
        logger.info(f"\nResults saved to {output_path}")
        
        # Generate summary
        successful = sum(1 for r in all_results.values() if 'data' in r)
        failed = len(all_results) - successful
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Analysis Summary:")
        logger.info(f"  Total banks processed: {len(all_results)}")
        logger.info(f"  Successful: {successful}")
        logger.info(f"  Failed: {failed}")
        
        if pd and successful > 0:
            # Create DataFrame for analysis
            data_rows = []
            for ticker, result in all_results.items():
                if 'data' in result:
                    for item in result['data']:
                        row = {
                            'ticker': ticker,
                            'name': result['name'],
                            'type': result['type'],
                            **item
                        }
                        data_rows.append(row)
            
            df = pd.DataFrame(data_rows)
            
            # Save to CSV for easy viewing
            csv_file = output_path.with_suffix('.csv')
            df.to_csv(csv_file, index=False)
            logger.info(f"  Data also saved to: {csv_file}")
            
            # Show summary statistics
            logger.info(f"\nMetrics coverage:")
            metric_counts = df.groupby('metric')['ticker'].nunique()
            for metric, count in metric_counts.items():
                coverage = (count / successful) * 100
                logger.info(f"  {metric}: {count}/{successful} banks ({coverage:.1f}%)")
        
        return all_results
    
    def __del__(self):
        """Cleanup temporary SSL certificate on exit"""
        if hasattr(self, 'ssl_cert_path') and self.ssl_cert_path:
            try:
                os.unlink(self.ssl_cert_path)
                logger.info("Temporary SSL certificate cleaned up")
            except:
                pass


def main():
    """Main execution function"""
    logger.info("="*60)
    logger.info("FactSet Fundamentals Analysis Tool")
    logger.info("="*60)
    
    # Show current configuration
    logger.info("\nConfiguration:")
    logger.info(f"  API_USERNAME: {'Set' if os.getenv('API_USERNAME') else 'Not set'}")
    logger.info(f"  API_PASSWORD: {'Set' if os.getenv('API_PASSWORD') else 'Not set'}")
    logger.info(f"  PROXY_URL: {os.getenv('PROXY_URL', 'Not set')}")
    logger.info(f"  USE_PROXY: {os.getenv('USE_PROXY', 'true')}")
    logger.info(f"  SSL_CERT_PATH: {os.getenv('SSL_CERT_PATH', 'Not set')}")
    
    # Create client
    client = FactSetFundamentalsClient()
    
    # Test connection
    if not client.test_connection():
        logger.error("Failed to connect to FactSet API. Please check credentials and network.")
        sys.exit(1)
    
    # Analyze banks
    logger.info("\n" + "="*60)
    logger.info("Starting bank analysis...")
    results = client.analyze_banks()
    
    logger.info("\n" + "="*60)
    logger.info("Analysis complete!")
    
    # Show next steps
    logger.info("\nNext steps:")
    logger.info("1. Review bank_fundamentals.json for detailed results")
    logger.info("2. Check bank_fundamentals.csv for tabular data")
    logger.info("3. Review factset_fundamentals.log for any errors")
    logger.info("4. Update config/banks_config.py with working ticker formats")


if __name__ == "__main__":
    main()