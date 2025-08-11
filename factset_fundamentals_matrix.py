#!/usr/bin/env python3
"""
FactSet Fundamentals Availability Matrix Builder
Discovers all available metrics and builds a matrix showing which metrics 
are available for each of the 91 monitored financial institutions
"""

import os
import json
import sys
import logging
import tempfile
from datetime import datetime
from typing import Dict, Any, List, Optional, Set
import time
from pathlib import Path
import urllib.parse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fundamentals_matrix.log')
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
except ImportError as e:
    logger.error(f"Failed to import FactSet SDK: {e}")
    sys.exit(1)

# Import Pandas for data analysis
try:
    import pandas as pd
    import numpy as np
except ImportError:
    logger.error("pandas and numpy are required. Install with: pip install pandas numpy")
    sys.exit(1)


class FundamentalsMatrixBuilder:
    """Builds availability matrix for FactSet Fundamentals data"""
    
    # Valid categories from API documentation
    VALID_CATEGORIES = [
        'INCOME_STATEMENT',
        'BALANCE_SHEET', 
        'CASH_FLOW',
        'RATIOS',
        'FINANCIAL_SERVICES',
        'INDUSTRY_METRICS',
        'PENSION_AND_POSTRETIREMENT',
        'MARKET_DATA',
        'MISCELLANEOUS',
        'DATES'
    ]
    
    def __init__(self):
        """Initialize the matrix builder"""
        self.configuration = None
        self.api_client = None
        self.fundamentals_api = None
        self.metrics_api = None
        self.ssl_cert_path = None
        self.all_metrics = {}  # Dict of metric_code: metric_info
        self.availability_matrix = {}  # Dict of ticker: set of available metrics
        self._setup_authentication()
        
    def _setup_ssl_certificate(self):
        """Setup SSL certificate from file or environment"""
        try:
            ssl_cert_path = os.getenv('SSL_CERT_PATH')
            
            if ssl_cert_path and os.path.exists(ssl_cert_path):
                with open(ssl_cert_path, 'rb') as cert_file:
                    cert_data = cert_file.read()
                    
                temp_cert = tempfile.NamedTemporaryFile(mode='wb', suffix='.cer', delete=False)
                temp_cert.write(cert_data)
                temp_cert.close()
                
                os.environ['REQUESTS_CA_BUNDLE'] = temp_cert.name
                os.environ['SSL_CERT_FILE'] = temp_cert.name
                
                logger.info(f"SSL certificate configured from: {ssl_cert_path}")
                return temp_cert.name
            else:
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
            
            if not proxy_url:
                logger.info("No proxy configured")
                return
            
            use_proxy = os.getenv('USE_PROXY', 'true').lower() == 'true'
            if not use_proxy:
                logger.info("Proxy disabled via USE_PROXY=false")
                return
            
            proxy_user = os.getenv('PROXY_USER')
            proxy_password = os.getenv('PROXY_PASSWORD')
            
            if proxy_user and proxy_password:
                proxy_domain = os.getenv('PROXY_DOMAIN', 'MAPLE')
                escaped_user = urllib.parse.quote(f"{proxy_domain}\\{proxy_user}")
                escaped_pass = urllib.parse.quote(proxy_password)
                full_proxy_url = f"http://{escaped_user}:{escaped_pass}@{proxy_url}"
            else:
                full_proxy_url = f"http://{proxy_url}"
            
            self.configuration.proxy = full_proxy_url
            logger.info(f"Proxy configured: {proxy_url}")
            
        except Exception as e:
            logger.warning(f"Failed to configure proxy: {e}")
    
    def test_apple_ticker(self):
        """Test with Apple ticker to verify API connection works"""
        logger.info("\n" + "="*60)
        logger.info("Testing API Connection with Apple (AAPL-US)")
        logger.info("="*60)
        
        test_metrics = ['FF_SALES', 'FF_NET_INC', 'FF_EPS']
        
        try:
            # Create request
            ids = IdsBatchMax30000(['AAPL-US'])
            metrics_obj = Metrics(test_metrics)
            
            request_body = FundamentalRequestBody(
                ids=ids,
                metrics=metrics_obj
            )
            
            request = FundamentalsRequest(data=request_body)
            
            # Make API call
            response = self.fundamentals_api.get_fds_fundamentals_for_list(request)
            
            if response and hasattr(response, 'data') and response.data:
                logger.info(f"✅ SUCCESS! Apple test passed. Got {len(response.data)} data points:")
                for item in response.data[:3]:  # Show first 3 items
                    metric = getattr(item, 'metric', 'N/A')
                    value = getattr(item, 'value', 'N/A')
                    date = getattr(item, 'date', 'N/A')
                    logger.info(f"  - {metric}: {value} (as of {date})")
                return True
            else:
                logger.error("❌ FAILED! No data returned for Apple (AAPL-US)")
                logger.error("This suggests an authentication or API access issue.")
                return False
                
        except Exception as e:
            logger.error(f"❌ FAILED! Error testing Apple ticker: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_all_available_metrics(self):
        """Get ALL available metrics from the API across all categories"""
        logger.info("\n" + "="*60)
        logger.info("Fetching All Available Metrics from FactSet API")
        logger.info("="*60)
        
        all_metrics = {}
        
        # First, get all metrics without category filter
        try:
            logger.info("Fetching ALL metrics (no category filter)...")
            response = self.metrics_api.get_fds_fundamentals_metrics()
            
            if response and response.data:
                for metric in response.data:
                    metric_code = getattr(metric, 'metric', None)
                    if metric_code:
                        all_metrics[metric_code] = {
                            'code': metric_code,
                            'name': getattr(metric, 'name', 'N/A'),
                            'category': getattr(metric, 'category', 'N/A'),
                            'subcategory': getattr(metric, 'subcategory', 'N/A'),
                            'oa_page_id': getattr(metric, 'oa_page_id', 'N/A'),
                        }
                logger.info(f"  Found {len(response.data)} total metrics")
        except Exception as e:
            logger.error(f"Error fetching all metrics: {e}")
            import traceback
            traceback.print_exc()
        
        # Then get metrics by each valid category for additional detail
        for category in self.VALID_CATEGORIES:
            try:
                logger.info(f"Fetching metrics for category: {category}")
                response = self.metrics_api.get_fds_fundamentals_metrics(category=category)
                
                if response and response.data:
                    count = 0
                    for metric in response.data:
                        metric_code = getattr(metric, 'metric', None)
                        if metric_code and metric_code not in all_metrics:
                            all_metrics[metric_code] = {
                                'code': metric_code,
                                'name': getattr(metric, 'name', 'N/A'),
                                'category': getattr(metric, 'category', 'N/A'),
                                'subcategory': getattr(metric, 'subcategory', 'N/A'),
                                'oa_page_id': getattr(metric, 'oa_page_id', 'N/A'),
                            }
                            count += 1
                    logger.info(f"  Found {len(response.data)} metrics ({count} new)")
            except Exception as e:
                logger.error(f"Error fetching metrics for category {category}: {e}")
                # Don't stop, continue with other categories
        
        self.all_metrics = all_metrics
        logger.info(f"\nTotal unique metrics discovered: {len(all_metrics)}")
        
        # Show breakdown by category
        category_counts = {}
        for metric_info in all_metrics.values():
            cat = metric_info['category']
            category_counts[cat] = category_counts.get(cat, 0) + 1
        
        logger.info("\nMetrics by category:")
        for cat, count in sorted(category_counts.items()):
            logger.info(f"  {cat}: {count} metrics")
        
        # Save metrics list for reference
        with open('all_metrics.json', 'w') as f:
            json.dump(all_metrics, f, indent=2)
        logger.info("\nSaved complete metrics list to all_metrics.json")
        
        return all_metrics
    
    def test_metrics_for_ticker(self, ticker: str, metric_codes: List[str], batch_size: int = 30):
        """
        Test which metrics are available for a specific ticker
        Process in batches to avoid request size limits
        """
        available_metrics = set()
        errors = []
        
        # Process metrics in batches
        for i in range(0, len(metric_codes), batch_size):
            batch = metric_codes[i:i+batch_size]
            
            try:
                # Create request
                ids = IdsBatchMax30000([ticker])
                metrics_obj = Metrics(batch)
                
                request_body = FundamentalRequestBody(
                    ids=ids,
                    metrics=metrics_obj
                )
                
                request = FundamentalsRequest(data=request_body)
                
                # Make API call
                response = self.fundamentals_api.get_fds_fundamentals_for_list(request)
                
                if response and hasattr(response, 'data') and response.data:
                    for item in response.data:
                        metric = getattr(item, 'metric', None)
                        value = getattr(item, 'value', None)
                        if metric and value is not None:
                            available_metrics.add(metric)
                
            except Exception as e:
                error_msg = str(e)
                errors.append(f"Batch {i//batch_size + 1}: {error_msg[:100]}")
                # Log the full error for debugging
                logger.debug(f"Full error for batch {i//batch_size + 1}: {error_msg}")
        
        # If we had errors, log them
        if errors and not available_metrics:
            logger.warning(f"  Errors encountered for {ticker}:")
            for error in errors[:3]:  # Show first 3 errors
                logger.warning(f"    - {error}")
        
        return available_metrics
    
    def build_availability_matrix(self):
        """Build the complete availability matrix for all banks and metrics"""
        logger.info("\n" + "="*60)
        logger.info("Building Availability Matrix")
        logger.info("="*60)
        
        # Step 1: Test Apple first
        if not self.test_apple_ticker():
            logger.error("Apple test failed. Please check your API credentials and connection.")
            return None
        
        # Step 2: Get all metrics
        if not self.all_metrics:
            self.get_all_available_metrics()
        
        if not self.all_metrics:
            logger.error("No metrics found. Cannot proceed.")
            return None
        
        metric_codes = list(self.all_metrics.keys())
        logger.info(f"\nTesting {len(metric_codes)} metrics across all banks...")
        
        # Step 3: Import bank configuration
        try:
            from config.banks_config import monitored_institutions
        except ImportError:
            logger.error("Could not import banks config")
            return None
        
        # Step 4: Test each bank
        results = {}
        total_banks = len(monitored_institutions)
        
        for idx, (ticker, info) in enumerate(monitored_institutions.items(), 1):
            logger.info(f"\n[{idx}/{total_banks}] Testing {info['name']} ({ticker})...")
            
            # The ticker in config should already have the correct format (e.g., JPM-US)
            # But we'll also try without suffix if it fails
            test_tickers = [ticker]
            
            # If ticker has a suffix, also try without it
            if '-' in ticker:
                base_ticker = ticker.split('-')[0]
                test_tickers.append(base_ticker)
            
            bank_metrics = set()
            working_ticker = None
            
            for test_ticker in test_tickers:
                logger.info(f"  Trying ticker: {test_ticker}")
                available = self.test_metrics_for_ticker(test_ticker, metric_codes)
                
                if available:
                    bank_metrics = available
                    working_ticker = test_ticker
                    logger.info(f"  ✅ Found {len(available)} metrics with ticker: {test_ticker}")
                    break
                else:
                    logger.debug(f"  No data with ticker: {test_ticker}")
            
            if working_ticker:
                results[ticker] = {
                    'name': info['name'],
                    'type': info['type'],
                    'working_ticker': working_ticker,
                    'available_metrics': list(bank_metrics),
                    'metric_count': len(bank_metrics)
                }
            else:
                logger.warning(f"  ❌ No data found for any ticker format tried")
                results[ticker] = {
                    'name': info['name'],
                    'type': info['type'],
                    'error': 'No valid ticker format found',
                    'tested_tickers': test_tickers,
                    'available_metrics': [],
                    'metric_count': 0
                }
            
            # Rate limiting
            time.sleep(1)
        
        self.availability_matrix = results
        
        # Save raw results
        with open('availability_matrix_raw.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info("\n" + "="*60)
        logger.info("Matrix Build Complete")
        logger.info("="*60)
        
        # Show summary
        successful = sum(1 for r in results.values() if r['metric_count'] > 0)
        failed = total_banks - successful
        
        logger.info(f"Successfully retrieved data for {successful}/{total_banks} banks")
        logger.info(f"Failed to retrieve data for {failed} banks")
        
        if failed > 0:
            logger.info("\nFailed banks:")
            for ticker, data in results.items():
                if data['metric_count'] == 0:
                    logger.info(f"  - {data['name']} ({ticker}): Tested {data.get('tested_tickers', [ticker])}")
        
        return results
    
    def analyze_matrix(self):
        """Analyze the availability matrix to find common metrics"""
        if not self.availability_matrix:
            logger.error("No availability matrix to analyze")
            return
        
        logger.info("\n" + "="*60)
        logger.info("Analyzing Availability Matrix")
        logger.info("="*60)
        
        # Filter to only successful banks
        successful_banks = {k: v for k, v in self.availability_matrix.items() 
                          if v.get('metric_count', 0) > 0}
        
        if not successful_banks:
            logger.error("No successful banks to analyze")
            return
        
        # Create DataFrame for analysis
        matrix_data = []
        
        for ticker, data in successful_banks.items():
            for metric in data['available_metrics']:
                matrix_data.append({
                    'ticker': ticker,
                    'name': data['name'],
                    'type': data['type'],
                    'metric': metric,
                    'available': 1
                })
        
        if not matrix_data:
            logger.error("No data to analyze")
            return
        
        df = pd.DataFrame(matrix_data)
        
        # Create pivot table (banks x metrics)
        pivot = df.pivot_table(
            index='ticker',
            columns='metric',
            values='available',
            fill_value=0
        )
        
        # Save full matrix to CSV
        pivot.to_csv('availability_matrix_full.csv')
        logger.info(f"Saved full matrix to availability_matrix_full.csv")
        logger.info(f"Matrix dimensions: {pivot.shape[0]} banks × {pivot.shape[1]} metrics")
        
        # Analyze metric coverage
        metric_coverage = pivot.sum(axis=0).sort_values(ascending=False)
        total_banks = len(successful_banks)
        
        # Find metrics available for all banks
        universal_metrics = metric_coverage[metric_coverage == total_banks]
        
        logger.info(f"\nMetrics available for ALL {total_banks} successful banks: {len(universal_metrics)}")
        if len(universal_metrics) > 0:
            logger.info("\nTop universal metrics:")
            for i, metric in enumerate(universal_metrics.index[:20], 1):
                if metric in self.all_metrics:
                    logger.info(f"  {i}. {metric}: {self.all_metrics[metric]['name']}")
        
        # Find metrics available for most banks (>80%)
        threshold = int(total_banks * 0.8)
        common_metrics = metric_coverage[metric_coverage >= threshold]
        
        logger.info(f"\nMetrics available for >80% of banks: {len(common_metrics)}")
        
        # Group analysis by bank type
        logger.info("\nCoverage by bank type:")
        for bank_type in df['type'].unique():
            type_banks = df[df['type'] == bank_type]['ticker'].unique()
            type_count = len(type_banks)
            avg_metrics = df[df['type'] == bank_type].groupby('ticker')['metric'].count().mean()
            logger.info(f"  {bank_type}: {type_count} banks, avg {avg_metrics:.0f} metrics per bank")
        
        # Find best covered banks
        bank_coverage = pivot.sum(axis=1).sort_values(ascending=False)
        logger.info("\nTop 10 banks by metric coverage:")
        for i, (ticker, count) in enumerate(bank_coverage.head(10).items(), 1):
            bank_name = self.availability_matrix[ticker]['name']
            logger.info(f"  {i}. {bank_name} ({ticker}): {int(count)} metrics")
        
        # Save summary report
        summary = {
            'total_banks_tested': len(self.availability_matrix),
            'successful_banks': total_banks,
            'failed_banks': len(self.availability_matrix) - total_banks,
            'total_metrics_in_api': len(self.all_metrics),
            'total_metrics_found': len(metric_coverage),
            'universal_metrics_count': len(universal_metrics),
            'universal_metrics': list(universal_metrics.index),
            'common_metrics_80pct_count': len(common_metrics),
            'common_metrics_80pct': list(common_metrics.index[:50]),  # Top 50
            'coverage_by_type': df.groupby('type')['ticker'].nunique().to_dict(),
            'top_metrics': {
                metric: {
                    'name': self.all_metrics.get(metric, {}).get('name', 'Unknown'),
                    'category': self.all_metrics.get(metric, {}).get('category', 'Unknown'),
                    'coverage': int(count),
                    'percentage': f"{(count/total_banks)*100:.1f}%"
                }
                for metric, count in metric_coverage.head(100).items()
            },
            'failed_banks': [
                {
                    'ticker': ticker,
                    'name': data['name'],
                    'type': data['type'],
                    'tested_formats': data.get('tested_tickers', [ticker])
                }
                for ticker, data in self.availability_matrix.items()
                if data.get('metric_count', 0) == 0
            ]
        }
        
        with open('availability_matrix_summary.json', 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info("\nSaved analysis results to:")
        logger.info("  - availability_matrix_raw.json (raw data)")
        logger.info("  - availability_matrix_full.csv (full matrix)")
        logger.info("  - availability_matrix_summary.json (summary statistics)")
        
        return summary
    
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
    logger.info("FactSet Fundamentals Availability Matrix Builder")
    logger.info("="*60)
    
    # Show current configuration
    logger.info("\nConfiguration:")
    logger.info(f"  API_USERNAME: {'Set' if os.getenv('API_USERNAME') else 'Not set'}")
    logger.info(f"  API_PASSWORD: {'Set' if os.getenv('API_PASSWORD') else 'Not set'}")
    logger.info(f"  PROXY_URL: {os.getenv('PROXY_URL', 'Not set')}")
    logger.info(f"  USE_PROXY: {os.getenv('USE_PROXY', 'true')}")
    logger.info(f"  SSL_CERT_PATH: {os.getenv('SSL_CERT_PATH', 'Not set')}")
    
    # Create builder
    builder = FundamentalsMatrixBuilder()
    
    # Build the matrix
    logger.info("\nStarting matrix build process...")
    matrix = builder.build_availability_matrix()
    
    if matrix:
        # Analyze results
        logger.info("\nAnalyzing results...")
        summary = builder.analyze_matrix()
        
        logger.info("\n" + "="*60)
        logger.info("Process Complete!")
        logger.info("="*60)
        logger.info("\nOutput files generated:")
        logger.info("  1. all_metrics.json - Complete list of all API metrics")
        logger.info("  2. availability_matrix_raw.json - Raw availability data")
        logger.info("  3. availability_matrix_full.csv - Full matrix (banks × metrics)")
        logger.info("  4. availability_matrix_summary.json - Summary statistics")
        logger.info("  5. fundamentals_matrix.log - Detailed execution log")
    else:
        logger.error("\nMatrix build failed. Please check the log for errors.")


if __name__ == "__main__":
    main()