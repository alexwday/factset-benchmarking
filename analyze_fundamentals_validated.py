"""
FactSet Fundamentals API Analysis Script - Validated Version
Uses the exact patterns from the example files for authentication and API calls
"""

import os
import json
import tempfile
import logging
from datetime import datetime
from urllib.parse import quote
from typing import Dict, Any, List, Optional
import time
import io

import pandas as pd
import numpy as np
from dotenv import load_dotenv
import yaml

# FactSet SDK imports - matching the example pattern
import fds.sdk.FactSetFundamentals as ff
from fds.sdk.FactSetFundamentals.api import metrics_api, factset_fundamentals_api
from fds.sdk.FactSetFundamentals.model.fundamentals_request import FundamentalsRequest
from fds.sdk.FactSetFundamentals.model.fundamental_request_body import FundamentalRequestBody
from fds.sdk.FactSetFundamentals.model.fiscal_period import FiscalPeriod

from pathlib import Path
from tqdm import tqdm

# Import banks configuration
from config.banks_config import monitored_institutions

# Load environment variables
load_dotenv()

# Setup logging - matching example pattern
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fundamentals_analysis.log')
    ]
)
logger = logging.getLogger(__name__)


class FundamentalsAnalyzer:
    """Analyzes FactSet Fundamentals data using validated API patterns"""
    
    def __init__(self):
        self.configuration = None
        self.api_client = None
        self.metrics_api = None
        self.fundamentals_api = None
        self.ssl_cert_path = None
        
        # Store results
        self.all_metrics = {}
        self.bank_metrics_matrix = {}
        self.analysis_results = []
        
    def setup_ssl_certificate(self) -> Optional[str]:
        """Setup SSL certificate - following example pattern"""
        try:
            # Check for certificate in config path (from example)
            cert_path = os.getenv('SSL_CERT_PATH', 'certs/rbc-ca-bundle.cer')
            
            if os.path.exists(cert_path):
                with open(cert_path, 'rb') as cert_file:
                    cert_data = cert_file.read()
                
                # Create temporary certificate file (matching example)
                temp_cert = tempfile.NamedTemporaryFile(mode='wb', suffix='.cer', delete=False)
                temp_cert.write(cert_data)
                temp_cert.close()
                
                # Set environment variables for SSL (matching example)
                os.environ['REQUESTS_CA_BUNDLE'] = temp_cert.name
                os.environ['SSL_CERT_FILE'] = temp_cert.name
                
                logger.info(f"SSL certificate configured successfully: {temp_cert.name}")
                return temp_cert.name
            else:
                logger.warning(f"SSL certificate not found at: {cert_path}")
                return None
                
        except Exception as e:
            logger.error(f"Error setting up SSL certificate: {e}")
            return None
    
    def setup_proxy_configuration(self) -> str:
        """Configure proxy URL - matching example pattern exactly"""
        try:
            proxy_user = os.getenv('PROXY_USER')
            proxy_password = os.getenv('PROXY_PASSWORD') 
            proxy_url = os.getenv('PROXY_URL')
            proxy_domain = os.getenv('PROXY_DOMAIN', 'MAPLE')
            
            if not all([proxy_user, proxy_password, proxy_url]):
                logger.info("Proxy configuration not found, proceeding without proxy")
                return None
            
            # Escape domain and user for NTLM authentication (matching example)
            escaped_domain = quote(proxy_domain + "\\" + proxy_user)
            quoted_password = quote(proxy_password)
            
            # Construct proxy URL (matching example format)
            proxy_url_formatted = f"http://{escaped_domain}:{quoted_password}@{proxy_url}"
            
            logger.info("Proxy configuration completed successfully")
            return proxy_url_formatted
            
        except Exception as e:
            logger.error(f"Error configuring proxy: {e}")
            return None
    
    def setup_factset_api_client(self):
        """Setup FactSet API client - matching example pattern"""
        try:
            # Setup SSL first
            self.ssl_cert_path = self.setup_ssl_certificate()
            
            # Setup proxy
            proxy_url = self.setup_proxy_configuration()
            
            # Get API credentials
            api_username = os.getenv('API_USERNAME')
            api_password = os.getenv('API_PASSWORD')
            
            if not api_username or not api_password:
                raise ValueError("API_USERNAME and API_PASSWORD must be set in environment")
            
            # Configure FactSet API - matching example pattern
            self.configuration = ff.Configuration(
                username=api_username,
                password=api_password
            )
            
            if proxy_url:
                self.configuration.proxy = proxy_url
                
            if self.ssl_cert_path:
                self.configuration.ssl_ca_cert = self.ssl_cert_path
            
            # Create API client with configuration
            self.api_client = ff.ApiClient(self.configuration)
            
            # Initialize API instances
            self.metrics_api = metrics_api.MetricsApi(self.api_client)
            self.fundamentals_api = factset_fundamentals_api.FactSetFundamentalsApi(self.api_client)
            
            logger.info("FactSet API client configured successfully")
            
        except Exception as e:
            logger.error(f"Error setting up FactSet API client: {e}")
            raise
    
    def fetch_all_metrics(self):
        """Fetch all available metrics from the API"""
        logger.info("Fetching all available FactSet Fundamentals metrics...")
        
        categories = [
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
        
        for category in categories:
            try:
                logger.info(f"  Fetching metrics for category: {category}")
                
                # Call metrics API
                response = self.metrics_api.get_fds_fundamentals_metrics(
                    category=category
                )
                
                if response and hasattr(response, 'data') and response.data:
                    for metric in response.data:
                        # Extract metric details
                        metric_id = getattr(metric, 'metric', None)
                        if metric_id:
                            self.all_metrics[metric_id] = {
                                'metric_id': metric_id,
                                'name': getattr(metric, 'name', ''),
                                'description': getattr(metric, 'description', ''),
                                'category': category,
                                'subcategory': getattr(metric, 'subcategory', ''),
                                'data_type': getattr(metric, 'data_type', ''),
                                'unit': getattr(metric, 'unit', ''),
                                'scale': getattr(metric, 'scale', '')
                            }
                
                # Rate limiting (10 requests per second max)
                time.sleep(0.15)
                
            except Exception as e:
                logger.error(f"Error fetching metrics for {category}: {e}")
                continue
        
        logger.info(f"Total unique metrics fetched: {len(self.all_metrics)}")
        self.save_metrics_catalog()
    
    def save_metrics_catalog(self):
        """Save the complete metrics catalog"""
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        catalog = {
            'generated_at': datetime.now().isoformat(),
            'total_metrics': len(self.all_metrics),
            'metrics': self.all_metrics,
            'by_category': {}
        }
        
        # Group by category
        for metric_id, details in self.all_metrics.items():
            category = details['category']
            if category not in catalog['by_category']:
                catalog['by_category'][category] = []
            catalog['by_category'][category].append(details)
        
        # Save to JSON
        catalog_file = output_dir / 'factset_metrics_catalog.json'
        with open(catalog_file, 'w') as f:
            json.dump(catalog, f, indent=2)
        
        logger.info(f"Metrics catalog saved to {catalog_file}")
    
    def analyze_bank(self, ticker: str, bank_info: Dict):
        """Analyze a single bank's available metrics"""
        logger.info(f"Analyzing {ticker}: {bank_info['name']}")
        
        bank_metrics = {}
        
        try:
            # Get all metric IDs to check
            metric_ids = list(self.all_metrics.keys())
            
            # Process in batches (API limit: 250 ids for non-batch, but we use smaller batches)
            batch_size = 30  # Conservative batch size
            
            for i in range(0, len(metric_ids), batch_size):
                batch = metric_ids[i:i+batch_size]
                
                try:
                    # Create fiscal period for date range
                    fiscal_period = FiscalPeriod(
                        start='2018-01-01',
                        end=datetime.now().strftime('%Y-%m-%d')
                    )
                    
                    # Create request body
                    request_body = FundamentalRequestBody(
                        ids=[ticker],
                        metrics=batch,
                        fiscal_period=fiscal_period,
                        periodicity='QTR',  # Quarterly
                        currency='LOCAL'
                    )
                    
                    # Create request
                    request = FundamentalsRequest(data=request_body)
                    
                    # Make API call
                    response = self.fundamentals_api.get_fds_fundamentals_for_list(request)
                    
                    if response and hasattr(response, 'data') and response.data:
                        # Process each data point
                        for item in response.data:
                            metric_id = getattr(item, 'metric', None)
                            value = getattr(item, 'value', None)
                            
                            if metric_id and value is not None:
                                if metric_id not in bank_metrics:
                                    bank_metrics[metric_id] = 0
                                bank_metrics[metric_id] += 1
                    
                except Exception as e:
                    logger.debug(f"Error fetching batch for {ticker}: {e}")
                    continue
                
                # Rate limiting
                time.sleep(0.15)
            
            self.bank_metrics_matrix[ticker] = bank_metrics
            
            # Log summary
            if bank_metrics:
                logger.info(f"  Found {len(bank_metrics)} metrics with data for {ticker}")
            else:
                logger.warning(f"  No data found for {ticker}")
                
        except Exception as e:
            logger.error(f"Error analyzing {ticker}: {e}")
            self.bank_metrics_matrix[ticker] = {}
    
    def analyze_all_banks(self):
        """Analyze all monitored banks"""
        logger.info(f"\nAnalyzing {len(monitored_institutions)} monitored banks...")
        
        for ticker, bank_info in tqdm(monitored_institutions.items(), desc="Processing banks"):
            self.analyze_bank(ticker, bank_info)
            
            # Small delay between banks
            time.sleep(0.5)
    
    def generate_matrix_report(self):
        """Generate the complete matrix report"""
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Build matrix dataframe
        matrix_data = []
        
        for metric_id, metric_info in self.all_metrics.items():
            row = {
                'Metric_ID': metric_id,
                'Metric_Name': metric_info['name'],
                'Description': metric_info['description'],
                'Category': metric_info['category'],
                'Subcategory': metric_info['subcategory'],
                'Data_Type': metric_info['data_type']
            }
            
            # Add data for each bank
            total_banks = 0
            total_quarters = 0
            
            for ticker in monitored_institutions.keys():
                quarters = self.bank_metrics_matrix.get(ticker, {}).get(metric_id, 0)
                row[f'{ticker}_Quarters'] = quarters
                row[f'{ticker}_Available'] = 'Y' if quarters > 0 else 'N'
                
                if quarters > 0:
                    total_banks += 1
                    total_quarters += quarters
            
            row['Total_Banks_With_Data'] = total_banks
            row['Coverage_Percentage'] = f"{(total_banks / len(monitored_institutions) * 100):.1f}%"
            row['Avg_Quarters'] = f"{(total_quarters / total_banks):.1f}" if total_banks > 0 else "0"
            
            matrix_data.append(row)
        
        # Create DataFrame and sort by coverage
        df = pd.DataFrame(matrix_data)
        df['_sort_coverage'] = df['Total_Banks_With_Data']
        df = df.sort_values('_sort_coverage', ascending=False)
        df = df.drop('_sort_coverage', axis=1)
        
        # Save to Excel
        excel_file = output_dir / f'fundamentals_matrix_{timestamp}.xlsx'
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Main matrix sheet
            columns_main = ['Metric_ID', 'Metric_Name', 'Description', 'Category', 
                           'Total_Banks_With_Data', 'Coverage_Percentage']
            
            # Add quarter columns for each bank
            for ticker in monitored_institutions.keys():
                columns_main.append(f'{ticker}_Quarters')
            
            df[columns_main].to_excel(writer, sheet_name='Metrics_Matrix', index=False)
            
            # Bank summary sheet
            bank_summary = []
            for ticker, bank_info in monitored_institutions.items():
                metrics = self.bank_metrics_matrix.get(ticker, {})
                bank_summary.append({
                    'Ticker': ticker,
                    'Bank_Name': bank_info['name'],
                    'Type': bank_info['type'],
                    'Total_Metrics': len(metrics),
                    'Total_Quarters': sum(metrics.values())
                })
            
            pd.DataFrame(bank_summary).to_excel(writer, sheet_name='Bank_Summary', index=False)
        
        logger.info(f"Matrix report saved to {excel_file}")
        
        # Generate summary
        self.generate_summary()
    
    def generate_summary(self):
        """Generate text summary"""
        output_dir = Path('output')
        summary_file = output_dir / 'ANALYSIS_SUMMARY.txt'
        
        total_banks = len(monitored_institutions)
        banks_with_data = sum(1 for m in self.bank_metrics_matrix.values() if m)
        
        summary = []
        summary.append('=' * 80)
        summary.append('FACTSET FUNDAMENTALS ANALYSIS SUMMARY')
        summary.append(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        summary.append('=' * 80)
        summary.append('')
        summary.append(f'Total Metrics in API: {len(self.all_metrics)}')
        summary.append(f'Total Banks Analyzed: {total_banks}')
        summary.append(f'Banks with Data: {banks_with_data}')
        summary.append('')
        
        # Banks by data availability
        bank_stats = [(t, len(m)) for t, m in self.bank_metrics_matrix.items() if m]
        bank_stats.sort(key=lambda x: x[1], reverse=True)
        
        summary.append('TOP BANKS BY METRIC AVAILABILITY:')
        for ticker, metric_count in bank_stats[:10]:
            name = monitored_institutions[ticker]['name']
            summary.append(f'  {ticker}: {name} - {metric_count} metrics')
        
        summary_text = '\n'.join(summary)
        
        with open(summary_file, 'w') as f:
            f.write(summary_text)
        
        print('\n' + summary_text)
    
    def cleanup(self):
        """Clean up temporary files"""
        if self.ssl_cert_path and os.path.exists(self.ssl_cert_path):
            try:
                os.unlink(self.ssl_cert_path)
                logger.info("Cleaned up temporary SSL certificate")
            except Exception as e:
                logger.warning(f"Failed to cleanup SSL certificate: {e}")
    
    def run(self):
        """Main execution"""
        try:
            logger.info("Starting FactSet Fundamentals Analysis...")
            logger.info("=" * 60)
            
            # Step 1: Setup API client
            self.setup_factset_api_client()
            
            # Step 2: Fetch all metrics
            self.fetch_all_metrics()
            
            # Step 3: Analyze all banks
            self.analyze_all_banks()
            
            # Step 4: Generate reports
            self.generate_matrix_report()
            
            logger.info("=" * 60)
            logger.info("Analysis completed successfully!")
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            raise
        finally:
            self.cleanup()


def main():
    """Main entry point"""
    analyzer = FundamentalsAnalyzer()
    analyzer.run()


if __name__ == "__main__":
    main()