"""
FactSet Fundamentals API Analysis - Corrected Version
Uses the correct model types for all API parameters
"""

import os
import json
import tempfile
import logging
from datetime import datetime
from urllib.parse import quote
from pathlib import Path
import time

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# Correct FactSet SDK imports - using 'model' (singular)
import fds.sdk.FactSetFundamentals as ff
from fds.sdk.FactSetFundamentals.api.fact_set_fundamentals_api import FactSetFundamentalsApi
from fds.sdk.FactSetFundamentals.api.metrics_api import MetricsApi

# Import the correct model types (all required)
from fds.sdk.FactSetFundamentals.model.fundamentals_request import FundamentalsRequest
from fds.sdk.FactSetFundamentals.model.fundamental_request_body import FundamentalRequestBody
from fds.sdk.FactSetFundamentals.model.ids_batch_max30000 import IdsBatchMax30000
from fds.sdk.FactSetFundamentals.model.metrics import Metrics as MetricsModel
from fds.sdk.FactSetFundamentals.model.fiscal_period import FiscalPeriod
from fds.sdk.FactSetFundamentals.model.periodicity import Periodicity

# Import banks configuration
from config.banks_config import monitored_institutions

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fundamentals_analysis.log')
    ]
)
logger = logging.getLogger(__name__)


class FundamentalsMatrixAnalyzer:
    """Analyzes FactSet Fundamentals data with correct model types"""
    
    def __init__(self):
        self.configuration = None
        self.api_client = None
        self.metrics_api = None
        self.fundamentals_api = None
        self.ssl_cert_path = None
        
        # Results storage
        self.all_metrics = {}
        self.bank_metrics_matrix = {}
        
    def setup_ssl_certificate(self):
        """Setup SSL certificate if configured"""
        try:
            cert_path = os.getenv('SSL_CERT_PATH', 'certs/rbc-ca-bundle.cer')
            
            if os.path.exists(cert_path):
                with open(cert_path, 'rb') as cert_file:
                    cert_data = cert_file.read()
                
                temp_cert = tempfile.NamedTemporaryFile(mode='wb', suffix='.cer', delete=False)
                temp_cert.write(cert_data)
                temp_cert.close()
                
                os.environ['REQUESTS_CA_BUNDLE'] = temp_cert.name
                os.environ['SSL_CERT_FILE'] = temp_cert.name
                
                logger.info(f"SSL certificate configured: {temp_cert.name}")
                return temp_cert.name
            else:
                logger.info(f"SSL certificate not found at: {cert_path}, proceeding without")
                return None
                
        except Exception as e:
            logger.error(f"Error setting up SSL certificate: {e}")
            return None
    
    def setup_proxy_configuration(self):
        """Configure proxy if needed"""
        try:
            proxy_url = os.getenv('PROXY_URL')
            if not proxy_url:
                return None
                
            proxy_user = os.getenv('PROXY_USER')
            proxy_password = os.getenv('PROXY_PASSWORD')
            proxy_domain = os.getenv('PROXY_DOMAIN', 'MAPLE')
            
            if proxy_user and proxy_password:
                # Escape domain and user for NTLM authentication
                escaped_domain = quote(f"{proxy_domain}\\{proxy_user}")
                quoted_password = quote(proxy_password)
                
                # Construct proxy URL
                proxy_url_formatted = f"http://{escaped_domain}:{quoted_password}@{proxy_url}"
                
                logger.info("Proxy configuration completed")
                return proxy_url_formatted
            else:
                logger.info("Proxy URL found but no credentials, skipping proxy")
                return None
                
        except Exception as e:
            logger.error(f"Error configuring proxy: {e}")
            return None
    
    def setup_api_client(self):
        """Setup FactSet API client with authentication"""
        try:
            # Setup SSL
            self.ssl_cert_path = self.setup_ssl_certificate()
            
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
            proxy_url = self.setup_proxy_configuration()
            if proxy_url:
                self.configuration.proxy = proxy_url
                
            if self.ssl_cert_path:
                self.configuration.ssl_ca_cert = self.ssl_cert_path
            
            # Create API client
            self.api_client = ff.ApiClient(self.configuration)
            
            # Initialize API instances
            self.metrics_api = MetricsApi(self.api_client)
            self.fundamentals_api = FactSetFundamentalsApi(self.api_client)
            
            logger.info("FactSet API client configured successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup API client: {e}")
            raise
    
    def test_connection(self):
        """Test API connection with a known ticker"""
        logger.info("Testing API connection...")
        
        try:
            # Test with Apple ticker in different formats
            test_tickers = ['AAPL-US', 'AAPL', 'FDS-US', 'FDS']
            
            for ticker in test_tickers:
                try:
                    # Create model objects
                    ids = IdsBatchMax30000([ticker])
                    metrics = MetricsModel(['FF_SALES'])
                    
                    # Create request
                    request_body = FundamentalRequestBody(
                        ids=ids,
                        metrics=metrics
                    )
                    
                    request = FundamentalsRequest(data=request_body)
                    response = self.fundamentals_api.get_fds_fundamentals_for_list(request)
                    
                    if response and hasattr(response, 'data') and response.data:
                        logger.info(f"✅ API connection successful with ticker format: {ticker}")
                        return True
                        
                except Exception as e:
                    continue
            
            logger.warning("Could not connect with any ticker format")
            return False
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
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
            'MARKET_DATA',
            'MISCELLANEOUS'
        ]
        
        for category in categories:
            try:
                logger.info(f"  Fetching metrics for category: {category}")
                
                response = self.metrics_api.get_fds_fundamentals_metrics(category=category)
                
                if response and response.data:
                    for metric in response.data:
                        metric_id = getattr(metric, 'metric', None)
                        if metric_id:
                            self.all_metrics[metric_id] = {
                                'metric_id': metric_id,
                                'name': getattr(metric, 'name', ''),
                                'description': getattr(metric, 'description', ''),
                                'category': category,
                                'subcategory': getattr(metric, 'subcategory', ''),
                                'data_type': getattr(metric, 'data_type', '')
                            }
                
                time.sleep(0.15)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error fetching metrics for {category}: {e}")
                continue
        
        logger.info(f"Total unique metrics fetched: {len(self.all_metrics)}")
        self.save_metrics_catalog()
    
    def save_metrics_catalog(self):
        """Save complete metrics catalog to JSON"""
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        catalog = {
            'generated_at': datetime.now().isoformat(),
            'total_metrics': len(self.all_metrics),
            'metrics': self.all_metrics
        }
        
        catalog_file = output_dir / 'factset_metrics_catalog.json'
        with open(catalog_file, 'w') as f:
            json.dump(catalog, f, indent=2)
        
        logger.info(f"Metrics catalog saved to {catalog_file}")
    
    def analyze_bank(self, ticker, bank_info):
        """Analyze metrics availability for a single bank"""
        logger.info(f"Analyzing {ticker}: {bank_info['name']}")
        
        bank_metrics = {}
        
        try:
            # Get all metric IDs
            metric_ids = list(self.all_metrics.keys())
            
            # Process in batches
            batch_size = 30
            
            for i in range(0, len(metric_ids), batch_size):
                batch = metric_ids[i:i+batch_size]
                
                try:
                    # Create model objects
                    ids = IdsBatchMax30000([ticker])
                    metrics = MetricsModel(batch)  # Use MetricsModel
                    fiscal_period = FiscalPeriod(
                        start='2018-01-01',
                        end=datetime.now().strftime('%Y-%m-%d')
                    )
                    periodicity = Periodicity('QTR')
                    
                    # Create request
                    request_body = FundamentalRequestBody(
                        ids=ids,
                        metrics=metrics,
                        fiscal_period=fiscal_period,
                        periodicity=periodicity
                    )
                    
                    request = FundamentalsRequest(data=request_body)
                    response = self.fundamentals_api.get_fds_fundamentals_for_list(request)
                    
                    if response and hasattr(response, 'data') and response.data:
                        for item in response.data:
                            metric_id = getattr(item, 'metric', None)
                            if metric_id:
                                if metric_id not in bank_metrics:
                                    bank_metrics[metric_id] = 0
                                bank_metrics[metric_id] += 1
                    
                except Exception as e:
                    logger.debug(f"Batch error for {ticker}: {e}")
                    continue
                
                time.sleep(0.15)  # Rate limiting
            
            self.bank_metrics_matrix[ticker] = bank_metrics
            
            if bank_metrics:
                logger.info(f"  Found {len(bank_metrics)} metrics with data")
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
            time.sleep(0.5)
    
    def generate_matrix_report(self):
        """Generate the complete matrix report"""
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Build matrix dataframe
        logger.info("Building matrix dataframe...")
        matrix_data = []
        
        for metric_id, metric_info in self.all_metrics.items():
            row = {
                'Metric_ID': metric_id,
                'Metric_Name': metric_info['name'],
                'Description': metric_info['description'],
                'Category': metric_info['category'],
                'Data_Type': metric_info['data_type']
            }
            
            # Add columns for each bank
            total_banks = 0
            total_quarters = 0
            
            for ticker in monitored_institutions.keys():
                quarters = self.bank_metrics_matrix.get(ticker, {}).get(metric_id, 0)
                row[f'{ticker}_Quarters'] = quarters
                
                if quarters > 0:
                    total_banks += 1
                    total_quarters += quarters
            
            row['Total_Banks_With_Data'] = total_banks
            row['Coverage_Percentage'] = f"{(total_banks / len(monitored_institutions) * 100):.1f}%" if len(monitored_institutions) > 0 else "0%"
            
            matrix_data.append(row)
        
        # Create DataFrame and save to Excel
        df = pd.DataFrame(matrix_data)
        df = df.sort_values('Total_Banks_With_Data', ascending=False)
        
        excel_file = output_dir / f'fundamentals_matrix_{timestamp}.xlsx'
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Main matrix sheet
            columns_main = ['Metric_ID', 'Metric_Name', 'Description', 'Category', 
                           'Total_Banks_With_Data', 'Coverage_Percentage']
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
                    'Total_Data_Points': sum(metrics.values())
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
        summary.append('FACTSET FUNDAMENTALS MATRIX ANALYSIS SUMMARY')
        summary.append(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        summary.append('=' * 80)
        summary.append('')
        summary.append(f'Total Metrics in API: {len(self.all_metrics)}')
        summary.append(f'Total Banks Analyzed: {total_banks}')
        summary.append(f'Banks with Data: {banks_with_data}')
        summary.append('')
        
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
                logger.warning(f"Failed to cleanup: {e}")
    
    def run(self):
        """Main execution"""
        try:
            logger.info("Starting FactSet Fundamentals Matrix Analysis...")
            logger.info("=" * 60)
            
            # Setup API client
            self.setup_api_client()
            
            # Test connection
            if not self.test_connection():
                logger.error("API connection test failed. Check credentials in .env file")
                logger.info("Make sure API_USERNAME and API_PASSWORD are correct")
                return
            
            # Fetch all metrics
            self.fetch_all_metrics()
            
            # Analyze all banks
            self.analyze_all_banks()
            
            # Generate reports
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
    analyzer = FundamentalsMatrixAnalyzer()
    analyzer.run()


if __name__ == "__main__":
    main()