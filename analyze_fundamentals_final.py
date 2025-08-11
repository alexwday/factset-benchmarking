"""
FactSet Fundamentals API Analysis - Final Validated Version
Creates a complete matrix of metrics availability across all monitored banks
"""

import os
import json
import tempfile
import logging
from datetime import datetime
from urllib.parse import quote
from typing import Dict, Any, List, Optional
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# Correct FactSet SDK imports based on actual structure
import fds.sdk.FactSetFundamentals as ff
from fds.sdk.FactSetFundamentals.api.metrics_api import MetricsApi
from fds.sdk.FactSetFundamentals.api.fact_set_fundamentals_api import FactSetFundamentalsApi
from fds.sdk.FactSetFundamentals.model.fundamentals_request import FundamentalsRequest
from fds.sdk.FactSetFundamentals.model.fundamental_request_body import FundamentalRequestBody
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
    """Complete matrix analysis of FactSet Fundamentals data"""
    
    def __init__(self):
        self.configuration = None
        self.api_client = None
        self.metrics_api = None
        self.fundamentals_api = None
        self.ssl_cert_path = None
        
        # Results storage
        self.all_metrics = {}
        self.bank_metrics_matrix = {}
        
    def setup_ssl_certificate(self) -> Optional[str]:
        """Setup SSL certificate following example pattern"""
        try:
            cert_path = os.getenv('SSL_CERT_PATH', 'certs/rbc-ca-bundle.cer')
            
            if os.path.exists(cert_path):
                with open(cert_path, 'rb') as cert_file:
                    cert_data = cert_file.read()
                
                # Create temporary certificate file
                temp_cert = tempfile.NamedTemporaryFile(mode='wb', suffix='.cer', delete=False)
                temp_cert.write(cert_data)
                temp_cert.close()
                
                # Set environment variables for SSL
                os.environ['REQUESTS_CA_BUNDLE'] = temp_cert.name
                os.environ['SSL_CERT_FILE'] = temp_cert.name
                
                logger.info(f"SSL certificate configured: {temp_cert.name}")
                return temp_cert.name
            else:
                logger.warning(f"SSL certificate not found at: {cert_path}")
                return None
                
        except Exception as e:
            logger.error(f"Error setting up SSL certificate: {e}")
            return None
    
    def setup_proxy_configuration(self) -> Optional[str]:
        """Configure proxy URL for API authentication"""
        try:
            proxy_user = os.getenv('PROXY_USER')
            proxy_password = os.getenv('PROXY_PASSWORD')
            proxy_url = os.getenv('PROXY_URL')
            proxy_domain = os.getenv('PROXY_DOMAIN', 'MAPLE')
            
            if not all([proxy_user, proxy_password, proxy_url]):
                logger.info("Proxy configuration not found, proceeding without proxy")
                return None
            
            # Escape domain and user for NTLM authentication
            escaped_domain = quote(proxy_domain + "\\" + proxy_user)
            quoted_password = quote(proxy_password)
            
            # Construct proxy URL
            proxy_url_formatted = f"http://{escaped_domain}:{quoted_password}@{proxy_url}"
            
            logger.info("Proxy configuration completed successfully")
            return proxy_url_formatted
            
        except Exception as e:
            logger.error(f"Error configuring proxy: {e}")
            return None
    
    def setup_api_client(self):
        """Setup FactSet API client with authentication"""
        try:
            # Setup SSL
            self.ssl_cert_path = self.setup_ssl_certificate()
            
            # Setup proxy
            proxy_url = self.setup_proxy_configuration()
            
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
                
                if response and response.data:
                    for metric in response.data:
                        # Extract metric details
                        metric_id = metric.metric if hasattr(metric, 'metric') else None
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
        """Save complete metrics catalog to JSON"""
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
        """Analyze metrics availability for a single bank"""
        logger.info(f"Analyzing {ticker}: {bank_info['name']}")
        
        bank_metrics = {}
        
        try:
            # Get all metric IDs
            metric_ids = list(self.all_metrics.keys())
            
            # Process in batches to respect API limits
            batch_size = 30
            
            for i in range(0, len(metric_ids), batch_size):
                batch = metric_ids[i:i+batch_size]
                
                try:
                    # Create fiscal period for date range (2018 to present)
                    fiscal_period_obj = FiscalPeriod(
                        start='2018-01-01',
                        end=datetime.now().strftime('%Y-%m-%d')
                    )
                    
                    # Create periodicity object for quarterly data
                    periodicity_obj = Periodicity('QTR')
                    
                    # Create request body
                    request_body = FundamentalRequestBody(
                        ids=[ticker],
                        metrics=batch,
                        fiscal_period=fiscal_period_obj,
                        periodicity=periodicity_obj,
                        currency='LOCAL'
                    )
                    
                    # Create request
                    request = FundamentalsRequest(data=request_body)
                    
                    # Make API call
                    response = self.fundamentals_api.get_fds_fundamentals_for_list(request)
                    
                    if response and response.data:
                        # Process each data point
                        for item in response.data:
                            metric_id = getattr(item, 'metric', None)
                            value = getattr(item, 'value', None)
                            
                            if metric_id and value is not None:
                                if metric_id not in bank_metrics:
                                    bank_metrics[metric_id] = 0
                                bank_metrics[metric_id] += 1
                    
                except Exception as e:
                    logger.debug(f"Batch error for {ticker}: {e}")
                    continue
                
                # Rate limiting
                time.sleep(0.15)
            
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
            time.sleep(0.5)  # Additional delay between banks
    
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
                'Subcategory': metric_info['subcategory'],
                'Data_Type': metric_info['data_type']
            }
            
            # Add columns for each bank
            total_banks = 0
            total_quarters = 0
            
            for ticker in monitored_institutions.keys():
                quarters = self.bank_metrics_matrix.get(ticker, {}).get(metric_id, 0)
                row[f'{ticker}_Quarters'] = quarters
                row[f'{ticker}_Available'] = 'Y' if quarters > 0 else 'N'
                
                if quarters > 0:
                    total_banks += 1
                    total_quarters += quarters
            
            # Summary columns
            row['Total_Banks_With_Data'] = total_banks
            row['Coverage_Percentage'] = f"{(total_banks / len(monitored_institutions) * 100):.1f}%" if len(monitored_institutions) > 0 else "0%"
            row['Avg_Quarters'] = f"{(total_quarters / total_banks):.1f}" if total_banks > 0 else "0"
            
            matrix_data.append(row)
        
        # Create DataFrame
        df = pd.DataFrame(matrix_data)
        
        # Sort by coverage
        df['_sort_key'] = df['Total_Banks_With_Data']
        df = df.sort_values('_sort_key', ascending=False)
        df = df.drop('_sort_key', axis=1)
        
        # Save to Excel
        excel_file = output_dir / f'fundamentals_matrix_{timestamp}.xlsx'
        
        logger.info(f"Writing Excel report to {excel_file}...")
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Sheet 1: Full Matrix with Quarter Counts
            columns_quarters = ['Metric_ID', 'Metric_Name', 'Description', 'Category', 
                               'Total_Banks_With_Data', 'Coverage_Percentage']
            for ticker in monitored_institutions.keys():
                columns_quarters.append(f'{ticker}_Quarters')
            
            df[columns_quarters].to_excel(writer, sheet_name='Metrics_Matrix_Quarters', index=False)
            
            # Sheet 2: Y/N Availability Matrix
            columns_yn = ['Metric_ID', 'Metric_Name', 'Category', 
                         'Total_Banks_With_Data', 'Coverage_Percentage']
            for ticker in monitored_institutions.keys():
                columns_yn.append(f'{ticker}_Available')
            
            df[columns_yn].to_excel(writer, sheet_name='Metrics_Matrix_YN', index=False)
            
            # Sheet 3: Bank Summary
            bank_summary = []
            for ticker, bank_info in monitored_institutions.items():
                metrics = self.bank_metrics_matrix.get(ticker, {})
                bank_summary.append({
                    'Ticker': ticker,
                    'Bank_Name': bank_info['name'],
                    'Type': bank_info['type'],
                    'Total_Metrics_Available': len(metrics),
                    'Total_Data_Points': sum(metrics.values()),
                    'Avg_Quarters_Per_Metric': f"{sum(metrics.values()) / len(metrics):.1f}" if metrics else "0"
                })
            
            bank_df = pd.DataFrame(bank_summary)
            bank_df = bank_df.sort_values('Total_Metrics_Available', ascending=False)
            bank_df.to_excel(writer, sheet_name='Bank_Summary', index=False)
            
            # Sheet 4: Top 100 Metrics by Coverage
            top_metrics = df.nlargest(100, 'Total_Banks_With_Data')[
                ['Metric_ID', 'Metric_Name', 'Description', 'Category', 
                 'Total_Banks_With_Data', 'Coverage_Percentage', 'Avg_Quarters']
            ]
            top_metrics.to_excel(writer, sheet_name='Top_100_Metrics', index=False)
        
        logger.info(f"Excel report saved to {excel_file}")
        
        # Generate summary
        self.generate_summary(output_dir)
    
    def generate_summary(self, output_dir: Path):
        """Generate text summary"""
        summary_file = output_dir / 'ANALYSIS_SUMMARY.txt'
        
        total_banks = len(monitored_institutions)
        banks_with_data = sum(1 for m in self.bank_metrics_matrix.values() if m)
        
        summary = []
        summary.append('=' * 80)
        summary.append('FACTSET FUNDAMENTALS MATRIX ANALYSIS SUMMARY')
        summary.append(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        summary.append('=' * 80)
        summary.append('')
        summary.append('OVERVIEW')
        summary.append('-' * 40)
        summary.append(f'Total Metrics in API: {len(self.all_metrics)}')
        summary.append(f'Total Banks Analyzed: {total_banks}')
        summary.append(f'Banks with Data: {banks_with_data}')
        summary.append(f'Date Range: 2018-01-01 to {datetime.now().strftime("%Y-%m-%d")} (Quarterly)')
        summary.append('')
        
        # Top banks by metric availability
        bank_stats = [(t, len(m)) for t, m in self.bank_metrics_matrix.items() if m]
        bank_stats.sort(key=lambda x: x[1], reverse=True)
        
        summary.append('TOP 10 BANKS BY METRIC AVAILABILITY:')
        summary.append('-' * 40)
        for ticker, metric_count in bank_stats[:10]:
            name = monitored_institutions[ticker]['name']
            summary.append(f'{ticker:10} {name[:40]:40} Metrics: {metric_count}')
        
        summary.append('')
        summary.append('METRICS COVERAGE STATISTICS:')
        summary.append('-' * 40)
        
        # Calculate coverage stats
        metrics_with_data = sum(1 for m in self.all_metrics.keys() 
                               if any(self.bank_metrics_matrix.get(t, {}).get(m, 0) > 0 
                                     for t in monitored_institutions.keys()))
        
        summary.append(f'Metrics with at least 1 bank: {metrics_with_data}')
        summary.append(f'Metrics with no data: {len(self.all_metrics) - metrics_with_data}')
        
        # Write summary
        summary_text = '\n'.join(summary)
        
        with open(summary_file, 'w') as f:
            f.write(summary_text)
        
        print('\n' + summary_text)
        logger.info(f"Summary saved to {summary_file}")
    
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
            
            # Step 1: Setup API client
            self.setup_api_client()
            
            # Step 2: Fetch all metrics
            self.fetch_all_metrics()
            
            # Step 3: Analyze all banks
            self.analyze_all_banks()
            
            # Step 4: Generate reports
            self.generate_matrix_report()
            
            logger.info("=" * 60)
            logger.info("Analysis completed successfully!")
            logger.info("Check the 'output' folder for results:")
            logger.info("  - fundamentals_matrix_*.xlsx - Complete matrix with all metrics vs all banks")
            logger.info("  - factset_metrics_catalog.json - All available metrics with descriptions")
            logger.info("  - ANALYSIS_SUMMARY.txt - Summary statistics")
            
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