"""
FactSet Fundamentals API Analysis Script
Analyzes data availability for 91 monitored financial institutions
"""

import os
import json
import tempfile
import logging
from datetime import datetime, timedelta
from urllib.parse import quote
from typing import Dict, Any, List, Optional
import time

import pandas as pd
import numpy as np
from dotenv import load_dotenv
import fds.sdk.FactSetFundamentals
from fds.sdk.FactSetFundamentals.api import metrics_api, factset_fundamentals_api
from fds.sdk.FactSetFundamentals.models import fundamentals_request, fundamental_request_body
from pathlib import Path
from tqdm import tqdm

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


class FundamentalsAnalyzer:
    """Main class for analyzing FactSet Fundamentals data"""
    
    def __init__(self):
        self.api_configuration = None
        self.metrics_api_instance = None
        self.fundamentals_api_instance = None
        self.ssl_cert_path = None
        self.all_metrics = []
        self.bank_results = {}
        self.coverage_stats = {}
        
        # Key metrics to analyze for banks
        self.key_metrics = [
            'FF_SALES',      # Revenue
            'FF_NET_INC',    # Net Income  
            'FF_EPS',        # Earnings per share
            'FF_ASSETS',     # Total Assets
            'FF_LIAB',       # Total Liabilities
            'FF_EQUITY',     # Total Equity
            'FF_ROE',        # Return on Equity
            'FF_ROA',        # Return on Assets
            'FF_BPS',        # Book Value per Share
            'FF_DIV_YLD',    # Dividend Yield
            'FF_NIM',        # Net Interest Margin (for banks)
            'FF_TIER1_RATIO' # Tier 1 Capital Ratio (for banks)
        ]
        
    def setup_ssl_certificate(self) -> Optional[str]:
        """Setup SSL certificate from file or environment"""
        try:
            ssl_cert_path = os.getenv('SSL_CERT_PATH', 'certs/rbc-ca-bundle.cer')
            
            if os.path.exists(ssl_cert_path):
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
                
                logger.info(f"SSL certificate configured: {temp_cert.name}")
                return temp_cert.name
            else:
                logger.warning(f"SSL certificate not found at: {ssl_cert_path}")
                return None
                
        except Exception as e:
            logger.error(f"Error setting up SSL certificate: {e}")
            return None
    
    def setup_proxy_configuration(self) -> str:
        """Configure proxy URL for API authentication"""
        try:
            proxy_user = os.getenv('PROXY_USER')
            proxy_password = os.getenv('PROXY_PASSWORD')
            proxy_url = os.getenv('PROXY_URL')
            proxy_domain = os.getenv('PROXY_DOMAIN', 'MAPLE')
            
            if not all([proxy_user, proxy_password, proxy_url]):
                logger.info("Proxy configuration not found, skipping proxy setup")
                return None
            
            # Escape domain and user for NTLM authentication
            escaped_domain = quote(f"{proxy_domain}\\{proxy_user}")
            quoted_password = quote(proxy_password)
            
            # Construct proxy URL
            proxy_url_formatted = f"http://{escaped_domain}:{quoted_password}@{proxy_url}"
            
            logger.info("Proxy configuration completed")
            return proxy_url_formatted
            
        except Exception as e:
            logger.error(f"Error configuring proxy: {e}")
            return None
    
    def setup_api_client(self):
        """Setup FactSet API client with authentication"""
        try:
            # Setup SSL certificate
            self.ssl_cert_path = self.setup_ssl_certificate()
            
            # Setup proxy
            proxy_url = self.setup_proxy_configuration()
            
            # Configure FactSet API
            self.api_configuration = fds.sdk.FactSetFundamentals.Configuration(
                username=os.getenv('API_USERNAME'),
                password=os.getenv('API_PASSWORD')
            )
            
            if proxy_url:
                self.api_configuration.proxy = proxy_url
                
            if self.ssl_cert_path:
                self.api_configuration.ssl_ca_cert = self.ssl_cert_path
            
            # Create API client
            api_client = fds.sdk.FactSetFundamentals.ApiClient(self.api_configuration)
            
            # Initialize API instances
            self.metrics_api_instance = metrics_api.MetricsApi(api_client)
            self.fundamentals_api_instance = factset_fundamentals_api.FactSetFundamentalsApi(api_client)
            
            logger.info("FactSet API client configured successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup API client: {e}")
            raise
    
    def fetch_all_metrics(self):
        """Fetch all available FactSet Fundamentals metrics"""
        logger.info("Fetching all available metrics...")
        
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
        
        all_metrics = []
        
        for category in categories:
            try:
                logger.info(f"Fetching metrics for category: {category}")
                
                # API call to get metrics
                api_response = self.metrics_api_instance.get_fds_fundamentals_metrics(
                    category=category
                )
                
                if api_response and api_response.data:
                    for metric in api_response.data:
                        metric_dict = metric.to_dict() if hasattr(metric, 'to_dict') else metric
                        metric_dict['category'] = category
                        all_metrics.append(metric_dict)
                
                # Rate limiting
                time.sleep(0.15)
                
            except Exception as e:
                logger.error(f"Error fetching metrics for {category}: {e}")
        
        self.all_metrics = all_metrics
        logger.info(f"Total metrics fetched: {len(all_metrics)}")
        
        # Save metrics to file
        self.save_metrics_catalog()
    
    def save_metrics_catalog(self):
        """Save metrics catalog to JSON file"""
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        metrics_file = output_dir / 'factset_metrics_catalog.json'
        
        metrics_data = {
            'generated_at': datetime.now().isoformat(),
            'total_metrics': len(self.all_metrics),
            'by_category': {},
            'metrics': self.all_metrics
        }
        
        # Group by category
        for metric in self.all_metrics:
            category = metric.get('category', 'UNKNOWN')
            if category not in metrics_data['by_category']:
                metrics_data['by_category'][category] = []
            metrics_data['by_category'][category].append(metric)
        
        with open(metrics_file, 'w') as f:
            json.dump(metrics_data, f, indent=2, default=str)
        
        logger.info(f"Metrics catalog saved to {metrics_file}")
    
    def analyze_bank(self, ticker: str, bank_info: Dict) -> Dict:
        """Analyze fundamentals data for a single bank"""
        result = {
            'ticker': ticker,
            'name': bank_info['name'],
            'type': bank_info['type'],
            'data_available': False,
            'date_range': None,
            'latest_data': None,
            'available_metrics': [],
            'quarterly_data_points': 0,
            'error': None
        }
        
        try:
            # Create request for fundamentals data
            request_body = fundamental_request_body.FundamentalRequestBody(
                ids=[ticker],
                metrics=self.key_metrics,
                start_date='2018-01-01',
                end_date=datetime.now().strftime('%Y-%m-%d'),
                frequency='QTR'  # Quarterly data
            )
            
            request = fundamentals_request.FundamentalsRequest(data=request_body)
            
            # Make API call
            api_response = self.fundamentals_api_instance.get_fds_fundamentals_for_list(request)
            
            if api_response and api_response.data:
                result['data_available'] = True
                
                # Convert response to dataframe for analysis
                data_points = []
                for item in api_response.data:
                    if hasattr(item, 'to_dict'):
                        data_points.append(item.to_dict())
                    else:
                        data_points.append(item)
                
                if data_points:
                    df = pd.DataFrame(data_points)
                    
                    # Analyze date range
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
                        result['date_range'] = {
                            'start': df['date'].min().strftime('%Y-%m-%d'),
                            'end': df['date'].max().strftime('%Y-%m-%d')
                        }
                        
                        # Get latest quarter data
                        latest_date = df['date'].max()
                        latest_data = df[df['date'] == latest_date].to_dict('records')
                        result['latest_data'] = latest_data
                    
                    # Count available metrics
                    if 'metric' in df.columns:
                        result['available_metrics'] = df['metric'].unique().tolist()
                    
                    result['quarterly_data_points'] = len(df)
            
        except Exception as e:
            result['error'] = str(e)
            logger.warning(f"Failed to fetch data for {ticker}: {e}")
        
        return result
    
    def analyze_all_banks(self):
        """Analyze all monitored banks"""
        logger.info(f"Analyzing {len(monitored_institutions)} banks...")
        
        # Process banks with progress bar
        for ticker, bank_info in tqdm(monitored_institutions.items(), desc="Analyzing banks"):
            result = self.analyze_bank(ticker, bank_info)
            self.bank_results[ticker] = result
            
            # Update coverage statistics
            inst_type = bank_info['type']
            if inst_type not in self.coverage_stats:
                self.coverage_stats[inst_type] = {
                    'total': 0,
                    'with_data': 0,
                    'banks': []
                }
            
            self.coverage_stats[inst_type]['total'] += 1
            if result['data_available']:
                self.coverage_stats[inst_type]['with_data'] += 1
            self.coverage_stats[inst_type]['banks'].append(ticker)
            
            # Rate limiting
            time.sleep(0.15)
    
    def generate_reports(self):
        """Generate analysis reports"""
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 1. Generate JSON report
        self.generate_json_report(output_dir, timestamp)
        
        # 2. Generate Excel report
        self.generate_excel_report(output_dir, timestamp)
        
        # 3. Generate summary text report
        self.generate_summary_report(output_dir)
    
    def generate_json_report(self, output_dir: Path, timestamp: str):
        """Generate comprehensive JSON report"""
        report = {
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_banks_analyzed': len(self.bank_results),
                'banks_with_data': sum(1 for b in self.bank_results.values() if b['data_available']),
                'total_metrics_available': len(self.all_metrics),
                'date_range_analyzed': '2018-01-01 to ' + datetime.now().strftime('%Y-%m-%d')
            },
            'coverage_by_type': self.coverage_stats,
            'bank_details': self.bank_results
        }
        
        report_file = output_dir / f'fundamentals_analysis_{timestamp}.json'
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"JSON report saved to {report_file}")
    
    def generate_excel_report(self, output_dir: Path, timestamp: str):
        """Generate Excel report with multiple sheets"""
        excel_file = output_dir / f'fundamentals_analysis_{timestamp}.xlsx'
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Sheet 1: Coverage Summary
            coverage_data = []
            for inst_type, stats in self.coverage_stats.items():
                coverage_data.append({
                    'Institution Type': inst_type,
                    'Total Banks': stats['total'],
                    'Banks with Data': stats['with_data'],
                    'Coverage %': f"{(stats['with_data']/stats['total']*100):.1f}%"
                })
            
            df_coverage = pd.DataFrame(coverage_data)
            df_coverage.to_excel(writer, sheet_name='Coverage Summary', index=False)
            
            # Sheet 2: Bank Details
            bank_data = []
            for ticker, result in self.bank_results.items():
                bank_data.append({
                    'Ticker': ticker,
                    'Name': result['name'],
                    'Type': result['type'],
                    'Data Available': 'Yes' if result['data_available'] else 'No',
                    'Date Range Start': result['date_range']['start'] if result['date_range'] else 'N/A',
                    'Date Range End': result['date_range']['end'] if result['date_range'] else 'N/A',
                    'Quarterly Data Points': result['quarterly_data_points'],
                    'Available Metrics': len(result['available_metrics'])
                })
            
            df_banks = pd.DataFrame(bank_data)
            df_banks.to_excel(writer, sheet_name='Bank Details', index=False)
            
            # Sheet 3: Metrics Catalog (first 1000)
            if self.all_metrics:
                metrics_data = []
                for metric in self.all_metrics[:1000]:
                    metrics_data.append({
                        'Metric ID': metric.get('metric', ''),
                        'Name': metric.get('name', ''),
                        'Description': metric.get('description', ''),
                        'Category': metric.get('category', ''),
                        'Data Type': metric.get('data_type', '')
                    })
                
                df_metrics = pd.DataFrame(metrics_data)
                df_metrics.to_excel(writer, sheet_name='Metrics Catalog', index=False)
        
        logger.info(f"Excel report saved to {excel_file}")
    
    def generate_summary_report(self, output_dir: Path):
        """Generate text summary report"""
        summary_file = output_dir / 'ANALYSIS_SUMMARY.txt'
        
        total_banks = len(self.bank_results)
        banks_with_data = sum(1 for b in self.bank_results.values() if b['data_available'])
        coverage_rate = (banks_with_data / total_banks * 100) if total_banks > 0 else 0
        
        summary = []
        summary.append('=' * 80)
        summary.append('FACTSET FUNDAMENTALS API ANALYSIS SUMMARY')
        summary.append(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        summary.append('=' * 80)
        summary.append('')
        summary.append('OVERVIEW')
        summary.append('-' * 40)
        summary.append(f'Total Banks Analyzed: {total_banks}')
        summary.append(f'Banks with Data Available: {banks_with_data}')
        summary.append(f'Coverage Rate: {coverage_rate:.1f}%')
        summary.append(f'Total Metrics Available: {len(self.all_metrics)}')
        summary.append(f'Date Range Analyzed: 2018-01-01 to {datetime.now().strftime("%Y-%m-%d")}')
        summary.append('')
        summary.append('COVERAGE BY INSTITUTION TYPE')
        summary.append('-' * 40)
        
        for inst_type, stats in self.coverage_stats.items():
            coverage_pct = (stats['with_data']/stats['total']*100) if stats['total'] > 0 else 0
            summary.append(f"{inst_type}: {stats['with_data']}/{stats['total']} ({coverage_pct:.1f}%)")
        
        summary.append('')
        summary.append('BANKS WITHOUT DATA')
        summary.append('-' * 40)
        
        banks_without_data = [
            f"- {ticker}: {result['name']}"
            for ticker, result in self.bank_results.items()
            if not result['data_available']
        ]
        
        if banks_without_data:
            summary.extend(banks_without_data)
        else:
            summary.append('All banks have data available!')
        
        # Write summary
        summary_text = '\n'.join(summary)
        with open(summary_file, 'w') as f:
            f.write(summary_text)
        
        # Also print to console
        print('\n' + summary_text)
        
        logger.info(f"Summary report saved to {summary_file}")
    
    def cleanup(self):
        """Clean up temporary files"""
        if self.ssl_cert_path and os.path.exists(self.ssl_cert_path):
            try:
                os.unlink(self.ssl_cert_path)
                logger.info("Cleaned up temporary SSL certificate")
            except Exception as e:
                logger.warning(f"Failed to clean up SSL certificate: {e}")
    
    def run(self):
        """Main execution method"""
        try:
            logger.info("Starting FactSet Fundamentals analysis...")
            
            # Setup API client
            self.setup_api_client()
            
            # Fetch all available metrics
            self.fetch_all_metrics()
            
            # Analyze all banks
            self.analyze_all_banks()
            
            # Generate reports
            self.generate_reports()
            
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