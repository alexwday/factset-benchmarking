"""
FactSet Fundamentals API Analysis Script - Matrix View
Creates a comprehensive matrix of all metrics vs all banks with availability counts
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
        logging.FileHandler('fundamentals_matrix_analysis.log')
    ]
)
logger = logging.getLogger(__name__)


class FundamentalsMatrixAnalyzer:
    """Analyzes FactSet Fundamentals data in a matrix format"""
    
    def __init__(self):
        self.api_configuration = None
        self.metrics_api_instance = None
        self.fundamentals_api_instance = None
        self.ssl_cert_path = None
        self.all_metrics = {}  # Dict of metric_id: metric_info
        self.bank_metrics_matrix = {}  # Dict of ticker: {metric_id: quarter_count}
        self.complete_matrix_df = None
        
    def setup_ssl_certificate(self) -> Optional[str]:
        """Setup SSL certificate from file or environment"""
        try:
            ssl_cert_path = os.getenv('SSL_CERT_PATH', 'certs/rbc-ca-bundle.cer')
            
            if os.path.exists(ssl_cert_path):
                with open(ssl_cert_path, 'rb') as cert_file:
                    cert_data = cert_file.read()
                    
                temp_cert = tempfile.NamedTemporaryFile(mode='wb', suffix='.cer', delete=False)
                temp_cert.write(cert_data)
                temp_cert.close()
                
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
            
            escaped_domain = quote(f"{proxy_domain}\\{proxy_user}")
            quoted_password = quote(proxy_password)
            proxy_url_formatted = f"http://{escaped_domain}:{quoted_password}@{proxy_url}"
            
            logger.info("Proxy configuration completed")
            return proxy_url_formatted
            
        except Exception as e:
            logger.error(f"Error configuring proxy: {e}")
            return None
    
    def setup_api_client(self):
        """Setup FactSet API client with authentication"""
        try:
            self.ssl_cert_path = self.setup_ssl_certificate()
            proxy_url = self.setup_proxy_configuration()
            
            self.api_configuration = fds.sdk.FactSetFundamentals.Configuration(
                username=os.getenv('API_USERNAME'),
                password=os.getenv('API_PASSWORD')
            )
            
            if proxy_url:
                self.api_configuration.proxy = proxy_url
                
            if self.ssl_cert_path:
                self.api_configuration.ssl_ca_cert = self.ssl_cert_path
            
            api_client = fds.sdk.FactSetFundamentals.ApiClient(self.api_configuration)
            
            self.metrics_api_instance = metrics_api.MetricsApi(api_client)
            self.fundamentals_api_instance = factset_fundamentals_api.FactSetFundamentalsApi(api_client)
            
            logger.info("FactSet API client configured successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup API client: {e}")
            raise
    
    def fetch_all_metrics(self):
        """Fetch ALL available FactSet Fundamentals metrics"""
        logger.info("Fetching all available metrics...")
        
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
                logger.info(f"Fetching metrics for category: {category}")
                
                # Get all subcategories for this category
                api_response = self.metrics_api_instance.get_fds_fundamentals_metrics(
                    category=category
                )
                
                if api_response and api_response.data:
                    for metric in api_response.data:
                        metric_dict = metric.to_dict() if hasattr(metric, 'to_dict') else metric
                        
                        # Store metric with all its details
                        metric_id = metric_dict.get('metric', metric_dict.get('id', ''))
                        if metric_id:
                            self.all_metrics[metric_id] = {
                                'metric_id': metric_id,
                                'name': metric_dict.get('name', ''),
                                'description': metric_dict.get('description', ''),
                                'category': category,
                                'subcategory': metric_dict.get('subcategory', ''),
                                'data_type': metric_dict.get('data_type', metric_dict.get('dataType', '')),
                                'unit': metric_dict.get('unit', ''),
                                'scale': metric_dict.get('scale', '')
                            }
                
                time.sleep(0.15)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error fetching metrics for {category}: {e}")
        
        logger.info(f"Total unique metrics fetched: {len(self.all_metrics)}")
    
    def analyze_bank_metrics(self, ticker: str) -> Dict[str, int]:
        """Get all available metrics for a bank with quarter counts"""
        metrics_availability = {}
        
        try:
            # Get list of all metric IDs to check (in batches)
            all_metric_ids = list(self.all_metrics.keys())
            batch_size = 50  # Process metrics in batches
            
            for i in range(0, len(all_metric_ids), batch_size):
                batch_metrics = all_metric_ids[i:i+batch_size]
                
                try:
                    # Create request for this batch of metrics
                    request_body = fundamental_request_body.FundamentalRequestBody(
                        ids=[ticker],
                        metrics=batch_metrics,
                        start_date='2018-01-01',
                        end_date=datetime.now().strftime('%Y-%m-%d'),
                        frequency='QTR'
                    )
                    
                    request = fundamentals_request.FundamentalsRequest(data=request_body)
                    api_response = self.fundamentals_api_instance.get_fds_fundamentals_for_list(request)
                    
                    if api_response and api_response.data:
                        # Process response to count quarters per metric
                        for item in api_response.data:
                            item_dict = item.to_dict() if hasattr(item, 'to_dict') else item
                            metric_id = item_dict.get('metric', '')
                            
                            if metric_id and item_dict.get('value') is not None:
                                if metric_id not in metrics_availability:
                                    metrics_availability[metric_id] = 0
                                metrics_availability[metric_id] += 1
                    
                    time.sleep(0.15)  # Rate limiting
                    
                except Exception as e:
                    logger.debug(f"Error fetching batch metrics for {ticker}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error analyzing metrics for {ticker}: {e}")
        
        return metrics_availability
    
    def analyze_all_banks(self):
        """Analyze all banks and build the complete matrix"""
        logger.info(f"Analyzing metrics availability for {len(monitored_institutions)} banks...")
        
        # Analyze each bank
        for ticker, bank_info in tqdm(monitored_institutions.items(), desc="Analyzing banks"):
            logger.info(f"Analyzing {ticker}: {bank_info['name']}")
            
            metrics_availability = self.analyze_bank_metrics(ticker)
            self.bank_metrics_matrix[ticker] = metrics_availability
            
            # Log progress
            if metrics_availability:
                logger.info(f"  Found {len(metrics_availability)} metrics with data for {ticker}")
            else:
                logger.warning(f"  No data found for {ticker}")
    
    def build_complete_matrix(self):
        """Build the complete matrix dataframe"""
        logger.info("Building complete matrix...")
        
        # Create matrix structure
        matrix_data = []
        
        for metric_id, metric_info in self.all_metrics.items():
            row = {
                'Metric_ID': metric_id,
                'Metric_Name': metric_info['name'],
                'Description': metric_info['description'],
                'Category': metric_info['category'],
                'Subcategory': metric_info['subcategory'],
                'Data_Type': metric_info['data_type'],
                'Unit': metric_info['unit']
            }
            
            # Add availability for each bank
            total_banks_with_metric = 0
            total_quarters_available = 0
            
            for ticker in monitored_institutions.keys():
                bank_metrics = self.bank_metrics_matrix.get(ticker, {})
                quarters_available = bank_metrics.get(metric_id, 0)
                
                # Add column for this bank showing quarter count
                row[f"{ticker}_Quarters"] = quarters_available
                
                # Add binary indicator if metric is available
                row[f"{ticker}_Available"] = 'Y' if quarters_available > 0 else 'N'
                
                if quarters_available > 0:
                    total_banks_with_metric += 1
                    total_quarters_available += quarters_available
            
            # Add summary columns
            row['Total_Banks_With_Data'] = total_banks_with_metric
            row['Coverage_Percentage'] = f"{(total_banks_with_metric / len(monitored_institutions) * 100):.1f}%"
            row['Avg_Quarters_When_Available'] = (
                f"{(total_quarters_available / total_banks_with_metric):.1f}" 
                if total_banks_with_metric > 0 else "0"
            )
            
            matrix_data.append(row)
        
        # Create DataFrame
        self.complete_matrix_df = pd.DataFrame(matrix_data)
        
        # Sort by coverage percentage (most available metrics first)
        self.complete_matrix_df['Coverage_Numeric'] = self.complete_matrix_df['Total_Banks_With_Data']
        self.complete_matrix_df = self.complete_matrix_df.sort_values('Coverage_Numeric', ascending=False)
        self.complete_matrix_df = self.complete_matrix_df.drop('Coverage_Numeric', axis=1)
        
        logger.info(f"Matrix built with {len(self.complete_matrix_df)} metrics x {len(monitored_institutions)} banks")
    
    def generate_reports(self):
        """Generate comprehensive reports"""
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 1. Generate Full Matrix Excel
        self.generate_matrix_excel(output_dir, timestamp)
        
        # 2. Generate Summary Excel
        self.generate_summary_excel(output_dir, timestamp)
        
        # 3. Generate JSON Report
        self.generate_json_report(output_dir, timestamp)
        
        # 4. Generate Text Summary
        self.generate_text_summary(output_dir)
    
    def generate_matrix_excel(self, output_dir: Path, timestamp: str):
        """Generate comprehensive matrix Excel file"""
        excel_file = output_dir / f'fundamentals_matrix_{timestamp}.xlsx'
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Sheet 1: Full Matrix (all metrics, showing quarter counts)
            logger.info("Writing full matrix to Excel...")
            
            # Select columns for the main matrix view
            matrix_columns = ['Metric_ID', 'Metric_Name', 'Description', 'Category', 
                            'Total_Banks_With_Data', 'Coverage_Percentage']
            
            # Add quarter count columns for each bank
            for ticker in monitored_institutions.keys():
                matrix_columns.append(f"{ticker}_Quarters")
            
            matrix_df = self.complete_matrix_df[matrix_columns].copy()
            matrix_df.to_excel(writer, sheet_name='Metrics_Matrix_Quarters', index=False)
            
            # Sheet 2: Availability Matrix (Y/N view)
            availability_columns = ['Metric_ID', 'Metric_Name', 'Category', 
                                   'Total_Banks_With_Data', 'Coverage_Percentage']
            
            # Add Y/N columns for each bank
            for ticker in monitored_institutions.keys():
                availability_columns.append(f"{ticker}_Available")
            
            availability_df = self.complete_matrix_df[availability_columns].copy()
            availability_df.to_excel(writer, sheet_name='Metrics_Matrix_YN', index=False)
            
            # Sheet 3: Bank Summary
            bank_summary = []
            for ticker, bank_info in monitored_institutions.items():
                bank_metrics = self.bank_metrics_matrix.get(ticker, {})
                bank_summary.append({
                    'Ticker': ticker,
                    'Bank_Name': bank_info['name'],
                    'Type': bank_info['type'],
                    'Total_Metrics_Available': len(bank_metrics),
                    'Total_Data_Points': sum(bank_metrics.values()),
                    'Avg_Quarters_Per_Metric': (
                        f"{sum(bank_metrics.values()) / len(bank_metrics):.1f}" 
                        if bank_metrics else "0"
                    )
                })
            
            bank_summary_df = pd.DataFrame(bank_summary)
            bank_summary_df = bank_summary_df.sort_values('Total_Metrics_Available', ascending=False)
            bank_summary_df.to_excel(writer, sheet_name='Bank_Summary', index=False)
            
            # Sheet 4: Top Metrics by Coverage
            top_metrics = self.complete_matrix_df.nlargest(100, 'Total_Banks_With_Data')[
                ['Metric_ID', 'Metric_Name', 'Description', 'Category', 
                 'Total_Banks_With_Data', 'Coverage_Percentage', 'Avg_Quarters_When_Available']
            ]
            top_metrics.to_excel(writer, sheet_name='Top_100_Metrics', index=False)
        
        logger.info(f"Matrix Excel saved to {excel_file}")
    
    def generate_summary_excel(self, output_dir: Path, timestamp: str):
        """Generate a focused summary Excel file"""
        excel_file = output_dir / f'fundamentals_summary_{timestamp}.xlsx'
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Coverage by Category
            category_coverage = []
            for category in self.complete_matrix_df['Category'].unique():
                cat_metrics = self.complete_matrix_df[self.complete_matrix_df['Category'] == category]
                metrics_with_data = cat_metrics[cat_metrics['Total_Banks_With_Data'] > 0]
                
                category_coverage.append({
                    'Category': category,
                    'Total_Metrics': len(cat_metrics),
                    'Metrics_With_Data': len(metrics_with_data),
                    'Avg_Bank_Coverage': f"{cat_metrics['Total_Banks_With_Data'].mean():.1f}",
                    'Max_Bank_Coverage': cat_metrics['Total_Banks_With_Data'].max()
                })
            
            category_df = pd.DataFrame(category_coverage)
            category_df.to_excel(writer, sheet_name='Category_Summary', index=False)
            
            # Coverage by Institution Type
            institution_coverage = []
            for inst_type in set(bank['type'] for bank in monitored_institutions.values()):
                banks_of_type = [t for t, b in monitored_institutions.items() if b['type'] == inst_type]
                
                total_metrics = 0
                for ticker in banks_of_type:
                    total_metrics += len(self.bank_metrics_matrix.get(ticker, {}))
                
                institution_coverage.append({
                    'Institution_Type': inst_type,
                    'Number_of_Banks': len(banks_of_type),
                    'Avg_Metrics_Per_Bank': f"{total_metrics / len(banks_of_type):.1f}" if banks_of_type else "0"
                })
            
            inst_df = pd.DataFrame(institution_coverage)
            inst_df.to_excel(writer, sheet_name='Institution_Type_Summary', index=False)
        
        logger.info(f"Summary Excel saved to {excel_file}")
    
    def generate_json_report(self, output_dir: Path, timestamp: str):
        """Generate detailed JSON report"""
        report = {
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_metrics_in_api': len(self.all_metrics),
                'total_banks_analyzed': len(monitored_institutions),
                'metrics_with_any_data': len(self.complete_matrix_df[self.complete_matrix_df['Total_Banks_With_Data'] > 0]),
                'date_range': '2018-01-01 to ' + datetime.now().strftime('%Y-%m-%d')
            },
            'metrics_catalog': self.all_metrics,
            'bank_metrics_availability': self.bank_metrics_matrix,
            'top_metrics': self.complete_matrix_df.nlargest(50, 'Total_Banks_With_Data')[
                ['Metric_ID', 'Metric_Name', 'Total_Banks_With_Data', 'Coverage_Percentage']
            ].to_dict('records')
        }
        
        json_file = output_dir / f'fundamentals_analysis_{timestamp}.json'
        with open(json_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"JSON report saved to {json_file}")
    
    def generate_text_summary(self, output_dir: Path):
        """Generate human-readable text summary"""
        summary_file = output_dir / 'MATRIX_ANALYSIS_SUMMARY.txt'
        
        total_metrics = len(self.all_metrics)
        metrics_with_data = len(self.complete_matrix_df[self.complete_matrix_df['Total_Banks_With_Data'] > 0])
        
        summary = []
        summary.append('=' * 80)
        summary.append('FACTSET FUNDAMENTALS MATRIX ANALYSIS')
        summary.append(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        summary.append('=' * 80)
        summary.append('')
        summary.append('OVERVIEW')
        summary.append('-' * 40)
        summary.append(f'Total Metrics in API: {total_metrics}')
        summary.append(f'Metrics with Data for at Least 1 Bank: {metrics_with_data}')
        summary.append(f'Total Banks Analyzed: {len(monitored_institutions)}')
        summary.append(f'Date Range: 2018-01-01 to {datetime.now().strftime("%Y-%m-%d")} (Quarterly)')
        summary.append('')
        
        summary.append('TOP 20 METRICS BY COVERAGE')
        summary.append('-' * 40)
        top_metrics = self.complete_matrix_df.nlargest(20, 'Total_Banks_With_Data')
        for _, row in top_metrics.iterrows():
            summary.append(f"{row['Metric_ID']:20} {row['Metric_Name'][:40]:40} Banks: {row['Total_Banks_With_Data']:3}/{len(monitored_institutions)}")
        
        summary.append('')
        summary.append('BANKS BY DATA AVAILABILITY')
        summary.append('-' * 40)
        
        bank_metrics_count = [(t, len(m)) for t, m in self.bank_metrics_matrix.items()]
        bank_metrics_count.sort(key=lambda x: x[1], reverse=True)
        
        for ticker, metric_count in bank_metrics_count[:10]:
            bank_name = monitored_institutions[ticker]['name']
            summary.append(f"{ticker:10} {bank_name[:40]:40} Metrics: {metric_count}")
        
        summary_text = '\n'.join(summary)
        with open(summary_file, 'w') as f:
            f.write(summary_text)
        
        print('\n' + summary_text)
        
        logger.info(f"Text summary saved to {summary_file}")
    
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
            logger.info("Starting FactSet Fundamentals Matrix Analysis...")
            
            # Setup API client
            self.setup_api_client()
            
            # Step 1: Fetch all available metrics
            self.fetch_all_metrics()
            
            # Step 2: Analyze each bank for all metrics
            self.analyze_all_banks()
            
            # Step 3: Build the complete matrix
            self.build_complete_matrix()
            
            # Step 4: Generate reports
            self.generate_reports()
            
            logger.info("Matrix analysis completed successfully!")
            
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