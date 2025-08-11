#!/usr/bin/env python3
"""
FactSet Fundamentals Metrics Availability Checker for RBC
This script fetches all available metrics from the FactSet Fundamentals API
and checks which metrics have data available for Royal Bank of Canada (RY-CA).
Outputs results to both CSV and HTML for easy analysis.
"""

import os
import sys
import json
import time
import tempfile
import logging
from datetime import datetime, timedelta
from urllib.parse import quote
from typing import Dict, List, Optional, Any, Tuple
import warnings

import pandas as pd
import fds.sdk.FactSetFundamentals
from fds.sdk.FactSetFundamentals.api import metrics_api, fact_set_fundamentals_api
from fds.sdk.FactSetFundamentals.models import *
from fds.sdk.FactSetFundamentals.model.ids_batch_max30000 import IdsBatchMax30000
from fds.sdk.FactSetFundamentals.model.metrics import Metrics
from fds.sdk.FactSetFundamentals.model.periodicity import Periodicity
from fds.sdk.FactSetFundamentals.model.update_type import UpdateType
from fds.sdk.FactSetFundamentals.model.fiscal_period import FiscalPeriod
from fds.sdk.FactSetFundamentals.model.batch import Batch
from fds.sdk.FactSetFundamentals.model.fundamental_request_body import FundamentalRequestBody
from fds.sdk.FactSetFundamentals.model.fundamentals_request import FundamentalsRequest
from dotenv import load_dotenv

# Suppress warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'rbc_metrics_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# RBC ticker
RBC_TICKER = "RY-CA"

# Validate environment variables
def validate_env_vars():
    """Validate required environment variables."""
    required_vars = [
        'API_USERNAME', 'API_PASSWORD', 
        'PROXY_USER', 'PROXY_PASSWORD', 'PROXY_URL',
        'PROXY_DOMAIN'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)
    
    logger.info("✅ All required environment variables present")

def setup_ssl_certificate() -> Optional[str]:
    """Setup SSL certificate for API connection."""
    # Check for certificate in input folder first
    input_cert_path = "input/rbc-ca-bundle.cer"
    
    if os.path.exists(input_cert_path):
        logger.info(f"✅ Using SSL certificate from input folder: {input_cert_path}")
        return input_cert_path
    
    # Check for certificate in current directory
    local_cert_path = "rbc-ca-bundle.cer"
    if os.path.exists(local_cert_path):
        logger.info(f"✅ Using SSL certificate from current directory: {local_cert_path}")
        return local_cert_path
    
    # Check environment variable for custom path
    env_cert_path = os.getenv('SSL_CERT_PATH')
    if env_cert_path and os.path.exists(env_cert_path):
        logger.info(f"✅ Using SSL certificate from environment variable: {env_cert_path}")
        return env_cert_path
    
    # Certificate not found
    logger.error("❌ SSL certificate not found!")
    logger.info("Please place the certificate in one of these locations:")
    logger.info("  1. input/rbc-ca-bundle.cer (recommended)")
    logger.info("  2. ./rbc-ca-bundle.cer (current directory)")
    logger.info("  3. Set SSL_CERT_PATH environment variable to certificate path")
    return None

def setup_api_configuration(ssl_cert_path: Optional[str]):
    """Setup FactSet API configuration with proxy and SSL."""
    api_username = os.getenv('API_USERNAME')
    api_password = os.getenv('API_PASSWORD')
    proxy_user = os.getenv('PROXY_USER')
    proxy_password = os.getenv('PROXY_PASSWORD')
    proxy_url = os.getenv('PROXY_URL')
    proxy_domain = os.getenv('PROXY_DOMAIN', 'MAPLE')
    
    # Setup proxy URL with NTLM authentication
    escaped_domain = quote(f"{proxy_domain}\\{proxy_user}")
    quoted_password = quote(proxy_password)
    full_proxy_url = f"http://{escaped_domain}:{quoted_password}@{proxy_url}"
    
    # Configure API client
    configuration = fds.sdk.FactSetFundamentals.Configuration(
        username=api_username,
        password=api_password,
        proxy=full_proxy_url,
        ssl_ca_cert=ssl_cert_path
    )
    
    # Generate authentication token
    configuration.get_basic_auth_token()
    
    logger.info("✅ FactSet API client configured")
    return configuration

def get_all_available_metrics(api_client) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch all available metrics from the API grouped by category."""
    logger.info("🔍 Fetching all available metrics from FactSet Fundamentals API...")
    
    data_api = metrics_api.MetricsApi(api_client)
    
    # Categories to fetch metrics for
    categories = [
        "INCOME_STATEMENT",
        "BALANCE_SHEET", 
        "CASH_FLOW",
        "RATIOS",
        "FINANCIAL_SERVICES",
        "INDUSTRY_METRICS",
        "PENSION_AND_POSTRETIREMENT",
        "MARKET_DATA",
        "MISCELLANEOUS",
        "DATES"
    ]
    
    all_metrics = {}
    total_metrics = 0
    
    for category in categories:
        try:
            logger.info(f"  📂 Fetching {category} metrics...")
            
            # API call to get metrics for category
            response = data_api.get_fds_fundamentals_metrics(category=category)
            
            if response and hasattr(response, 'data') and response.data:
                metrics_list = []
                for metric in response.data:
                    metric_dict = {
                        'metric': metric.metric if hasattr(metric, 'metric') else None,
                        'description': metric.description if hasattr(metric, 'description') else None,
                        'data_type': metric.data_type if hasattr(metric, 'data_type') else None,
                        'category': category
                    }
                    metrics_list.append(metric_dict)
                
                all_metrics[category] = metrics_list
                count = len(metrics_list)
                total_metrics += count
                logger.info(f"    ✅ Found {count} metrics in {category}")
            else:
                all_metrics[category] = []
                logger.warning(f"    ⚠️ No metrics found for {category}")
            
            time.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            logger.error(f"    ❌ Error fetching {category}: {str(e)}")
            all_metrics[category] = []
    
    logger.info(f"📊 Total metrics discovered: {total_metrics}")
    return all_metrics

def check_metric_availability_for_rbc(
    api_client, 
    metrics: List[Dict[str, Any]], 
    ticker: str = RBC_TICKER
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Check which metrics have data available for RBC."""
    
    fund_api = fact_set_fundamentals_api.FactSetFundamentalsApi(api_client)
    
    # Group metrics by data type for efficient API calls
    metrics_by_type = {}
    for metric in metrics:
        data_type = metric.get('data_type', 'unknown')
        if data_type not in metrics_by_type:
            metrics_by_type[data_type] = []
        metrics_by_type[data_type].append(metric['metric'])
    
    available_metrics = []
    sample_data = {}
    
    # Set date range for data retrieval (last 2 years)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=730)
    
    for data_type, metric_codes in metrics_by_type.items():
        if not metric_codes:
            continue
        
        # Test in batches of 10 metrics
        for i in range(0, len(metric_codes), 10):
            batch = metric_codes[i:i+10]
            
            try:
                # Create request using the proper model classes
                ids_instance = IdsBatchMax30000([ticker])
                metrics_instance = Metrics(batch)
                periodicity_instance = Periodicity("QTR")
                update_type_instance = UpdateType("RP")
                fiscal_period_instance = FiscalPeriod(
                    start=start_date.strftime('%Y-%m-%d'),
                    end=end_date.strftime('%Y-%m-%d')
                )
                batch_instance = Batch("N")
                
                request_data = FundamentalRequestBody(
                    ids=ids_instance,
                    metrics=metrics_instance,
                    periodicity=periodicity_instance,
                    fiscal_period=fiscal_period_instance,
                    currency="CAD",  # Standardized to CAD
                    update_type=update_type_instance,
                    batch=batch_instance
                )
                
                request = FundamentalsRequest(data=request_data)
                
                # Make API call
                response_wrapper = fund_api.get_fds_fundamentals_for_list(request)
                
                # Unwrap response
                if hasattr(response_wrapper, 'get_response_200'):
                    response = response_wrapper.get_response_200()
                else:
                    response = response_wrapper
                
                # Process response
                if response and hasattr(response, 'data') and response.data:
                    for item in response.data:
                        if hasattr(item, 'metric') and hasattr(item, 'value'):
                            metric_code = item.metric
                            if metric_code not in sample_data:
                                sample_data[metric_code] = {
                                    'value': item.value,
                                    'date': item.fiscal_end_date if hasattr(item, 'fiscal_end_date') else None,
                                    'fiscal_year': item.fiscal_year if hasattr(item, 'fiscal_year') else None,
                                    'fiscal_period': item.fiscal_period if hasattr(item, 'fiscal_period') else None
                                }
                                
                                # Mark this metric as available
                                for m in metrics:
                                    if m['metric'] == metric_code:
                                        available_metrics.append(m)
                                        break
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.debug(f"Error checking batch {i//10 + 1}: {str(e)}")
                continue
    
    return available_metrics, sample_data

def generate_results_dataframe(
    all_metrics: Dict[str, List[Dict[str, Any]]],
    available_for_rbc: Dict[str, List[Dict[str, Any]]],
    sample_data: Dict[str, Dict[str, Any]]
) -> pd.DataFrame:
    """Generate a DataFrame with all metrics and their RBC availability."""
    
    rows = []
    
    for category, metrics in all_metrics.items():
        available_in_category = available_for_rbc.get(category, [])
        available_codes = [m['metric'] for m in available_in_category]
        
        for metric in metrics:
            metric_code = metric['metric']
            
            # Check if available for RBC
            is_available = metric_code in available_codes
            
            # Get sample data if available
            sample = sample_data.get(metric_code, {})
            
            row = {
                'Category': category,
                'Metric Code': metric_code,
                'Description': metric.get('description', ''),
                'Data Type': metric.get('data_type', ''),
                'Available for RBC': '✅' if is_available else '❌',
                'Sample Value': sample.get('value', '') if sample else '',
                'Sample Date': sample.get('date', '') if sample else '',
                'Sample Period': f"FY{sample.get('fiscal_year', '')} Q{sample.get('fiscal_period', '')}" if sample and sample.get('fiscal_year') else ''
            }
            rows.append(row)
    
    df = pd.DataFrame(rows)
    
    # Sort by category and availability
    df = df.sort_values(['Category', 'Available for RBC', 'Metric Code'], 
                       ascending=[True, False, True])
    
    return df

def generate_html_report(df: pd.DataFrame, summary_stats: Dict[str, Any]) -> str:
    """Generate an HTML report with the results."""
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>RBC Fundamentals Metrics Availability Report</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }}
            .header {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                border-radius: 10px;
                margin-bottom: 30px;
            }}
            .summary {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }}
            .stat-card {{
                background: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .stat-value {{
                font-size: 2em;
                font-weight: bold;
                color: #667eea;
            }}
            .stat-label {{
                color: #666;
                margin-top: 5px;
            }}
            table {{
                width: 100%;
                background: white;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            th {{
                background: #667eea;
                color: white;
                padding: 12px;
                text-align: left;
                position: sticky;
                top: 0;
            }}
            td {{
                padding: 10px 12px;
                border-bottom: 1px solid #eee;
            }}
            tr:hover {{
                background: #f9f9f9;
            }}
            .available {{
                color: #10b981;
                font-weight: bold;
            }}
            .not-available {{
                color: #ef4444;
            }}
            .metric-code {{
                font-family: 'Monaco', 'Consolas', monospace;
                font-size: 0.9em;
                color: #667eea;
            }}
            .category-badge {{
                display: inline-block;
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 0.85em;
                font-weight: bold;
            }}
            .income-statement {{ background: #fef3c7; color: #92400e; }}
            .balance-sheet {{ background: #dbeafe; color: #1e40af; }}
            .cash-flow {{ background: #d1fae5; color: #065f46; }}
            .ratios {{ background: #fce7f3; color: #9f1239; }}
            .financial-services {{ background: #e9d5ff; color: #6b21a8; }}
            .filter-buttons {{
                margin-bottom: 20px;
            }}
            .filter-btn {{
                padding: 8px 16px;
                margin-right: 10px;
                border: 2px solid #667eea;
                background: white;
                color: #667eea;
                border-radius: 6px;
                cursor: pointer;
                transition: all 0.3s;
            }}
            .filter-btn:hover {{
                background: #667eea;
                color: white;
            }}
            .filter-btn.active {{
                background: #667eea;
                color: white;
            }}
        </style>
        <script>
            function filterTable(category) {{
                const rows = document.querySelectorAll('tbody tr');
                const buttons = document.querySelectorAll('.filter-btn');
                
                // Update button states
                buttons.forEach(btn => {{
                    if (btn.dataset.category === category) {{
                        btn.classList.add('active');
                    }} else {{
                        btn.classList.remove('active');
                    }}
                }});
                
                // Filter rows
                rows.forEach(row => {{
                    if (category === 'all') {{
                        row.style.display = '';
                    }} else if (category === 'available') {{
                        row.style.display = row.dataset.available === 'true' ? '' : 'none';
                    }} else {{
                        row.style.display = row.dataset.category === category ? '' : 'none';
                    }}
                }});
            }}
        </script>
    </head>
    <body>
        <div class="header">
            <h1>🏦 RBC (RY-CA) Fundamentals Metrics Availability</h1>
            <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="summary">
            <div class="stat-card">
                <div class="stat-value">{summary_stats['total_metrics']}</div>
                <div class="stat-label">Total Metrics</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{summary_stats['available_metrics']}</div>
                <div class="stat-label">Available for RBC</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{summary_stats['coverage_percent']:.1f}%</div>
                <div class="stat-label">Coverage Rate</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{summary_stats['categories_count']}</div>
                <div class="stat-label">Categories</div>
            </div>
        </div>
        
        <div class="filter-buttons">
            <button class="filter-btn active" data-category="all" onclick="filterTable('all')">All Metrics</button>
            <button class="filter-btn" data-category="available" onclick="filterTable('available')">Available Only</button>
            <button class="filter-btn" data-category="INCOME_STATEMENT" onclick="filterTable('INCOME_STATEMENT')">Income Statement</button>
            <button class="filter-btn" data-category="BALANCE_SHEET" onclick="filterTable('BALANCE_SHEET')">Balance Sheet</button>
            <button class="filter-btn" data-category="CASH_FLOW" onclick="filterTable('CASH_FLOW')">Cash Flow</button>
            <button class="filter-btn" data-category="RATIOS" onclick="filterTable('RATIOS')">Ratios</button>
        </div>
        
        <table>
            <thead>
                <tr>
                    <th>Category</th>
                    <th>Metric Code</th>
                    <th>Description</th>
                    <th>Data Type</th>
                    <th>RBC Available</th>
                    <th>Sample Value</th>
                    <th>Sample Period</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for _, row in df.iterrows():
        category_class = row['Category'].lower().replace('_', '-')
        is_available = row['Available for RBC'] == '✅'
        availability_class = 'available' if is_available else 'not-available'
        
        html += f"""
                <tr data-category="{row['Category']}" data-available="{str(is_available).lower()}">
                    <td><span class="category-badge {category_class}">{row['Category']}</span></td>
                    <td class="metric-code">{row['Metric Code']}</td>
                    <td>{row['Description'][:100]}{'...' if len(str(row['Description'])) > 100 else ''}</td>
                    <td>{row['Data Type']}</td>
                    <td class="{availability_class}">{row['Available for RBC']}</td>
                    <td>{row['Sample Value']}</td>
                    <td>{row['Sample Period']}</td>
                </tr>
        """
    
    html += """
            </tbody>
        </table>
    </body>
    </html>
    """
    
    return html

def main():
    """Main function to check RBC metrics availability."""
    logger.info("="*80)
    logger.info("🏦 RBC FUNDAMENTALS METRICS AVAILABILITY CHECKER")
    logger.info("="*80)
    
    # Validate environment
    validate_env_vars()
    
    # Setup SSL certificate
    ssl_cert_path = setup_ssl_certificate()
    if not ssl_cert_path:
        logger.warning("⚠️ No SSL certificate available - continuing without SSL verification")
    
    # Configure API
    configuration = setup_api_configuration(ssl_cert_path)
    
    try:
        with fds.sdk.FactSetFundamentals.ApiClient(configuration) as api_client:
            
            # Phase 1: Get all available metrics
            logger.info("\n📊 PHASE 1: Discovering all available metrics")
            logger.info("-"*60)
            all_metrics = get_all_available_metrics(api_client)
            
            # Phase 2: Check availability for RBC
            logger.info("\n📊 PHASE 2: Checking metric availability for RBC")
            logger.info("-"*60)
            
            available_for_rbc = {}
            all_sample_data = {}
            
            for category, metrics in all_metrics.items():
                if not metrics:
                    continue
                
                logger.info(f"\n🔍 Checking {category} ({len(metrics)} metrics)...")
                available, sample_data = check_metric_availability_for_rbc(api_client, metrics)
                
                available_for_rbc[category] = available
                all_sample_data.update(sample_data)
                
                logger.info(f"  ✅ {len(available)} out of {len(metrics)} metrics have data for RBC")
            
            # Phase 3: Generate results
            logger.info("\n📊 PHASE 3: Generating results")
            logger.info("-"*60)
            
            # Create DataFrame
            df = generate_results_dataframe(all_metrics, available_for_rbc, all_sample_data)
            
            # Calculate summary statistics
            total_metrics = len(df)
            available_metrics = len(df[df['Available for RBC'] == '✅'])
            coverage_percent = (available_metrics / total_metrics * 100) if total_metrics > 0 else 0
            categories_count = df['Category'].nunique()
            
            summary_stats = {
                'total_metrics': total_metrics,
                'available_metrics': available_metrics,
                'coverage_percent': coverage_percent,
                'categories_count': categories_count
            }
            
            # Save to CSV
            csv_filename = f"rbc_metrics_availability_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(csv_filename, index=False)
            logger.info(f"✅ Results saved to {csv_filename}")
            
            # Generate and save HTML report
            html_report = generate_html_report(df, summary_stats)
            html_filename = f"rbc_metrics_availability_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            with open(html_filename, 'w', encoding='utf-8') as f:
                f.write(html_report)
            logger.info(f"✅ HTML report saved to {html_filename}")
            
            # Print summary
            logger.info("\n" + "="*80)
            logger.info("📊 SUMMARY RESULTS")
            logger.info("="*80)
            logger.info(f"Total Metrics Available: {total_metrics}")
            logger.info(f"Metrics with RBC Data: {available_metrics}")
            logger.info(f"Coverage Rate: {coverage_percent:.1f}%")
            logger.info(f"Categories Analyzed: {categories_count}")
            
            # Category breakdown
            logger.info("\n📂 Category Breakdown:")
            for category in df['Category'].unique():
                cat_df = df[df['Category'] == category]
                cat_available = len(cat_df[cat_df['Available for RBC'] == '✅'])
                cat_total = len(cat_df)
                cat_percent = (cat_available / cat_total * 100) if cat_total > 0 else 0
                logger.info(f"  {category}: {cat_available}/{cat_total} ({cat_percent:.1f}%)")
            
            logger.info("\n✅ Analysis complete!")
            logger.info(f"📄 View the HTML report for interactive results: {html_filename}")
            logger.info(f"📊 CSV file for further analysis: {csv_filename}")
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}")
        raise
    
    finally:
        # Cleanup
        if ssl_cert_path and ssl_cert_path.startswith('/tmp/'):
            try:
                os.unlink(ssl_cert_path)
            except:
                pass

if __name__ == "__main__":
    main()