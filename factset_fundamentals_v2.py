"""
FactSet Fundamentals v2 API - Getting Started Examples
Using SSL certificate and proxy connection methods for internal work environment
"""

import os
import tempfile
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote
from typing import Dict, Any, Optional, List
import yaml
from dotenv import load_dotenv
import fds.sdk.FactSetFundamentals
from fds.sdk.FactSetFundamentals.api import fundamentals_api
from fds.sdk.FactSetFundamentals.models import fundamentals_request, fundamentals_response

# Load environment variables
load_dotenv()

# Global variables
config = {}
logger = None


def setup_logging() -> logging.Logger:
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


def validate_environment_variables() -> None:
    """Validate all required environment variables are present."""
    required_env_vars = [
        "API_USERNAME",
        "API_PASSWORD",
        "PROXY_USER", 
        "PROXY_PASSWORD",
        "PROXY_URL",
        "PROXY_DOMAIN",
    ]
    
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info("✓ Environment variables validated successfully")


def load_config() -> Dict[str, Any]:
    """Load configuration from config.yaml file."""
    try:
        # Try loading from example folder first
        config_path = "example/config.yaml"
        if not os.path.exists(config_path):
            config_path = "config.yaml"
        
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        logger.info(f"✓ Configuration loaded from {config_path}")
        logger.info(f"  Found {len(config.get('monitored_institutions', {}))} monitored institutions")
        
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise


def download_ssl_certificate(cert_path: str) -> Optional[str]:
    """Download/setup SSL certificate for API use."""
    try:
        # For this example, we'll assume the certificate is in a local file
        # In production, you might download it from NAS as shown in the example
        
        if os.path.exists(cert_path):
            logger.info(f"✓ Using SSL certificate: {cert_path}")
            return cert_path
        
        # Create a temporary certificate file if needed
        # In production, download from NAS or other secure location
        logger.warning(f"SSL certificate not found at {cert_path}, proceeding without custom cert")
        return None
        
    except Exception as e:
        logger.error(f"Error setting up SSL certificate: {e}")
        return None


def setup_proxy_configuration() -> str:
    """Configure proxy URL for API authentication."""
    try:
        proxy_user = os.getenv("PROXY_USER")
        proxy_password = os.getenv("PROXY_PASSWORD")
        proxy_url = os.getenv("PROXY_URL")
        proxy_domain = os.getenv("PROXY_DOMAIN", "MAPLE")
        
        # Escape domain and user for NTLM authentication
        escaped_domain = quote(proxy_domain + "\\" + proxy_user)
        quoted_password = quote(proxy_password)
        
        # Construct proxy URL
        proxy_url_formatted = f"http://{escaped_domain}:{quoted_password}@{proxy_url}"
        
        logger.info("✓ Proxy configuration completed successfully")
        return proxy_url_formatted
        
    except Exception as e:
        logger.error(f"Error configuring proxy: {e}")
        raise


def setup_factset_api_client(proxy_url: str, ssl_cert_path: Optional[str]):
    """Configure FactSet Fundamentals API client with proxy and SSL settings."""
    try:
        api_username = os.getenv("API_USERNAME")
        api_password = os.getenv("API_PASSWORD")
        
        # Configure FactSet API client
        configuration = fds.sdk.FactSetFundamentals.Configuration(
            username=api_username,
            password=api_password,
            proxy=proxy_url,
        )
        
        # Add SSL certificate if available
        if ssl_cert_path:
            configuration.ssl_ca_cert = ssl_cert_path
        
        # Generate authentication token
        configuration.get_basic_auth_token()
        
        logger.info("✓ FactSet Fundamentals API client configured successfully")
        return configuration
        
    except Exception as e:
        logger.error(f"Error setting up FactSet API client: {e}")
        raise


def get_fundamentals_metrics(api_instance, tickers: List[str]) -> pd.DataFrame:
    """
    Example 1: Get fundamental metrics for companies
    Retrieves key financial metrics like P/E ratio, Market Cap, etc.
    """
    logger.info("\n=== Example 1: Getting Fundamental Metrics ===")
    
    try:
        # Define metrics to retrieve
        metrics = [
            "FF_PE",           # P/E Ratio
            "FF_MKTCAP",       # Market Cap
            "FF_SALES",        # Sales/Revenue
            "FF_EPS",          # Earnings Per Share
            "FF_DIV_YLD",      # Dividend Yield
            "FF_ROE",          # Return on Equity
            "FF_BPS",          # Book Value Per Share
            "FF_DEBT_TO_EQ"    # Debt to Equity Ratio
        ]
        
        # Create request
        request = fundamentals_request.FundamentalsRequest(
            ids=tickers,
            metrics=metrics,
            periodicity="QTR",  # Quarterly data
            fiscal_period_start="2023",
            fiscal_period_end="2024"
        )
        
        # Get data
        response = api_instance.get_fundamentals(fundamentals_request=request)
        
        # Convert to DataFrame for easier viewing
        data = []
        for item in response.data:
            data.append({
                'ticker': item.requestId,
                'metric': item.metric,
                'value': item.value,
                'date': item.date,
                'fiscal_period': item.fiscalPeriod
            })
        
        df = pd.DataFrame(data)
        logger.info(f"✓ Retrieved {len(df)} data points for {len(tickers)} companies")
        
        # Display sample data
        logger.info("\nSample data (first 10 rows):")
        print(df.head(10).to_string())
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting fundamentals: {e}")
        return pd.DataFrame()


def get_company_snapshot(api_instance, ticker: str) -> Dict[str, Any]:
    """
    Example 2: Get company snapshot/profile
    Retrieves company overview information
    """
    logger.info(f"\n=== Example 2: Getting Company Snapshot for {ticker} ===")
    
    try:
        # Define snapshot metrics
        snapshot_metrics = [
            "FF_CO_NAME",      # Company Name
            "FF_INDUSTRY",     # Industry
            "FF_SECTOR",       # Sector
            "FF_EMPLOYEES",    # Number of Employees
            "FF_CO_FOUNDED",   # Founded Date
            "FF_EXCHANGE",     # Exchange
        ]
        
        request = fundamentals_request.FundamentalsRequest(
            ids=[ticker],
            metrics=snapshot_metrics
        )
        
        response = api_instance.get_fundamentals(fundamentals_request=request)
        
        # Create snapshot dictionary
        snapshot = {}
        for item in response.data:
            snapshot[item.metric] = item.value
        
        logger.info(f"✓ Company Snapshot for {ticker}:")
        for key, value in snapshot.items():
            logger.info(f"  {key}: {value}")
        
        return snapshot
        
    except Exception as e:
        logger.error(f"Error getting company snapshot: {e}")
        return {}


def get_financial_statements(api_instance, ticker: str) -> pd.DataFrame:
    """
    Example 3: Get financial statement items
    Retrieves income statement, balance sheet, and cash flow data
    """
    logger.info(f"\n=== Example 3: Getting Financial Statements for {ticker} ===")
    
    try:
        # Financial statement metrics
        statement_metrics = [
            # Income Statement
            "FF_SALES",        # Revenue
            "FF_GROSS_PROFIT", # Gross Profit
            "FF_OPER_INC",     # Operating Income
            "FF_NET_INC",      # Net Income
            
            # Balance Sheet
            "FF_ASSETS",       # Total Assets
            "FF_LIAB",         # Total Liabilities
            "FF_EQUITY",       # Total Equity
            "FF_CASH_ST",      # Cash and Short-term Investments
            
            # Cash Flow
            "FF_OPER_CF",      # Operating Cash Flow
            "FF_INVEST_CF",    # Investing Cash Flow
            "FF_FIN_CF"        # Financing Cash Flow
        ]
        
        request = fundamentals_request.FundamentalsRequest(
            ids=[ticker],
            metrics=statement_metrics,
            periodicity="ANN",  # Annual data
            fiscal_period_start="2021",
            fiscal_period_end="2023"
        )
        
        response = api_instance.get_fundamentals(fundamentals_request=request)
        
        # Convert to DataFrame
        data = []
        for item in response.data:
            data.append({
                'ticker': item.requestId,
                'metric': item.metric,
                'value': item.value,
                'fiscal_year': item.fiscalPeriod,
                'date': item.date
            })
        
        df = pd.DataFrame(data)
        
        # Pivot for better display
        if not df.empty:
            pivot_df = df.pivot_table(
                values='value',
                index='metric',
                columns='fiscal_year',
                aggfunc='first'
            )
            
            logger.info(f"✓ Financial Statements for {ticker}:")
            print(pivot_df.to_string())
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting financial statements: {e}")
        return pd.DataFrame()


def get_banks_fundamentals(api_instance, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Example 4: Get fundamentals for banks from config
    Uses the monitored institutions from config.yaml
    """
    logger.info("\n=== Example 4: Getting Fundamentals for Banks ===")
    
    try:
        # Get first 5 banks from different categories
        banks = []
        categories_to_use = ["Canadian_Banks", "US_Banks", "European_Banks"]
        
        for ticker, info in config.get("monitored_institutions", {}).items():
            if info.get("type") in categories_to_use and len(banks) < 5:
                banks.append({
                    'ticker': ticker,
                    'name': info.get('name'),
                    'type': info.get('type')
                })
        
        if not banks:
            logger.warning("No banks found in configuration")
            return pd.DataFrame()
        
        # Get bank-specific metrics
        bank_metrics = [
            "FF_NET_INT_INC",   # Net Interest Income
            "FF_NET_INT_MRGN",  # Net Interest Margin
            "FF_TIER1_CAP_R",   # Tier 1 Capital Ratio
            "FF_LOAN_LOSS_PROV",# Loan Loss Provision
            "FF_NPL_RATIO",     # Non-Performing Loans Ratio
            "FF_ROA",           # Return on Assets
            "FF_ROE",           # Return on Equity
            "FF_EFF_RATIO"      # Efficiency Ratio
        ]
        
        bank_tickers = [b['ticker'] for b in banks]
        
        request = fundamentals_request.FundamentalsRequest(
            ids=bank_tickers,
            metrics=bank_metrics,
            periodicity="QTR",
            fiscal_period_start="2024-Q1",
            fiscal_period_end="2024-Q3"
        )
        
        response = api_instance.get_fundamentals(fundamentals_request=request)
        
        # Convert to DataFrame
        data = []
        for item in response.data:
            # Find bank name
            bank_name = next((b['name'] for b in banks if b['ticker'] == item.requestId), '')
            data.append({
                'ticker': item.requestId,
                'bank_name': bank_name,
                'metric': item.metric,
                'value': item.value,
                'period': item.fiscalPeriod,
                'date': item.date
            })
        
        df = pd.DataFrame(data)
        
        logger.info(f"✓ Retrieved {len(df)} data points for {len(bank_tickers)} banks")
        
        # Display summary by bank
        if not df.empty:
            summary = df.groupby(['ticker', 'bank_name'])['metric'].count().reset_index()
            summary.columns = ['ticker', 'bank_name', 'metrics_count']
            logger.info("\nBank Data Summary:")
            print(summary.to_string())
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting bank fundamentals: {e}")
        return pd.DataFrame()


def get_ratios_and_multiples(api_instance, ticker: str) -> pd.DataFrame:
    """
    Example 5: Get financial ratios and valuation multiples
    """
    logger.info(f"\n=== Example 5: Getting Ratios and Multiples for {ticker} ===")
    
    try:
        # Ratios and multiples
        ratio_metrics = [
            # Valuation Multiples
            "FF_PE",            # P/E Ratio
            "FF_PB",            # Price to Book
            "FF_PS",            # Price to Sales
            "FF_EV_EBITDA",     # EV/EBITDA
            
            # Profitability Ratios
            "FF_GROSS_MRGN",    # Gross Margin
            "FF_OPER_MRGN",     # Operating Margin
            "FF_NET_MRGN",      # Net Margin
            
            # Liquidity Ratios
            "FF_CURR_RATIO",    # Current Ratio
            "FF_QUICK_RATIO",   # Quick Ratio
            
            # Leverage Ratios
            "FF_DEBT_TO_EQ",    # Debt to Equity
            "FF_DEBT_TO_ASSETS",# Debt to Assets
            "FF_INT_COV"        # Interest Coverage
        ]
        
        request = fundamentals_request.FundamentalsRequest(
            ids=[ticker],
            metrics=ratio_metrics,
            periodicity="QTR",
            fiscal_period_start="2023-Q1",
            fiscal_period_end="2024-Q3"
        )
        
        response = api_instance.get_fundamentals(fundamentals_request=request)
        
        # Convert to DataFrame
        data = []
        for item in response.data:
            data.append({
                'metric': item.metric,
                'value': item.value,
                'period': item.fiscalPeriod,
                'date': item.date
            })
        
        df = pd.DataFrame(data)
        
        if not df.empty:
            # Get latest values for each metric
            latest_values = df.sort_values('date').groupby('metric').last()
            logger.info(f"✓ Latest Ratios and Multiples for {ticker}:")
            print(latest_values[['value', 'period']].to_string())
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting ratios and multiples: {e}")
        return pd.DataFrame()


def main():
    """Main function to demonstrate FactSet Fundamentals v2 API usage."""
    global logger, config
    
    # Initialize logging
    logger = setup_logging()
    
    logger.info("=== FactSet Fundamentals v2 API - Getting Started ===\n")
    
    try:
        # Step 1: Validate environment variables
        logger.info("Step 1: Validating environment variables...")
        validate_environment_variables()
        
        # Step 2: Load configuration
        logger.info("\nStep 2: Loading configuration...")
        config = load_config()
        
        # Step 3: Set up SSL certificate (optional)
        logger.info("\nStep 3: Setting up SSL certificate...")
        ssl_cert_path = None
        if 'ssl_cert_path' in config:
            ssl_cert_path = download_ssl_certificate(config['ssl_cert_path'])
        
        # Step 4: Configure proxy
        logger.info("\nStep 4: Configuring proxy authentication...")
        proxy_url = setup_proxy_configuration()
        
        # Step 5: Set up FactSet API client
        logger.info("\nStep 5: Setting up FactSet API client...")
        api_configuration = setup_factset_api_client(proxy_url, ssl_cert_path)
        
        logger.info("\n✓ Setup complete - ready for API calls\n")
        logger.info("=" * 60)
        
        # Create API client and instance
        with fds.sdk.FactSetFundamentals.ApiClient(api_configuration) as api_client:
            api_instance = fundamentals_api.FundamentalsApi(api_client)
            
            # Example companies to analyze
            sample_tickers = ["AAPL-US", "MSFT-US", "GOOGL-US"]
            
            # Run examples
            
            # Example 1: Get fundamental metrics
            fundamentals_df = get_fundamentals_metrics(api_instance, sample_tickers)
            
            # Example 2: Get company snapshot
            snapshot = get_company_snapshot(api_instance, "AAPL-US")
            
            # Example 3: Get financial statements
            statements_df = get_financial_statements(api_instance, "MSFT-US")
            
            # Example 4: Get bank fundamentals from config
            banks_df = get_banks_fundamentals(api_instance, config)
            
            # Example 5: Get ratios and multiples
            ratios_df = get_ratios_and_multiples(api_instance, "GOOGL-US")
            
            # Save results to CSV files
            logger.info("\n=== Saving Results ===")
            output_dir = "factset_fundamentals_output"
            os.makedirs(output_dir, exist_ok=True)
            
            if not fundamentals_df.empty:
                fundamentals_df.to_csv(f"{output_dir}/fundamentals_metrics.csv", index=False)
                logger.info(f"✓ Saved fundamentals metrics to {output_dir}/fundamentals_metrics.csv")
            
            if not statements_df.empty:
                statements_df.to_csv(f"{output_dir}/financial_statements.csv", index=False)
                logger.info(f"✓ Saved financial statements to {output_dir}/financial_statements.csv")
            
            if not banks_df.empty:
                banks_df.to_csv(f"{output_dir}/banks_fundamentals.csv", index=False)
                logger.info(f"✓ Saved banks fundamentals to {output_dir}/banks_fundamentals.csv")
            
            if not ratios_df.empty:
                ratios_df.to_csv(f"{output_dir}/ratios_multiples.csv", index=False)
                logger.info(f"✓ Saved ratios and multiples to {output_dir}/ratios_multiples.csv")
            
            logger.info("\n=== FactSet Fundamentals v2 Examples Complete ===")
            
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise


if __name__ == "__main__":
    main()