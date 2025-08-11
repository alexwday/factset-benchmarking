#!/usr/bin/env python3
"""
Diagnostic script to test FactSet API connection and identify issues
"""

import os
import sys
import logging
import tempfile
import urllib.parse
from pathlib import Path

# Setup detailed logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for maximum detail
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.info("Loaded .env file")
except ImportError:
    logger.warning("python-dotenv not installed. Using system environment variables.")

# Import FactSet SDK
try:
    import fds.sdk.FactSetFundamentals as ff
    from fds.sdk.FactSetFundamentals.api.fact_set_fundamentals_api import FactSetFundamentalsApi
    from fds.sdk.FactSetFundamentals.api.metrics_api import MetricsApi
    from fds.sdk.FactSetFundamentals.model.fundamentals_request import FundamentalsRequest
    from fds.sdk.FactSetFundamentals.model.fundamental_request_body import FundamentalRequestBody
    from fds.sdk.FactSetFundamentals.model.ids_batch_max30000 import IdsBatchMax30000
    from fds.sdk.FactSetFundamentals.model.metrics import Metrics
    logger.info("✅ FactSet SDK imported successfully")
except ImportError as e:
    logger.error(f"❌ Failed to import FactSet SDK: {e}")
    sys.exit(1)


def test_environment():
    """Test environment variables"""
    print("\n" + "="*60)
    print("1. ENVIRONMENT VARIABLES CHECK")
    print("="*60)
    
    username = os.getenv('API_USERNAME')
    password = os.getenv('API_PASSWORD')
    proxy_url = os.getenv('PROXY_URL')
    ssl_cert = os.getenv('SSL_CERT_PATH')
    
    print(f"API_USERNAME: {'✅ Set' if username else '❌ Not set'}")
    print(f"API_PASSWORD: {'✅ Set' if password else '❌ Not set'}")
    print(f"PROXY_URL: {proxy_url if proxy_url else 'Not set'}")
    print(f"SSL_CERT_PATH: {ssl_cert if ssl_cert else 'Not set'}")
    
    if ssl_cert and not os.path.exists(ssl_cert):
        print(f"  ⚠️  Warning: SSL cert path set but file doesn't exist: {ssl_cert}")
    
    return username and password


def setup_ssl_certificate():
    """Setup SSL certificate if provided"""
    ssl_cert_path = os.getenv('SSL_CERT_PATH')
    
    if ssl_cert_path and os.path.exists(ssl_cert_path):
        with open(ssl_cert_path, 'rb') as cert_file:
            cert_data = cert_file.read()
            
        temp_cert = tempfile.NamedTemporaryFile(mode='wb', suffix='.cer', delete=False)
        temp_cert.write(cert_data)
        temp_cert.close()
        
        os.environ['REQUESTS_CA_BUNDLE'] = temp_cert.name
        os.environ['SSL_CERT_FILE'] = temp_cert.name
        
        logger.info(f"SSL certificate configured: {temp_cert.name}")
        return temp_cert.name
    
    return None


def test_basic_connection():
    """Test basic API connection"""
    print("\n" + "="*60)
    print("2. BASIC API CONNECTION TEST")
    print("="*60)
    
    try:
        # Setup SSL
        ssl_cert_path = setup_ssl_certificate()
        
        # Create configuration
        username = os.getenv('API_USERNAME')
        password = os.getenv('API_PASSWORD')
        
        configuration = ff.Configuration(
            username=username,
            password=password
        )
        
        if ssl_cert_path:
            configuration.ssl_ca_cert = ssl_cert_path
        
        # Setup proxy if needed
        proxy_url = os.getenv('PROXY_URL')
        if proxy_url and os.getenv('USE_PROXY', 'true').lower() == 'true':
            proxy_user = os.getenv('PROXY_USER')
            proxy_password = os.getenv('PROXY_PASSWORD')
            
            if proxy_user and proxy_password:
                proxy_domain = os.getenv('PROXY_DOMAIN', 'MAPLE')
                escaped_user = urllib.parse.quote(f"{proxy_domain}\\{proxy_user}")
                escaped_pass = urllib.parse.quote(proxy_password)
                configuration.proxy = f"http://{escaped_user}:{escaped_pass}@{proxy_url}"
            else:
                configuration.proxy = f"http://{proxy_url}"
            
            print(f"Proxy configured: {proxy_url}")
        
        # Create API client
        api_client = ff.ApiClient(configuration)
        print("✅ API client created successfully")
        
        return api_client, configuration
        
    except Exception as e:
        print(f"❌ Failed to create API client: {e}")
        import traceback
        traceback.print_exc()
        return None, None


def test_metrics_api(api_client):
    """Test the metrics API endpoint"""
    print("\n" + "="*60)
    print("3. METRICS API TEST")
    print("="*60)
    
    try:
        metrics_api = MetricsApi(api_client)
        
        # Try to get metrics for INCOME_STATEMENT category
        print("Fetching metrics for INCOME_STATEMENT category...")
        response = metrics_api.get_fds_fundamentals_metrics(category='INCOME_STATEMENT')
        
        if response and response.data:
            print(f"✅ SUCCESS! Retrieved {len(response.data)} metrics")
            
            # Show first 5 metrics
            print("\nFirst 5 metrics:")
            for i, metric in enumerate(response.data[:5], 1):
                code = getattr(metric, 'metric', 'N/A')
                name = getattr(metric, 'name', 'N/A')
                print(f"  {i}. {code}: {name}")
            
            return True
        else:
            print("❌ No metrics returned")
            return False
            
    except Exception as e:
        print(f"❌ Metrics API error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_fundamentals_simple(api_client):
    """Test fundamentals with the simplest possible request"""
    print("\n" + "="*60)
    print("4. SIMPLE FUNDAMENTALS TEST")
    print("="*60)
    
    try:
        fundamentals_api = FactSetFundamentalsApi(api_client)
        
        # Test different ID formats
        test_cases = [
            ('AAPL-US', 'Apple with US suffix'),
            ('AAPL', 'Apple without suffix'),
            ('000A1Q-US', 'Apple FactSet ID'),
            ('FDS-US', 'FactSet Inc'),
            ('MSFT-US', 'Microsoft')
        ]
        
        for ticker, description in test_cases:
            print(f"\nTesting {description}: {ticker}")
            
            try:
                # Create the simplest possible request
                ids = IdsBatchMax30000([ticker])
                metrics = Metrics(['FF_SALES'])  # Just one metric
                
                request_body = FundamentalRequestBody(
                    ids=ids,
                    metrics=metrics
                )
                
                request = FundamentalsRequest(data=request_body)
                
                # Make API call
                response = fundamentals_api.get_fds_fundamentals_for_list(request)
                
                if response:
                    print(f"  Response type: {type(response)}")
                    
                    if hasattr(response, 'data'):
                        if response.data:
                            print(f"  ✅ SUCCESS! Got {len(response.data)} data points")
                            
                            # Show the data
                            for item in response.data[:3]:
                                request_id = getattr(item, 'request_id', 'N/A')
                                metric = getattr(item, 'metric', 'N/A')
                                value = getattr(item, 'value', 'N/A')
                                date = getattr(item, 'date', 'N/A')
                                print(f"    - Request ID: {request_id}, Metric: {metric}, Value: {value}, Date: {date}")
                            
                            return True
                        else:
                            print(f"  ⚠️  Empty data array")
                    else:
                        print(f"  ⚠️  No 'data' attribute in response")
                        # Try to print the response to see what we got
                        print(f"  Response dir: {dir(response)}")
                else:
                    print(f"  ⚠️  No response object")
                    
            except Exception as e:
                error_str = str(e)
                if "400" in error_str:
                    print(f"  ❌ Bad Request (400): Check if ticker format is correct")
                elif "401" in error_str:
                    print(f"  ❌ Unauthorized (401): Check API credentials")
                elif "403" in error_str:
                    print(f"  ❌ Forbidden (403): Check API permissions")
                elif "404" in error_str:
                    print(f"  ❌ Not Found (404): Ticker might not exist")
                else:
                    print(f"  ❌ Error: {error_str[:200]}")
                
                # If it's a validation error, show details
                if "is not a valid value" in error_str:
                    print(f"  ⚠️  Validation error - metric or parameter might be invalid")
        
        return False
        
    except Exception as e:
        print(f"❌ Fundamentals API error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_with_different_id_types(api_client):
    """Test with different ID types from the documentation"""
    print("\n" + "="*60)
    print("5. TESTING DIFFERENT ID TYPES")
    print("="*60)
    
    try:
        fundamentals_api = FactSetFundamentalsApi(api_client)
        
        # According to docs: Market Tickers, SEDOL, ISINs, CUSIPs, or FactSet Permanent Ids
        test_ids = [
            ('AAPL-US', 'Market Ticker with exchange'),
            ('AAPL', 'Market Ticker without exchange'),
            ('US0378331005', 'Apple ISIN'),
            ('037833100', 'Apple CUSIP'),
            ('2046251', 'Apple SEDOL'),
        ]
        
        for id_value, id_type in test_ids:
            print(f"\nTesting {id_type}: {id_value}")
            
            try:
                # Use IdsBatchMax30000 as seen in the test files
                ids = IdsBatchMax30000([id_value])
                metrics = Metrics(['FF_SALES', 'FF_NET_INC'])
                
                request_body = FundamentalRequestBody(
                    ids=ids,
                    metrics=metrics
                )
                
                request = FundamentalsRequest(data=request_body)
                response = fundamentals_api.get_fds_fundamentals_for_list(request)
                
                if response and hasattr(response, 'data') and response.data:
                    print(f"  ✅ SUCCESS with {id_type}!")
                    for item in response.data[:2]:
                        request_id = getattr(item, 'request_id', 'N/A')
                        metric = getattr(item, 'metric', 'N/A')
                        value = getattr(item, 'value', 'N/A')
                        print(f"    - ID: {request_id}, Metric: {metric}, Value: {value}")
                    return True
                else:
                    print(f"  ⚠️  No data returned for {id_type}")
                    
            except Exception as e:
                print(f"  ❌ Error with {id_type}: {str(e)[:100]}")
        
        return False
        
    except Exception as e:
        print(f"❌ Error in ID type testing: {e}")
        return False


def main():
    print("="*60)
    print("FACTSET API CONNECTION DIAGNOSTIC")
    print("="*60)
    
    # Test 1: Environment
    if not test_environment():
        print("\n❌ Environment variables not properly set. Cannot continue.")
        print("Please ensure API_USERNAME and API_PASSWORD are set in your .env file")
        return
    
    # Test 2: Basic connection
    api_client, configuration = test_basic_connection()
    if not api_client:
        print("\n❌ Could not create API client. Check credentials and network.")
        return
    
    # Test 3: Metrics API
    metrics_success = test_metrics_api(api_client)
    
    # Test 4: Simple fundamentals
    fundamentals_success = test_fundamentals_simple(api_client)
    
    # Test 5: Different ID types
    if not fundamentals_success:
        id_type_success = test_with_different_id_types(api_client)
    else:
        id_type_success = True
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Metrics API: {'✅ Working' if metrics_success else '❌ Failed'}")
    print(f"Fundamentals API: {'✅ Working' if (fundamentals_success or id_type_success) else '❌ Failed'}")
    
    if not fundamentals_success and not id_type_success:
        print("\nPossible issues:")
        print("1. API credentials might not have access to Fundamentals data")
        print("2. Ticker format might need to be different")
        print("3. Account might need specific permissions enabled")
        print("4. Check if you need to use FactSet Entity IDs instead of tickers")
        print("\nContact FactSet support with the error messages above for assistance.")


if __name__ == "__main__":
    main()