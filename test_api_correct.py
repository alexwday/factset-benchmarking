#!/usr/bin/env python3
"""
Test FactSet API with the CORRECT object construction
"""

import os
import sys
import logging
import json
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Import FactSet SDK
import fds.sdk.FactSetFundamentals as ff
from fds.sdk.FactSetFundamentals.api.fact_set_fundamentals_api import FactSetFundamentalsApi
from fds.sdk.FactSetFundamentals.api.metrics_api import MetricsApi
from fds.sdk.FactSetFundamentals.model.fundamentals_request import FundamentalsRequest
from fds.sdk.FactSetFundamentals.model.fundamental_request_body import FundamentalRequestBody
from fds.sdk.FactSetFundamentals.model.ids_batch_max30000 import IdsBatchMax30000
from fds.sdk.FactSetFundamentals.model.metrics import Metrics
from fds.sdk.FactSetFundamentals.model.fiscal_period import FiscalPeriod
from fds.sdk.FactSetFundamentals.model.periodicity import Periodicity
from fds.sdk.FactSetFundamentals.model.update_type import UpdateType

print("="*60)
print("TESTING FACTSET API WITH CORRECT OBJECT CONSTRUCTION")
print("="*60)

# Setup configuration
username = os.getenv('API_USERNAME')
password = os.getenv('API_PASSWORD')

if not username or not password:
    print("❌ Missing credentials")
    sys.exit(1)

configuration = ff.Configuration(
    username=username,
    password=password
)

# Add SSL cert if available
ssl_cert = os.getenv('SSL_CERT_PATH')
if ssl_cert and os.path.exists(ssl_cert):
    configuration.ssl_ca_cert = ssl_cert
    print(f"SSL cert configured: {ssl_cert}")

# Setup proxy if needed
proxy_url = os.getenv('PROXY_URL')
if proxy_url and os.getenv('USE_PROXY', 'true').lower() == 'true':
    import urllib.parse
    proxy_user = os.getenv('PROXY_USER')
    proxy_password = os.getenv('PROXY_PASSWORD')
    
    if proxy_user and proxy_password:
        proxy_domain = os.getenv('PROXY_DOMAIN', 'MAPLE')
        escaped_user = urllib.parse.quote(f"{proxy_domain}\\{proxy_user}")
        escaped_pass = urllib.parse.quote(proxy_password)
        configuration.proxy = f"http://{escaped_user}:{escaped_pass}@{proxy_url}"
        print(f"Proxy configured: {proxy_url}")

# Create API client
api_client = ff.ApiClient(configuration)
fundamentals_api = FactSetFundamentalsApi(api_client)
metrics_api = MetricsApi(api_client)

print("\n1. Testing Metrics API...")
print("-" * 40)
try:
    # Test metrics endpoint first
    response = metrics_api.get_fds_fundamentals_metrics(category='INCOME_STATEMENT')
    if response and response.data:
        print(f"✅ Metrics API works! Found {len(response.data)} metrics")
        # Show first few metrics
        for metric in response.data[:3]:
            print(f"   - {getattr(metric, 'metric', 'N/A')}: {getattr(metric, 'name', 'N/A')}")
except Exception as e:
    print(f"❌ Metrics API failed: {e}")

print("\n2. Testing Fundamentals with proper object construction...")
print("-" * 40)

test_cases = [
    ("FDS-US with proper objects", ["FDS-US"]),
    ("Apple with proper objects", ["AAPL-US"]),
    ("Multiple companies", ["FDS-US", "AAPL-US", "IBM-US"]),
    ("JPMorgan", ["JPM-US"]),
    ("Royal Bank of Canada", ["RY-CA"]),
]

for test_name, ids_list in test_cases:
    print(f"\nTesting: {test_name}")
    
    try:
        # Create proper model objects
        ids = IdsBatchMax30000(ids_list)
        metrics_obj = Metrics(["FF_SALES", "FF_NET_INC"])
        
        # Create fiscal period object
        fiscal_period = FiscalPeriod(
            start="2020-01-01",
            end="2022-12-31"
        )
        
        # Create periodicity
        periodicity = Periodicity("ANN")
        
        # Create request body with proper objects
        request_body = FundamentalRequestBody(
            ids=ids,
            metrics=metrics_obj,
            fiscal_period=fiscal_period,
            periodicity=periodicity,
            currency="USD"
        )
        
        # Create request
        request = FundamentalsRequest(data=request_body)
        
        # Make the API call
        response = fundamentals_api.get_fds_fundamentals_for_list(request)
        
        if response and hasattr(response, 'data'):
            if response.data:
                print(f"  ✅ SUCCESS! Got {len(response.data)} data points")
                # Show first few results
                for item in response.data[:3]:
                    request_id = getattr(item, 'request_id', 'N/A')
                    fsym_id = getattr(item, 'fsym_id', 'N/A')
                    metric = getattr(item, 'metric', 'N/A')
                    value = getattr(item, 'value', 'N/A')
                    date = getattr(item, 'fiscal_end_date', 'N/A')
                    fiscal_year = getattr(item, 'fiscal_year', 'N/A')
                    print(f"     {request_id} ({fsym_id}): {metric} = {value} (FY{fiscal_year}, {date})")
            else:
                print(f"  ⚠️ Empty response data")
        else:
            print(f"  ⚠️ No data attribute in response")
            
    except Exception as e:
        print(f"  ❌ Error: {e}")
        
        # If it's a validation error, try to understand what's wrong
        error_str = str(e)
        if "400" in error_str:
            print(f"     Bad Request - check parameter format")
        elif "401" in error_str:
            print(f"     Unauthorized - check credentials")
        elif "403" in error_str:
            print(f"     Forbidden - check permissions")
        elif "is not a valid value" in error_str:
            print(f"     Validation error - check metric names or parameters")

print("\n3. Testing with simpler request (no fiscal period)...")
print("-" * 40)

# Try without fiscal period - just get latest data
for test_name, ids_list in [("AAPL-US simple", ["AAPL-US"]), ("JPM-US simple", ["JPM-US"])]:
    print(f"\nTesting: {test_name}")
    
    try:
        # Create minimal request
        ids = IdsBatchMax30000(ids_list)
        metrics_obj = Metrics(["FF_SALES"])
        
        # Create request body without fiscal period
        request_body = FundamentalRequestBody(
            ids=ids,
            metrics=metrics_obj
        )
        
        # Create request
        request = FundamentalsRequest(data=request_body)
        
        # Make the API call
        response = fundamentals_api.get_fds_fundamentals_for_list(request)
        
        if response and hasattr(response, 'data'):
            if response.data:
                print(f"  ✅ SUCCESS! Got {len(response.data)} data points")
                for item in response.data[:2]:
                    request_id = getattr(item, 'request_id', 'N/A')
                    metric = getattr(item, 'metric', 'N/A')
                    value = getattr(item, 'value', 'N/A')
                    print(f"     {request_id}: {metric} = {value}")
            else:
                print(f"  ⚠️ Empty response data")
        else:
            print(f"  ⚠️ No data attribute in response")
            
    except Exception as e:
        print(f"  ❌ Error: {e}")

print("\n" + "="*60)
print("Test Complete")
print("="*60)