"""
Test FactSet API with correct imports from local SDK
"""

import os
import sys
sys.path.insert(0, '..')  # Add parent directory to path
from dotenv import load_dotenv

# Load environment
load_dotenv('../.env')

# Correct imports - it's 'model' (singular) not 'models'
import fds.sdk.FactSetFundamentals as ff
from fds.sdk.FactSetFundamentals.api.fact_set_fundamentals_api import FactSetFundamentalsApi
from fds.sdk.FactSetFundamentals.api.metrics_api import MetricsApi

# Use 'model' (singular) based on check_models.py output
from fds.sdk.FactSetFundamentals.model.fundamentals_request import FundamentalsRequest
from fds.sdk.FactSetFundamentals.model.fundamental_request_body import FundamentalRequestBody
from fds.sdk.FactSetFundamentals.model.ids_batch_max30000 import IdsBatchMax30000
from fds.sdk.FactSetFundamentals.model.fiscal_period import FiscalPeriod
from fds.sdk.FactSetFundamentals.model.periodicity import Periodicity

print("Testing FactSet API with correct local SDK imports...")
print("=" * 60)

# Setup API client
configuration = ff.Configuration(
    username=os.getenv('API_USERNAME'),
    password=os.getenv('API_PASSWORD')
)

# Add proxy if configured
proxy_url = os.getenv('PROXY_URL')
if proxy_url:
    proxy_user = os.getenv('PROXY_USER')
    proxy_password = os.getenv('PROXY_PASSWORD')
    if proxy_user and proxy_password:
        proxy_domain = os.getenv('PROXY_DOMAIN', 'MAPLE')
        # Format: http://DOMAIN\\user:pass@proxy:port
        import urllib.parse
        escaped_user = urllib.parse.quote(f"{proxy_domain}\\{proxy_user}")
        escaped_pass = urllib.parse.quote(proxy_password)
        configuration.proxy = f"http://{escaped_user}:{escaped_pass}@{proxy_url}"
        print(f"Proxy configured: {proxy_url}")

api_client = ff.ApiClient(configuration)
fundamentals_api = FactSetFundamentalsApi(api_client)
metrics_api = MetricsApi(api_client)

# Test 1: Get metrics
print("\n1. Testing metrics API...")
try:
    response = metrics_api.get_fds_fundamentals_metrics(category='INCOME_STATEMENT')
    if response and response.data:
        print(f"  ✅ Metrics API works! Got {len(response.data)} metrics")
        if len(response.data) > 0:
            first_metric = response.data[0]
            print(f"     Example metric: {getattr(first_metric, 'metric', 'N/A')} - {getattr(first_metric, 'name', 'N/A')}")
    else:
        print("  ❌ No metrics returned")
except Exception as e:
    print(f"  ❌ Metrics API error: {e}")

# Test 2: Test fundamentals with known ticker
print("\n2. Testing fundamentals API with Apple (AAPL)...")

test_tickers = ['AAPL-US', 'AAPL', 'AAPL-USA']

for ticker in test_tickers:
    try:
        print(f"\n  Testing ticker format: {ticker}")
        
        # Create IDs using IdsBatchMax30000
        ids = IdsBatchMax30000([ticker])
        print(f"    Created IDs object: {type(ids)}")
        
        # Create request body - note: metrics should be a list, not a model
        request_body = FundamentalRequestBody(
            ids=ids,
            metrics=['FF_SALES', 'FF_NET_INC']  # Plain list of metrics
        )
        print(f"    Created request body: {type(request_body)}")
        
        # Create request
        request = FundamentalsRequest(data=request_body)
        print(f"    Created request: {type(request)}")
        
        # Make API call
        response = fundamentals_api.get_fds_fundamentals_for_list(request)
        
        if response:
            print(f"    Response type: {type(response)}")
            if hasattr(response, 'data'):
                if response.data:
                    print(f"    ✅ SUCCESS! Got {len(response.data)} data points")
                    # Show first data point
                    if len(response.data) > 0:
                        first = response.data[0]
                        if hasattr(first, 'value'):
                            print(f"    Sample value: {first.value}")
                        if hasattr(first, 'metric'):
                            print(f"    Sample metric: {first.metric}")
                else:
                    print(f"    ❌ Empty data list")
            else:
                print(f"    ❌ No data attribute in response")
        else:
            print(f"    ❌ No response")
            
    except Exception as e:
        print(f"    ❌ Error: {e}")
        import traceback
        traceback.print_exc()

print("\n" + "=" * 60)
print("Test complete!")