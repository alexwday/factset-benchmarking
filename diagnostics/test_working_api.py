"""
Test FactSet API with correct imports from local SDK
"""

import os
import sys
sys.path.insert(0, '..')  # Add parent directory to path
from dotenv import load_dotenv

# Load environment
load_dotenv('../.env')

# Correct imports based on check_models.py output
import fds.sdk.FactSetFundamentals as ff
from fds.sdk.FactSetFundamentals.api.fact_set_fundamentals_api import FactSetFundamentalsApi
from fds.sdk.FactSetFundamentals.api.metrics_api import MetricsApi

# Use 'models' (plural) not 'model'
from fds.sdk.FactSetFundamentals.models.fundamentals_request import FundamentalsRequest
from fds.sdk.FactSetFundamentals.models.fundamental_request_body import FundamentalRequestBody
from fds.sdk.FactSetFundamentals.models.ids_batch_max30000 import IdsBatchMax30000
from fds.sdk.FactSetFundamentals.models.fiscal_period import FiscalPeriod
from fds.sdk.FactSetFundamentals.models.periodicity import Periodicity

print("Testing FactSet API with correct local SDK imports...")
print("=" * 60)

# Setup API client
configuration = ff.Configuration(
    username=os.getenv('API_USERNAME'),
    password=os.getenv('API_PASSWORD')
)

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
        
        # Create request body - note: metrics should be a list, not a model
        request_body = FundamentalRequestBody(
            ids=ids,
            metrics=['FF_SALES', 'FF_NET_INC']  # Plain list of metrics
        )
        
        # Create request
        request = FundamentalsRequest(data=request_body)
        
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
                        print(f"    First item attributes: {dir(first)[:5]}...")
                else:
                    print(f"    ❌ Empty data list")
            else:
                print(f"    ❌ No data attribute in response")
        else:
            print(f"    ❌ No response")
            
    except Exception as e:
        print(f"    ❌ Error: {e}")

# Test 3: With date range
print("\n3. Testing with date range...")
try:
    # Create IDs
    ids = IdsBatchMax30000(['AAPL'])
    
    # Create fiscal period
    fiscal_period = FiscalPeriod(
        start='2023-01-01',
        end='2023-12-31'
    )
    
    # Create periodicity
    periodicity = Periodicity('QTR')
    
    # Create request body with all parameters
    request_body = FundamentalRequestBody(
        ids=ids,
        metrics=['FF_SALES'],
        fiscal_period=fiscal_period,
        periodicity=periodicity
    )
    
    request = FundamentalsRequest(data=request_body)
    response = fundamentals_api.get_fds_fundamentals_for_list(request)
    
    if response and hasattr(response, 'data') and response.data:
        print(f"  ✅ Date range request works! Got {len(response.data)} quarterly data points")
    else:
        print(f"  ❌ Date range request failed")
        
except Exception as e:
    print(f"  ❌ Date range error: {e}")

print("\n" + "=" * 60)
print("Test complete!")