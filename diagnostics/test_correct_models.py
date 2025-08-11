"""
Test FactSet API with ALL correct model types
"""

import os
import sys
sys.path.insert(0, '..')
from dotenv import load_dotenv
load_dotenv('../.env')

# Correct imports
import fds.sdk.FactSetFundamentals as ff
from fds.sdk.FactSetFundamentals.api.fact_set_fundamentals_api import FactSetFundamentalsApi
from fds.sdk.FactSetFundamentals.api.metrics_api import MetricsApi

# Import ALL the model types we need
from fds.sdk.FactSetFundamentals.model.fundamentals_request import FundamentalsRequest
from fds.sdk.FactSetFundamentals.model.fundamental_request_body import FundamentalRequestBody
from fds.sdk.FactSetFundamentals.model.ids_batch_max30000 import IdsBatchMax30000
from fds.sdk.FactSetFundamentals.model.metrics import Metrics
from fds.sdk.FactSetFundamentals.model.fiscal_period import FiscalPeriod
from fds.sdk.FactSetFundamentals.model.periodicity import Periodicity

print("Testing FactSet API with ALL correct model types...")
print("=" * 60)

# Setup API client (without proxy for now to simplify)
configuration = ff.Configuration(
    username=os.getenv('API_USERNAME'),
    password=os.getenv('API_PASSWORD')
)

api_client = ff.ApiClient(configuration)
fundamentals_api = FactSetFundamentalsApi(api_client)

print("\nTesting fundamentals API with correct model types...")

test_tickers = ['AAPL-US', 'AAPL', 'FDS-US', 'FDS']

for ticker in test_tickers:
    try:
        print(f"\nTesting ticker: {ticker}")
        
        # Create IDs model
        ids = IdsBatchMax30000([ticker])
        print(f"  ✓ Created IDs model")
        
        # Create Metrics model (not a plain list!)
        metrics = Metrics(['FF_SALES', 'FF_NET_INC'])
        print(f"  ✓ Created Metrics model")
        
        # Create request body with both models
        request_body = FundamentalRequestBody(
            ids=ids,
            metrics=metrics  # Using Metrics model now
        )
        print(f"  ✓ Created request body")
        
        # Create request
        request = FundamentalsRequest(data=request_body)
        print(f"  ✓ Created request")
        
        # Make API call
        response = fundamentals_api.get_fds_fundamentals_for_list(request)
        
        if response:
            if hasattr(response, 'data') and response.data:
                print(f"  ✅ SUCCESS! Got {len(response.data)} data points")
                if len(response.data) > 0:
                    first = response.data[0]
                    print(f"     First item type: {type(first)}")
                    if hasattr(first, 'value'):
                        print(f"     Value: {first.value}")
                    if hasattr(first, 'metric'):
                        print(f"     Metric: {first.metric}")
                    if hasattr(first, 'date'):
                        print(f"     Date: {first.date}")
            else:
                print(f"  ❌ No data in response")
        else:
            print(f"  ❌ No response")
            
    except Exception as e:
        print(f"  ❌ Error: {e}")

print("\n" + "=" * 60)
print("\nNow testing with date range and periodicity...")

try:
    # Use the most likely working ticker
    ticker = 'AAPL-US'
    
    # Create all model objects
    ids = IdsBatchMax30000([ticker])
    metrics = Metrics(['FF_SALES', 'FF_NET_INC', 'FF_EPS'])
    fiscal_period = FiscalPeriod(start='2023-01-01', end='2023-12-31')
    periodicity = Periodicity('QTR')
    
    print(f"Testing {ticker} with quarterly data for 2023...")
    
    # Create request with all parameters
    request_body = FundamentalRequestBody(
        ids=ids,
        metrics=metrics,
        fiscal_period=fiscal_period,
        periodicity=periodicity
    )
    
    request = FundamentalsRequest(data=request_body)
    response = fundamentals_api.get_fds_fundamentals_for_list(request)
    
    if response and hasattr(response, 'data') and response.data:
        print(f"✅ Full request works! Got {len(response.data)} quarterly data points")
        
        # Show data structure
        for i, item in enumerate(response.data[:3]):  # Show first 3
            print(f"\nData point {i+1}:")
            if hasattr(item, '__dict__'):
                for key, value in item.__dict__.items():
                    if not key.startswith('_'):
                        print(f"  {key}: {value}")
    else:
        print(f"❌ Full request failed")
        
except Exception as e:
    print(f"❌ Error with full request: {e}")

print("\n" + "=" * 60)
print("Testing complete!")