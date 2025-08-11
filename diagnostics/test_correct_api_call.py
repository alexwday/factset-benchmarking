"""
Test FactSet API with the correct model types
"""

import os
from dotenv import load_dotenv
import fds.sdk.FactSetFundamentals as ff
from fds.sdk.FactSetFundamentals.api.fact_set_fundamentals_api import FactSetFundamentalsApi
from fds.sdk.FactSetFundamentals.models.fundamentals_request import FundamentalsRequest
from fds.sdk.FactSetFundamentals.models.fundamental_request_body import FundamentalRequestBody
from fds.sdk.FactSetFundamentals.models.ids import Ids
from fds.sdk.FactSetFundamentals.models.metrics import Metrics
from fds.sdk.FactSetFundamentals.models.fiscal_period import FiscalPeriod

load_dotenv()

# Setup API client
configuration = ff.Configuration(
    username=os.getenv('API_USERNAME'),
    password=os.getenv('API_PASSWORD')
)

api_client = ff.ApiClient(configuration)
api = FactSetFundamentalsApi(api_client)

print("Testing FactSet API with correct model types...")
print("=" * 60)

# Test different ID formats
test_tickers = [
    'AAPL-US',
    'AAPL',
    'JPM-US', 
    'JPM',
    'RY-CA',
    'RY',
    'FDS-US',  # FactSet's own ticker
    'FDS'
]

for ticker in test_tickers:
    try:
        print(f"\nTesting ticker: {ticker}")
        
        # Create Ids model object
        ids_obj = Ids([ticker])
        
        # Create Metrics model object
        metrics_obj = Metrics(['FF_SALES'])
        
        # Create FiscalPeriod object
        fiscal_period = FiscalPeriod(
            start='2023-01-01',
            end='2023-12-31'
        )
        
        # Create request body with model objects
        request_body = FundamentalRequestBody(
            ids=ids_obj,
            metrics=metrics_obj,
            fiscal_period=fiscal_period
        )
        
        # Create request
        request = FundamentalsRequest(data=request_body)
        
        # Make API call
        response = api.get_fds_fundamentals_for_list(request)
        
        if response and hasattr(response, 'data') and response.data:
            print(f"  ✅ SUCCESS! Got {len(response.data)} data points")
            # Print first data point details
            if len(response.data) > 0:
                first = response.data[0]
                print(f"     Sample: {first}")
        else:
            print(f"  ❌ No data returned")
            
    except Exception as e:
        print(f"  ❌ Error: {e}")

print("\n" + "=" * 60)
print("\nNow testing without the model wrappers (just lists):")

# Try without wrapping in model objects
for ticker in ['AAPL', 'JPM', 'FDS']:
    try:
        print(f"\nTesting {ticker} with plain lists...")
        
        # Try with just lists (this might be what actually works)
        request_body = FundamentalRequestBody(
            ids=[ticker],  # Plain list
            metrics=['FF_SALES', 'FF_NET_INC']  # Plain list
        )
        
        request = FundamentalsRequest(data=request_body)
        response = api.get_fds_fundamentals_for_list(request)
        
        if response and hasattr(response, 'data') and response.data:
            print(f"  ✅ SUCCESS with plain lists! Got {len(response.data)} data points")
        else:
            print(f"  ❌ No data with plain lists")
            
    except Exception as e:
        print(f"  ❌ Error with plain lists: {e}")