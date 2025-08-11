"""
Test the most basic FactSet API call to verify connectivity and ID format
"""

import os
from dotenv import load_dotenv
import fds.sdk.FactSetFundamentals as ff
from fds.sdk.FactSetFundamentals.api.fact_set_fundamentals_api import FactSetFundamentalsApi
from fds.sdk.FactSetFundamentals.model.fundamentals_request import FundamentalsRequest
from fds.sdk.FactSetFundamentals.model.fundamental_request_body import FundamentalRequestBody

load_dotenv()

# Setup API client
configuration = ff.Configuration(
    username=os.getenv('API_USERNAME'),
    password=os.getenv('API_PASSWORD')
)

api_client = ff.ApiClient(configuration)
api = FactSetFundamentalsApi(api_client)

print("Testing basic FactSet Fundamentals API call...")
print("=" * 60)

# Test with various ID formats for Apple as a known good ticker
test_ids = [
    'AAPL',           # Simple ticker
    'AAPL-US',        # With country
    'AAPL-USA',       # Alternative country
    'AAPL.O',         # With exchange
    'AAPL-NAS',       # NASDAQ
    'US0378331005',   # ISIN
    '037833100',      # CUSIP
    'AAPL US',        # Bloomberg style
]

for test_id in test_ids:
    try:
        print(f"\nTesting ID: {test_id}")
        
        # Create the most basic request - no dates, just current data
        request_body = FundamentalRequestBody(
            ids=[test_id],
            metrics=['FF_SALES']  # Just revenue
        )
        
        request = FundamentalsRequest(data=request_body)
        
        # Make API call
        response = api.get_fds_fundamentals_for_list(request)
        
        if response:
            print(f"  Response object type: {type(response)}")
            if hasattr(response, 'data'):
                print(f"  Data attribute exists: {response.data is not None}")
                if response.data:
                    print(f"  Number of data points: {len(response.data)}")
                    if len(response.data) > 0:
                        first_item = response.data[0]
                        print(f"  First item type: {type(first_item)}")
                        if hasattr(first_item, '__dict__'):
                            print(f"  First item attributes: {first_item.__dict__.keys()}")
                        print(f"  ✅ SUCCESS - Got data!")
                else:
                    print(f"  ❌ No data in response")
            else:
                print(f"  Response attributes: {dir(response)}")
        else:
            print(f"  ❌ No response object")
            
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")

print("\n" + "=" * 60)
print("\nNow let's test the exact way we're creating the request in our script:")

# Test the exact way our script creates requests
try:
    from fds.sdk.FactSetFundamentals.model.fiscal_period import FiscalPeriod
    from fds.sdk.FactSetFundamentals.model.periodicity import Periodicity
    
    fiscal_period_obj = FiscalPeriod(
        start='2023-01-01',
        end='2023-12-31'
    )
    
    periodicity_obj = Periodicity('QTR')
    
    request_body = FundamentalRequestBody(
        ids=['AAPL'],
        metrics=['FF_SALES'],
        fiscal_period=fiscal_period_obj,
        periodicity=periodicity_obj
    )
    
    print(f"Request body type: {type(request_body)}")
    print(f"Request body attributes: {dir(request_body)}")
    
    request = FundamentalsRequest(data=request_body)
    response = api.get_fds_fundamentals_for_list(request)
    
    if response and response.data:
        print(f"✅ Full request with dates works! Got {len(response.data)} data points")
    else:
        print(f"❌ Full request with dates failed")
        
except Exception as e:
    print(f"❌ Error with full request: {e}")