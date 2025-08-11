"""
Test to determine the correct FactSet ID format
Based on FactSet documentation, they typically accept:
- Exchange ticker format: TICKER-EXCHANGE (e.g., AAPL-NAS, RY-TOR)
- FactSet entity IDs
- SEDOL, ISIN, CUSIP
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

print("Testing FactSet ID formats based on documentation...")
print("=" * 60)

# Test US companies with exchange codes
us_test_ids = [
    'AAPL-NAS',       # Apple - NASDAQ
    'JPM-NYS',        # JPMorgan - NYSE
    'BAC-NYS',        # Bank of America - NYSE
    'WFC-NYS',        # Wells Fargo - NYSE
    'GS-NYS',         # Goldman Sachs - NYSE
    'MS-NYS',         # Morgan Stanley - NYSE
]

print("\nTesting US companies with exchange codes:")
for test_id in us_test_ids:
    try:
        request_body = FundamentalRequestBody(
            ids=[test_id],
            metrics=['FF_SALES', 'FF_NET_INC']  # Revenue and Net Income
        )
        
        request = FundamentalsRequest(data=request_body)
        response = api.get_fds_fundamentals_for_list(request)
        
        if response and hasattr(response, 'data') and response.data:
            print(f"✅ {test_id:15} - SUCCESS! Got {len(response.data)} data points")
        else:
            print(f"❌ {test_id:15} - No data returned")
            
    except Exception as e:
        error_msg = str(e)[:100]
        print(f"❌ {test_id:15} - Error: {error_msg}")

# Test Canadian companies with Toronto exchange
canadian_test_ids = [
    'RY-TOR',         # Royal Bank - Toronto
    'TD-TOR',         # TD Bank - Toronto
    'BMO-TOR',        # Bank of Montreal - Toronto
    'BNS-TOR',        # Bank of Nova Scotia - Toronto
    'CM-TOR',         # CIBC - Toronto
    'NA-TOR',         # National Bank - Toronto
]

print("\nTesting Canadian companies with Toronto exchange:")
for test_id in canadian_test_ids:
    try:
        request_body = FundamentalRequestBody(
            ids=[test_id],
            metrics=['FF_SALES', 'FF_NET_INC']
        )
        
        request = FundamentalsRequest(data=request_body)
        response = api.get_fds_fundamentals_for_list(request)
        
        if response and hasattr(response, 'data') and response.data:
            print(f"✅ {test_id:15} - SUCCESS! Got {len(response.data)} data points")
        else:
            print(f"❌ {test_id:15} - No data returned")
            
    except Exception as e:
        error_msg = str(e)[:100]
        print(f"❌ {test_id:15} - Error: {error_msg}")

# Also test some alternative formats
print("\nTesting alternative formats:")
alternative_ids = [
    'AAPL-US',        # Country code instead of exchange
    'AAPL',           # Just ticker
    '037833100',      # Apple CUSIP
    'US0378331005',   # Apple ISIN
]

for test_id in alternative_ids:
    try:
        request_body = FundamentalRequestBody(
            ids=[test_id],
            metrics=['FF_SALES']
        )
        
        request = FundamentalsRequest(data=request_body)
        response = api.get_fds_fundamentals_for_list(request)
        
        if response and hasattr(response, 'data') and response.data:
            print(f"✅ {test_id:15} - SUCCESS! Got {len(response.data)} data points")
        else:
            print(f"❌ {test_id:15} - No data returned")
            
    except Exception as e:
        error_msg = str(e)[:100]
        print(f"❌ {test_id:15} - Error: {error_msg}")

print("\n" + "=" * 60)
print("\nBased on results above, update the banks_config.py file with the working format.")