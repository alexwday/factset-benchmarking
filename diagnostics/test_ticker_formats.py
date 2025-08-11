"""
Test script to find the correct ticker format for FactSet API
"""

import os
from dotenv import load_dotenv
import fds.sdk.FactSetFundamentals as ff
from fds.sdk.FactSetFundamentals.api.fact_set_fundamentals_api import FactSetFundamentalsApi
from fds.sdk.FactSetFundamentals.model.fundamentals_request import FundamentalsRequest
from fds.sdk.FactSetFundamentals.model.fundamental_request_body import FundamentalRequestBody
from fds.sdk.FactSetFundamentals.model.fiscal_period import FiscalPeriod
from fds.sdk.FactSetFundamentals.model.periodicity import Periodicity

load_dotenv()

# Test different ticker formats for JPMorgan
test_tickers = [
    'JPM-US',           # Original format
    'JPM',              # Simple ticker
    'JPM-NYSE',         # Exchange suffix
    'JPM.N',            # Reuters format
    'NYSE:JPM',         # Exchange prefix
    'JPM US',           # Bloomberg format
    'JPM US Equity',    # Bloomberg extended
    '46625H100',        # CUSIP
    'US46625H1005',     # ISIN
    'JPM-USA',          # USA suffix
]

# Setup API client
configuration = ff.Configuration(
    username=os.getenv('API_USERNAME'),
    password=os.getenv('API_PASSWORD')
)

api_client = ff.ApiClient(configuration)
fundamentals_api = FactSetFundamentalsApi(api_client)

print("Testing ticker formats for JPMorgan Chase...")
print("=" * 60)

for ticker in test_tickers:
    try:
        # Create request for a simple metric
        fiscal_period_obj = FiscalPeriod(
            start='2023-01-01',
            end='2023-12-31'
        )
        
        periodicity_obj = Periodicity('QTR')
        
        request_body = FundamentalRequestBody(
            ids=[ticker],
            metrics=['FF_SALES'],  # Just test with revenue
            fiscal_period=fiscal_period_obj,
            periodicity=periodicity_obj
        )
        
        request = FundamentalsRequest(data=request_body)
        response = fundamentals_api.get_fds_fundamentals_for_list(request)
        
        if response and response.data and len(response.data) > 0:
            print(f"✅ {ticker:20} - SUCCESS! Got {len(response.data)} data points")
        else:
            print(f"❌ {ticker:20} - No data returned")
            
    except Exception as e:
        print(f"❌ {ticker:20} - Error: {str(e)[:50]}")

print("\n" + "=" * 60)
print("\nNow testing some Canadian banks with different formats:")
print("=" * 60)

canadian_tests = [
    'RY-CA',            # Original
    'RY',               # Simple
    'RY-TSE',           # Toronto Stock Exchange
    'RY.TO',            # TSX suffix
    'TSE:RY',           # Exchange prefix
    'RY CN',            # Bloomberg Canada
    'RY CN Equity',     # Bloomberg extended
]

for ticker in canadian_tests:
    try:
        fiscal_period_obj = FiscalPeriod(
            start='2023-01-01',
            end='2023-12-31'
        )
        
        periodicity_obj = Periodicity('QTR')
        
        request_body = FundamentalRequestBody(
            ids=[ticker],
            metrics=['FF_SALES'],
            fiscal_period=fiscal_period_obj,
            periodicity=periodicity_obj
        )
        
        request = FundamentalsRequest(data=request_body)
        response = fundamentals_api.get_fds_fundamentals_for_list(request)
        
        if response and response.data and len(response.data) > 0:
            print(f"✅ {ticker:20} - SUCCESS! Got {len(response.data)} data points")
        else:
            print(f"❌ {ticker:20} - No data returned")
            
    except Exception as e:
        print(f"❌ {ticker:20} - Error: {str(e)[:50]}")