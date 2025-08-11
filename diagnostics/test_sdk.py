"""
Test script to validate FactSet SDK structure and methods
"""

import fds.sdk.FactSetFundamentals as ff
from fds.sdk.FactSetFundamentals.api import metrics_api, factset_fundamentals_api
from fds.sdk.FactSetFundamentals.models import (
    fundamentals_request,
    fundamental_request_body,
    fiscal_period,
    periodicity
)
import inspect

print("=" * 60)
print("FactSet Fundamentals SDK Structure Analysis")
print("=" * 60)

# Check available modules
print("\n1. Available modules in SDK:")
print(dir(ff))

# Check API classes
print("\n2. MetricsApi methods:")
metrics_api_class = metrics_api.MetricsApi
for method in dir(metrics_api_class):
    if not method.startswith('_'):
        print(f"  - {method}")

print("\n3. FactSetFundamentalsApi methods:")
fundamentals_api_class = factset_fundamentals_api.FactSetFundamentalsApi
for method in dir(fundamentals_api_class):
    if not method.startswith('_'):
        print(f"  - {method}")

# Check models
print("\n4. Available models:")
try:
    from fds.sdk.FactSetFundamentals import models
    for item in dir(models):
        if not item.startswith('_'):
            print(f"  - {item}")
except Exception as e:
    print(f"  Error loading models: {e}")

# Check request body structure
print("\n5. FundamentalRequestBody parameters:")
try:
    from fds.sdk.FactSetFundamentals.models.fundamental_request_body import FundamentalRequestBody
    print("  Found FundamentalRequestBody")
    sig = inspect.signature(FundamentalRequestBody.__init__)
    for param in sig.parameters:
        if param != 'self':
            print(f"    - {param}")
except Exception as e:
    print(f"  Error: {e}")

# Check FiscalPeriod structure
print("\n6. FiscalPeriod parameters:")
try:
    from fds.sdk.FactSetFundamentals.models.fiscal_period import FiscalPeriod
    print("  Found FiscalPeriod")
    sig = inspect.signature(FiscalPeriod.__init__)
    for param in sig.parameters:
        if param != 'self':
            print(f"    - {param}")
except Exception as e:
    print(f"  Error: {e}")

# Check Configuration
print("\n7. Configuration class:")
try:
    config = ff.Configuration()
    print("  Configuration attributes:")
    for attr in dir(config):
        if not attr.startswith('_') and not callable(getattr(config, attr)):
            print(f"    - {attr}")
except Exception as e:
    print(f"  Error: {e}")

print("\n" + "=" * 60)