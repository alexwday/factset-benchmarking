"""
Check what models are available in the FactSet SDK
"""

import fds.sdk.FactSetFundamentals as ff

print("Checking FactSet Fundamentals SDK structure...")
print("=" * 60)

# Check for models module
print("\n1. Checking for models module:")
if hasattr(ff, 'models'):
    print("  ✓ ff.models exists")
    print("  Available models:", dir(ff.models))
elif hasattr(ff, 'model'):
    print("  ✓ ff.model exists")
    print("  Available models:", dir(ff.model))
else:
    print("  ✗ No models/model module found")

# Try importing directly
print("\n2. Trying direct imports:")
try:
    from fds.sdk.FactSetFundamentals.models import FundamentalsRequest
    print("  ✓ Can import from .models")
except ImportError:
    try:
        from fds.sdk.FactSetFundamentals.model import FundamentalsRequest
        print("  ✓ Can import from .model (singular)")
    except ImportError:
        print("  ✗ Cannot import FundamentalsRequest")

# Check what's in the model module
print("\n3. Checking model contents:")
try:
    from fds.sdk.FactSetFundamentals import model
    print("  Available in model module:")
    for item in dir(model):
        if not item.startswith('_'):
            print(f"    - {item}")
except Exception as e:
    print(f"  Error: {e}")

# Check for the specific types we need
print("\n4. Checking for specific model types:")
models_to_check = [
    'ids',
    'ids_batch_max30000',
    'fundamental_request_body',
    'fundamentals_request',
    'fiscal_period',
    'periodicity',
    'metrics'
]

for model_name in models_to_check:
    try:
        module = __import__(f'fds.sdk.FactSetFundamentals.model.{model_name}', fromlist=[''])
        print(f"  ✓ {model_name} module exists")
        # Check what's in it
        for item in dir(module):
            if not item.startswith('_') and item != model_name:
                obj = getattr(module, item)
                if isinstance(obj, type):
                    print(f"      Class: {item}")
    except ImportError:
        print(f"  ✗ {model_name} not found")

print("\n5. Testing IdsBatchMax30000 model:")
try:
    from fds.sdk.FactSetFundamentals.model.ids_batch_max30000 import IdsBatchMax30000
    print("  ✓ Found IdsBatchMax30000")
    
    # Try creating an instance
    ids = IdsBatchMax30000(['AAPL', 'JPM'])
    print(f"  ✓ Created IdsBatchMax30000 instance: {type(ids)}")
except Exception as e:
    print(f"  ✗ Error with IdsBatchMax30000: {e}")