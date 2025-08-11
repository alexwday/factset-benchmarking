"""
Inspect FactSet SDK structure
"""

import fds.sdk.FactSetFundamentals as ff
import pkgutil

print("FactSet Fundamentals SDK Modules:")
print("=" * 60)

# List all submodules
for importer, modname, ispkg in pkgutil.walk_packages(ff.__path__, ff.__name__ + '.'):
    print(modname)

print("\n" + "=" * 60)
print("Top-level attributes:")
for attr in dir(ff):
    if not attr.startswith('_'):
        print(f"  - {attr}")