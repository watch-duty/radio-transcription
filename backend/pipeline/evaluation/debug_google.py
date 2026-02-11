# debug_google.py
import sys
import os

print("--- DEBUG INFO ---")
print(f"Current Working Directory: {os.getcwd()}")
print(f"Python Path: {sys.path[0]}")

try:
    import google
    print(f"\n1. 'google' imported from: {google.__file__}")
except ImportError:
    print("\n1. Could not import 'google'")

try:
    import google.cloud
    print(f"2. 'google.cloud' imported from: {google.cloud.__file__}")
    
    if not hasattr(google.cloud, '__path__'):
        print("   >>> ALERT: 'google.cloud' is a FILE, not a package. This is the bug.")
except ImportError:
    print("2. Could not import 'google.cloud'")

print("\n--- CHECKING FOR LOCAL CONFLICTS ---")
# Check if common conflicting files exist in the current folder
conflicts = ['google.py', 'google/__init__.py', 'cloud.py', 'google/cloud.py']
for f in conflicts:
    if os.path.exists(f):
        print(f"   >>> FOUND CONFLICTING FILE: {f} <--- DELETE OR RENAME THIS")
    else:
        print(f"   (Safe) No {f} found here.")