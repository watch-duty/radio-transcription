"""
Utilities for authentication in Colab and Jupyter notebooks.
"""

import os
from getpass import getpass


def login_to_huggingface() -> bool:
    """
    Log in to Hugging Face using a token from environment variables, Colab Secrets, or interactive prompt.

    This function attempts to find a Hugging Face token in the following order:
    1. `HF_TOKEN` environment variable.
    2. Google Colab Secrets (if running in Colab).
    3. Secure interactive prompt using `getpass`.

    Returns:
        bool: True if login was successful, False otherwise.
    """
    try:
        from huggingface_hub import login
    except ImportError as e:
        raise ImportError(
            "login_to_huggingface requires the huggingface_hub package: "
            "pip install huggingface_hub"
        ) from e

    HF_TOKEN = os.environ.get("HF_TOKEN")

    if not HF_TOKEN:
        # Attempt to fetch from Google Colab Secrets
        try:
            from google.colab import userdata

            HF_TOKEN = userdata.get("HF_TOKEN")
        except Exception:
            pass

    if not HF_TOKEN:
        # Fallback to secure interactive prompt if running in a notebook/terminal
        print("HF_TOKEN not found in environment or Colab Secrets.")
        HF_TOKEN = getpass("Enter your Hugging Face token: ")

    if HF_TOKEN:
        try:
            login(token=HF_TOKEN)
            return True
        except Exception as e:
            print(f"Failed to log in to Hugging Face: {e}")
            return False

    print("No Hugging Face token provided.")
    return False
