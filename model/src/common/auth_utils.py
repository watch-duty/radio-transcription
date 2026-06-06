"""Utilities for authentication in Colab and Jupyter notebooks."""

import logging
import os
from getpass import getpass

logger = logging.getLogger(__name__)

try:
    from huggingface_hub import login as hf_login
except ImportError as _e:
    _HF_MISSING = _e
    hf_login = None
else:
    _HF_MISSING = None

try:
    from google.colab import userdata as colab_userdata
except Exception as _e:
    _COLAB_USERDATA_MISSING = _e
    colab_userdata = None
else:
    _COLAB_USERDATA_MISSING = None


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
    if _HF_MISSING:
        msg = (
            "login_to_huggingface requires the huggingface_hub package: "
            "pip install huggingface_hub"
        )
        raise ImportError(msg) from _HF_MISSING

    HF_TOKEN = os.environ.get("HF_TOKEN")

    if not HF_TOKEN and colab_userdata is not None:
        # Attempt to fetch from Google Colab Secrets
        try:
            HF_TOKEN = colab_userdata.get("HF_TOKEN")
        except Exception:
            logger.debug(
                "Unable to read HF_TOKEN from Colab Secrets", exc_info=True
            )
    elif not HF_TOKEN and _COLAB_USERDATA_MISSING is not None:
        logger.debug(
            "Colab Secrets are unavailable: %s", _COLAB_USERDATA_MISSING
        )

    if not HF_TOKEN:
        # Fallback to secure interactive prompt if running in a notebook/terminal
        HF_TOKEN = getpass("Enter your Hugging Face token: ")

    if not HF_TOKEN:
        return False

    try:
        hf_login(token=HF_TOKEN)
    except Exception:
        return False
    else:
        return True
