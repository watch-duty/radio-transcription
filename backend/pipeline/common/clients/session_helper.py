from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

DEFAULT_STATUS_FORCELIST = [502, 503, 504]


def create_resilient_session(
    max_retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: list[int] | None = None,
    *,
    raise_on_status: bool = False,
) -> requests.Session:
    """
    Creates a requests.Session configured with exponential backoff retries
    for transient network and gateway faults.

    Args:
        max_retries: The maximum number of retries. If <= 0, retries are disabled.
        backoff_factor: Exponential backoff factor.
        status_forcelist: List of HTTP status codes to retry. Defaults to [502, 503, 504].
        raise_on_status: Whether to raise an exception immediately on matched status codes.
            Must be passed as a keyword argument.

    Returns:
        A requests.Session instance.
    """
    session = requests.Session()
    if max_retries > 0:
        retries = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist
            if status_forcelist is not None
            else DEFAULT_STATUS_FORCELIST,
            raise_on_status=raise_on_status,
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
    return session
