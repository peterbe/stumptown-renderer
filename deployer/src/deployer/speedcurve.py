import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from .utils import log

POST_URL = "https://api.speedcurve.com/v1/deploys"


def deploy_ping(
    api_key: str, site_id: str, note: str, detail: str, dry_run: bool = False
):
    """Based on https://api.speedcurve.com/#add-a-deploy"""
    data = {
        "site_id": site_id,
    }
    if note:
        data["note"] = note
    if detail:
        data["detail"] = detail

    if dry_run:
        log.info(f"Posting {data} to {POST_URL} with API key {api_key[:3] + '...'!r}")
        return

    adapter = HTTPAdapter(
        max_retries=Retry(
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["POST"],
        )
    )
    session = requests.Session()
    session.mount("https://", adapter)
    auth = (api_key, "x")
    response = session.post(POST_URL, data=data, auth=auth)
    response.raise_for_status()
    log.info(response.json())
