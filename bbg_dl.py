from datetime import timedelta
import random
from time import sleep
from oauthlib.oauth2 import BackendApplicationClient
import requests
from requests_oauthlib import OAuth2Session
from urllib.parse import urljoin
import pandas as pd
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class RequestAdapter(requests.adapters.HTTPAdapter):
    """
    Custom HTTPAdapter to handle requests with OAuth2 session.
    """

    def send(self, request, **kwargs):
        request.headers["api-version"] = 2
        return super().send(request, **kwargs)


credentials = {
    "client_id": "08f7f83fa87cd8653a04f1e3aa83005f",
    "client_secret": "485043173871d481500b1e8dd6c8ffe1fdcb635e7905cc7551c3096bd69bcd42",
    "name": "mosaic_bbg_credentials",
    "scopes": ["dlrest-high-rate-limit", "dlrest-medium-rate-limit", "reportingapi"],
    "expiration_date": 1764772281015,
    "created_date": 1717421481015,
}


CLIENT = BackendApplicationClient(client_id=credentials["client_id"])
OAUTH2_ENDPOINT = "https://bsso.blpprofessional.com/ext/api/as/token.oauth2"
SESSION = OAuth2Session(client=CLIENT)
SESSION.mount("https://", RequestAdapter())
TOKEN = SESSION.fetch_token(
    token_url=OAUTH2_ENDPOINT,
    client_id=credentials["client_id"],
    client_secret=credentials["client_secret"],
    include_client_id=True,
)

HOST = "https://api.bloomberg.com"


def get_catalog_id():
    ############################################################################
    # - Discover catalog identifier for scheduling requests.
    catalogs_url = urljoin(HOST, "/eap/catalogs/")
    response = SESSION.get(catalogs_url)

    # We got back a good response. Let's extract our account number.
    catalogs = response.json()["contains"]
    for catalog in catalogs:
        if catalog["subscriptionType"] == "scheduled":
            # Take the catalog having "scheduled" subscription type,
            # which corresponds to the Data License account number.
            catalog_id = catalog["identifier"]
            return catalog_id
            break
    else:
        # We exhausted the catalogs, but didn't find a non-'bbg' catalog.
        logger.error("Scheduled catalog not in %r", response.json()["contains"])
        raise RuntimeError("Scheduled catalog not found")


CATALOG_ID = get_catalog_id()
CATALOG_URL = urljoin(HOST, "/eap/catalogs/{c}/".format(c=CATALOG_ID))
REQUESTS_URL = urljoin(CATALOG_URL, "requests/")
RESPONSES_URL = urljoin(
    HOST, "/eap/catalogs/{c}/content/responses/".format(c=CATALOG_ID)
)

# payload = create_request_payload(TICKERS)
# request_payload = {
#     '@type': 'HistoryRequest',
#     'name': request_name,
#     'description': 'Some description',
#     'universe': {
#         '@type': 'Universe',
#         'contains' : payload
#     },
#     'fieldList': {
#         '@type': 'HistoryFieldList',
#         'contains': [
#             {'mnemonic': 'NAME'},
#             {'mnemonic': 'SECURITY_TYP'},
#             {'mnemonic': 'PX_SETTLE'},
#             {'mnemonic': 'PX_BID'},
#             {'mnemonic': 'PX_MID'},
#             {'mnemonic': 'PX_ASK'},
#             {'mnemonic': 'PX_LAST'},
#             {'mnemonic': 'PX_HIGH'}
#         ],
#     },
#     'trigger': {
#         "@type": "SubmitTrigger",
#     },
#     'runtimeOptions': {
#         '@type': 'HistoryRuntimeOptions',
#         'dateRange': {
#             '@type': 'IntervalDateRange',
#             'startDate': '2024-01-01',
#             'endDate': '2024-02-01'
#         },
#     },
#     'formatting': {
#         '@type': 'MediaType',
#         'outputMediaType': 'application/json',
#     }


def get_utc():
    """
    Get the current UTC time in ISO 8601 format.

    Returns:
        str: The current UTC time as a string.
    """
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)


def download(request_payload):
    """
    Download data from Bloomberg using the provided request payload.

    Args:
        request_payload (dict): The payload containing the request parameters.

    Returns:
        dict: The response from the Bloomberg API.
    """

    response = SESSION.post(REQUESTS_URL, json=request_payload)
    if "Location" not in response.headers:
        print("[ERROR] No 'Location' header in response. Status code:", response.status_code)
        print("[ERROR] Response content:", response.text)
        response.raise_for_status()
        return None
    request_location = response.headers["Location"]
    request_url = urljoin(HOST, request_location)
    SESSION.get(request_url)

    params = {
        "prefix": request_payload["name"],
    }

    expiration = get_utc() + timedelta(minutes=5)
    while get_utc() < expiration:
        response = SESSION.get(RESPONSES_URL, params=params)
        if response.status_code == 200:
            responses = response.json()["contains"]
            if responses:
                output_url = urljoin(
                    HOST,
                    "/eap/catalogs/{c}/content/responses/{key}".format(
                        c=CATALOG_ID, key=responses[0]["key"]
                    ),
                )
                with SESSION.get(output_url, stream=True) as response:
                    if response.status_code == 200:
                        content = pd.read_csv(response.raw, compression="gzip")
                        logger.info("Data downloaded successfully.")
                        return content
                    else:
                        logger.error("Error downloading data: %s", response.text)
                        break

            else:
                # No responses yet, let's wait and try again.
                wait = random.choice([1, 3, 5, 7, 11, 13, 17, 19])
                logger.info(f"No responses yet, waiting {wait} seconds...")
                sleep(wait)  # Wait for a while before retrying
        else:
            logger.error("Error fetching responses: %s", response.text)
            break
    return None
