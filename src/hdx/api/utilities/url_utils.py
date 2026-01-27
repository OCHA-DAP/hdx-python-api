import re

from hdx.utilities.session import get_session
from requests import RequestException, Session

import hdx.data.dataset
import hdx.data.resource
from hdx.api.configuration import Configuration

GOOGLE_DRIVE_URL = re.compile(r"^https?://drive.google.com/open\?id=([0-9A-Za-z_-]+)$")
GOOGLE_SHEETS_URL = re.compile(
    r"^https?://[^/]+google.com/.*[^0-9A-Za-z_-]([0-9A-Za-z_-]{44})(?:.*gid=([0-9]+))?.*$"
)
GOOGLE_SHEETS_XLSX_URL = re.compile(
    r"^https?://[^/]+google.com/.*[^0-9A-Za-z_-]([0-9A-Za-z_-]{33})(?:.*gid=([0-9]+))?.*$"
)
GOOGLE_FILE_URL = re.compile(r"https?://drive.google.com/file/d/([0-9A-Za-z_-]+)/.*$")
DROPBOX_URL = re.compile(r"^https://www.dropbox.com/s/([0-9a-z]{15})/([^?]+)\?dl=[01]$")
CKAN_URL = re.compile(
    r"^(https?://[^/]+)/dataset/([^/]+)(?:/resource/([a-z0-9-]{36}))?$"
)


def get_ckan_ready_session(configuration: Configuration) -> Session:
    """Get Session object setup to access CKAN

    Args:
        configuration: Configuration object

    Returns:
        Session object setup to access CKAN
    """
    apikey = configuration.get_api_key()
    if apikey:
        headers = {"Authorization": apikey}
    else:
        headers = None
    session = get_session(
        full_agent=configuration.get_user_agent(), use_env=False, headers=headers
    )
    return session


def follow_url(
    url: str, configuration: Configuration | None = None, session: Session | None = None
) -> str:
    """Follow a URL to get the direct-download URL after any redirects. Either a
    Configuration object or Session object must be provided

    Args:
        url: URL to follow
        configuration: Configuration object. Defaults to None.
        session: Session object to use. Defaults to None.

    Returns:
        The direct-download URL

    """
    try:
        if session is None:
            if configuration is None:
                raise ValueError(
                    "Either Session or Configuration object must be provided!"
                )
            session = get_ckan_ready_session(configuration)

        # Is it a CKAN resource? (Assumes the v.3 API for now)
        result = CKAN_URL.match(url)
        if result:
            result = _get_ckan_urls(result.group(2), result.group(3))
            if result:
                return result

        # Is it a Google Drive "open" URL?
        result = GOOGLE_DRIVE_URL.match(url)
        if result:
            response = session.head(url)
            if response.is_redirect:
                return response.headers["Location"]

        #
        # Stage 2: rewrite URLs to get direct-download links
        #

        # Is it a Google *Sheet*?
        result = GOOGLE_SHEETS_URL.match(url)
        if result and not re.search(r"/pub", url):
            if result.group(2):
                return f"https://docs.google.com/spreadsheets/d/{result.group(1)}/export?format=csv&gid={result.group(2)}"
            return f"https://docs.google.com/spreadsheets/d/{result.group(1)}/export?format=csv"

        # Is it a Google Drive *file*?
        result = GOOGLE_FILE_URL.match(url)
        if not result:
            result = GOOGLE_SHEETS_XLSX_URL.match(url)
        if result and not re.search(r"/pub", url):
            return f"https://drive.google.com/uc?export=download&id={result.group(1)}"

        # Is it a Dropbox URL?
        result = DROPBOX_URL.match(url)
        if result:
            return f"https://www.dropbox.com/s/{result.group(1)}/{result.group(2)}?dl=1"
    except RequestException:
        pass
    # No changes
    return url


def _get_ckan_urls(dataset_id: str, resource_id: str) -> str | None:
    """Look up a CKAN download URL starting from a dataset or resource page

    If the link is to a dataset page, try the first resource. If it's
    to a resource page, look up the resource's download link. Either
    dataset_id or resource_id is required (will prefer resource_id
    over dataset_id).

    Args:
        dataset_id: the CKAN dataset ID, or None if unavailable
        resource_id: the CKAN resource ID, or None if unavailable

    Returns:
        The direct-download URL for the CKAN dataset
    """
    if resource_id:
        resource = hdx.data.resource.Resource.read_from_hdx(resource_id)
        if resource:
            return resource["url"]

    dataset = hdx.data.dataset.Dataset.read_from_hdx(dataset_id)
    if not dataset:
        return None
    for resource in dataset.get_resources():
        if url := resource.get("url"):
            return url
    return None
