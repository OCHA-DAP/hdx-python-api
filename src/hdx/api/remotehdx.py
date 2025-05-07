"""Connection to HDX with rate limiting
Currently unused awaiting HDX server to add Retry-After header
"""

import logging
from time import sleep

from ckanapi import RemoteCKAN

logger = logging.getLogger(__name__)


class RemoteHDX(RemoteCKAN):
    def _request_fn(self, url, data, headers, files, requests_kwargs):
        r = self.session.post(
            url,
            data=data,
            headers=headers,
            files=files,
            allow_redirects=False,
            **requests_kwargs,
        )
        # allow_redirects=False because: if a post is redirected (e.g. 301 due
        # to a http to https redirect), then the second request is made to the
        # new URL, but *without* the data. This gives a confusing "No request
        # body data" error. It is better to just return the 301 to the user, so
        # we disallow redirects.
        if r.status_code == 429:
            retryafter = r.headers.get("Retry-After")
            if retryafter:
                logger.warning(
                    f"429 Too Many Requests response. Sleeping for {retryafter} seconds."
                )
                sleep(retryafter)
                r = self.session.post(
                    url,
                    data=data,
                    headers=headers,
                    files=files,
                    allow_redirects=False,
                    **requests_kwargs,
                )
        return r.status_code, r.text

    def _request_fn_get(self, url, data_dict, headers, requests_kwargs):
        r = self.session.get(url, params=data_dict, headers=headers, **requests_kwargs)
        if r.status_code == 429:
            retryafter = r.headers.get("Retry-After")
            if retryafter:
                logger.warning(
                    f"429 Too Many Requests response. Sleeping for {retryafter} seconds."
                )
                sleep(retryafter)
                r = self.session.get(
                    url, params=data_dict, headers=headers, **requests_kwargs
                )
        return r.status_code, r.text
