import re
from urllib.parse import urlsplit

from pyquery import PyQuery

import pytest

from . import INDEXED_WEB_DOMAINS, request


META_ROBOTS_RE = re.compile(
    r"""(?x)    # Verbose regex mode
    <meta\s+                        # meta tag followed by whitespace
    name="robots"\s*                # name=robots
    content="(?P<content>[^"]+)"    # capture the content
    \s*>                            # end meta tag
"""
)


@pytest.fixture()
def is_indexed(base_url):
    hostname = urlsplit(base_url).netloc
    return hostname in INDEXED_WEB_DOMAINS


def test_document(base_url, is_indexed):
    url = base_url + "/en-US/docs/Web"
    resp = request("get", url)
    assert resp.status_code == 200
    assert resp.headers["Content-Type"] == "text/html; charset=utf-8"
    meta = META_ROBOTS_RE.search(resp.text)
    assert meta
    content = meta.group("content")
    if is_indexed:
        assert content == "index, follow"
    else:
        assert content == "noindex, nofollow"


def test_document_based_redirection(base_url):
    """Ensure that content-based redirects properly redirect."""
    url = base_url + "/en-US/docs/concat"
    resp = request("get", url)
    assert resp.status_code == 301
    assert (
        resp.headers["Location"]
        == "/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/concat"
    )


def test_home(base_url, is_indexed):
    url = base_url + "/en-US/"
    resp = request("get", url)
    assert resp.status_code == 200
    assert resp.headers["Content-Type"] == "text/html; charset=utf-8"
    meta = META_ROBOTS_RE.search(resp.text)
    assert meta
    content = meta.group("content")
    if is_indexed:
        assert content == "index, follow"
    else:
        assert content == "noindex, nofollow"
    html = PyQuery(resp.text)
    assert html.find("head > title").text() == "MDN Web Docs"


def test_hreflang_basic(base_url):
    """Ensure that we're specifying the correct value for lang and hreflang."""
    url = base_url + "/en-US/docs/Web/HTTP"
    resp = request("get", url)
    assert resp.status_code == 200
    html = PyQuery(resp.text)
    assert html.attr("lang") == "en"
    assert html.find('head > link[hreflang="en"][href="{}"]'.format(url))


@pytest.mark.parametrize(
    "uri,expected_keys",
    [["/api/v1/whoami", [("waffle", ("flags", "switches", "samples"))]]],
    ids=("whoami",),
)
def test_api_basic(base_url, uri, expected_keys):
    """Basic test of site's api endpoints."""
    resp = request("get", base_url + uri)
    assert resp.status_code == 200
    assert resp.headers.get("content-type") == "application/json"
    data = resp.json()
    for item in expected_keys:
        if isinstance(item, tuple):
            key, sub_keys = item
        else:
            key, sub_keys = item, ()
        assert key in data
        for sub_key in sub_keys:
            assert sub_key in data[key]


# Test value tuple is:
# - Expected locale prefix
# - "preferredlocale" cookie value
# - Accept-Language header value
LOCALE_SELECTORS = {
    "en-US-1": ("en-US", None, None),
    "en-US-2": ("en-US", None, "en-US"),
    "en-US-3": ("en-US", "en-US", None),
    "en-US-4": ("en-US", "en-US", "fr"),
    "es-1": ("es", "es", None),
    "es-2": ("es", "es", "en-US"),
    "es-3": ("es", None, "es"),
    "de-1": ("de", "de", None),
    "de-2": ("de", "de", "en-US"),
    "de-3": ("de", None, "de"),
}


@pytest.mark.parametrize(
    "expected,cookie,accept", LOCALE_SELECTORS.values(), ids=list(LOCALE_SELECTORS),
)
@pytest.mark.parametrize(
    "slug",
    [
        "/",
        "/docs/Web",
        "/search",
        "/events",
        "/profile",
        "/profiles/sheppy",
        "/users/signin",
        "/promote",
        "/account",
        "/docs/Web/HTML",
        "/docs/Learn/CSS/Styling_text/Fundamentals#Color",
    ],
)
def test_locale_selection(base_url, slug, expected, cookie, accept):
    """
    Ensure that locale selection, which depends on the "preferredlocale"
    cookie and the "Accept-Language" header, works for the provided URL's.
    """
    url = base_url + slug
    assert expected, "expected must be set to the expected locale prefix."
    request_kwargs = {}
    if accept:
        request_kwargs["headers"] = {"Accept-Language": accept}
    if cookie:
        request_kwargs["cookies"] = {"preferredlocale": cookie}
    response = request("get", url, **request_kwargs)
    assert response.status_code == 302
    assert response.headers["location"].startswith("/{}/".format(expected))
