"""
Checks every repository/other-website link on a generated Data Package
board for dead links, likely-restricted-access, and a few spam/parked-domain
hints -- purely a reporting tool. It never removes or changes anything;
every link gets a category in the output CSV and a human decides what to
actually do about it.

Deliberately NOT part of monday_package_board_lambda or any schedule: it
makes one HTTP request per unique external link (can be 300+), with
unpredictable latency (some sites hang, some rate-limit or block scripted
User-Agents) -- a bad fit for a job that has to run reliably every month.
Run this by hand whenever you want a check, not on a cadence.

Categories are a starting point for YOUR review, not a verdict:
  - ok                        2xx
  - likely_dead                404/410
  - possibly_restricted_access 401/403 (HEAL has many legitimate
                                restricted-access repos -- this is NOT
                                the same as "likely_dead")
  - client_error_other         other 4xx (400, 429 rate-limited, etc.)
  - server_error                5xx
  - connection_error           DNS failure, timeout, connection refused, SSL error

possible_spam_hint is a light heuristic (parked-domain phrases in the page
text) -- a human still needs to look at anything it flags; it will miss
real spam and can false-positive on legitimate pages that happen to mention
"domain" or "for sale".

Usage:
    python check_data_package_links.py \\
        --board /path/to/DataPackageBoard_2026-10-19.xlsx \\
        --output /path/to/link_health_report.csv \\
        [--concurrency 8] [--timeout 15]
"""

import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import click
import httpx
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout
)
logger = logging.getLogger(__name__)

# A plain python-httpx default User-Agent gets blocked or served a
# different (often worse) response by some sites; a normal browser UA
# avoids that without being deceptive about what's making the request --
# this is a read-only health check, not scraping content for reuse.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_SPAM_PHRASES = [
    "domain is for sale", "buy this domain", "domain may be for sale",
    "domain parking", "this web page is parked", "related searches",
    "sponsored listings", "domain name registration",
]

_RESTRICTED_CODES = {401, 403}
_DEAD_CODES = {404, 410}

# DOI/handle resolvers are *supposed* to redirect to a different domain --
# that's their whole job -- so a plain domain mismatch there is expected,
# not a spam signal. Anything else landing on a completely different
# registrable domain (not just a www./http-vs-https difference) is unusual.
_RESOLVER_DOMAINS = {"doi.org", "dx.doi.org", "handle.net"}


def _registrable_domain(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    parts = host.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def check_one(client: httpx.Client, url: str) -> dict:
    # A link with no http(s):// scheme at all (seen live: "www.chearr.org",
    # a data-quality bug in the MDS record) isn't a valid absolute URL --
    # rather than fail it outright, infer https:// so the actual site still
    # gets checked. The chearr.org case also happened to be the request
    # that crashed the whole batch below before that catch was broadened.
    request_url = url if urlparse(url).scheme else f"https://{url}"
    try:
        resp = client.get(request_url, follow_redirects=True)
    except Exception as e:
        # Not just httpx.RequestError: a malformed/relative Location header
        # deep in a redirect chain can raise a bare ValueError from httpx's
        # internal cookie handling (urllib.request parsing it as a URL) --
        # seen live on a chain that bounced through a dozen unrelated
        # domains before hitting one. Catch everything per-link so one bad
        # chain doesn't take down the whole batch; the failure itself is
        # also a signal (a well-behaved redirect chain doesn't do this).
        return {
            "url": url, "final_url": "", "http_status": "",
            "category": "connection_error", "error": f"{type(e).__name__}: {e}",
            "possible_spam_hint": "",
        }

    status = resp.status_code
    if 200 <= status < 300:
        category = "ok"
    elif status in _DEAD_CODES:
        category = "likely_dead"
    elif status in _RESTRICTED_CODES:
        category = "possibly_restricted_access"
    elif 400 <= status < 500:
        category = "client_error_other"
    else:
        category = "server_error"

    # httpx resolves each hop's .url to an absolute URL already (that's
    # exactly why this is safer than parsing the raw Location header
    # string, which is what crashed above on a malformed relative one).
    # Report the full ordered chain, not just the final host: a hijacked
    # domain's redirect can pass through other unrelated-looking domains
    # before landing somewhere that returns 200 -- a "distinct domain
    # count" alone misses a plain domain-swap (one hop, two hosts, below
    # any reasonable threshold) and a human scanning final_url alone would
    # miss everything the chain passed through on the way.
    chain_hosts = [str(h.url.host) for h in resp.history] + [str(resp.url.host)]
    redirect_chain = " -> ".join(chain_hosts) if len(set(chain_hosts)) > 1 else ""

    hints = []
    # request_url, not url: urlparse can't find a hostname in a scheme-less
    # string like "www.chearr.org", which would otherwise silently compute
    # an empty registrable domain for exactly the case this is meant to catch.
    orig_registrable = _registrable_domain(request_url)
    if orig_registrable not in _RESOLVER_DOMAINS and orig_registrable != _registrable_domain(str(resp.url)):
        hints.append(f"original domain ({orig_registrable}) doesn't match where it landed ({chain_hosts[-1]})")

    content_type = resp.headers.get("content-type", "")
    if category == "ok" and "html" in content_type.lower():
        text_lower = resp.text[:20000].lower()  # page text can be huge; a prefix is enough
        hints += [p for p in _SPAM_PHRASES if p in text_lower]

    return {
        "url": url,
        "final_url": str(resp.url) if str(resp.url) != url else "",
        "redirect_chain": redirect_chain,
        "http_status": status,
        "category": category,
        "error": "",
        "possible_spam_hint": "; ".join(hints),
    }


def check_links(urls: list[str], concurrency: int, timeout: float) -> pd.DataFrame:
    timeout_cfg = httpx.Timeout(connect=10.0, read=timeout, write=10.0, pool=10.0)
    results = []
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=timeout_cfg, max_redirects=10) as client:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {pool.submit(check_one, client, url): url for url in urls}
            done = 0
            for future in as_completed(futures):
                results.append(future.result())
                done += 1
                if done % 20 == 0 or done == len(urls):
                    logger.info("Checked %d/%d links", done, len(urls))
    return pd.DataFrame(results)


@click.command()
@click.option("--board", required=True, help="Path to a generated DataPackageBoard_<date>.xlsx.")
@click.option("--output", default=None, help="Output CSV path. Defaults to link_health_report_<date>.csv next to --board.")
@click.option("--concurrency", default=8, show_default=True, help="Max simultaneous requests.")
@click.option("--timeout", default=15.0, show_default=True, help="Per-request read timeout, in seconds.")
def main(board: str, output: str, concurrency: int, timeout: float):
    board_path = Path(board)
    out_path = Path(output) if output else board_path.parent / f"link_health_report_{datetime.now().strftime('%Y-%m-%d')}.csv"

    df = pd.read_excel(board_path)
    if "Repository Link" not in df.columns:
        raise click.ClickException(f"'Repository Link' column not found in {board_path}")

    links = df[df["Repository Link"] != "-"]
    unique_urls = links["Repository Link"].drop_duplicates().tolist()
    logger.info("%d link rows, %d unique URLs to check", len(links), len(unique_urls))

    checked = check_links(unique_urls, concurrency, timeout)

    merged = pd.merge(links, checked, how="left", left_on="Repository Link", right_on="url")
    merged.drop(columns=["url"], inplace=True)
    merged["checked_at"] = datetime.now(timezone.utc).isoformat()

    merged.to_csv(out_path, index=False)
    logger.info("Wrote %s", out_path)

    logger.info("Category counts:\n%s", merged["category"].value_counts().to_string())
    spam_candidates = merged[merged["possible_spam_hint"] != ""]
    if len(spam_candidates):
        logger.warning(
            "%d link(s) with a possible spam/parked-domain hint -- review these specifically:\n%s",
            len(spam_candidates),
            spam_candidates[["HDPID", "Repository Link", "possible_spam_hint"]].to_string(index=False),
        )


if __name__ == "__main__":
    main()
