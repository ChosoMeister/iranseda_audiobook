
from __future__ import annotations
import re, logging
from typing import List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from .utils import retry

BASE = "https://book.iranseda.ir/"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept-Language": "fa,en;q=0.8"}

def fix_url(u: str) -> str:
    u = u.strip()
    if u.startswith("http"):
        return u
    return urljoin(BASE, u.lstrip("./"))

@retry(max_attempts=3, base_delay=0.6)
def _get(url: str) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=25)
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
        r.encoding = r.apparent_encoding or "utf-8"
    if r.status_code >= 500:
        raise RuntimeError(f"server error {r.status_code}")
    return r

def crawl_taglist(start_url: str, pages: int) -> List[tuple[int,str]]:
    log = logging.getLogger()
    out: List[tuple[int,str]] = []
    for page in range(1, pages+1):
        url = start_url.format(page=page)
        r = _get(url)
        if r.status_code != 200:
            log.warning(f"[crawl] HTTP {r.status_code} on page {page}")
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "DetailsAlbum" in href and "g=" in href:
                m = re.search(r"[?&]g=(\d+)", href)
                if m:
                    g = int(m.group(1))
                    out.append((g, fix_url(href)))
        log.info(f"[crawl] page {page} parsed")
    seen, unique = set(), []
    for g,u in out:
        if g not in seen:
            seen.add(g); unique.append((g,u))
    log.info(f"[crawl] found {len(unique)} unique IDs")
    return unique
