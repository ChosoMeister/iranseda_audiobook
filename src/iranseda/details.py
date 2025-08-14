
from __future__ import annotations
import re
from typing import Dict, List, Any
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

from .utils import retry

BASE = "https://book.iranseda.ir/"
API  = "https://apisec.iranseda.ir/book/Details/?VALID=TRUE&g={g}&attid={attid}"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept-Language": "fa,en;q=0.8"}

CSV_FIELDS = [
    "AudioBook_ID","AudioBook_attID","Book_Title","Book_Description","Book_Detail",
    "Book_Language","Book_Country","Book_Author","Book_Translator","Book_Narrator",
    "Book_Director","Book_Producer","Book_SoundEngineer","Book_Effector","Book_Actors",
    "Book_Genre","Book_Category","Book_Duration","Episode_Count",
    "Cover_Image_URL","Source_URL","Player_Link","FullBook_MP3_URL","All_MP3s_Found"
]

def fix_url(u: str) -> str:
    u = u.strip()
    if u.startswith("http"): return u
    return urljoin(BASE, u.lstrip("./"))

@retry(max_attempts=3, base_delay=0.6)
def req_get(url: str) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=25)
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
        r.encoding = r.apparent_encoding or "utf-8"
    if r.status_code in (429, 403, 408):
        raise RuntimeError(f"retryable status {r.status_code}")
    if r.status_code >= 500:
        raise RuntimeError(f"server error {r.status_code}")
    return r

def text_or_none(el):
    if not el: return None
    txt = el.get_text(" ", strip=True)
    return txt if txt else None

def parse_label_from_iteminfo(soup, label_fa):
    for dd in soup.select(".item-info dd.field"):
        strong = dd.find("strong")
        if strong and label_fa in strong.get_text(strip=True):
            items = [t.get_text(" ", strip=True) for t in dd.find_all(["a","span"])]
            items = [t for t in items if t and t != label_fa]
            return "، ".join(dict.fromkeys(items)) or None
    return None

def parse_from_metadata_list(soup, dt_text):
    for dt in soup.select("#tags dt"):
        if dt_text in dt.get_text(strip=True):
            dd = dt.find_next_sibling("dd")
            if dd:
                vals = [sp.get_text(" ", strip=True) for sp in dd.select("span")]
                vals = [v for v in vals if v and v != ","]
                if vals:
                    return "، ".join(dict.fromkeys(vals))
    return None

def get_og_image(soup):
    tag = soup.find("meta", attrs={"property": "og:image"})
    if tag and (tag.get("content") or tag.get("value")):
        return fix_url(tag.get("content") or tag.get("value"))
    return None

def find_first_image_src(soup):
    img = soup.select_one(".product-view .item .image img") or soup.select_one(".cover img") or soup.find("img")
    if img and img.has_attr("src"):
        return fix_url(img["src"])
    return None

def extract_attid(soup):
    og = get_og_image(soup)
    if og:
        m = re.search(r"[?&]AttID=(\d+)", og, re.I)
        if m: return int(m.group(1))
    for img in soup.find_all("img", src=True):
        m = re.search(r"[?&]AttID=(\d+)", img["src"], re.I)
        if m: return int(m.group(1))
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]attid=(\d+)", a["href"], re.I)
        if m: return int(m.group(1))
    return None

def parse_duration_and_episodes(soup):
    dur = None; ep = None
    dur_keys = ["مدت", "مدت زمان", "زمان"]
    ep_keys  = ["تعداد قسمت", "تعداد قطعه", "تعداد قطعات", "تعداد قسمت‌ها"]
    for dd in soup.select(".item-info dd.field"):
        s = dd.get_text(" ", strip=True)
        for k in dur_keys:
            if k in s and not dur:
                m = re.search(r"(\d{1,2}:\d{2}:\d{2}|\d{1,3}:\d{2})", s)
                if m: dur = m.group(1)
        for k in ep_keys:
            if k in s and not ep:
                m = re.search(r"(\d+)", s)
                if m: ep = int(m.group(1))
    for dt in soup.select("#tags dt"):
        t = dt.get_text(strip=True)
        if any(k in t for k in dur_keys) and not dur:
            dd = dt.find_next_sibling("dd")
            if dd:
                s = dd.get_text(" ", strip=True)
                m = re.search(r"(\d{1,2}:\d{2}:\d{2}|\d{1,3}:\d{2})", s)
                if m: dur = m.group(1)
        if any(k in t for k in ep_keys) and not ep:
            dd = dt.find_next_sibling("dd")
            if dd:
                m = re.search(r"(\d+)", dd.get_text(" ", strip=True))
                if m: ep = int(m.group(1))
    return dur, ep

def build_player_link(audio_id, attid):
    if audio_id and attid:
        return f"https://player.iranseda.ir/book-player/?VALID=TRUE&g={audio_id}&attid={attid}"
    return None

def parse_details_page(html: str, url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    data: Dict[str, Any] = {}
    data["Book_Title"] = text_or_none(soup.select_one("h1.titel"))
    data["Book_Description"] = text_or_none(soup.select_one("#about .body-module"))
    data["Book_Detail"] = text_or_none(soup.select_one("#review .body-module .more")) or text_or_none(soup.select_one("#review .body-module"))
    lang = soup.find("meta", {"property":"og:locale"})
    data["Book_Language"] = "فارسی" if (lang and "fa" in lang.get("content","")) else None
    data["Book_Country"] = None
    data["Book_Author"]        = parse_label_from_iteminfo(soup, "نویسنده") or parse_from_metadata_list(soup, "عنوان كتاب مرجع") or parse_from_metadata_list(soup, "نویسنده")
    data["Book_Translator"]    = parse_from_metadata_list(soup, "ترجمه")
    data["Book_Narrator"]      = parse_from_metadata_list(soup, "راوی")
    data["Book_Director"]      = parse_label_from_iteminfo(soup, "کارگردان") or parse_from_metadata_list(soup, "کارگردان")
    data["Book_Producer"]      = parse_from_metadata_list(soup, "تهیه‌کننده")
    data["Book_SoundEngineer"] = parse_from_metadata_list(soup, "صدابردار")
    data["Book_Effector"]      = parse_from_metadata_list(soup, "افکتور") or parse_from_metadata_list(soup, "افكتور")
    data["Book_Actors"]        = parse_from_metadata_list(soup, "بازیگران")
    data["Book_Genre"]         = parse_from_metadata_list(soup, "کلمه کلیدی") or parse_from_metadata_list(soup, "نوع متن")
    data["Book_Category"]      = parse_from_metadata_list(soup, "دسته بندی ها") or parse_label_from_iteminfo(soup, "دسته‌بندی")
    dur_txt, ep_cnt = parse_duration_and_episodes(soup)
    data["Book_Duration"] = dur_txt
    data["Episode_Count"] = ep_cnt
    cover = get_og_image(soup) or find_first_image_src(soup)
    data["Cover_Image_URL"] = cover
    data["AudioBook_attID"] = extract_attid(soup)
    try:
        q = parse_qs(urlparse(url).query)
        g = q.get("g", [None])[0]
        data["AudioBook_ID"] = int(g) if g else None
    except Exception:
        data["AudioBook_ID"] = None
    data["Source_URL"] = url
    data["Player_Link"] = None
    data["FullBook_MP3_URL"] = None
    data["All_MP3s_Found"] = None
    return data

@retry(max_attempts=3, base_delay=0.6)
def get_mp3s_from_api(audio_id: int, attid: int):
    url = API.format(g=audio_id, attid=attid)
    r = req_get(url)
    if r.status_code != 200:
        return []
    try:
        j = r.json()
    except Exception:
        return []
    mp3s = []
    for it in (j.get("items") or []):
        for d in (it.get("download") or []):
            if str(d.get("extension")).lower() == "mp3" and d.get("downloadUrl"):
                mp3s.append({
                    "url": fix_url(d["downloadUrl"]),
                    "bitrate": d.get("bitRate"),
                    "size": int(d.get("fileSize") or 0),
                    "attid": it.get("FileID"),
                })
    return mp3s
