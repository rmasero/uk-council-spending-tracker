# discover.py
import requests
import re
from bs4 import BeautifulSoup
import time

DATA_GOV_SEARCH = "https://catalogue.data.gov.uk/api/3/action/package_search"
LOCAL_AUTH_URL = "https://www.data.gov.uk/dataset/a0abdb2c-f210-4f07-bb36-9ff553bf4a23/local-authority-services"

def search_data_gov(query, rows=100):
    # returns list of resource dicts {council, resource_url, format, title}
    q = {"q": query, "rows": rows}
    r = requests.get(DATA_GOV_SEARCH, params=q, timeout=30)
    r.raise_for_status()
    data = r.json()
    results = []
    for pkg in data.get("result", {}).get("results", []):
        title = pkg.get("title")
        org = pkg.get("organization", {}).get("title") or pkg.get("publisher") or ""
        for res in pkg.get("resources", []):
            url = res.get("url") or res.get("access_url")
            fmt = (res.get("format") or "").lower()
            if not url:
                continue
            # Accept CSV/xls/xlsx or urls that end in csv or have csv query
            if fmt in ("csv","xls","xlsx") or re.search(r'\.csv($|\?)', url, re.I) or "csv" in fmt:
                results.append({"council": org or title, "resource_url": url, "format": fmt, "title": res.get("name") or title})
    return results

def get_local_authority_domains():
    # parse the local-authority-services page to extract council URLs
    r = requests.get(LOCAL_AUTH_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    domains = set()
    # links list likely in <a>
    for a in soup.find_all("a", href=True):
        href = a['href']
        if href.startswith("http") and any(p in href for p in [".gov.uk", "gov.uk", ".org.uk"]):
            domains.add(href)
    # coarse normalization to base domains
    norm = set()
    for d in domains:
        m = re.match(r'(https?://[^/]+)', d)
        if m:
            norm.add(m.group(1))
    return list(norm)

def crawl_for_payment_csv(domain, max_pages=20):
    # fetch domain root and look for links that look like payment CSVs or open data pages
    found = []
    try:
        r = requests.get(domain, timeout=20)
    except Exception:
        return found
    soup = BeautifulSoup(r.text, "lxml")
    # search present page links
    for a in soup.find_all("a", href=True):
        href = a['href']
        text = (a.get_text() or "").lower()
        if re.search(r'payment|payments|supplier|spend|transparency', href, re.I) or re.search(r'payment|payments|supplier|spend|transparency', text, re.I):
            # make absolute
            if href.startswith("/"):
                href = domain.rstrip("/") + href
            if href.startswith("http"):
                if re.search(r'\.csv($|\?)', href, re.I) or href.lower().endswith(('.csv','.xls','.xlsx')):
                    found.append(href)
                else:
                    # try to fetch the page and look for CSV links there
                    try:
                        sub = requests.get(href, timeout=15)
                        sub.raise_for_status()
                        subsoup = BeautifulSoup(sub.text, "lxml")
                        for b in subsoup.find_all("a", href=True):
                            hb = b['href']
                            if hb.startswith("/"):
                                hb = domain.rstrip("/") + hb
                            if re.search(r'\.csv($|\?)', hb, re.I) or hb.lower().endswith(('.csv','.xls','.xlsx')):
                                found.append(hb)
                    except Exception:
                        continue
    return list(set(found))

if __name__ == "__main__":
    # quick demo
    print("Searching data.gov.uk for payment datasets...")
    res = search_data_gov("payments suppliers council")
    print("Found", len(res), "resources from data.gov.uk")
    print("Fetching council domains list...")
    domains = get_local_authority_domains()
    print("Found", len(domains), "domains; crawling the first 10 for CSVs...")
    for d in domains[:10]:
        f = crawl_for_payment_csv(d)
        if f:
            print(d, "->", f)
        time.sleep(0.5)
