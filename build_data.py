import json
import os
import re
import urllib.request
from bs4 import BeautifulSoup

import pandas as pd
import lxml.html

BASE = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
os.makedirs(BASE, exist_ok=True)


def _normalize_ticker(value):
    ticker = str(value or "").strip().upper().replace(".", "-").replace("/", "-")
    if "$" in ticker:
        base, preferred = ticker.split("$", 1)
        ticker = f"{base}-P{preferred}" if base and preferred else ""
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9.-]*", ticker) or ticker == "-":
        return ""
    return ticker


def _atomic_dump(path, payload, min_count):
    count = len(payload.get("stocks") or [])
    if count < min_count:
        raise ValueError(f"رفض تحديث غير مكتمل: {count} < {min_count}")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=1)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    return count


def _get(url, timeout=60):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    return urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8", "ignore")


def wiki_tables(url):
    doc = lxml.html.document_fromstring(_get(url))
    for i, t in enumerate(doc.xpath("//table")):
        headers = [th.text_content().strip().lower() for th in t.xpath(".//tr[1]//th")]
        rows = []
        for tr in t.xpath(".//tr")[1:]:
            cells = [c.text_content().strip() for c in tr.xpath("./th|./td")]
            if len(cells) == len(headers):
                rows.append(dict(zip(headers, cells)))
        if rows:
            yield headers, rows


def find_table(url, required_headers):
    for headers, rows in wiki_tables(url):
        if all(any(rh in h for h in headers) for rh in required_headers):
            return headers, rows
    return None, []


def scrape_saudi(path):
    html = _get("https://tadawulallshareindex.com/en/all-shares-codes/")
    soup = BeautifulSoup(html, "lxml")
    records = []
    table = soup.select_one("table")
    if table:
        for tr in table.select("tr"):
            tds = tr.select("td")
            if len(tds) < 5:
                continue
            code = tds[0].get_text(" ", strip=True)
            if not code.isdigit():
                continue
            records.append({
                "ticker": f"{code}.SR",
                "code": code,
                "name_en": tds[1].get_text(" ", strip=True),
                "name_ar": tds[2].get_text(" ", strip=True),
                "sector": tds[3].get_text(" ", strip=True),
                "isin": tds[4].get_text(" ", strip=True),
            })
    if not records:
        for a in soup.select("a[href*='/en/stock/']"):
            text = a.get_text(" ", strip=True)
            code_m = re.search(r"\b(\d{4})\b", text)
            yahoo_m = re.search(r"Yahoo:\s*([\d.]+\.SR)", text)
            isin_m = re.search(r"ISIN:\s*([A-Z0-9]+)", text)
            if not code_m:
                continue
            spans = a.select("div div span")
            name_en = ""
            sector = ""
            for sp in spans:
                txt = sp.get_text(" ", strip=True)
                if txt.isdigit() and len(txt) == 4:
                    continue
                if sp.has_attr("dir") and sp["dir"] == "rtl":
                    continue
                if sp.has_attr("role") and sp["role"] == "status":
                    sector = txt
                elif txt and not name_en:
                    name_en = txt
            records.append({
                "ticker": yahoo_m.group(1) if yahoo_m else f"{code_m.group(1)}.SR",
                "code": code_m.group(1),
                "name_en": name_en,
                "name_ar": "",
                "sector": sector,
                "isin": isin_m.group(1) if isin_m else "",
            })
    for r in records:
        if not r["name_ar"]:
            r["name_ar"] = r["name_en"]
    seen = {}
    for r in records:
        seen[r["ticker"]] = r
    records = sorted(seen.values(), key=lambda x: int(x["code"]))
    return _atomic_dump(path, {
        "source": "tadawulallshareindex.com/en/all-shares-codes",
        "count": len(records), "stocks": records,
    }, 200)


def sp500(path):
    headers, rows = find_table("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
                               ["symbol", "gics sector"])
    out = []
    for r in rows:
        sym = r.get("symbol", "")
        if not sym:
            continue
        ticker = _normalize_ticker(sym)
        if not ticker:
            continue
        out.append({
            "ticker": ticker,
            "name": r.get("security", ""),
            "sector": r.get("gics sector", ""),
            "sub_industry": r.get("gics sub-industry", ""),
        })
    return _atomic_dump(path, {
        "source": "wikipedia:List_of_S&P_500_companies",
        "count": len(out), "stocks": out,
    }, 450)


def nasdaq100(path):
    url = "https://raw.githubusercontent.com/Gary-Strauss/NASDAQ100_Constituents/master/data/nasdaq100_constituents.csv"
    try:
        df = pd.read_csv(url)
    except Exception as e:
        print("nasdaq100 fetch failed:", e)
        return 0
    df.columns = [str(c).strip().lower() for c in df.columns]
    out = []
    for _, r in df.iterrows():
        ticker = str(r.get("ticker", "")).strip()
        if not ticker:
            continue
        ticker = _normalize_ticker(ticker)
        if not ticker:
            continue
        out.append({
            "ticker": ticker,
            "name": str(r.get("company", "")).strip(),
            "sector": str(r.get("gics_sector", "")).strip() or str(r.get("sector", "")).strip(),
        })
    return _atomic_dump(path, {
        "source": "github:Gary-Strauss/nasdaq100-scraper",
        "count": len(out), "stocks": out,
    }, 90)


def dow30(path):
    headers, rows = find_table("https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average",
                               ["company", "symbol"])
    if not rows:
        # fallback: pick table index 1
        doc = lxml.html.document_fromstring(_get("https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"))
        t = doc.xpath("//table")[1]
        headers = [x.text_content().strip().lower() for x in t.xpath(".//tr[1]//th")]
        rows = []
        for tr in t.xpath(".//tr")[1:]:
            cells = [c.text_content().strip() for c in tr.xpath("./td")]
            if len(cells) == len(headers):
                rows.append(dict(zip(headers, cells)))
    out = []
    for r in rows:
        sym = r.get("symbol", "")
        if not sym:
            continue
        ticker = _normalize_ticker(sym)
        if not ticker:
            continue
        out.append({"ticker": ticker,
                    "name": r.get("company", "") or r.get("name", "")})
    return _atomic_dump(path, {
        "source": "wikipedia:Dow_Jones_Industrial_Average",
        "count": len(out), "stocks": out,
    }, 25)


def russell1000(path):
    url = ("https://www.blackrock.com/us/financial-professionals/products/239707/"
           "ishares-russell-1000-etf/latest-holdings.csv")
    df = pd.read_csv(url, skiprows=9)
    df["Sector"] = df["Sector"].fillna("Other")
    df = df[df["Asset Class"].fillna("").astype(str).str.strip() == "Equity"]
    rows = []
    for _, r in df.iterrows():
        w = str(r["Weight (%)"]).replace(",", "")
        try:
            weight = float(w)
        except ValueError:
            weight = 0.0
        ticker = _normalize_ticker(r["Ticker"])
        if not ticker:
            continue
        rows.append({
            "ticker": ticker,
            "name": str(r["Name"]).strip(),
            "sector": str(r["Sector"]).strip(),
            "weight": weight,
        })
    return _atomic_dump(path, {
        "source": "blackrock iShares Russell 1000 ETF holdings",
        "count": len(rows), "stocks": rows,
    }, 800)


def russell3000(path):
    url = ("https://www.blackrock.com/us/financial-professionals/products/239714/"
           "ishares-russell-3000-etf/latest-holdings.csv")
    df = pd.read_csv(url, skiprows=9)
    df["Sector"] = df["Sector"].fillna("Other")
    df = df[df["Asset Class"].fillna("").astype(str).str.strip() == "Equity"]
    rows = []
    for _, r in df.iterrows():
        w = str(r["Weight (%)"]).replace(",", "")
        try:
            weight = float(w)
        except ValueError:
            weight = 0.0
        ticker = _normalize_ticker(r["Ticker"])
        if not ticker:
            continue
        rows.append({
            "ticker": ticker,
            "name": str(r["Name"]).strip(),
            "sector": str(r["Sector"]).strip(),
            "weight": weight,
        })
    return _atomic_dump(path, {
        "source": "blackrock iShares Russell 3000 ETF (IWV) holdings",
        "count": len(rows), "stocks": rows,
    }, 2000)


def all_us_stocks(path):
    """
    جميع الأسهم الأمريكية المدرجة: ناسداك + نيويورك + AMEX من دليل NASDAQ الرسمي.
    يُستبعد: الصناديق المتداولة (ETF)، أوراق الاختبار، والوحدات/الحقوق/الوَرّانات.
    """
    seen = {}
    sources = [
        ("nasdaq", "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt", 3, 6),
        ("nyse-amex", "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt", 6, 4),
    ]
    for src_name, url, test_idx, etf_idx in sources:
        try:
            text = _get(url)
        except Exception as e:
            print("all_us fetch failed:", src_name, e)
            continue
        for ln in text.splitlines():
            ln = ln.strip()
            if not ln or ln.startswith("Symbol|") or ln.startswith("ACT Symbol|"):
                continue
            parts = ln.split("|")
            if len(parts) < 2:
                continue
            sym = parts[0].strip()
            name = parts[1].strip()
            if not sym:
                continue

            def flag(i):
                return len(parts) > i and parts[i].strip().upper() == "Y"

            if flag(test_idx) or flag(etf_idx):
                continue
            if re.search(r"(?i)\b(warrants?|rights?|units?|when issued)\b", name):
                continue
            ticker = _normalize_ticker(sym)
            if not ticker:
                continue
            if ticker not in seen:
                seen[ticker] = {"ticker": ticker, "name": name, "source": src_name}
    rows = sorted(seen.values(), key=lambda r: r["ticker"])
    return _atomic_dump(path, {
        "source": "nasdaqtrader.com/dynamic/symdir",
        "count": len(rows), "stocks": rows,
    }, 5000)


if __name__ == "__main__":
    print("saudi:", scrape_saudi(os.path.join(BASE, "saudi_tickers.json")))
    print("sp500:", sp500(os.path.join(BASE, "sp500.json")))
    print("nasdaq100:", nasdaq100(os.path.join(BASE, "nasdaq100.json")))
    print("dow30:", dow30(os.path.join(BASE, "dow30.json")))
    print("russell3000:", russell3000(os.path.join(BASE, "russell3000.json")))
    print("us_all:", all_us_stocks(os.path.join(BASE, "us_all.json")))
