"""
markets.py
طبقة بيانات الأسواق: تحميل قوائم الأسهم وقطاعاتها، وتحديثها تلقائياً.

المصادر:
  - السوق السعودي (تاسي):      بيانات ثابتة مولّدة مسبقاً في data/saudi_tickers.json
                               (266 سهم + القطاعات الرسمية الـ 21).
  - S&P 500:                   ويكيبيديا (قطاعات GICS).
  - Nasdaq-100:                مستودع GitHub محدّث تلقائياً.
  - Dow 30:                    ويكيبيديا.
  - Russell 3000:              بيانات iShares Russell 3000 ETF (IWV) من BlackRock
                               (القطاع + الوزن) — تغطي ~98% من السوق الأمريكي،
                               وتُحدَّث تلقائياً من المصدر الحي.
  - كل الأسهم الأمريكية:       دليل NASDAQ الرسمي (ناسداك + نيويورك + AMEX)
                               data/us_all.json — جميع الأسهم المدرجة بدون ETF والوَرّانات.

تحديث تلقائي: عند تشغيل التطبيق يتحقق من عمر الملفات المخزنة في data/، وإن كانت
أقدم من REFRESH_DAYS يستبدلها من المصادر مباشرة.
"""

import json
import os
import threading
from datetime import datetime

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
REFRESH_DAYS = 7  # كم يوم قبل إعادة جلب قوائم الأسهم تلقائياً

# ===================== الترجمة العربية للقطاعات =====================
SAUDI_SECTOR_AR = {
    "Banks": "البنوك",
    "Materials": "المواد الأساسية",
    "Energy": "الطاقة",
    "Capital Goods": "السلع الرأسمالية",
    "Transportation": "النقل",
    "Consumer Durables & Apparel": "السلع المعمرة والملابس",
    "Consumer Services": "الخدمات الاستهلاكية",
    "Consumer Discretionary Retail": "التجزئة التقديرية",
    "Consumer Staples Retail": "تجزئة السلع الأساسية",
    "Food & Beverages": "الأغذية والمشروبات",
    "Health Care Equipment & Services": "معدات وخدمات الرعاية الصحية",
    "Pharma, Biotech & Life Sciences": "الأدوية والتقنية الحيوية",
    "Media & Entertainment": "الإعلام والترفيه",
    "Software & Services": "البرمجيات والخدمات",
    "Telecommunication Services": "خدمات الاتصالات",
    "Utilities": "المرافق العامة",
    "Insurance": "التأمين",
    "Real Estate Management & Development": "إدارة وتطوير العقارات",
    "REITs": "الصناديق العقارية (REITs)",
    "Diversified Financials": "الخدمات المالية المتنوعة",
    "Commercial & Professional Services": "الخدمات التجارية والمهنية",
}

US_SECTOR_AR = {
    "Information Technology": "تقنية المعلومات",
    "Health Care": "الرعاية الصحية",
    "Financials": "القطاع المالي",
    "Consumer Discretionary": "السلع الاستهلاكية التقديرية",
    "Communication Services": "خدمات الاتصالات",
    "Industrials": "الصناعات",
    "Consumer Staples": "السلع الاستهلاكية الأساسية",
    "Energy": "الطاقة",
    "Utilities": "المرافق",
    "Materials": "المواد الأساسية",
    "Real Estate": "العقارات",
    "Other": "أخرى / غير مصنفة",
}

RUSSELL_SECTOR_FIX = {"Communication": "Communication Services"}

# ============================ المؤشرات ============================
SAUDI_INDICES = [
    {"ticker": "^TASI.SR", "name": "مؤشر تاسي العام (TASI)", "sector": "indices"},
]

US_INDICES = [
    {"ticker": "^GSPC", "name": "مؤشر S&P 500", "sector": "indices"},
    {"ticker": "^IXIC", "name": "مؤشر ناسداك المركب", "sector": "indices"},
    {"ticker": "^DJI", "name": "مؤشر داو جونز الصناعي", "sector": "indices"},
]

_lock = threading.Lock()
_cache = {}


def _load(name):
    with _lock:
        if name in _cache:
            return _cache[name]
        path = os.path.join(DATA_DIR, name)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, ValueError):
                return None
            _cache[name] = data
            return data
        return None


def _file_age_days(name):
    path = os.path.join(DATA_DIR, name)
    if not os.path.exists(path):
        return REFRESH_DAYS + 1
    mtime = os.path.getmtime(path)
    return (datetime.now().timestamp() - mtime) / 86400.0


def refresh_lists(force=False):
    """يعيد جلب القوائم من المصادر إذا كانت قديمة. يعمل في الخلفية حتى لا يبطئ الإقلاع."""
    try:
        from build_data import (all_us_stocks, dow30, nasdaq100, russell3000,
                                scrape_saudi, sp500)
        jobs = {
            "saudi_tickers.json": scrape_saudi,
            "sp500.json": sp500,
            "nasdaq100.json": nasdaq100,
            "dow30.json": dow30,
            "russell3000.json": russell3000,
            "us_all.json": all_us_stocks,
        }
        for name, fn in jobs.items():
            if force or _file_age_days(name) > REFRESH_DAYS:
                try:
                    n = fn(os.path.join(DATA_DIR, name))
                    _cache.pop(name, None)
                    print(f"تحديث القائمة {name}: {n} سهم")
                except Exception as e:
                    print(f"فشل تحديث {name}: {e}")
    except ImportError:
        print("لا يوجد build_data.py — سيتم استخدام القوائم المخزنة فقط.")


def start_background_refresh():
    def _run():
        import time
        time.sleep(2)
        refresh_lists(force=False)
    threading.Thread(target=_run, daemon=True).start()


# ============================ الأسواق ============================
def saudi_universe():
    data = _load("saudi_tickers.json") or {"stocks": []}
    out = {}
    for s in data["stocks"]:
        out[s["ticker"]] = {
            "name": s.get("name_ar") or s.get("name_en", ""),
            "name_en": s.get("name_en", ""),
            "sector": s.get("sector", ""),
            "source": "saudi",
        }
    return out


def _norm_sector(sector):
    if not sector or sector in ("Other", "—", ""):
        return "Other"
    return RUSSELL_SECTOR_FIX.get(sector, sector)


def us_base_universe():
    """S&P 500 + ناسداك 100 + داو 30 مع القطاع."""
    out = {}
    sp = _load("sp500.json") or {"stocks": []}
    for s in sp["stocks"]:
        out[s["ticker"]] = {"name": s.get("name", ""), "sector": _norm_sector(s.get("sector", "")), "source": "sp500"}
    ndx = _load("nasdaq100.json") or {"stocks": []}
    for s in ndx["stocks"]:
        t = s["ticker"]
        if t not in out:
            out[t] = {"name": s.get("name", ""), "sector": "Other", "source": "nasdaq100"}
    dow = _load("dow30.json") or {"stocks": []}
    for s in dow["stocks"]:
        t = s["ticker"]
        if t not in out:
            out[t] = {"name": s.get("name", ""), "sector": "Other", "source": "dow30"}
    return out


def us_russell_full():
    """كل أسهم Russell 3000 مع القطاع والوزن، القطاع من المصدر نفسه."""
    data = _load("russell3000.json") or {"stocks": []}
    out = {}
    for s in data["stocks"]:
        t = s["ticker"]
        out[t] = {
            "name": s.get("name", ""),
            "sector": _norm_sector(s.get("sector", "")),
            "weight": s.get("weight", 0.0),
            "source": "russell",
        }
    return out


def us_all_stocks():
    """كل الأسهم الأمريكية المدرجة (ناسداك + نيويورك + AMEX) — القطاع غير مصنّف ما لم يرد في Russell."""
    data = _load("us_all.json") or {"stocks": []}
    out = {}
    for s in data["stocks"]:
        t = s["ticker"]
        out[t] = {
            "name": s.get("name", ""),
            "sector": "Other",
            "source": s.get("source", "all-us"),
        }
    return out


def us_combined_universe():
    """كل السوق الأمريكي: كل الأسهم المدرجة + Russell 3000 + S&P 500 + ناسداك 100 + داو 30."""
    combined = us_all_stocks()
    for t, m in us_base_universe().items():
        combined[t] = m
    for t, m in us_russell_full().items():
        if t in combined:
            combined[t].update(m)
        else:
            combined[t] = m
    return combined


def market_sectors(market):
    """يعيد قائمة القطاعات مع عدد الأسهم في كل قطاع."""
    if market == "saudi":
        uni = saudi_universe()
        ar = SAUDI_SECTOR_AR
    else:
        uni = us_combined_universe()
        ar = US_SECTOR_AR
    counts = {}
    for m in uni.values():
        s = m["sector"]
        counts[s] = counts.get(s, 0) + 1
    out = [{"id": s, "ar": ar.get(s, s), "en": s, "count": c} for s, c in sorted(counts.items(), key=lambda kv: -kv[1])]
    return out


def build_universe(market, sector):
    """
    يرجع قائمة (ticker, meta) حسب السوق والقطاع المطلوبين.
    sector = 'all' | 'indices' | اسم القطاع
    """
    if market == "saudi":
        if sector == "indices":
            return [(i["ticker"], i) for i in SAUDI_INDICES]
        uni = saudi_universe()
        if sector == "all":
            return list(uni.items())
        return [(t, m) for t, m in uni.items() if m["sector"] == sector]
    else:
        if sector == "indices":
            return [(i["ticker"], i) for i in US_INDICES]
        combined = us_combined_universe()
        if sector == "all":
            return list(combined.items())
        return [(t, m) for t, m in combined.items() if m["sector"] == sector]
