"""
app.py
فلتر السوق السعودي (تاسي) والسوق الأمريكي بقطاعاتهما، بنمط
"الانفراج + اختراق القمة/القاع على RSI" مع ربط كل فريم تنفيذ بفريم أعلى
لاستخراج منطقة الطلب/العرض منه.

الميزات:
  - اختيار السوق (السعودية / أمريكا) والقطاع والمؤشر من الواجهة.
  - اختيار فريم التنفيذ (15 دقيقة / ساعة / يومي).
  - فحص تلقائي دوري + زر فحص يدوي + زر تشغيل/إيقاف.
  - قوائم الأسهم والقطاعات تتحدث تلقائياً من المصادر.

التشغيل:
    pip install -r requirements.txt
    python app.py
    ثم افتح المتصفح على: http://localhost:5000
"""

import hashlib
import hmac
import html
import json
import os
import queue
import re
import secrets
import shutil
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, as_completed
from datetime import datetime

import requests

import pandas as pd
from flask import Flask, jsonify, redirect, render_template, request, session

from markets import (SAUDI_INDICES, US_INDICES, US_SECTOR_AR, SAUDI_SECTOR_AR,
                     build_universe, market_sectors, refresh_lists,
                     start_background_refresh)
from scanner import (backtest_signals, calculate_rsi, calculate_atr,
                     detect_signal, detect_signal_bearish, get_divergence_zone,
                     detect_triple_filter)

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
UI_VERSION = "strategy-results-20260806-9"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
BUNDLED_DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "alerts.db")
YF_CACHE_DIR = os.path.join(DATA_DIR, "yf_cache")
YF_TRIPLE_CACHE_DIR = os.path.join(DATA_DIR, "yf_cache_triple")

os.makedirs(DATA_DIR, exist_ok=True)
if os.path.abspath(DATA_DIR) != os.path.abspath(BUNDLED_DATA_DIR):
    for _seed_name in ("saudi_tickers.json", "sp500.json", "nasdaq100.json",
                       "dow30.json", "russell3000.json", "us_all.json"):
        _source = os.path.join(BUNDLED_DATA_DIR, _seed_name)
        _target = os.path.join(DATA_DIR, _seed_name)
        if os.path.exists(_source) and not os.path.exists(_target):
            shutil.copy2(_source, _target)

# ==================== الإعدادات ====================
EXECUTION_TIMEFRAMES = ["15m", "1h", "1d", "1wk", "1mo"]

TIMEFRAME_MAP = {   # فريم التنفيذ ← فريم منطقة الطلب/العرض
    "15m": "2h",
    "1h": "1d",
    "1d": "1wk",
    "1wk": "1mo",
    "1mo": "1mo",
}
DEFAULT_ZONE_TIMEFRAME = "1d"

TRIPLE_FILTER_MAP = {
    "1d": ("1wk", "1d", "1d"),
    "1h": ("1wk", "1d", "1h"),
    "15m": ("1d", "1h", "15m"),
}

# الفلتر الثلاثي يجلب فريم "المصدر" الأفضل مرة واحدة لكل سهم ثم يعيد تجميعه
# (resample) إلى الفريمات الثلاثة — فيبقى عدد الطلبات مساوياً لفحص RSI العادي
# (طلبا واحدا لكل سهم في معظم الحالات) وهو ما يتناسب مع مهلة الدقيقة على Render.
TRIPLE_SOURCE_MAP = {          # فريم التنفيذ ← (الفريم الهدف، المصدر، قاعدة إعادة التجميع)
    "1d": [
        ("1wk", "1d", "1W"),
        ("1d", "1d", None),
        ("1d", "1d", None),
    ],
    "1h": [
        ("1wk", "1h", "1W"),
        ("1d", "1h", "1D"),
        ("1h", "1h", None),
    ],
    "15m": [
        ("1d", "1d", None),
        ("1h", "15m", "1h"),
        ("15m", "15m", None),
    ],
}
TRIPLE_SOURCE_LOOKBACK = {"1d": "2y", "1h": "730d", "15m": "60d"}

RSI_PERIOD = 14
ATR_PERIOD = 14
PIVOT_LEFT = 3
PIVOT_RIGHT = 3
TOLERANCE = 3

LOOKBACK_MAP = {
    "1m": "7d", "2m": "60d", "5m": "60d", "15m": "60d", "30m": "60d",
    "60m": "180d", "1h": "180d", "90m": "60d",
    "1d": "1y", "5d": "1y", "1wk": "5y", "1mo": "max",
}
DEFAULT_LOOKBACK = "1y"
YF_NATIVE_INTERVALS = {"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h",
                       "1d", "5d", "1wk", "1mo", "3mo"}
CUSTOM_TIMEFRAMES = {"2h": ("60m", "2h")}

YF_WORKERS = 64           # النقطة المثبتة: 64 عاملاً أعطت 67/67 في 5.2 ثانية
ZONE_FETCH_WORKERS = 12
YF_REQUEST_TIMEOUT = 5    # مهلة قصيرة: الأسهم الميتة تفشل سريعاً دون شلّ الفحص
TRIPLE_FETCH_WORKERS = 32   # المصدر (سنتان) يُعاد تجميعه إلى الفريمات — حساس للازدحام عند Yahoo
ANALYSIS_WORKERS = 24     # عدد خيوط تحليل الفلتر الثلاثي بالتوازي
SCAN_INTERVAL_DEFAULT = 30  # دقائق بين دورات الفحص التلقائي
SCAN_BUDGET_SECONDS = 50    # يترك هامشاً للحفظ وإرجاع الحالة قبل الدقيقة
TRIPLE_BUDGET_SECONDS = 240 # الثلاثي أثقل: مصدر كبير (5 سنوات) يُعاد تجميعه — يحتاج
                            # وقتاً أطول من فحص RSI، ولا يوجد حد منصة على مدة الخيط الخلفي.
# ================================================================

MARKET_AR = {"saudi": "السوق السعودي (تاسي)", "us": "السوق الأمريكي"}

DEFAULTS = {
    "market": "saudi",
    "sector": "all",
    "timeframe": "1d",
    "filter_type": "rsi",
    "rsi_max": 50,          # تنبيه الشراء فقط عندما يكون RSI (عند الاختراق وعند القاع) <= هذه القيمة
    "auto": True,
    "interval_minutes": SCAN_INTERVAL_DEFAULT,
    "volume_filter": False,     # اشتراط حجم شمعة الاختراق >= 1.5 × متوسط آخر 20 شمعة
    "trend_filter": False,      # فلتر الاتجاه العام (السعر مقابل المتوسط المتحرك 200)
    "price_min": None,          # الحد الأدنى لسعر السهم في التنبيه (اختياري)
    "price_max": None,          # الحد الأقصى لسعر السهم في التنبيه (اختياري)
    "telegram_token": "",       # توكن بوت تيليجرام (اختياري) — أو عبر متغير البيئة TELEGRAM_BOT_TOKEN
    "telegram_chat": "",        # معرف الشات المستلم للتنبيهات (اختياري) — أو TELEGRAM_CHAT_ID
    "signal_filter": "both",    # both/bullish/bearish لفلتر RSI، وnone للفلتر الثلاثي
}

CONFIG_PATH = os.path.join(DATA_DIR, "config.json")


def _valid_sector_ids(market):
    try:
        return {s["id"] for s in market_sectors(market)} | {"all", "indices"}
    except Exception:
        return {"all", "indices"}


def _load_config():
    cfg = dict(DEFAULTS)
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            saved = json.load(f)
        if saved.get("market") in ("saudi", "us"):
            cfg["market"] = saved["market"]
        if saved.get("filter_type") in ("rsi", "triple"):
            cfg["filter_type"] = saved["filter_type"]
        if isinstance(saved.get("sector"), str):
            sectors = _valid_sector_ids(cfg["market"])
            if saved["sector"] in sectors:
                cfg["sector"] = saved["sector"]
        if saved.get("rsi_max") in (50, 40, 30, 20):
            cfg["rsi_max"] = saved["rsi_max"]
        elif saved.get("rsi_max") is None:
            cfg["rsi_max"] = None
        if isinstance(saved.get("auto"), bool):
            cfg["auto"] = saved["auto"]
        if isinstance(saved.get("interval_minutes"), (int, float)):
            cfg["interval_minutes"] = max(5, int(saved["interval_minutes"]))
        if isinstance(saved.get("volume_filter"), bool):
            cfg["volume_filter"] = saved["volume_filter"]
        if isinstance(saved.get("trend_filter"), bool):
            cfg["trend_filter"] = saved["trend_filter"]
        if "price_min" in saved and saved.get("price_min") is not None:
            try:
                cfg["price_min"] = float(saved["price_min"])
            except (TypeError, ValueError):
                cfg["price_min"] = None
        if "price_max" in saved and saved.get("price_max") is not None:
            try:
                cfg["price_max"] = float(saved["price_max"])
            except (TypeError, ValueError):
                cfg["price_max"] = None
        if isinstance(saved.get("telegram_token"), str):
            cfg["telegram_token"] = saved["telegram_token"]
        if isinstance(saved.get("telegram_chat"), str):
            cfg["telegram_chat"] = saved["telegram_chat"]
        if saved.get("signal_filter") in ("both", "bullish", "bearish", "none"):
            cfg["signal_filter"] = saved["signal_filter"]
        if saved.get("timeframe") in EXECUTION_TIMEFRAMES:
            cfg["timeframe"] = saved["timeframe"]
    except (OSError, ValueError):
        pass
    if cfg.get("filter_type") == "triple" and cfg["timeframe"] not in TRIPLE_FILTER_MAP:
        cfg["timeframe"] = "1d"
    return cfg


def _atomic_json_write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        return True
    except OSError as e:
        print("تعذر حفظ الملف:", e)
        return False
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _save_config(cfg=None):
    return _atomic_json_write(CONFIG_PATH, cfg if cfg is not None else _config)


_config = _load_config()
_state = {
    "running": False,
    "phase": "",
    "total": 0,
    "done": 0,
    "current": "",
    "bullish": 0,
    "bearish": 0,
    "errors": 0,
    "missing": 0,
    "stale": 0,
    "timed_out": False,
    "last_scan_at": None,
    "last_scan_duration": None,
    "universe_count": 0,
    "backtest_running": False,
    "backtest_result": None,
    "backtest_ticker": None,
}
_cfg_lock = threading.Lock()
_state_lock = threading.Lock()
_stop_event = threading.Event()

# ==================== الحماية بكلمة مرور ====================
SECRET_PATH = os.path.join(DATA_DIR, "secret.json")
_secret_lock = threading.Lock()


def _load_secret():
    try:
        with open(SECRET_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _save_secret(d):
    return _atomic_json_write(SECRET_PATH, d)


def _get_session_key():
    env_key = os.environ.get("SESSION_SECRET", "").strip()
    if len(env_key) >= 32:
        return env_key
    with _secret_lock:
        d = _load_secret()
        if not d.get("session_key"):
            d["session_key"] = secrets.token_hex(32)
            _save_secret(d)
        return d["session_key"]


def _hash_password(pw, salt_hex):
    return hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"),
                               bytes.fromhex(salt_hex), 200_000).hex()


def _set_password(pw):
    with _secret_lock:
        salt = secrets.token_hex(16)
        d = _load_secret()
        d["pwd_salt"] = salt
        d["pwd_hash"] = _hash_password(pw, salt)
        if not _save_secret(d):
            raise OSError("تعذر حفظ كلمة المرور")


def _has_password():
    d = _load_secret()
    return bool(d.get("pwd_hash") and d.get("pwd_salt"))


def _check_password(pw):
    d = _load_secret()
    h, s = d.get("pwd_hash"), d.get("pwd_salt")
    if not h or not s:
        return False
    return hmac.compare_digest(_hash_password(pw, s), h)


def _ensure_initial_password():
    if not _has_password():
        initial = os.environ.get("INITIAL_PASSWORD") or secrets.token_urlsafe(9)
        _set_password(initial)
        print("=" * 56)
        print("   كلمة المرور الابتدائية للتطبيق:", initial)
        print("   غيّرها من داخل التطبيق: زر «تغيير كلمة المرور»")
        print("=" * 56)
        return initial
    return None


app.secret_key = _get_session_key()
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
_secure_default = "true" if os.environ.get("RENDER") else "false"
app.config["SESSION_COOKIE_SECURE"] = os.environ.get("SESSION_COOKIE_SECURE", _secure_default).lower() == "true"
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["PERMANENT_SESSION_LIFETIME"] = 60 * 60 * 24 * 30


def _is_authed():
    return session.get("authed") is True


@app.before_request
def _guard():
    if request.method == "OPTIONS":
        return None
    p = request.path
    if p in ("/login", "/logout", "/api/login", "/api/health") or p.startswith("/static"):
        return None
    if _is_authed():
        return None
    if p == "/" or not p.startswith("/api/"):
        return render_template("login.html")
    return jsonify({"error": "unauthorized"}), 401


@app.after_request
def _disable_page_cache(response):
    if response.mimetype == "text/html":
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


# ==================== قاعدة البيانات ====================
def _db_connect(timeout=30):
    timeout = max(0.1, float(timeout))
    conn = sqlite3.connect(DB_PATH, timeout=timeout)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={max(100, int(timeout * 1000))}")
    except sqlite3.Error:
        pass
    return conn


def init_db():
    conn = _db_connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market TEXT,
            sector TEXT,
            ticker TEXT,
            name TEXT,
            direction TEXT,
            timeframe TEXT,
            signal_date TEXT,
            price REAL,
            rsi_value REAL,
            peak_level REAL,
            zone_timeframe TEXT,
            zone_low REAL,
            zone_high REAL,
            created_at TEXT,
            rsi_low REAL,
            stop_loss REAL,
            target_1 REAL,
            target_2 REAL,
            volume_ratio REAL,
            volume REAL,
            UNIQUE(market, ticker, timeframe, signal_date, direction)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scan_snapshots (
            cache_key TEXT PRIMARY KEY,
            completed_at REAL NOT NULL,
            total INTEGER NOT NULL,
            bullish INTEGER NOT NULL,
            bearish INTEGER NOT NULL,
            errors INTEGER NOT NULL
        )
    """)
    conn.commit()
    cols = [r[1] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()]
    for col, ctype in (("rsi_low", "REAL"), ("stop_loss", "REAL"),
                       ("target_1", "REAL"), ("target_2", "REAL"),
                       ("volume_ratio", "REAL"), ("volume", "REAL")):
        if col not in cols:
            conn.execute(f"ALTER TABLE alerts ADD COLUMN {col} {ctype}")
    conn.commit()
    conn.close()


def _canonical_date(value):
    try:
        ts = pd.to_datetime(value, utc=True)
        if pd.isna(ts):
            return str(value)
        return ts.tz_localize(None).isoformat()
    except Exception:
        return str(value)


def save_alert(market, sector, ticker, name, direction, timeframe, signal_date,
               price, rsi_value, peak_level, rsi_low=None,
               zone_tf=None, zone_low=None, zone_high=None,
               stop_loss=None, target_1=None, target_2=None,
               volume_ratio=None, volume=None):
    signal_date = _canonical_date(signal_date)
    conn = _db_connect()
    cur = conn.execute(
        """INSERT OR IGNORE INTO alerts
           (market, sector, ticker, name, direction, timeframe, signal_date,
            price, rsi_value, peak_level, rsi_low, zone_timeframe, zone_low, zone_high,
             stop_loss, target_1, target_2, volume_ratio, volume, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (market, sector, ticker, name, direction, timeframe, signal_date,
         price, rsi_value, peak_level, rsi_low, zone_tf, zone_low, zone_high,
          stop_loss, target_1, target_2, volume_ratio, volume,
         datetime.now().isoformat()),
    )
    inserted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return inserted


_ALERT_INSERT_SQL = """INSERT OR IGNORE INTO alerts
   (market, sector, ticker, name, direction, timeframe, signal_date,
    price, rsi_value, peak_level, rsi_low, zone_timeframe, zone_low, zone_high,
     stop_loss, target_1, target_2, volume_ratio, volume, created_at)
   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""


def save_alerts_batch(rows, deadline=None):
    """يدرج دفعة تنبيهات في اتصال واحد. يعيد قائمة منطقية لكل صف: هل أُدخل فعلاً؟"""
    if not rows:
        return []
    if deadline is not None and time.monotonic() >= deadline:
        return [False] * len(rows)
    remaining = 30 if deadline is None else max(0.1, deadline - time.monotonic())
    conn = _db_connect(timeout=min(30, remaining))
    inserted_flags = []
    try:
        conn.execute("BEGIN")
        for row in rows:
            if deadline is not None and time.monotonic() >= deadline:
                raise sqlite3.OperationalError("انتهت مهلة الفحص قبل حفظ التنبيهات")
            cur = conn.execute(_ALERT_INSERT_SQL, row)
            inserted_flags.append(cur.rowcount > 0)
        conn.commit()
    except sqlite3.Error as e:
        print("فشل إدراج دفعة التنبيهات:", e)
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        inserted_flags = [False] * len(rows)
    finally:
        conn.close()
    return inserted_flags


def _scan_cache_key(cfg, universe):
    tickers = "\n".join(ticker for ticker, _ in universe)
    universe_hash = hashlib.sha256(tickers.encode("utf-8")).hexdigest()[:16]
    profile = {
        "market": cfg["market"], "sector": cfg["sector"],
        "timeframe": cfg["timeframe"], "rsi_max": cfg.get("rsi_max"),
        "volume_filter": bool(cfg.get("volume_filter")),
        "trend_filter": bool(cfg.get("trend_filter")),
        "signal_filter": cfg.get("signal_filter", "both"),
        "price_min": cfg.get("price_min"),
        "price_max": cfg.get("price_max"),
        "universe": universe_hash,
    }
    return hashlib.sha256(json.dumps(profile, sort_keys=True).encode("utf-8")).hexdigest()


def _load_scan_snapshot(cache_key, timeframe):
    base_interval = CUSTOM_TIMEFRAMES.get(timeframe, (timeframe, None))[0]
    with _db_connect() as conn:
        row = conn.execute(
            "SELECT completed_at,total,bullish,bearish,errors FROM scan_snapshots WHERE cache_key=?",
            (cache_key,),
        ).fetchone()
    if not row or time.time() - row[0] > _cache_ttl(base_interval):
        return None
    return {"total": row[1], "bullish": row[2], "bearish": row[3], "errors": row[4]}


def _save_scan_snapshot(cache_key, summary, deadline=None):
    if deadline is not None and time.monotonic() >= deadline:
        return
    timeout = 30 if deadline is None else max(0.1, deadline - time.monotonic())
    with _db_connect(timeout=min(30, timeout)) as conn:
        conn.execute(
            """INSERT INTO scan_snapshots(cache_key,completed_at,total,bullish,bearish,errors)
               VALUES(?,?,?,?,?,?)
               ON CONFLICT(cache_key) DO UPDATE SET
                 completed_at=excluded.completed_at,total=excluded.total,
                 bullish=excluded.bullish,bearish=excluded.bearish,errors=excluded.errors""",
            (cache_key, time.time(), summary["total"], summary["bullish"],
             summary["bearish"], summary["errors"]),
        )


# ==================== جلب البيانات (جماعي) ====================
def resample_ohlc(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    agg = {"Open": "first", "High": "max", "Low": "min", "Close": "last"}
    if "Volume" in df.columns:
        agg["Volume"] = "sum"
    return df.resample(rule).agg(agg).dropna(how="any")


def _extract_ticker(raw, ticker, rule, tail=None):
    if isinstance(raw.columns, pd.MultiIndex):
        df = raw[ticker]
    else:
        df = raw
    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    if df.empty:
        return None
    if rule:
        df = resample_ohlc(df, rule)
    if tail:
        df = df.tail(tail)
    df = df.reset_index()
    date_col = "Datetime" if "Datetime" in df.columns else ("Date" if "Date" in df.columns else df.columns[0])
    return df.rename(columns={date_col: "Date"})


# ==================== كياش بيانات ياهو (يمنع إعادة التحميل كل دورة) ====================
def _cache_ttl(interval):
    """صلاحية متوازنة تمنع لقطة ما قبل الإغلاق من حجب شمعة مكتملة جديدة."""
    if interval in ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"):
        return 900
    if interval == "1d":
        return 7200
    return 21600


def _cache_stale_ttl(interval):
    """آخر لقطة سليمة مسموحة أثناء تحديثها في الخلفية."""
    if interval in ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"):
        return 7200
    if interval == "1d":
        return 3 * 86400
    if interval in ("5d", "1wk"):
        return 14 * 86400
    return 45 * 86400


def _cache_path(ticker, interval, base_dir=None):
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", str(ticker))
    d = os.path.join(base_dir or YF_CACHE_DIR, str(interval))
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, safe + ".csv")


_cache_write_queue = queue.Queue(maxsize=10000)


def _write_cache_worker():
    """خيط واحد يكتب ملفات الكاش لتجنب تزاحم أقراص Render عند كتابة آلاف الملفات."""
    while True:
        item = _cache_write_queue.get()
        if item is None:
            _cache_write_queue.task_done()
            break
        try:
            ticker, interval, df, base_dir = item
            if df is None or df.empty:
                continue
            out = df.copy()
            if isinstance(out.index, pd.DatetimeIndex) and out.index.tz is not None:
                out.index = out.index.tz_convert("UTC").tz_localize(None)
            path = _cache_path(ticker, interval, base_dir)
            tmp = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
            out.reset_index().to_csv(tmp, index=False)
            os.replace(tmp, path)
        except Exception:
            pass
        finally:
            _cache_write_queue.task_done()


def _start_cache_writer():
    for i in range(4):
        threading.Thread(target=_write_cache_worker, name=f"cache-writer-{i + 1}",
                         daemon=True).start()


def _read_cache(ticker, interval, max_age=None, base_dir=None):
    try:
        p = _cache_path(ticker, interval, base_dir)
        max_age = _cache_ttl(interval) if max_age is None else max_age
        if not os.path.exists(p) or time.time() - os.path.getmtime(p) > max_age:
            return None
        df = pd.read_csv(p)
        if df.empty:
            return None
        c = "Date" if "Date" in df.columns else ("Datetime" if "Datetime" in df.columns else None)
        if c is None:
            return None
        df[c] = pd.to_datetime(df[c])
        return df.set_index(c)
    except Exception:
        return None


def _write_cache(ticker, interval, df, base_dir=None):
    try:
        _cache_write_queue.put_nowait((ticker, interval, df, base_dir))
    except queue.Full:
        pass


_YF_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# تجمّع اتصالات دائم (keep-alive) لكل خيط: يزيل آلاف المصافحات (TLS handshake)
# التي كانت تُفتح لكل سهم، وهو أكبر مكسب زمني عند فحص سوق ضخم.
_local = threading.local()


def _session():
    s = getattr(_local, "sess", None)
    if s is None:
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=16, pool_maxsize=32, max_retries=0)
        s = requests.Session()
        s.headers["User-Agent"] = _YF_USER_AGENT
        s.headers["Accept"] = "application/json"
        s.headers["Accept-Encoding"] = "gzip, deflate"
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _local.sess = s
    return s


def _yahoo_json(host, path, deadline=None):
    last_error = None
    session = _session()
    for retry in range(2):
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("انتهت مهلة الفحص")
            timeout = max(0.2, min(YF_REQUEST_TIMEOUT, remaining))
        else:
            timeout = YF_REQUEST_TIMEOUT
        try:
            response = session.get(
                f"https://{host}{path}", timeout=timeout)
            if response.status_code != 200:
                raise RuntimeError(f"Yahoo HTTP {response.status_code}")
            return response.json()
        except (requests.RequestException, ValueError) as e:
            last_error = e
            if retry == 0:
                time.sleep(0.3 * (retry + 1))
    raise last_error or RuntimeError("تعذر الاتصال بـ Yahoo")


class FetchError(Exception):
    pass


_refresh_lock = threading.Lock()
_refreshing = set()


def _schedule_cache_refresh(tickers, timeframe, lookback=None, base_dir=None):
    """يحدّث الكاش خارج المسار الحرج، مع منع تنزيل الرمز نفسه مرتين."""
    tickers = list(dict.fromkeys(tickers))
    if not tickers:
        return
    base_interval = CUSTOM_TIMEFRAMES.get(timeframe, (timeframe, None))[0]
    if lookback is None:
        lookback = LOOKBACK_MAP.get(base_interval, DEFAULT_LOOKBACK)

    def _run():
        keys = []
        with _refresh_lock:
            for ticker in tickers:
                key = (ticker, base_interval, base_dir)
                if key not in _refreshing:
                    _refreshing.add(key)
                    keys.append(key)
        if not keys:
            return
        deadline = time.monotonic() + 900

        def _refresh(key):
            ticker, interval, bdir = key
            if _read_cache(ticker, interval, base_dir=bdir) is not None:
                return
            try:
                raw = _download_one(ticker, lookback, interval, deadline)
                if raw is not None and not raw.empty:
                    _write_cache(ticker, interval, raw, base_dir=bdir)
            except Exception:
                pass

        try:
            with ThreadPoolExecutor(max_workers=min(YF_WORKERS, len(keys))) as pool:
                list(pool.map(_refresh, keys))
        finally:
            with _refresh_lock:
                _refreshing.difference_update(keys)

    threading.Thread(target=_run, name=f"cache-refresh-{timeframe}", daemon=True).start()


def _download_one(ticker, lookback, base_interval, deadline=None):
    symbol = urllib.parse.quote(str(ticker), safe="")
    query = urllib.parse.urlencode({
        "range": lookback,
        "interval": base_interval,
        "includePrePost": "false",
        "events": "div,splits",
    })
    last_error = None
    hosts = ("query1.finance.yahoo.com", "query2.finance.yahoo.com",
             "query3.finance.yahoo.com")
    if sum(map(ord, str(ticker))) % 2:
        hosts = (hosts[1], hosts[2], hosts[0])
    for attempt, host in enumerate(hosts):
        if deadline is not None and time.monotonic() >= deadline:
            raise FetchError(f"انتهت مهلة جلب {ticker}")
        path = f"/v8/finance/chart/{symbol}?{query}"
        try:
            payload = _yahoo_json(host, path, deadline)
            result = (payload.get("chart", {}).get("result") or [None])[0]
            if not result:
                _note_blank_result(ticker, base_interval, lookback, payload)
                return None
            timestamps = result.get("timestamp") or []
            quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
            if not timestamps or not quote:
                _note_blank_result(ticker, base_interval, lookback, payload)
                return None

            def _values(name):
                values = list(quote.get(name) or [])
                return (values + [None] * len(timestamps))[:len(timestamps)]

            raw = pd.DataFrame({
                "Open": _values("open"),
                "High": _values("high"),
                "Low": _values("low"),
                "Close": _values("close"),
                "Volume": _values("volume"),
            }, index=pd.to_datetime(timestamps, unit="s", utc=True))
            raw.index.name = "Datetime" if base_interval.endswith(("m", "h")) else "Date"
            raw = raw.dropna(subset=["Open", "High", "Low", "Close"])
            return raw if not raw.empty else None
        except Exception as e:
            last_error = e
            if attempt == 0:
                time.sleep(0.2)
    raise FetchError(f"فشل جلب {ticker}: {last_error}")


def _iter_histories(tickers, timeframe, tail=None, stop_event=None, workers=YF_WORKERS,
                     analyze=None, deadline=None):
    base_interval, rule = timeframe, None
    if timeframe in CUSTOM_TIMEFRAMES:
        base_interval, rule = CUSTOM_TIMEFRAMES[timeframe]
    lookback = LOOKBACK_MAP.get(base_interval, DEFAULT_LOOKBACK)
    stale_tickers = []
    stale_lock = threading.Lock()

    def _analyze(ticker, df):
        if analyze is None or (deadline is not None and time.monotonic() >= deadline):
            return None
        return analyze(ticker, df)

    def _fetch_one(ticker):
        retryable = False
        try:
            cached = _read_cache(ticker, base_interval)
            stale = False
            if cached is None:
                cached = _read_cache(ticker, base_interval,
                                     max_age=_cache_stale_ttl(base_interval))
                stale = cached is not None
            if cached is not None:
                try:
                    df = _extract_ticker(cached, ticker, rule, tail)
                except Exception:
                    df = None
                if df is not None:
                    if stale:
                        df.attrs["cache_stale"] = True
                        with stale_lock:
                            stale_tickers.append(ticker)
                    return ticker, df, _analyze(ticker, df), False
            if deadline is not None and time.monotonic() >= deadline:
                return ticker, None, None, True
            raw = _download_one(ticker, lookback, base_interval, deadline)
            if raw is None:
                return ticker, None, _analyze(ticker, None), False
            _write_cache(ticker, base_interval, raw)
            try:
                df = _extract_ticker(raw, ticker, rule, tail)
            except Exception as e:
                print(f"فشل تجهيز بيانات {ticker}: {e}")
                df = None
            return ticker, df, _analyze(ticker, df), False
        except FetchError:
            return ticker, None, None, True
        except Exception as e:
            print(f"فشل جلب {ticker}: {e}")
            return ticker, None, _analyze(ticker, None), False

    tickers = list(tickers)
    pending = iter(tickers)
    workers = min(workers, max(1, len(tickers)))
    deferred = []
    ex = ThreadPoolExecutor(max_workers=workers)
    try:
        futs = {}
        for _ in range(workers):
            if deadline is not None and time.monotonic() >= deadline:
                break
            try:
                ticker = next(pending)
            except StopIteration:
                break
            futs[ex.submit(_fetch_one, ticker)] = ticker

        while futs:
            timeout = None
            if deadline is not None:
                timeout = max(0, deadline - time.monotonic())
                if timeout <= 0:
                    break
            done, _ = wait(futs, timeout=timeout, return_when=FIRST_COMPLETED)
            if not done:
                break
            for fut in done:
                ticker = futs.pop(fut)
                try:
                    ticker, df, result, retryable = fut.result()
                except Exception as e:
                    print(f"فشل جلب {ticker}: {e}")
                    ticker, df, result, retryable = (ticker, None, None, True)
                if retryable:
                    deferred.append(ticker)
                else:
                    yield ticker, df, result

                if stop_event is not None and stop_event.is_set():
                    continue
                if deadline is not None and time.monotonic() >= deadline:
                    continue
                try:
                    next_ticker = next(pending)
                except StopIteration:
                    continue
                futs[ex.submit(_fetch_one, next_ticker)] = next_ticker

            if stop_event is not None and stop_event.is_set():
                for fut in futs:
                    fut.cancel()
                break
            if deadline is not None and time.monotonic() >= deadline:
                break
    finally:
        for fut in futs:
            fut.cancel()
        ex.shutdown(wait=False, cancel_futures=True)

    if stale_tickers:
        _schedule_cache_refresh(stale_tickers, timeframe)

    if deferred and not (stop_event is not None and stop_event.is_set()) \
            and not (deadline is not None and time.monotonic() >= deadline):
        print(f"إعادة محاولة {len(deferred)} سهماً فاشلاً...")
        retry_workers = min(32, max(1, len(deferred)))
        for i in range(0, len(deferred), retry_workers):
            if stop_event is not None and stop_event.is_set():
                break
            if deadline is not None and time.monotonic() >= deadline:
                break
            batch = deferred[i:i + retry_workers]
            retry_pool = ThreadPoolExecutor(max_workers=len(batch))
            futs = {retry_pool.submit(_fetch_one, t): t for t in batch}
            try:
                timeout = None if deadline is None else max(0, deadline - time.monotonic())
                done, _ = wait(futs, timeout=timeout)
                for fut in done:
                    ticker = futs[fut]
                    try:
                        ticker, df, result, _ = fut.result()
                    except Exception as e:
                        print(f"فشل جلب {ticker}: {e}")
                        df, result = None, None
                    yield ticker, df, result
            finally:
                for fut in futs:
                    fut.cancel()
                retry_pool.shutdown(wait=False, cancel_futures=True)
            time.sleep(0.7)


def fetch_batch(tickers, timeframe, tail=None, pool=None, deadline=None):
    results = {}
    workers = YF_WORKERS if pool is None else max(1, int(pool))
    if deadline is None:
        deadline = time.monotonic() + SCAN_BUDGET_SECONDS
    for ticker, df, _ in _iter_histories(tickers, timeframe, tail=tail, workers=workers,
                                         deadline=deadline):
        if df is not None:
            results[ticker] = df
    return results


_fetch_failure_lock = threading.Lock()
_fetch_failure_count = 0


def _note_fetch_failure(ticker, interval, lookback, exc):
    """يسجّل أول عينات قليلة من فشل جلب المصدر لفحصها من السجل."""
    global _fetch_failure_count
    with _fetch_failure_lock:
        _fetch_failure_count += 1
        if _fetch_failure_count <= 15:
            print(f"فشل جلب {ticker} ({interval}/{lookback}): {type(exc).__name__}: {exc}")


_diag_count = 0


def _diag_note(ticker, msg):
    global _diag_count
    with _fetch_failure_lock:
        _diag_count += 1
        if _diag_count <= 12:
            print(f"DIAG {ticker}: {msg}")


_blank_count = 0


def _note_blank_result(ticker, interval, lookback, payload):
    """يسجّل أول استجابة فارغة من Yahoo مع رسالة الخطأ إن وُجدت."""
    global _blank_count
    with _fetch_failure_lock:
        _blank_count += 1
        if _blank_count <= 8:
            err = (payload.get("chart") or {}).get("error")
            print(f"Yahoo فارغ {ticker} ({interval}/{lookback}): error={err}")


def fetch_triple_batch(tickers, execution_tf, tail=None, workers=None, deadline=None):
    """يجلب فريمات الفلتر الثلاثي بطريقة "مصدر واحد لكل سهم".

    لكل فريم تنفيذ نحدد فريم المصدر الذي يُنزَّل مرة واحدة لكل سهم (مثال: لتنفيذ
    يومي ننزّل اليومي بمدى 5 سنوات ثم نعيد تجميعه إلى أسبوعي وشهري). هكذا يبقى
    عدد الطلبات ~طلبا واحدا لكل سهم فيتناسب الفحص مع مهلة الدقيقة على Render.
    يرجع (data_large, data_medium, data_small, stale_symbols).
    """
    if deadline is None:
        deadline = time.monotonic() + SCAN_BUDGET_SECONDS
    targets = TRIPLE_SOURCE_MAP.get(execution_tf, TRIPLE_SOURCE_MAP["1d"])
    large_tf, medium_tf, small_tf = TRIPLE_FILTER_MAP.get(
        execution_tf, ("1mo", "1wk", "1d"))
    sources = list(dict.fromkeys(src for _, src, _ in targets))
    data = {large_tf: {}, medium_tf: {}, small_tf: {}}
    stale_symbols = set()
    stale_lock = threading.Lock()
    results_lock = threading.Lock()
    failure_log = []

    def _load_source(ticker, source):
        lookback = TRIPLE_SOURCE_LOOKBACK.get(source, DEFAULT_LOOKBACK)
        cached = _read_cache(ticker, source, base_dir=YF_TRIPLE_CACHE_DIR)
        stale = False
        if cached is None:
            cached = _read_cache(ticker, source, base_dir=YF_TRIPLE_CACHE_DIR,
                                 max_age=_cache_stale_ttl(source))
            stale = cached is not None
        df = cached
        if df is not None:
            try:
                df = df.dropna(subset=["Open", "High", "Low", "Close"])
            except Exception:
                df = None
            if df is not None and df.empty:
                df = None
        if df is None:
            if deadline is not None and time.monotonic() >= deadline:
                _diag_note(ticker, f"deadline-hit source={source} remaining="
                           f"{deadline - time.monotonic():.1f}")
                return None, False
            try:
                raw = _download_one(ticker, lookback, source, deadline)
            except Exception as e:
                _note_fetch_failure(ticker, source, lookback, e)
                return None, False
            if raw is None or raw.empty:
                _diag_note(ticker, f"blank-download source={source} lookback={lookback}")
                return None, False
            _write_cache(ticker, source, raw, base_dir=YF_TRIPLE_CACHE_DIR)
            df = raw.dropna(subset=["Open", "High", "Low", "Close"])
            if df.empty:
                _diag_note(ticker, f"empty-after-dropna source={source}")
                return None, False
        return df, stale

    def _fetch_one(ticker):
        try:
            source_data = {}
            source_stale = {}
            for source in sources:
                if deadline is not None and time.monotonic() >= deadline:
                    return
                df, stale = _load_source(ticker, source)
                if df is None:
                    return
                if stale:
                    with stale_lock:
                        stale_symbols.add(ticker)
                source_data[source] = df
                source_stale[source] = stale
            frames = {}
            for target, source, rule in targets:
                try:
                    frames[target] = _extract_ticker(source_data[source], ticker, rule, tail)
                except Exception as e:
                    _diag_note(ticker, f"resample-error target={target} rule={rule}: "
                               f"{type(e).__name__}: {e}")
                    return
                if frames[target] is None:
                    _diag_note(ticker, f"resample-empty target={target} rule={rule}")
                    return
            if any(source_stale.get(src) for _, src, _ in targets):
                for frame in frames.values():
                    frame.attrs["cache_stale"] = True
            if all(target in frames for target, _, _ in targets):
                with results_lock:
                    for target, frame in frames.items():
                        data[target][ticker] = frame
        except Exception as e:
            _diag_note(ticker, f"fetch-crash: {type(e).__name__}: {e}")

    tickers = list(tickers)
    workers = min(workers or TRIPLE_FETCH_WORKERS, max(1, len(tickers)))
    pending = iter(tickers)
    futs = {}
    ex = ThreadPoolExecutor(max_workers=workers)
    try:
        for _ in range(workers):
            if deadline is not None and time.monotonic() >= deadline:
                break
            try:
                t = next(pending)
            except StopIteration:
                break
            futs[ex.submit(_fetch_one, t)] = t
        while futs:
            timeout = None
            if deadline is not None:
                timeout = max(0, deadline - time.monotonic())
                if timeout <= 0:
                    break
            done, _ = wait(futs, timeout=timeout, return_when=FIRST_COMPLETED)
            if not done:
                break
            for fut in done:
                t = futs.pop(fut)
                try:
                    fut.result()
                except Exception:
                    pass
                if deadline is not None and time.monotonic() >= deadline:
                    continue
                try:
                    nxt = next(pending)
                except StopIteration:
                    continue
                futs[ex.submit(_fetch_one, nxt)] = nxt
            if deadline is not None and time.monotonic() >= deadline:
                break
    finally:
        for fut in futs:
            fut.cancel()
        ex.shutdown(wait=False, cancel_futures=True)

    failed = [t for t in tickers if t not in data[large_tf]]
    if failed and (deadline is None or time.monotonic() < deadline):
        print(f"إعادة محاولة {len(failed)} سهماً فاشلاً في الفحص الثلاثي...")
        time.sleep(3)
        retry_workers = min(16, max(1, len(failed)))
        rp = ThreadPoolExecutor(max_workers=retry_workers)
        try:
            rfuts = {rp.submit(_fetch_one, t): t for t in failed}
            timeout = None if deadline is None else max(0, deadline - time.monotonic())
            done, _ = wait(rfuts, timeout=timeout)
            for fut in done:
                try:
                    fut.result()
                except Exception:
                    pass
        finally:
            rp.shutdown(wait=False, cancel_futures=True)
    return data[large_tf], data[medium_tf], data[small_tf], stale_symbols


def zone_timeframe_for(execution_tf: str) -> str:
    return TIMEFRAME_MAP.get(execution_tf, DEFAULT_ZONE_TIMEFRAME)


# ==================== الفحص ====================
def compute_stops(direction, entry, atr, zone_low, zone_high):
    """
    يقترح وقف الخسارة والهدفين بناءً على ATR ومنطقة الطلب/العرض.
    يرجع (stop_loss, target_1, target_2) أو (None, None, None) إذا تعذر الحساب.
    """
    if atr is None or atr <= 0 or entry is None:
        return None, None, None
    entry = float(entry)
    if direction == "bullish":
        stop = entry - 1.5 * atr
        if zone_low is not None and zone_low < entry - 0.75 * atr:
            stop = min(stop, zone_low)
        stop = min(max(stop, entry - 4 * atr), entry - 0.75 * atr)
        risk = entry - stop
        return round(stop, 2), round(entry + 1.5 * risk, 2), round(entry + 2.5 * risk, 2)
    stop = entry + 1.5 * atr
    if zone_high is not None and zone_high > entry + 0.75 * atr:
        stop = max(stop, zone_high)
    stop = max(min(stop, entry + 4 * atr), entry + 0.75 * atr)
    risk = stop - entry
    return round(stop, 2), round(entry - 1.5 * risk, 2), round(entry - 2.5 * risk, 2)


def _tg_credentials():
    token = os.environ.get("TELEGRAM_BOT_TOKEN") or _config.get("telegram_token") or ""
    chat = os.environ.get("TELEGRAM_CHAT_ID") or _config.get("telegram_chat") or ""
    return token.strip(), chat.strip()


def telegram_send(text, token=None, chat_id=None):
    """يرسل رسالة تيليجرام. يرجع True عند النجاح أو إذا كانت الإعدادات فارغة (لا رسائل)."""
    token = (token or "").strip() or _tg_credentials()[0]
    chat_id = (chat_id or "").strip() or _tg_credentials()[1]
    if not token or not chat_id:
        return False
    try:
        data = urllib.parse.urlencode({
            "chat_id": chat_id, "text": text, "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=data)
        with urllib.request.urlopen(req, timeout=20) as r:
            ok = r.status == 200
            if not ok:
                print("تيليجرام: استجابة غير متوقعة", r.status)
            return ok
    except Exception as e:
        print("فشل إرسال تيليجرام:", e)
        return False


def telegram_alert_message(a):
    market = "تاسي" if a["market"] == "saudi" else "أمريكا"
    is_triple = a.get("direction") == "triple_bullish"
    tag = "شراء / طلب (ثلاثي)" if is_triple else ("شراء / طلب" if a["direction"] == "bullish" else "بيع / عرض")
    icon = "🔵" if "bullish" in a.get("direction", "") else "🔴"
    ticker = html.escape(str(a.get("ticker") or "—"))
    name = html.escape(str(a.get("name") or "—"))
    sector = html.escape(str(a.get("sector") or "—"))
    timeframe = html.escape(str(a.get("timeframe") or "—"))
    signal_date = html.escape(str(a.get("signal_date") or "—"))
    lines = [
        f"{icon} <b>{tag}: {ticker}</b>",
        f"الاسم: {name}",
        f"السوق: {market} | القطاع: {sector}",
        f"فريم التنفيذ: {timeframe}",
        f"شمعة الإشارة: {signal_date}",
        f"السعر: {a['price']}",
    ]
    if is_triple:
        lg = a.get("large_timeframe") or {}
        md = a.get("medium_timeframe") or {}
        sm = a.get("small_timeframe") or {}
        lines.append(f"--- الفريم الكبير ({a.get('large_tf', '—')}) ---")
        lines.append(f"MACD: {lg.get('macd')} | Signal: {lg.get('macd_signal')} | SMA20: {lg.get('sma20')}")
        lines.append(f"--- الفريم الوسط ({a.get('medium_tf', '—')}) ---")
        lines.append(f"RSI: {md.get('rsi')} | SMA50: {md.get('sma50')}")
        lines.append(f"--- الفريم الصغير ({a.get('small_tf', '—')}) ---")
        lines.append(f"Stoch %K: {sm.get('stoch_k')} | %D: {sm.get('stoch_d')}")
    else:
        lines.append(f"RSI عند الاختراق: {round(a['rsi_value'], 2)}")
        if a.get("rsi_low") is not None:
            lines.append(f"قاع RSI: {round(a['rsi_low'], 2)}")
        if a.get("volume_ratio") is not None:
            lines.append(f"حجم الاختراق: ×{round(a['volume_ratio'], 2)} المتوسط")
        if a.get("volume") is not None:
            lines.append(f"الفوليوم: {round(a['volume']):,}")
        if a.get("zone_low") is not None and a.get("zone_high") is not None:
            lines.append(f"منطقة الطلب/العرض: {round(a['zone_low'], 2)} — {round(a['zone_high'], 2)}")
    if a.get("stop_loss") is not None:
        lines.append(f"وقف الخسارة: {a['stop_loss']}")
    if a.get("target_1") is not None:
        lines.append(f"الهدف الأول: {a['target_1']} | الهدف الثاني: {a['target_2']}")
    return "\n".join(lines)
def _claim_scan():
    with _state_lock:
        if _state["running"]:
            return False
        _state["running"] = True
        _state["phase"] = "تهيئة الفحص"
        return True


def _launch_scan(override=None):
    if not _claim_scan():
        return False
    _stop_event.clear()
    ft = (override or {}).get("filter_type") or _config.get("filter_type", "rsi")
    target = run_scan_triple if ft == "triple" else run_scan
    try:
        threading.Thread(target=target,
                         kwargs={"override": override, "claimed": True},
                         daemon=True).start()
    except Exception:
        with _state_lock:
            _state["running"] = False
            _state["phase"] = ""
        raise
    return True


def run_scan(override=None, claimed=False):
    if not claimed and not _claim_scan():
        return
    started = time.monotonic()
    _deadline = started + SCAN_BUDGET_SECONDS
    try:
        with _cfg_lock:
            cfg = dict(_config)
        if override:
            cfg.update(override)
        universe = build_universe(cfg["market"], cfg["sector"])
        zone_tf = zone_timeframe_for(cfg["timeframe"])
        rsi_max = cfg.get("rsi_max")
        vol_on = bool(cfg.get("volume_filter"))
        trend_on = bool(cfg.get("trend_filter"))
        min_bars = RSI_PERIOD + PIVOT_LEFT + PIVOT_RIGHT + 5
    except Exception as e:
        print(f"تعذر تهيئة الفحص: {e}")
        with _state_lock:
            _state.update({"running": False, "phase": "", "last_scan_status": "failed"})
        return

    with _state_lock:
        _state.update({
            "running": True, "phase": "جلب البيانات", "total": len(universe),
            "done": 0, "current": "", "bullish": 0, "bearish": 0, "errors": 0,
            "missing": 0, "stale": 0, "timed_out": False,
            "universe_count": len(universe),
            "market": cfg["market"], "sector": cfg["sector"], "strategy": "rsi",
        })

    cache_key = _scan_cache_key(cfg, universe)
    try:
        snapshot = _load_scan_snapshot(cache_key, cfg["timeframe"])
    except sqlite3.Error as e:
        print(f"تعذر قراءة نتيجة الفحص المحفوظة: {e}")
        snapshot = None
    if snapshot and snapshot["total"] == len(universe):
        with _state_lock:
            _state.update({
                "running": False, "phase": "", "done": snapshot["total"],
                "bullish": snapshot["bullish"], "bearish": snapshot["bearish"],
                "errors": snapshot["errors"], "last_scan_at": datetime.now().isoformat(),
                "last_scan_duration": round(time.monotonic() - started, 1),
                "last_scan_status": "cached", "missing": 0, "stale": 0,
                "timed_out": False,
            })
        print(f"استخدام نتيجة فحص محفوظة حديثة: {len(universe)} سهم")
        return

    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] بدء فحص {len(universe)} "
          f"| {MARKET_AR.get(cfg['market'], cfg['market'])} / {cfg['sector']} "
          f"| تنفيذ {cfg['timeframe']} | منطقة من {zone_tf} | RSI شراء <= {rsi_max if rsi_max else 'بدون حد'}")

    scan_failed = False
    budget_exhausted = False
    try:
        items = list(universe)
        ticker_meta = {ticker: meta for ticker, meta in items}

        with _state_lock:
            _state["phase"] = "جلب البيانات وتحليلها"

        alert_rows = []
        alert_meta = []
        inserted_total = 0
        alerts_lock = threading.Lock()
        price_min = cfg.get("price_min")
        price_max = cfg.get("price_max")

        def _analyze_one(t, meta, df):
            nonlocal inserted_total
            if df is None or len(df) < min_bars:
                return [], (0, 0, True)
            try:
                is_stale = bool(df.attrs.get("cache_stale"))
                bulls = bears = 0
                rsi_series = calculate_rsi(df["Close"], RSI_PERIOD)
                atr_series = (calculate_atr(df["High"], df["Low"], df["Close"], ATR_PERIOD)
                              if {"High", "Low"}.issubset(df.columns) else None)
                bullish = detect_signal(df, RSI_PERIOD, PIVOT_LEFT, PIVOT_RIGHT, TOLERANCE,
                                        rsi_max=rsi_max,
                                        min_volume_ratio=1.5 if vol_on else None,
                                        trend_filter=trend_on, rsi=rsi_series,
                                        _precomputed_atr=atr_series)
                bearish = detect_signal_bearish(df, RSI_PERIOD, PIVOT_LEFT, PIVOT_RIGHT, TOLERANCE,
                                                min_volume_ratio=1.5 if vol_on else None,
                                                trend_filter=trend_on, rsi=rsi_series,
                                                _precomputed_atr=atr_series)
                signals = []
                for result, direction in ((bullish, "bullish"), (bearish, "bearish")):
                    if not result or not result.get("fresh_breakout"):
                        continue
                    if is_stale:
                        continue
                    sig_filter = cfg.get("signal_filter", "both")
                    if sig_filter != "both" and sig_filter != direction:
                        continue
                    p = result["price"]
                    if price_min is not None and p < price_min:
                        continue
                    if price_max is not None and p > price_max:
                        continue
                    if zone_tf == cfg["timeframe"]:
                        zdf = df
                    else:
                        zdf = fetch_batch([t], zone_tf, tail=250, pool=1,
                                          deadline=_deadline).get(t)
                    zone = None
                    if zdf is not None and len(zdf) >= min_bars:
                        zone = get_divergence_zone(zdf, direction, RSI_PERIOD,
                                                   PIVOT_LEFT, PIVOT_RIGHT, TOLERANCE)
                    zone_low = zone["zone_low"] if zone else None
                    zone_high = zone["zone_high"] if zone else None
                    stop, t1, t2 = compute_stops(direction, result["price"],
                                                 result.get("atr"), zone_low, zone_high)
                    row = (
                        cfg["market"], meta.get("sector", cfg["sector"]), t, meta.get("name", ""),
                        direction, cfg["timeframe"], _canonical_date(result["signal_date"]),
                        result["price"], result["rsi_value"], result["peak_level"],
                        result.get("rsi_low"), zone_tf, zone_low, zone_high,
                        stop, t1, t2,
                        result.get("volume_ratio"), result.get("volume"),
                        datetime.now().isoformat(),
                    )
                    meta_info = {
                        "market": cfg["market"], "sector": meta.get("sector", cfg["sector"]),
                        "ticker": t, "name": meta.get("name", ""), "direction": direction,
                        "timeframe": cfg["timeframe"], "signal_date": result["signal_date"],
                        "price": result["price"], "rsi_value": result["rsi_value"],
                        "rsi_low": result.get("rsi_low"), "volume_ratio": result.get("volume_ratio"),
                        "volume": result.get("volume"),
                        "zone_low": zone_low, "zone_high": zone_high,
                        "stop_loss": stop, "target_1": t1, "target_2": t2,
                    }
                    with alerts_lock:
                        alert_rows.append(row)
                        alert_meta.append(meta_info)
                        inserted = save_alerts_batch([row], deadline=_deadline)[0]
                        if inserted:
                            inserted_total += 1
                    if inserted:
                        threading.Thread(target=telegram_send,
                                         args=(telegram_alert_message(meta_info),),
                                         daemon=True).start()
                    if direction == "bullish":
                        bulls += 1
                    else:
                        bears += 1
                return signals, (bulls, bears, False)
            except Exception as e:
                print(f"  خطأ أثناء فحص {t}: {e}")
                return [], (0, 0, True)

        all_signals = []
        sig_exec_dfs = {}
        tickers = [ticker for ticker, _ in items]
        data_deadline = min(_deadline, started + 40)
        for ticker, df, result in _iter_histories(
                tickers, cfg["timeframe"], tail=400,
                stop_event=_stop_event, workers=YF_WORKERS, deadline=data_deadline,
                analyze=lambda t, d: _analyze_one(t, ticker_meta[t], d)):
            if _stop_event.is_set():
                break
            if result is None:
                nb, ns, err = 0, 0, True
            else:
                signals, (nb, ns, err) = result
                for t, m, sig_result, direction in signals:
                    sig_exec_dfs[t] = df
                    all_signals.append((t, m, sig_result, direction))
            with _state_lock:
                _state["current"] = ticker
                _state["done"] = min(_state["done"] + 1, _state["total"])
                _state["bullish"] += nb
                _state["bearish"] += ns
                if err:
                    _state["errors"] += 1
                if df is not None and df.attrs.get("cache_stale"):
                    _state["stale"] += 1
                _state["phase"] = f"جلب وتحليل {_state['done']}/{_state['total']}"
        if alert_rows:
            print(f"إدراج {inserted_total} إشارة جديدة من {len(alert_rows)}")
    except Exception:
        import traceback
        traceback.print_exc()
        scan_failed = True
    else:
        scan_failed = False
    finally:
        cancelled = _stop_event.is_set()
        with _state_lock:
            missing = min(_state["total"], _state["errors"]
                          + max(0, _state["total"] - _state["done"])
                          + (1 if budget_exhausted else 0))
            timed_out = time.monotonic() >= _deadline and missing > 0
            summary = {
                "total": _state["total"], "done": _state["done"],
                "bullish": _state["bullish"], "bearish": _state["bearish"],
                "errors": _state["errors"],
            }
            _state.update({
                "running": False, "phase": "",
                "last_scan_at": datetime.now().isoformat(),
                "last_scan_duration": round(time.monotonic() - started, 1),
                "last_scan_status": ("cancelled" if cancelled else
                                     ("failed" if scan_failed else
                                      ("partial" if missing else "completed"))),
                "missing": missing, "timed_out": timed_out,
            })
        if missing and "tickers" in locals():
            _schedule_cache_refresh(tickers, cfg["timeframe"])
        if (not cancelled and not scan_failed and summary["done"] == summary["total"]
                and summary["errors"] == 0 and _state["stale"] == 0):
            _save_scan_snapshot(cache_key, summary, deadline=_deadline)
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] انتهى الفحص في {time.monotonic() - started:.1f} ث "
          f"| طلب: {_state['bullish']} | عرض: {_state['bearish']}")


def run_scan_triple(override=None, claimed=False):
    """الفلتر الثلاثي: MACD + RSI + Stochastic على 3 فريمات."""
    if not claimed and not _claim_scan():
        return
    started = time.monotonic()
    _deadline = started + TRIPLE_BUDGET_SECONDS
    try:
        with _cfg_lock:
            cfg = dict(_config)
        if override:
            cfg.update(override)
        universe = build_universe(cfg["market"], cfg["sector"])
        execution_tf = cfg["timeframe"]
        large_tf, medium_tf, small_tf = TRIPLE_FILTER_MAP.get(
            execution_tf, ("1mo", "1wk", "1d"))
        min_bars_small = 40
    except Exception as e:
        print(f"تعذر تهيئة الفحص الثلاثي: {e}")
        with _state_lock:
            _state.update({"running": False, "phase": "", "last_scan_status": "failed"})
        return

    with _state_lock:
        _state.update({
            "running": True, "phase": "جلب البيانات (3 فريمات)", "total": len(universe),
            "done": 0, "current": "", "bullish": 0, "bearish": 0, "errors": 0,
            "missing": 0, "stale": 0, "timed_out": False,
            "universe_count": len(universe),
            "market": cfg["market"], "sector": cfg["sector"], "strategy": "triple",
        })

    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] بدء فحص ثلاثي {len(universe)} "
          f"| {MARKET_AR.get(cfg['market'], cfg['market'])} / {cfg['sector']} "
          f"| الفريمات: {large_tf} ← {medium_tf} ← {small_tf}")

    scan_failed = False
    budget_exhausted = False
    try:
        items = list(universe)
        ticker_meta = {ticker: meta for ticker, meta in items}
        tickers = [t for t, _ in items]

        # جلب البيانات: فريم مصدر واحد لكل سهم يُنزَّل ثم يُعاد تجميعه إلى
        # الفريمات الثلاثة. الميزانية مطلقة من بداية الفحص ولا تتأثر بتأخير
        # التهيئة أو تنافس الخيوط بعد انتهاء فحص RSI مباشرة.
        fetch_deadline = _deadline - 8
        triple_sources = [src for _, src, _ in TRIPLE_SOURCE_MAP.get(
            execution_tf, TRIPLE_SOURCE_MAP["1d"])]
        triple_sources = list(dict.fromkeys(triple_sources))
        with _state_lock:
            _state["phase"] = f"جلب بيانات {large_tf}←{medium_tf}←{small_tf}"
        data_large, data_medium, data_small, stale_symbols = fetch_triple_batch(
            tickers, execution_tf, tail=400, deadline=fetch_deadline)
        with _state_lock:
            _state["phase"] = "تحليل الفلتر الثلاثي"
            _state["stale"] = len(stale_symbols)
        print(f"جلب البيانات الثلاثي في {time.monotonic() - started:.1f} ث "
              f"| نجح: {len(data_large)} / {len(tickers)}")

        price_min = cfg.get("price_min")
        price_max = cfg.get("price_max")

        def _analyze_one(t, meta):
            if time.monotonic() >= _deadline:
                return [], (0, 0, True)
            df_l = data_large.get(t)
            df_m = data_medium.get(t)
            df_s = data_small.get(t)
            if (df_l is None or df_m is None or df_s is None
                    or len(df_l) < 60 or len(df_m) < 60 or len(df_s) < min_bars_small):
                return [], (0, 0, True)
            if any(df.attrs.get("cache_stale") for df in (df_l, df_m, df_s)):
                return [], (0, 0, False)
            try:
                result = detect_triple_filter(df_l, df_m, df_s)
                if not result or not result.get("fresh_breakout"):
                    return [], (0, 0, False)
                p = result["price"]
                if price_min is not None and p < price_min:
                    return [], (0, 0, False)
                if price_max is not None and p > price_max:
                    return [], (0, 0, False)
                return [(t, meta, result)], (1, 0, False)
            except Exception as e:
                print(f"  خطأ أثناء فحص ثلاثي {t}: {e}")
                return [], (0, 0, True)

        # تحليل متوازٍ ضمن المهلة نفسها
        all_triple_signals = []
        pool = ThreadPoolExecutor(max_workers=ANALYSIS_WORKERS)
        futures = {pool.submit(_analyze_one, t, meta): t for t, meta in items}
        try:
            timeout = max(0, _deadline - time.monotonic())
            done, _ = wait(futures, timeout=timeout)
            for fut in done:
                if time.monotonic() >= _deadline:
                    budget_exhausted = True
                    break
                t = futures[fut]
                with _state_lock:
                    _state["current"] = t
                    _state["done"] = min(_state["done"] + 1, _state["total"])
                try:
                    signals, (nb, ns, err) = fut.result()
                    for t2, m2, sig_result in signals:
                        all_triple_signals.append((t2, m2, sig_result))
                    with _state_lock:
                        _state["bullish"] += nb
                        _state["bearish"] += ns
                        if err:
                            _state["errors"] += 1
                except Exception:
                    with _state_lock:
                        _state["errors"] += 1
        finally:
            for fut in futures:
                fut.cancel()
            pool.shutdown(wait=False, cancel_futures=True)

        # حفظ مجمع للإشارات المكتشفة
        if all_triple_signals:
            alert_rows = []
            alert_meta = []
            for t, meta, result in all_triple_signals:
                if time.monotonic() >= _deadline:
                    budget_exhausted = True
                    break
                stop, t1, t2 = compute_stops("triple_bullish", result["price"],
                                             result.get("atr"), None, None)
                row = (
                    cfg["market"], meta.get("sector", cfg["sector"]), t, meta.get("name", ""),
                    "triple_bullish", execution_tf, _canonical_date(result["signal_date"]),
                    result["price"], result.get("rsi_value"), result.get("peak_level"),
                    None, None, None, None,
                    stop, t1, t2, None, None,
                    datetime.now().isoformat(),
                )
                alert_rows.append(row)
                alert_meta.append({
                    "market": cfg["market"], "sector": meta.get("sector", cfg["sector"]),
                    "ticker": t, "name": meta.get("name", ""), "direction": "triple_bullish",
                    "timeframe": execution_tf, "signal_date": result["signal_date"],
                    "price": result["price"], "rsi_value": result.get("rsi_value"),
                    "large_tf": large_tf, "medium_tf": medium_tf, "small_tf": small_tf,
                    "large_timeframe": result.get("large_timeframe"),
                    "medium_timeframe": result.get("medium_timeframe"),
                    "small_timeframe": result.get("small_timeframe"),
                    "stop_loss": stop, "target_1": t1, "target_2": t2,
                })
            inserted_flags = save_alerts_batch(alert_rows, deadline=_deadline)
            new_count = sum(1 for f in inserted_flags if f)
            for inserted, meta in zip(inserted_flags, alert_meta):
                if inserted:
                    threading.Thread(target=telegram_send,
                                     args=(telegram_alert_message(meta),), daemon=True).start()
            print(f"إدراج {new_count} إشارة جديدة من {len(alert_rows)}")

    except Exception:
        import traceback
        traceback.print_exc()
        scan_failed = True
    finally:
        with _state_lock:
            missing = min(_state["total"], _state["errors"]
                          + max(0, _state["total"] - _state["done"])
                          + (1 if budget_exhausted else 0))
            timed_out = time.monotonic() >= _deadline and missing > 0
            _state.update({
                "running": False, "phase": "",
                "last_scan_at": datetime.now().isoformat(),
                "last_scan_duration": round(time.monotonic() - started, 1),
                "last_scan_status": ("failed" if scan_failed else
                                     ("partial" if missing else "completed")),
                "missing": missing, "timed_out": timed_out,
            })
        if missing and "tickers" in locals():
            for src in triple_sources:
                _schedule_cache_refresh(tickers, src,
                                        lookback=TRIPLE_SOURCE_LOOKBACK.get(src),
                                        base_dir=YF_TRIPLE_CACHE_DIR)
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] انتهى الفحص الثلاثي في {time.monotonic() - started:.1f} ث "
          f"| إشارات: {_state['bullish']}")


def scheduler_loop():
    while True:
        time.sleep(10)
        try:
            with _cfg_lock:
                auto = _config["auto"]
                interval = _config["interval_minutes"]
            with _state_lock:
                running = _state["running"]
                last = _state["last_scan_at"]
            if not auto or running:
                continue
            overdue = True
            if last:
                last_dt = datetime.fromisoformat(last)
                overdue = (datetime.now() - last_dt).total_seconds() >= interval * 60
            if overdue:
                _launch_scan()
        except Exception as e:
            print("خطأ في الجدولة:", e)


# ==================== الواجهات ====================
def _sectors_payload():
    return {
        "saudi": [{"id": s["id"], "ar": s["ar"], "count": s["count"]} for s in market_sectors("saudi")],
        "us": [{"id": s["id"], "ar": s["ar"], "count": s["count"]} for s in market_sectors("us")],
    }


def _config_payload():
    with _cfg_lock:
        cfg = dict(_config)
    with _state_lock:
        st = dict(_state)
    triple_tfs = TRIPLE_FILTER_MAP.get(cfg["timeframe"], ("1mo", "1wk", "1d"))
    return {**cfg, "status": st, "zone_timeframe": zone_timeframe_for(cfg["timeframe"]),
            "triple_timeframes": {"large": triple_tfs[0], "medium": triple_tfs[1], "small": triple_tfs[2]}}


@app.route("/")
def index():
    requested_strategy = request.args.get("strategy")
    changed = False
    with _cfg_lock:
        if requested_strategy in ("rsi", "triple"):
            if _config.get("filter_type") != requested_strategy:
                _config["filter_type"] = requested_strategy
                changed = True
            if requested_strategy == "triple":
                if _config.get("signal_filter") != "none":
                    _config["signal_filter"] = "none"
                    changed = True
                if _config["timeframe"] not in TRIPLE_FILTER_MAP:
                    _config["timeframe"] = "1d"
                    changed = True
            elif _config.get("signal_filter") == "none":
                _config["signal_filter"] = "both"
                changed = True
        cfg = dict(_config)
    if changed:
        _save_config(cfg)
    return render_template(
        "index.html",
        market=cfg["market"],
        sector=cfg["sector"],
        timeframe=cfg["timeframe"],
        rsi_max=cfg["rsi_max"],
        auto=cfg["auto"],
        interval=cfg["interval_minutes"],
        filter_type=cfg.get("filter_type", "rsi"),
        signal_filter=cfg.get("signal_filter", "both"),
    )


@app.route("/login")
def login_page():
    if _is_authed():
        return redirect("/")
    return render_template("login.html")


@app.route("/help")
def help_page():
    return render_template("help.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/api/health")
def api_health():
    return jsonify({
        "ok": True, "engine": "chart-v4", "workers": YF_WORKERS,
        "ui_version": UI_VERSION, "strategy_switch": True,
    })


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(force=True, silent=True) or {}
    if _check_password(str(data.get("password", ""))):
        session["authed"] = True
        session.permanent = True
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "كلمة المرور غير صحيحة"}), 401


@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/api/change-password", methods=["POST"])
def api_change_password():
    data = request.get_json(force=True, silent=True) or {}
    old = str(data.get("old", ""))
    new = str(data.get("new", ""))
    if not _check_password(old):
        return jsonify({"ok": False, "error": "كلمة المرور الحالية غير صحيحة"}), 401
    if len(new) < 8:
        return jsonify({"ok": False, "error": "كلمة المرور الجديدة قصيرة جداً (8 أحرف على الأقل)"}), 400
    _set_password(new)
    return jsonify({"ok": True})


@app.route("/api/meta")
def api_meta():
    return jsonify({
        "markets": [{"id": "saudi", "ar": MARKET_AR["saudi"]},
                    {"id": "us", "ar": MARKET_AR["us"]}],
        "sectors": _sectors_payload(),
        "timeframes": [
            {"id": "15m", "ar": "15 دقيقة"},
            {"id": "1h", "ar": "ساعة"},
            {"id": "1d", "ar": "يومي"},
            {"id": "1wk", "ar": "أسبوعي"},
            {"id": "1mo", "ar": "شهري"},
        ],
        "rsi_options": [
            {"value": None, "ar": "كل الحالات"},
            {"value": 50, "ar": "أقل من 50 (تحت خط الوسط)"},
            {"value": 40, "ar": "قريب من 30-40 (أقل من 40)"},
            {"value": 30, "ar": "تشبع بيعي (أقل من 30)"},
            {"value": 20, "ar": "تشبع بيعي قوي (أقل من 20)"},
        ],
        "saudi_indices": SAUDI_INDICES,
        "us_indices": US_INDICES,
        "sector_ar": {**SAUDI_SECTOR_AR, **US_SECTOR_AR},
        "interval_default": SCAN_INTERVAL_DEFAULT,
    })


@app.route("/api/config")
def api_config():
    return jsonify(_config_payload())


@app.route("/api/config", methods=["POST"])
def api_set_config():
    data = request.get_json(force=True, silent=True) or {}
    with _cfg_lock:
        if "market" in data and data["market"] in ("saudi", "us"):
            _config["market"] = data["market"]
            if _config["sector"] not in _valid_sector_ids(_config["market"]):
                _config["sector"] = "all"
        if "filter_type" in data and data["filter_type"] in ("rsi", "triple"):
            _config["filter_type"] = data["filter_type"]
        if "sector" in data:
            sec = str(data["sector"])
            if sec in _valid_sector_ids(_config["market"]):
                _config["sector"] = sec
        if "timeframe" in data and data["timeframe"] in EXECUTION_TIMEFRAMES:
            _config["timeframe"] = data["timeframe"]
        if "rsi_max" in data:
            v = data["rsi_max"]
            if v is None or v == "":
                _config["rsi_max"] = None
            elif v in (20, 30, 40, 50) or str(v) in ("20", "30", "40", "50"):
                _config["rsi_max"] = int(v)
        if "auto" in data and isinstance(data["auto"], bool):
            _config["auto"] = data["auto"]
        if "interval_minutes" in data:
            try:
                _config["interval_minutes"] = min(1440, max(5, int(data["interval_minutes"])))
            except (TypeError, ValueError):
                pass
        if "volume_filter" in data and isinstance(data["volume_filter"], bool):
            _config["volume_filter"] = data["volume_filter"]
        if "trend_filter" in data and isinstance(data["trend_filter"], bool):
            _config["trend_filter"] = data["trend_filter"]
        if "price_min" in data:
            v = data["price_min"]
            if v is None or v == "":
                _config["price_min"] = None
            else:
                try:
                    _config["price_min"] = max(0.0, float(v))
                except (TypeError, ValueError):
                    pass
        if "price_max" in data:
            v = data["price_max"]
            if v is None or v == "":
                _config["price_max"] = None
            else:
                try:
                    _config["price_max"] = max(0.0, float(v))
                except (TypeError, ValueError):
                    pass
        if "telegram_token" in data and isinstance(data["telegram_token"], str):
            _config["telegram_token"] = data["telegram_token"].strip()
        if "telegram_chat" in data and isinstance(data["telegram_chat"], str):
            _config["telegram_chat"] = data["telegram_chat"].strip()
        if "signal_filter" in data and data["signal_filter"] in ("both", "bullish", "bearish", "none"):
            _config["signal_filter"] = data["signal_filter"]
        if (_config.get("filter_type") == "triple"
                and _config["timeframe"] not in TRIPLE_FILTER_MAP):
            _config["timeframe"] = "1d"
        saved = dict(_config)
        saved_ok = _save_config(saved)
    if not saved_ok:
        return jsonify({"error": "تعذر حفظ الإعدادات"}), 500
    return jsonify(_config_payload())


@app.route("/api/scan-now", methods=["POST"])
def api_scan_now():
    data = request.get_json(force=True, silent=True) or {}
    override = {}
    if data.get("filter_type") in ("rsi", "triple"):
        override["filter_type"] = data["filter_type"]
    if data.get("market") in ("saudi", "us"):
        override["market"] = data["market"]
    if isinstance(data.get("sector"), str):
        mkt = override.get("market", _config["market"])
        if data["sector"] in _valid_sector_ids(mkt):
            override["sector"] = data["sector"]
        else:
            override["sector"] = "all"
    if data.get("timeframe") in EXECUTION_TIMEFRAMES:
        override["timeframe"] = data["timeframe"]
    if (override.get("filter_type", _config.get("filter_type")) == "triple"
            and override.get("timeframe", _config["timeframe"]) not in TRIPLE_FILTER_MAP):
        override["timeframe"] = "1d"
    if not _launch_scan(override):
        return jsonify({"status": "فحص جارٍ بالفعل"}), 409
    return jsonify({"status": "بدأ الفحص..."})


@app.route("/api/stop-scan", methods=["POST"])
def api_stop_scan():
    _stop_event.set()
    with _state_lock:
        _state["phase"] = "جارٍ الإيقاف..."
    return jsonify({"status": "جارٍ إيقاف الفحص..."})


@app.route("/api/alerts")
def api_alerts():
    market = request.args.get("market")
    sector = request.args.get("sector")
    direction = request.args.get("direction")
    strategy = request.args.get("strategy")
    try:
        limit = min(2000, max(1, int(request.args.get("limit", 300))))
    except (TypeError, ValueError):
        return jsonify({"error": "limit غير صالح"}), 400
    conn = _db_connect()
    conn.row_factory = sqlite3.Row
    sql = "SELECT * FROM alerts"
    where, params = [], []
    if market:
        where.append("market=?")
        params.append(market)
    if sector and sector != "all":
        where.append("sector=?")
        params.append(sector)
    if strategy == "triple":
        where.append("direction=?")
        params.append("triple_bullish")
    elif strategy == "rsi":
        if direction == "bullish":
            where.append("direction=?")
            params.append("bullish")
        elif direction == "bearish":
            where.append("direction=?")
            params.append("bearish")
        else:
            where.append("direction IN (?,?)")
            params.extend(("bullish", "bearish"))
    elif direction == "bullish":
        where.append("direction IN (?,?)")
        params.extend(("bullish", "triple_bullish"))
    elif direction == "bearish":
        where.append("direction=?")
        params.append(direction)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/clear-alerts", methods=["POST"])
def api_clear_alerts():
    with _db_connect() as conn:
        conn.execute("DELETE FROM alerts")
        conn.execute("DELETE FROM scan_snapshots")
    return jsonify({"status": "تم مسح التنبيهات"})


@app.route("/api/refresh-lists", methods=["POST"])
def api_refresh_lists():
    def _run():
        refresh_lists(force=True)
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "بدأ تحديث قوائم الأسهم والقطاعات..."})


@app.route("/api/telegram-test", methods=["POST"])
def api_telegram_test():
    data = request.get_json(force=True, silent=True) or {}
    token = str(data.get("token", "")).strip()
    chat = str(data.get("chat", "")).strip()
    token = token or _tg_credentials()[0]
    chat = chat or _tg_credentials()[1]
    if not token or not chat:
        return jsonify({"ok": False, "error": "أدخل توكن البوت ومعرف الشات أولاً"}), 400
    ok = telegram_send("✅ رسالة تجريبية من فلتر انفراج RSI — الإشعارات تعمل!", token, chat)
    if not ok:
        return jsonify({"ok": False, "error": "فشل الإرسال — تحقق من التوكن والمعرف (رسائل البوت إلى الشات ممنوعة حتى يبدأه المستخدم)"}), 400
    return jsonify({"ok": True, "status": "وصلت رسالة تجريبية ✓"})


@app.route("/api/backtest", methods=["POST"])
def api_backtest():
    data = request.get_json(force=True, silent=True) or {}
    ticker = str(data.get("ticker", "")).strip()
    timeframe = str(data.get("timeframe", "1d"))
    horizon = int(data.get("horizon", 10) or 10)
    if not ticker:
        return jsonify({"error": "أدخل رمز السهم أولاً (مثال: 2222.SR أو AAPL)"}), 400
    if timeframe not in EXECUTION_TIMEFRAMES:
        timeframe = "1d"
    with _state_lock:
        if _state.get("backtest_running"):
            return jsonify({"error": "باك-تست جارٍ بالفعل — انتظر النتيجة"}), 409
        _state["backtest_running"] = True
        _state["backtest_result"] = None
        _state["backtest_ticker"] = ticker

    def _run():
        try:
            with _cfg_lock:
                cfg = dict(_config)
            data_map = fetch_batch([ticker], timeframe)
            df = data_map.get(ticker)
            if df is None or len(df) < 60:
                with _state_lock:
                    _state["backtest_running"] = False
                    _state["backtest_result"] = {"error": "لا توجد بيانات كافية للسهم — تحقق من الرمز والفريم"}
                return
            result = backtest_signals(
                df, RSI_PERIOD, PIVOT_LEFT, PIVOT_RIGHT, TOLERANCE,
                rsi_max=cfg.get("rsi_max"),
                min_volume_ratio=1.5 if cfg.get("volume_filter") else None,
                trend_filter=bool(cfg.get("trend_filter")),
                horizons=(5, 10, 20), max_bars=400,
            )
            with _state_lock:
                _state["backtest_result"] = result
        except Exception as e:
            with _state_lock:
                _state["backtest_result"] = {"error": f"خطأ في الباك-تست: {e}"}
        finally:
            with _state_lock:
                _state["backtest_running"] = False

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "جارِ حساب الباك-تست...", "ticker": ticker, "timeframe": timeframe})


@app.route("/api/backtest/result")
def api_backtest_result():
    with _state_lock:
        return jsonify({
            "running": _state.get("backtest_running", False),
            "result": _state.get("backtest_result"),
            "ticker": _state.get("backtest_ticker"),
        })


_services_started = False


def keepalive_loop():
    url = os.environ.get("APP_URL", "https://rsi-scanner-3xka.onrender.com/api/health")
    while True:
        try:
            urllib.request.urlopen(url, timeout=15)
        except Exception:
            pass
        time.sleep(600)


def start_services():
    global _services_started
    if _services_started:
        return
    _services_started = True
    init_db()
    print(f"[env] pandas={pd.__version__} python-threads ok")
    _ensure_initial_password()
    _start_cache_writer()
    start_background_refresh()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    threading.Thread(target=keepalive_loop, daemon=True).start()


start_services()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
