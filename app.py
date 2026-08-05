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
import json
import os
import re
import secrets
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FutureTimeoutError
from datetime import datetime

import pandas as pd
import yfinance as yf
from flask import Flask, jsonify, redirect, render_template, request, session

from markets import (SAUDI_INDICES, US_INDICES, US_SECTOR_AR, SAUDI_SECTOR_AR,
                     build_universe, market_sectors, refresh_lists,
                     start_background_refresh)
from scanner import (backtest_signals, detect_signal, detect_signal_bearish,
                     get_divergence_zone)

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
DB_PATH = os.path.join(DATA_DIR, "alerts.db")
YF_CACHE_DIR = os.path.join(DATA_DIR, "yf_cache")

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

RSI_PERIOD = 14
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

BATCH_SIZE = 100          # عدد الأسهم في كل طلب جلب جماعي
SCAN_INTERVAL_DEFAULT = 30  # دقائق بين دورات الفحص التلقائي
# ================================================================

MARKET_AR = {"saudi": "السوق السعودي (تاسي)", "us": "السوق الأمريكي"}

DEFAULTS = {
    "market": "saudi",
    "sector": "all",
    "timeframe": "1d",
    "rsi_max": 50,          # تنبيه الشراء فقط عندما يكون RSI (عند الاختراق وعند القاع) <= هذه القيمة
    "auto": True,
    "interval_minutes": SCAN_INTERVAL_DEFAULT,
    "volume_filter": False,     # اشتراط حجم شمعة الاختراق >= 1.5 × متوسط آخر 20 شمعة
    "trend_filter": False,      # فلتر الاتجاه العام (السعر مقابل المتوسط المتحرك 200)
    "telegram_token": "",       # توكن بوت تيليجرام (اختياري) — أو عبر متغير البيئة TELEGRAM_BOT_TOKEN
    "telegram_chat": "",        # معرف الشات المستلم للتنبيهات (اختياري) — أو TELEGRAM_CHAT_ID
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
        if isinstance(saved.get("telegram_token"), str):
            cfg["telegram_token"] = saved["telegram_token"]
        if isinstance(saved.get("telegram_chat"), str):
            cfg["telegram_chat"] = saved["telegram_chat"]
        # فريم التنفيذ: يومي دائماً عند الإقلاع — المستخدم يغيّره وقت الحاجة فقط
        cfg["timeframe"] = DEFAULTS["timeframe"]
    except (OSError, ValueError):
        pass
    return cfg


def _save_config():
    try:
        os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(_config, f, ensure_ascii=False, indent=1)
    except OSError as e:
        print("تعذر حفظ الإعدادات:", e)


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
    "last_scan_at": None,
    "last_scan_duration": None,
    "universe_count": 0,
    "backtest_running": False,
    "backtest_result": None,
    "backtest_ticker": None,
}
_cfg_lock = threading.Lock()
_state_lock = threading.Lock()

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
    try:
        os.makedirs(os.path.dirname(SECRET_PATH), exist_ok=True)
        with open(SECRET_PATH, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=1)
    except OSError as e:
        print("تعذر حفظ أسرار التطبيق:", e)


def _get_session_key():
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
        _save_secret(d)


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


# ==================== قاعدة البيانات ====================
def init_db():
    conn = sqlite3.connect(DB_PATH)
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
            UNIQUE(market, ticker, timeframe, signal_date, direction)
        )
    """)
    conn.commit()
    cols = [r[1] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()]
    for col, ctype in (("rsi_low", "REAL"), ("stop_loss", "REAL"),
                       ("target_1", "REAL"), ("target_2", "REAL"),
                       ("volume_ratio", "REAL")):
        if col not in cols:
            conn.execute(f"ALTER TABLE alerts ADD COLUMN {col} {ctype}")
    conn.commit()
    conn.close()


def save_alert(market, sector, ticker, name, direction, timeframe, signal_date,
               price, rsi_value, peak_level, rsi_low=None,
               zone_tf=None, zone_low=None, zone_high=None,
               stop_loss=None, target_1=None, target_2=None, volume_ratio=None):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.execute(
        """INSERT OR IGNORE INTO alerts
           (market, sector, ticker, name, direction, timeframe, signal_date,
            price, rsi_value, peak_level, rsi_low, zone_timeframe, zone_low, zone_high,
            stop_loss, target_1, target_2, volume_ratio, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (market, sector, ticker, name, direction, timeframe, str(signal_date),
         price, rsi_value, peak_level, rsi_low, zone_tf, zone_low, zone_high,
         stop_loss, target_1, target_2, volume_ratio,
         datetime.now().isoformat()),
    )
    inserted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return inserted


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
    """صلاحية الكياش بالثواني: اللحظي أقصر (15 د)، اليومي والأسبوعي أطول (4 ساعات)
    حتى تتكرر دورات الفحص اليومي من الكياش بسرعة دون إعادة تحميل السوق كاملاً."""
    return 900 if interval in ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h") else 14400


def _cache_path(ticker, interval):
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", str(ticker))
    d = os.path.join(YF_CACHE_DIR, str(interval))
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, safe + ".csv")


def _read_cache(ticker, interval):
    try:
        p = _cache_path(ticker, interval)
        if not os.path.exists(p) or time.time() - os.path.getmtime(p) > _cache_ttl(interval):
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


def _write_cache(ticker, interval, df):
    try:
        if df is None or df.empty:
            return
        out = df.copy()
        if isinstance(out.index, pd.DatetimeIndex) and out.index.tz is not None:
            out.index = out.index.tz_convert("UTC").tz_localize(None)
        out.reset_index().to_csv(_cache_path(ticker, interval), index=False)
    except Exception:
        pass


def _download_batch(grp, lookback, base_interval):
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(yf.download, grp, period=lookback, interval=base_interval,
                        group_by="ticker", progress=False, threads=True,
                        auto_adjust=False)
        try:
            return fut.result(timeout=60)
        except _FutureTimeoutError:
            print(f"انتهت مهلة جلب الدفعة {grp[0]}..{grp[-1]}")
            return None


def fetch_batch(tickers, timeframe, tail=None):
    base_interval, rule = timeframe, None
    if timeframe in CUSTOM_TIMEFRAMES:
        base_interval, rule = CUSTOM_TIMEFRAMES[timeframe]
    lookback = LOOKBACK_MAP.get(base_interval, DEFAULT_LOOKBACK)
    results = {}
    needed = []
    for t in tickers:
        cached = _read_cache(t, base_interval)
        if cached is not None:
            df = _extract_ticker(cached, t, rule, tail)
            if df is not None:
                results[t] = df
                continue
        needed.append(t)
    for i in range(0, len(needed), BATCH_SIZE):
        grp = [t for t in needed[i:i + BATCH_SIZE]]
        if not grp:
            continue
        try:
            raw = _download_batch(grp, lookback, base_interval)
        except Exception as e:
            print(f"فشل جلب دفعة {grp[0]}..: {e}")
            continue
        if raw is None or raw.empty:
            continue
        for t in grp:
            try:
                single = raw[t] if isinstance(raw.columns, pd.MultiIndex) else raw
                df = _extract_ticker(raw, t, rule, tail)
                if df is not None:
                    results[t] = df
                    _write_cache(t, base_interval, single)
            except Exception:
                continue
    return results


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
    tag = "شراء / طلب" if a["direction"] == "bullish" else "بيع / عرض"
    icon = "🔵" if a["direction"] == "bullish" else "🔴"
    lines = [
        f"{icon} <b>{tag}: {a['ticker']}</b>",
        f"الاسم: {a.get('name') or '—'}",
        f"السوق: {market} | القطاع: {a.get('sector') or '—'}",
        f"فريم التنفيذ: {a['timeframe']}",
        f"شمعة الإشارة: {a['signal_date']}",
        f"السعر: {a['price']}",
        f"RSI عند الاختراق: {round(a['rsi_value'], 2)}",
    ]
    if a.get("rsi_low") is not None:
        lines.append(f"قاع RSI: {round(a['rsi_low'], 2)}")
    if a.get("volume_ratio") is not None:
        lines.append(f"حجم الاختراق: ×{round(a['volume_ratio'], 2)} المتوسط")
    if a.get("zone_low") is not None and a.get("zone_high") is not None:
        lines.append(f"منطقة الطلب/العرض: {round(a['zone_low'], 2)} — {round(a['zone_high'], 2)}")
    if a.get("stop_loss") is not None:
        lines.append(f"وقف الخسارة: {a['stop_loss']}")
    if a.get("target_1") is not None:
        lines.append(f"الهدف الأول: {a['target_1']} | الهدف الثاني: {a['target_2']}")
    return "\n".join(lines)
def run_scan(override=None):
    with _state_lock:
        if _state["running"]:
            return
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

    with _state_lock:
        _state.update({
            "running": True, "phase": "جلب البيانات", "total": len(universe),
            "done": 0, "current": "", "bullish": 0, "bearish": 0, "errors": 0,
            "universe_count": len(universe),
            "market": cfg["market"], "sector": cfg["sector"],
        })

    started = time.time()
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] بدء فحص {len(universe)} "
          f"| {MARKET_AR.get(cfg['market'], cfg['market'])} / {cfg['sector']} "
          f"| تنفيذ {cfg['timeframe']} | منطقة من {zone_tf} | RSI شراء <= {rsi_max if rsi_max else 'بدون حد'}")

    try:
        tickers = [t for t, _ in universe]
        exec_data = fetch_batch(tickers, cfg["timeframe"], tail=400)

        with _state_lock:
            _state["phase"] = "تحليل المؤشرات"

        def _process_one(item):
            t, meta = item
            df = exec_data.get(t)
            if df is None or len(df) < min_bars:
                return 0, 0, False
            try:
                bulls = bears = 0
                zone_cache = {}
                bullish = detect_signal(df, RSI_PERIOD, PIVOT_LEFT, PIVOT_RIGHT, TOLERANCE,
                                        rsi_max=rsi_max,
                                        min_volume_ratio=1.5 if vol_on else None,
                                        trend_filter=trend_on)
                bearish = detect_signal_bearish(df, RSI_PERIOD, PIVOT_LEFT, PIVOT_RIGHT, TOLERANCE,
                                                min_volume_ratio=1.5 if vol_on else None,
                                                trend_filter=trend_on)
                for result, direction in ((bullish, "bullish"), (bearish, "bearish")):
                    if not result or not result.get("fresh_breakout"):
                        continue
                    if t not in zone_cache:
                        if zone_tf == cfg["timeframe"]:
                            zone_cache[t] = df
                        else:
                            zone_cache[t] = fetch_batch([t], zone_tf, tail=250).get(t)
                    zdf = zone_cache[t]
                    zone = None
                    if zdf is not None and len(zdf) >= min_bars:
                        zone = get_divergence_zone(zdf, direction, RSI_PERIOD,
                                                   PIVOT_LEFT, PIVOT_RIGHT, TOLERANCE)
                    zone_low = zone["zone_low"] if zone else None
                    zone_high = zone["zone_high"] if zone else None
                    stop, t1, t2 = compute_stops(direction, result["price"],
                                                 result.get("atr"), zone_low, zone_high)
                    inserted = save_alert(
                        cfg["market"], meta.get("sector", cfg["sector"]), t, meta.get("name", ""),
                        direction, cfg["timeframe"], result["signal_date"],
                        result["price"], result["rsi_value"], result["peak_level"],
                        rsi_low=result.get("rsi_low"),
                        zone_tf=zone_tf,
                        zone_low=zone_low,
                        zone_high=zone_high,
                        stop_loss=stop,
                        target_1=t1,
                        target_2=t2,
                        volume_ratio=result.get("volume_ratio"),
                    )
                    if inserted:
                        a = {
                            "market": cfg["market"], "sector": meta.get("sector", cfg["sector"]),
                            "ticker": t, "name": meta.get("name", ""), "direction": direction,
                            "timeframe": cfg["timeframe"], "signal_date": result["signal_date"],
                            "price": result["price"], "rsi_value": result["rsi_value"],
                            "rsi_low": result.get("rsi_low"), "volume_ratio": result.get("volume_ratio"),
                            "zone_low": zone_low, "zone_high": zone_high,
                            "stop_loss": stop, "target_1": t1, "target_2": t2,
                        }
                        threading.Thread(target=telegram_send,
                                         args=(telegram_alert_message(a),), daemon=True).start()
                    if direction == "bullish":
                        bulls += 1
                    else:
                        bears += 1
                return bulls, bears, False
            except Exception as e:
                print(f"  خطأ أثناء فحص {t}: {e}")
                return 0, 0, True

        workers = min(8, max(1, (os.cpu_count() or 2)))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for (t, meta), (nb, ns, err) in zip(universe, ex.map(_process_one, universe)):
                with _state_lock:
                    _state["current"] = t
                    _state["done"] += 1
                    _state["bullish"] += nb
                    _state["bearish"] += ns
                    if err:
                        _state["errors"] += 1
    except Exception:
        import traceback
        traceback.print_exc()
    finally:
        with _state_lock:
            _state.update({
                "running": False, "phase": "",
                "last_scan_at": datetime.now().isoformat(),
                "last_scan_duration": round(time.time() - started, 1),
            })
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] انتهى الفحص في {time.time() - started:.1f} ث "
          f"| طلب: {_state['bullish']} | عرض: {_state['bearish']}")


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
                threading.Thread(target=run_scan, daemon=True).start()
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
    return {**cfg, "status": st, "zone_timeframe": zone_timeframe_for(cfg["timeframe"])}


@app.route("/")
def index():
    with _cfg_lock:
        cfg = dict(_config)
    return render_template(
        "index.html",
        market=cfg["market"],
        sector=cfg["sector"],
        timeframe=cfg["timeframe"],
        rsi_max=cfg["rsi_max"],
        auto=cfg["auto"],
        interval=cfg["interval_minutes"],
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
    return jsonify({"ok": True})


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
    if len(new) < 4:
        return jsonify({"ok": False, "error": "كلمة المرور الجديدة قصيرة جداً (4 أحرف على الأقل)"}), 400
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
            else:
                try:
                    _config["rsi_max"] = int(v)
                except (TypeError, ValueError):
                    pass
        if "auto" in data:
            _config["auto"] = bool(data["auto"])
        if "interval_minutes" in data:
            try:
                _config["interval_minutes"] = max(5, int(data["interval_minutes"]))
            except (TypeError, ValueError):
                pass
        if "volume_filter" in data:
            _config["volume_filter"] = bool(data["volume_filter"])
        if "trend_filter" in data:
            _config["trend_filter"] = bool(data["trend_filter"])
        if "telegram_token" in data and isinstance(data["telegram_token"], str):
            _config["telegram_token"] = data["telegram_token"].strip()
        if "telegram_chat" in data and isinstance(data["telegram_chat"], str):
            _config["telegram_chat"] = data["telegram_chat"].strip()
    _save_config()
    return jsonify(_config_payload())


@app.route("/api/scan-now", methods=["POST"])
def api_scan_now():
    if _state["running"]:
        return jsonify({"status": "فحص جارٍ بالفعل"}), 409
    data = request.get_json(force=True, silent=True) or {}
    override = {}
    if data.get("market") in ("saudi", "us"):
        override["market"] = data["market"]
    if isinstance(data.get("sector"), str):
        mkt = data.get("market") if data.get("market") in ("saudi", "us") else _config["market"]
        if data["sector"] in _valid_sector_ids(mkt):
            override["sector"] = data["sector"]
    if data.get("timeframe") in EXECUTION_TIMEFRAMES:
        override["timeframe"] = data["timeframe"]
    threading.Thread(target=run_scan, kwargs={"override": override}, daemon=True).start()
    return jsonify({"status": "بدأ الفحص..."})


@app.route("/api/alerts")
def api_alerts():
    market = request.args.get("market")
    sector = request.args.get("sector")
    limit = min(int(request.args.get("limit", 300)), 2000)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    sql = "SELECT * FROM alerts"
    where, params = [], []
    if market:
        where.append("market=?")
        params.append(market)
    if sector and sector != "all":
        where.append("sector=?")
        params.append(sector)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/clear-alerts", methods=["POST"])
def api_clear_alerts():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM alerts")
    conn.commit()
    conn.close()
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
    _ensure_initial_password()
    start_background_refresh()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    threading.Thread(target=keepalive_loop, daemon=True).start()


start_services()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
