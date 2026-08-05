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
import secrets
import sqlite3
import threading
import time
import urllib.request
from datetime import datetime

import pandas as pd
import yfinance as yf
from flask import Flask, jsonify, redirect, render_template, request, session

from markets import (SAUDI_INDICES, US_INDICES, US_SECTOR_AR, SAUDI_SECTOR_AR,
                     build_universe, market_sectors, refresh_lists,
                     start_background_refresh)
from scanner import detect_signal, detect_signal_bearish, get_divergence_zone

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
DB_PATH = os.path.join(DATA_DIR, "alerts.db")

# ==================== الإعدادات ====================
EXECUTION_TIMEFRAMES = ["15m", "1h", "1d"]

TIMEFRAME_MAP = {   # فريم التنفيذ ← فريم منطقة الطلب/العرض
    "15m": "2h",
    "1h": "1d",
    "1d": "1wk",
}
DEFAULT_ZONE_TIMEFRAME = "1d"

RSI_PERIOD = 14
PIVOT_LEFT = 3
PIVOT_RIGHT = 3
TOLERANCE = 3

LOOKBACK_MAP = {
    "1m": "7d", "2m": "60d", "5m": "60d", "15m": "60d", "30m": "60d",
    "60m": "730d", "1h": "730d", "90m": "60d",
    "1d": "2y", "5d": "2y", "1wk": "5y", "1mo": "10y",
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
}

CONFIG_PATH = os.path.join(DATA_DIR, "config.json")


def _load_config():
    cfg = dict(DEFAULTS)
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            saved = json.load(f)
        if saved.get("market") in ("saudi", "us"):
            cfg["market"] = saved["market"]
        if isinstance(saved.get("sector"), str):
            sectors = {s["id"] for s in market_sectors(cfg["market"])}
            if saved["sector"] in sectors | {"all", "indices"}:
                cfg["sector"] = saved["sector"]
        if saved.get("rsi_max") in (50, 40, 30, 20):
            cfg["rsi_max"] = saved["rsi_max"]
        elif saved.get("rsi_max") is None:
            cfg["rsi_max"] = None
        if isinstance(saved.get("auto"), bool):
            cfg["auto"] = saved["auto"]
        if isinstance(saved.get("interval_minutes"), (int, float)):
            cfg["interval_minutes"] = max(5, int(saved["interval_minutes"]))
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
            UNIQUE(market, ticker, timeframe, signal_date, direction)
        )
    """)
    conn.commit()
    cols = [r[1] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()]
    if "rsi_low" not in cols:
        conn.execute("ALTER TABLE alerts ADD COLUMN rsi_low REAL")
        conn.commit()
    conn.close()


def save_alert(market, sector, ticker, name, direction, timeframe, signal_date,
               price, rsi_value, peak_level, rsi_low=None,
               zone_tf=None, zone_low=None, zone_high=None):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """INSERT OR IGNORE INTO alerts
           (market, sector, ticker, name, direction, timeframe, signal_date,
            price, rsi_value, peak_level, rsi_low, zone_timeframe, zone_low, zone_high, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (market, sector, ticker, name, direction, timeframe, str(signal_date),
         price, rsi_value, peak_level, rsi_low, zone_tf, zone_low, zone_high,
         datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()


# ==================== جلب البيانات (جماعي) ====================
def resample_ohlc(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    agg = {"Open": "first", "High": "max", "Low": "min", "Close": "last"}
    if "Volume" in df.columns:
        agg["Volume"] = "sum"
    return df.resample(rule).agg(agg).dropna(how="any")


def _extract_ticker(raw, ticker, rule):
    if isinstance(raw.columns, pd.MultiIndex):
        df = raw[ticker]
    else:
        df = raw
    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    if df.empty:
        return None
    if rule:
        df = resample_ohlc(df, rule)
    df = df.reset_index()
    date_col = "Datetime" if "Datetime" in df.columns else ("Date" if "Date" in df.columns else df.columns[0])
    return df.rename(columns={date_col: "Date"})


def fetch_batch(tickers, timeframe):
    base_interval, rule = timeframe, None
    if timeframe in CUSTOM_TIMEFRAMES:
        base_interval, rule = CUSTOM_TIMEFRAMES[timeframe]
    lookback = LOOKBACK_MAP.get(base_interval, DEFAULT_LOOKBACK)
    results = {}
    for i in range(0, len(tickers), BATCH_SIZE):
        grp = [t for t in tickers[i:i + BATCH_SIZE]]
        if not grp:
            continue
        try:
            raw = yf.download(grp, period=lookback, interval=base_interval,
                              group_by="ticker", progress=False, threads=True,
                              auto_adjust=False)
        except Exception as e:
            print(f"فشل جلب دفعة {grp[0]}..: {e}")
            continue
        if raw is None or raw.empty:
            continue
        for t in grp:
            try:
                df = _extract_ticker(raw, t, rule)
                if df is not None:
                    results[t] = df
            except Exception:
                continue
    return results


def zone_timeframe_for(execution_tf: str) -> str:
    return TIMEFRAME_MAP.get(execution_tf, DEFAULT_ZONE_TIMEFRAME)


# ==================== الفحص ====================
def run_scan():
    with _state_lock:
        if _state["running"]:
            return
    with _cfg_lock:
        cfg = dict(_config)

    universe = build_universe(cfg["market"], cfg["sector"])
    zone_tf = zone_timeframe_for(cfg["timeframe"])

    with _state_lock:
        _state.update({
            "running": True, "phase": "جلب البيانات", "total": len(universe),
            "done": 0, "current": "", "bullish": 0, "bearish": 0, "errors": 0,
            "universe_count": len(universe),
        })

    started = time.time()
    rsi_max = cfg.get("rsi_max")
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] بدء فحص {len(universe)} "
          f"| {MARKET_AR.get(cfg['market'], cfg['market'])} / {cfg['sector']} "
          f"| تنفيذ {cfg['timeframe']} | منطقة من {zone_tf} | RSI شراء <= {rsi_max if rsi_max else 'بدون حد'}")

    tickers = [t for t, _ in universe]
    exec_data = fetch_batch(tickers, cfg["timeframe"])
    zone_data = {}
    if zone_tf != cfg["timeframe"]:
        with _state_lock:
            _state["phase"] = "جلب بيانات منطقة الطلب/العرض"
        zone_data = fetch_batch(tickers, zone_tf)

    min_bars = RSI_PERIOD + PIVOT_LEFT + PIVOT_RIGHT + 5

    with _state_lock:
        _state["phase"] = "تحليل المؤشرات"

    for t, meta in universe:
        with _state_lock:
            _state["current"] = t
            _state["done"] += 1
        exec_df = exec_data.get(t)
        if exec_df is None or len(exec_df) < min_bars:
            continue
        try:
            bullish = detect_signal(exec_df, RSI_PERIOD, PIVOT_LEFT, PIVOT_RIGHT,
                                    TOLERANCE, rsi_max=rsi_max)
            bearish = detect_signal_bearish(exec_df, RSI_PERIOD, PIVOT_LEFT, PIVOT_RIGHT, TOLERANCE)
            for result, direction in ((bullish, "bullish"), (bearish, "bearish")):
                if not result or not result.get("fresh_breakout"):
                    continue
                zone = None
                zdf = zone_data.get(t)
                if zdf is not None and len(zdf) >= min_bars:
                    zone = get_divergence_zone(zdf, direction, RSI_PERIOD,
                                               PIVOT_LEFT, PIVOT_RIGHT, TOLERANCE)
                save_alert(
                    cfg["market"], meta.get("sector", cfg["sector"]), t, meta.get("name", ""),
                    direction, cfg["timeframe"], result["signal_date"],
                    result["price"], result["rsi_value"], result["peak_level"],
                    rsi_low=result.get("rsi_low"),
                    zone_tf=zone_tf,
                    zone_low=zone["zone_low"] if zone else None,
                    zone_high=zone["zone_high"] if zone else None,
                )
                with _state_lock:
                    _state["bullish"] += direction == "bullish"
                    _state["bearish"] += direction == "bearish"
        except Exception as e:
            with _state_lock:
                _state["errors"] += 1
            print(f"  خطأ أثناء فحص {t}: {e}")

    duration = time.time() - started
    with _state_lock:
        _state.update({
            "running": False, "phase": "",
            "last_scan_at": datetime.now().isoformat(),
            "last_scan_duration": round(duration, 1),
        })
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] انتهى الفحص في {duration:.1f} ث "
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
            if data["market"] == "saudi":
                sectors = {s["id"] for s in market_sectors("saudi")}
                if _config["sector"] not in sectors | {"all", "indices"}:
                    _config["sector"] = "all"
            else:
                sectors = {s["id"] for s in market_sectors("us")}
                if _config["sector"] not in sectors | {"all", "indices"}:
                    _config["sector"] = "all"
        if "sector" in data:
            sec = str(data["sector"])
            sectors = {s["id"] for s in market_sectors(_config["market"])}
            if sec in sectors | {"all", "indices"}:
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
    _save_config()
    return jsonify(_config_payload())


@app.route("/api/scan-now", methods=["POST"])
def api_scan_now():
    if _state["running"]:
        return jsonify({"status": "فحص جارٍ بالفعل"}), 409
    threading.Thread(target=run_scan, daemon=True).start()
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
