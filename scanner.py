"""
scanner.py
منطق الكشف عن:
  1) انفراج إيجابي (طلب/شراء): قاع سعري أدنى + قاع RSI أعلى، ثم اختراق "الرقبة"
     (أعلى قيمتي RSI عند القاعين) = تنبيه يُلتقط في شمعة الاختراق نفسها.
     يُشترط اختيارياً أن يكون RSI عند الاختراق وعند القاع في التشبع البيعي
     (أسفل rsi_max — عادة 30/40/50 وليس فوق خط الوسط).
  2) انفراج سلبي (عرض/بيع): قمة سعرية أعلى + قمة RSI أدنى، ثم كسر "الرقبة"
     (أدنى قيمتي RSI عند القمتين) = تنبيه يُلتقط في شمعة الكسر نفسها.
  3) دالة لاستخراج "منطقة الطلب/العرض" من فريم أعلى: نطاق سعر آخر قاع/قمة
     شكّلت الانفراج (تُستخدم كسياق/تأكيد إضافي).
"""

import pandas as pd
import numpy as np


def calculate_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """حساب RSI بطريقة Wilder's smoothing (تقارب طريقة TradingView)."""
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """متوسط المدى الحقيقي (ATR) بطريقة Wilder — مقياس التقلب لضبط وقف الخسارة والأهداف."""
    prev_close = close.shift(1)
    tr = pd.concat([high - low,
                    (high - prev_close).abs(),
                    (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def calculate_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """حساب MACD: يرجع (macd_line, signal_line, histogram)."""
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def calculate_stochastic(high: pd.Series, low: pd.Series, close: pd.Series,
                         k_period: int = 5, k_smooth: int = 5, d_period: int = 30):
    """حساب Stochastic: يرجع (%K مُنعّق, %D)."""
    lowest_low = low.rolling(window=k_period).min()
    highest_high = high.rolling(window=k_period).max()
    raw_k = 100 * (close - lowest_low) / (highest_high - lowest_low)
    raw_k = raw_k.replace([np.inf, -np.inf], np.nan)
    k_smoothed = raw_k.rolling(window=k_smooth).mean()
    d_line = k_smoothed.rolling(window=d_period).mean()
    return k_smoothed, d_line


def detect_triple_filter(df_large: pd.DataFrame, df_medium: pd.DataFrame,
                         df_small: pd.DataFrame = None):
    """
    الفلتر الرباعي (فريمين):
    - الكبير: MACD تقاطع صاعد تحت خط الصفر + السعر فوق SMA20
    - الوسط: RSI فوق 50 + السعر فوق SMA50
    (الفريم الصغير/الستوكاستك أُزيل بناءً على طلب المستخدم.)
    """
    if df_large is None or len(df_large) < 60:
        return None
    close_l = df_large["Close"]
    macd_line, signal_line, _ = calculate_macd(close_l)
    sma20 = close_l.rolling(20).mean()
    last_idx = len(df_large) - 1
    if last_idx < 1:
        return None
    macd_now = macd_line.iloc[last_idx]
    macd_prev = macd_line.iloc[last_idx - 1]
    signal_now = signal_line.iloc[last_idx]
    signal_prev = signal_line.iloc[last_idx - 1]
    if any(pd.isna(v) for v in (macd_now, macd_prev, signal_now, signal_prev)):
        return None
    # تقاطع حقيقي في الشمعة الحالية، ويقع خط MACD تحت خط الصفر.
    if not (macd_now < 0 and macd_prev <= signal_prev and macd_now > signal_now):
        return None
    if pd.isna(sma20.iloc[last_idx]) or float(close_l.iloc[last_idx]) <= float(sma20.iloc[last_idx]):
        return None
    large_date = df_large["Date"].iloc[last_idx] if "Date" in df_large.columns else str(last_idx)

    if df_medium is None or len(df_medium) < 60:
        return None
    close_m = df_medium["Close"]
    rsi_m = calculate_rsi(close_m, 14)
    sma50 = close_m.rolling(50).mean()
    last_m = len(df_medium) - 1
    rsi_val = float(rsi_m.iloc[last_m])
    if rsi_val <= 50:
        return None
    if pd.isna(sma50.iloc[last_m]) or float(close_m.iloc[last_m]) <= float(sma50.iloc[last_m]):
        return None
    medium_date = df_medium["Date"].iloc[last_m] if "Date" in df_medium.columns else str(last_m)

    return {
        "direction": "triple_bullish", "fresh_breakout": True,
        "signal_date": medium_date,
        "price": float(close_m.iloc[last_m]),
        "large_timeframe": {"macd": round(float(macd_now), 4),
                            "macd_signal": round(float(signal_line.iloc[last_idx]), 4),
                            "sma20": round(float(sma20.iloc[last_idx]), 2), "date": str(large_date)},
        "medium_timeframe": {"rsi": round(rsi_val, 2),
                             "sma50": round(float(sma50.iloc[last_m]), 2), "date": str(medium_date)},
        "rsi_value": rsi_val, "rsi_low": None, "peak_level": None, "atr": None, "volume_ratio": None,
    }


def _volume_at_break(df: pd.DataFrame, pos: int, threshold: float = 1.5):
    """
    يفحص حجم شمعة الاختراق مقابل متوسط آخر 20 شمعة (قبل الاختراق).
    يرجع (ratio, passed): ratio = حجم الاختراق ÷ متوسط الحجم، passed = هل اجتاز الشرط.
    """
    if "Volume" not in df.columns:
        return None, False
    vol = df["Volume"]
    try:
        avg_vol = vol.iloc[:pos].tail(20).mean()
    except Exception:
        return None, False
    if pd.isna(avg_vol) or avg_vol <= 0:
        return None, False
    bar_vol = _volume_value(df, pos)
    if bar_vol is None:
        return None, False
    ratio = float(bar_vol / avg_vol)
    return ratio, ratio >= threshold


def _volume_value(df: pd.DataFrame, pos: int):
    if "Volume" not in df.columns:
        return None
    try:
        value = float(df["Volume"].iloc[pos])
    except (TypeError, ValueError):
        return None
    return value if np.isfinite(value) and value >= 0 else None


def _trend_at_break(df: pd.DataFrame, direction: str, pos: int, period: int = 200):
    """
    فلتر الاتجاه العام: الشراء فقط إذا كان السعر فوق المتوسط المتحرك (اتجاه صاعد)،
    والبيع فقط إذا كان السعر تحت المتوسط (اتجاه هابط).
    يرجع True/False، أو None إذا كانت البيانات لا تكفي لتحديد الاتجاه.
    """
    if "Close" not in df.columns:
        return None
    sma = df["Close"].rolling(period).mean()
    v = sma.iloc[pos]
    if pd.isna(v):
        return None
    close_at_break = float(df["Close"].iloc[pos])
    if direction == "bullish":
        return close_at_break > float(v)
    return close_at_break < float(v)


def find_pivots(series: pd.Series, left: int = 3, right: int = 3, mode: str = "low"):
    """
    يرجع مواقع (index مواقع صحيحة) القمم أو القيعان التأرجحية.
    mode='low' يبحث عن قيعان، mode='high' يبحث عن قمم.
    """
    values = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
    width = left + right + 1
    if len(values) < width:
        return []
    windows = np.lib.stride_tricks.sliding_window_view(values, width)
    centers = windows[:, left]
    valid = ~np.isnan(windows).any(axis=1)
    extrema = np.min(windows, axis=1) if mode == "low" else np.max(windows, axis=1)
    pivots = (np.flatnonzero(valid & (centers == extrema)) + left).tolist()

    cleaned = []
    for p in pivots:
        if cleaned and p - cleaned[-1] <= left:
            cleaned[-1] = p
        else:
            cleaned.append(p)
    return cleaned


def find_pivot_lows(series: pd.Series, left: int = 3, right: int = 3):
    return find_pivots(series, left, right, mode="low")


def find_pivot_highs(series: pd.Series, left: int = 3, right: int = 3):
    return find_pivots(series, left, right, mode="high")


def match_pivots(price_pivots, rsi_pivots, tolerance: int = 3):
    """يربط كل قاع/قمة سعرية بأقرب قاع/قمة RSI ضمن نافذة زمنية قريبة."""
    matched = []
    used_rsi = set()
    for pp in price_pivots:
        candidates = [rp for rp in rsi_pivots if rp not in used_rsi and abs(rp - pp) <= tolerance]
        if candidates:
            rp = min(candidates, key=lambda x: abs(x - pp))
            used_rsi.add(rp)
            matched.append((pp, rp))
    return matched


def _find_last_divergence(df: pd.DataFrame, direction: str, rsi_period: int = 14,
                           pivot_left: int = 3, pivot_right: int = 3, tolerance: int = 3,
                           rsi: pd.Series = None):
    """
    منطق مشترك لإيجاد آخر انفراج (إيجابي أو سلبي).
    direction: 'bullish' (طلب) أو 'bearish' (عرض)
    rsi: سلسلة RSI محسوبة مسبقاً لتجنب إعادة الحساب (اختياري).

    الفكرة: القاع/القمة الحالي يُؤخذ من آخر pivot_right شموع مباشرة (بدون انتظار
    تأكيد القمم التأرجحية)، ويُقارن بآخر قاع/قمة تأرجحية مؤكّدة قبله. مستوى الاختراق
    هو "الرقبة": أعلى قيمتي RSI (للشراء) أو أدناهما (للبيع) عند النقطتين.

    يرجع dict فيه: rsi, close, price_series, prev_pos, cur_pos, neckline, rsi_low
    أو None إذا لم يوجد انفراج.
    """
    price_col = "Low" if direction == "bullish" else "High"
    if price_col not in df.columns or "Close" not in df.columns:
        return None

    close = df["Close"]
    price_series = df[price_col]
    if rsi is None:
        rsi = calculate_rsi(close, rsi_period)

    mode = "low" if direction == "bullish" else "high"
    price_pivots = find_pivots(price_series, pivot_left, pivot_right, mode=mode)

    # القاع/القمة الحالي (غير مؤكد بعد) من آخر pivot_right شموع
    n = len(price_series)
    start = max(0, n - pivot_right)
    valid = [(i, price_series.iloc[i]) for i in range(start, n)
             if not pd.isna(price_series.iloc[i])]
    if not valid:
        return None
    cur_pos = (min if mode == "low" else max)(valid, key=lambda x: x[1])[0]

    # آخر قاع/قمة تأرجحية مؤكّدة قبل القاع/القمة الحالي
    prev_candidates = [p for p in price_pivots if p < cur_pos]
    if not prev_candidates:
        return None
    prev_pos = prev_candidates[-1]

    prev_price = float(price_series.iloc[prev_pos])
    cur_price = float(price_series.iloc[cur_pos])
    prev_rsi = float(rsi.iloc[prev_pos])
    cur_rsi = float(rsi.iloc[cur_pos])

    if direction == "bullish":
        if not (cur_price < prev_price and cur_rsi > prev_rsi):  # قاع أدنى + قاع RSI أعلى
            return None
        neckline = max(prev_rsi, cur_rsi)
    else:
        if not (cur_price > prev_price and cur_rsi < prev_rsi):  # قمة أعلى + قمة RSI أدنى
            return None
        neckline = min(prev_rsi, cur_rsi)

    return {
        "rsi": rsi,
        "close": close,
        "price_series": price_series,
        "prev_pos": prev_pos,
        "cur_pos": cur_pos,
        "neckline": float(neckline),
        "rsi_low": float(cur_rsi),
    }


def detect_signal(df: pd.DataFrame, rsi_period: int = 14, pivot_left: int = 3,
                   pivot_right: int = 3, tolerance: int = 3, rsi_max: float = None,
                   fresh_window: int = None, min_volume_ratio: float = None,
                   trend_filter: bool = False, trend_period: int = 200,
                   rsi: pd.Series = None, _precomputed_atr: pd.Series = None):
    """
    انفراج إيجابي + اختراق الرقبة (أعلى قيمتي RSI عند القاعين) = تنبيه شراء.
    - يُلتقط التنبيه في شمعة الاختراق نفسها (وليس بعد شموع لاحقة).
    - إذا أُعطي rsi_max: يُشترط أن يكون RSI عند الاختراق وعند القاع الحالي <= rsi_max
      (تشبع بيعي / ليس فوق خط الوسط).
    - min_volume_ratio: يُشترط أن يكون حجم شمعة الاختراق >= النسبة × متوسط آخر 20 شمعة.
    - trend_filter: يُشترط أن يكون السعر فوق المتوسط المتحرك (اتجاه صاعد).
    - rsi: سلسلة RSI محسوبة مسبقاً (اختياري) لتجنب إعادة الحساب.
    - _precomputed_atr: ATR محسوب مسبقاً (اختياري) لتجنب إعادة الحساب.
    يرجع dict أو None.
    """
    info = _find_last_divergence(df, "bullish", rsi_period, pivot_left, pivot_right,
                                 tolerance, rsi=rsi)
    if info is None:
        return None

    rsi, close = info["rsi"], info["close"]
    after = rsi.iloc[info["cur_pos"] + 1:]
    if after.empty:
        return None

    above_mask = after > info["neckline"]
    if not above_mask.any():
        return None

    first_break_pos = info["cur_pos"] + 1 + int(np.flatnonzero(above_mask.to_numpy())[0])
    if fresh_window is None:
        fresh_window = pivot_right
    is_fresh = bool((len(df) - 1 - first_break_pos) <= fresh_window)

    rsi_at_break = float(rsi.iloc[first_break_pos])
    if rsi_max is not None and (rsi_at_break > rsi_max or info["rsi_low"] > rsi_max):
        return None

    vol_ratio, vol_ok = _volume_at_break(df, first_break_pos, min_volume_ratio or 1.5)
    if min_volume_ratio is not None and not vol_ok:
        return None

    trend_ok = _trend_at_break(df, "bullish", first_break_pos, trend_period)
    if trend_filter and trend_ok is not True:
        return None

    atr_val = None
    if _precomputed_atr is not None:
        v = _precomputed_atr.iloc[first_break_pos]
        if not pd.isna(v):
            atr_val = float(v)
    elif {"High", "Low"}.issubset(df.columns):
        a = calculate_atr(df["High"], df["Low"], df["Close"], 14)
        v = a.iloc[first_break_pos]
        if not pd.isna(v):
            atr_val = float(v)

    return {
        "direction": "bullish",
        "fresh_breakout": is_fresh,
        "signal_date": df["Date"].iloc[first_break_pos] if "Date" in df.columns else str(first_break_pos),
        "signal_pos": first_break_pos,
        "price": float(close.iloc[first_break_pos]),
        "rsi_value": rsi_at_break,
        "rsi_low": info["rsi_low"],
        "peak_level": info["neckline"],
        "volume_ratio": vol_ratio,
        "volume": _volume_value(df, first_break_pos),
        "trend_ok": trend_ok,
        "atr": atr_val,
    }


def detect_signal_bearish(df: pd.DataFrame, rsi_period: int = 14, pivot_left: int = 3,
                           pivot_right: int = 3, tolerance: int = 3, rsi_min: float = None,
                           fresh_window: int = None, min_volume_ratio: float = None,
                           trend_filter: bool = False, trend_period: int = 200,
                           rsi: pd.Series = None, _precomputed_atr: pd.Series = None):
    """
    انفراج سلبي + كسر الرقبة (أدنى قيمتي RSI عند القمتين) = تنبيه بيع.
    يُلتقط التنبيه في شمعة الكسر نفسها.
    إذا أُعطي rsi_min: يُشترط أن يكون RSI عند الكسر وعند القمة الحالية >= rsi_min.
    - min_volume_ratio: يُشترط أن يكون حجم شمعة الكسر >= النسبة × متوسط آخر 20 شمعة.
    - trend_filter: يُشترط أن يكون السعر تحت المتوسط المتحرك (اتجاه هابط).
    - rsi: سلسلة RSI محسوبة مسبقاً (اختياري) لتجنب إعادة الحساب.
    - _precomputed_atr: ATR محسوب مسبقاً (اختياري) لتجنب إعادة الحساب.
    يرجع dict أو None.
    """
    info = _find_last_divergence(df, "bearish", rsi_period, pivot_left, pivot_right,
                                 tolerance, rsi=rsi)
    if info is None:
        return None

    rsi, close = info["rsi"], info["close"]
    after = rsi.iloc[info["cur_pos"] + 1:]
    if after.empty:
        return None

    below_mask = after < info["neckline"]
    if not below_mask.any():
        return None

    first_break_pos = info["cur_pos"] + 1 + int(np.flatnonzero(below_mask.to_numpy())[0])
    if fresh_window is None:
        fresh_window = pivot_right
    is_fresh = bool((len(df) - 1 - first_break_pos) <= fresh_window)

    rsi_at_break = float(rsi.iloc[first_break_pos])
    if rsi_min is not None and (rsi_at_break < rsi_min or info["rsi_low"] < rsi_min):
        return None

    vol_ratio, vol_ok = _volume_at_break(df, first_break_pos, min_volume_ratio or 1.5)
    if min_volume_ratio is not None and not vol_ok:
        return None

    trend_ok = _trend_at_break(df, "bearish", first_break_pos, trend_period)
    if trend_filter and trend_ok is not True:
        return None

    atr_val = None
    if _precomputed_atr is not None:
        v = _precomputed_atr.iloc[first_break_pos]
        if not pd.isna(v):
            atr_val = float(v)
    elif {"High", "Low"}.issubset(df.columns):
        a = calculate_atr(df["High"], df["Low"], df["Close"], 14)
        v = a.iloc[first_break_pos]
        if not pd.isna(v):
            atr_val = float(v)

    return {
        "direction": "bearish",
        "fresh_breakout": is_fresh,
        "signal_date": df["Date"].iloc[first_break_pos] if "Date" in df.columns else str(first_break_pos),
        "signal_pos": first_break_pos,
        "price": float(close.iloc[first_break_pos]),
        "rsi_value": rsi_at_break,
        "rsi_low": info["rsi_low"],
        "peak_level": info["neckline"],
        "volume_ratio": vol_ratio,
        "volume": _volume_value(df, first_break_pos),
        "trend_ok": trend_ok,
        "atr": atr_val,
    }


def get_divergence_zone(df: pd.DataFrame, direction: str, rsi_period: int = 14,
                         pivot_left: int = 3, pivot_right: int = 3, tolerance: int = 3,
                         rsi: pd.Series = None):
    """
    يستخرج 'منطقة الطلب/العرض' من فريم أعلى: نطاق الشمعة (Low..High) عند آخر
    قاع/قمة تأرجحية شكّلت الانفراج، بغض النظر عن حدوث الاختراق فعلياً في هذا الفريم.
    direction: 'bullish' (منطقة طلب) أو 'bearish' (منطقة عرض)
    rsi: سلسلة RSI محسوبة مسبقاً (اختياري) لتجنب إعادة الحساب.
    يرجع dict {zone_low, zone_high, zone_date} أو None.
    """
    info = _find_last_divergence(df, direction, rsi_period, pivot_left, pivot_right,
                                 tolerance, rsi=rsi)
    if info is None:
        return None

    idx = info["cur_pos"]
    if "Low" not in df.columns or "High" not in df.columns:
        return None

    zone_low = float(df["Low"].iloc[idx])
    zone_high = float(df["High"].iloc[idx])
    zone_date = df["Date"].iloc[idx] if "Date" in df.columns else str(idx)

    return {"zone_low": zone_low, "zone_high": zone_high, "zone_date": zone_date}


def backtest_signals(df: pd.DataFrame, rsi_period: int = 14, pivot_left: int = 3,
                     pivot_right: int = 3, tolerance: int = 3, rsi_max: float = None,
                     min_volume_ratio: float = None, trend_filter: bool = False,
                     horizons=(5, 10, 20), max_bars: int = 400):
    """
    باك-تست بسيط: يمشي على تاريخ الشموع ويقبض كل إشارة (اختراق طازج عند آخر شمعة)،
    ثم يقيس العائد بعد h شمعة من كل إشارة، ويجمّع إحصائيات النجاح لكل اتجاه.
    يرجع dict:
      {
        "bars": عدد الشموع المفحوصة,
        "signals": { "bullish": [...], "bearish": [...] },
        "summary": { "bullish": {...stats...}, "bearish": {...} }
      }
    """
    df = df.tail(max_bars).reset_index(drop=True)
    min_bars = rsi_period + pivot_left + pivot_right + 5
    out = {"bullish": [], "bearish": []}
    seen = {"bullish": set(), "bearish": set()}
    n = len(df)
    for i in range(min_bars, n):
        sl = df.iloc[: i + 1]
        sigs = (
            ("bullish", detect_signal(sl, rsi_period, pivot_left, pivot_right, tolerance,
                                      rsi_max=rsi_max, fresh_window=0,
                                      min_volume_ratio=min_volume_ratio,
                                      trend_filter=trend_filter)),
            ("bearish", detect_signal_bearish(sl, rsi_period, pivot_left, pivot_right, tolerance,
                                              fresh_window=0,
                                              min_volume_ratio=min_volume_ratio,
                                              trend_filter=trend_filter)),
        )
        for direction, sig in sigs:
            if not sig or not sig.get("fresh_breakout"):
                continue
            signal_pos = int(sig["signal_pos"])
            if signal_pos in seen[direction]:
                continue
            seen[direction].add(signal_pos)
            entry = sig["price"]
            if pd.isna(entry) or entry <= 0:
                continue
            rec = {
                "date": sig.get("signal_date"),
                "entry": round(float(entry), 2),
                "rsi": round(float(sig["rsi_value"]), 2),
                "volume": round(float(sig["volume_ratio"]), 2) if sig.get("volume_ratio") is not None else None,
            }
            for h in horizons:
                if signal_pos + h < n:
                    raw_return = (float(df["Close"].iloc[signal_pos + h]) / entry - 1) * 100
                    rec["r%d" % h] = round(raw_return if direction == "bullish" else -raw_return, 2)
            out[direction].append(rec)

    def _stats(rows):
        if not rows:
            return None
        horiz = [f"r{h}" for h in horizons if any(r.get(f"r{h}") is not None for r in rows)]
        res = {"count": len(rows)}
        for h in horiz:
            vals = [r[h] for r in rows if r.get(h) is not None]
            if not vals:
                continue
            wins = [v for v in vals if v > 0]
            res[h] = {
                "win_rate": round(100 * len(wins) / len(vals), 1),
                "avg_return": round(sum(vals) / len(vals), 2),
                "avg_win": round(sum(wins) / len(wins), 2) if wins else None,
                "best": round(max(vals), 2),
                "worst": round(min(vals), 2),
            }
        return res

    return {
        "bars": n,
        "signals": out,
        "summary": {
            "bullish": _stats(out["bullish"]),
            "bearish": _stats(out["bearish"]),
        },
    }
