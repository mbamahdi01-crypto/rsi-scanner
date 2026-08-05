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


def find_pivots(series: pd.Series, left: int = 3, right: int = 3, mode: str = "low"):
    """
    يرجع مواقع (index مواقع صحيحة) القمم أو القيعان التأرجحية.
    mode='low' يبحث عن قيعان، mode='high' يبحث عن قمم.
    """
    n = len(series)
    pivots = []
    for i in range(left, n - right):
        val = series.iloc[i]
        if pd.isna(val):
            continue
        window = series.iloc[i - left: i + right + 1]
        if window.isna().any():
            continue
        is_pivot = (val == window.min()) if mode == "low" else (val == window.max())
        if is_pivot:
            pivots.append(i)

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
                           pivot_left: int = 3, pivot_right: int = 3, tolerance: int = 3):
    """
    منطق مشترك لإيجاد آخر انفراج (إيجابي أو سلبي).
    direction: 'bullish' (طلب) أو 'bearish' (عرض)

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
                   fresh_window: int = None):
    """
    انفراج إيجابي + اختراق الرقبة (أعلى قيمتي RSI عند القاعين) = تنبيه شراء.
    - يُلتقط التنبيه في شمعة الاختراق نفسها (وليس بعد شموع لاحقة).
    - إذا أُعطي rsi_max: يُشترط أن يكون RSI عند الاختراق وعند القاع الحالي <= rsi_max
      (تشبع بيعي / ليس فوق خط الوسط).
    يرجع dict أو None.
    """
    info = _find_last_divergence(df, "bullish", rsi_period, pivot_left, pivot_right, tolerance)
    if info is None:
        return None

    rsi, close = info["rsi"], info["close"]
    after = rsi.iloc[info["cur_pos"] + 1:]
    if after.empty:
        return None

    above_mask = after > info["neckline"]
    if not above_mask.any():
        return None

    first_break_pos = above_mask.idxmax()
    last_bar_idx = df.index[-1]
    if fresh_window is None:
        fresh_window = pivot_right
    is_fresh = bool((last_bar_idx - first_break_pos) <= fresh_window)

    rsi_at_break = float(rsi.loc[first_break_pos])
    if rsi_max is not None and (rsi_at_break > rsi_max or info["rsi_low"] > rsi_max):
        return None

    return {
        "direction": "bullish",
        "fresh_breakout": is_fresh,
        "signal_date": df.loc[first_break_pos, "Date"] if "Date" in df.columns else str(first_break_pos),
        "price": float(close.loc[first_break_pos]),
        "rsi_value": rsi_at_break,
        "rsi_low": info["rsi_low"],
        "peak_level": info["neckline"],
    }


def detect_signal_bearish(df: pd.DataFrame, rsi_period: int = 14, pivot_left: int = 3,
                           pivot_right: int = 3, tolerance: int = 3, rsi_min: float = None,
                           fresh_window: int = None):
    """
    انفراج سلبي + كسر الرقبة (أدنى قيمتي RSI عند القمتين) = تنبيه بيع.
    يُلتقط التنبيه في شمعة الكسر نفسها.
    إذا أُعطي rsi_min: يُشترط أن يكون RSI عند الكسر وعند القمة الحالية >= rsi_min.
    يرجع dict أو None.
    """
    info = _find_last_divergence(df, "bearish", rsi_period, pivot_left, pivot_right, tolerance)
    if info is None:
        return None

    rsi, close = info["rsi"], info["close"]
    after = rsi.iloc[info["cur_pos"] + 1:]
    if after.empty:
        return None

    below_mask = after < info["neckline"]
    if not below_mask.any():
        return None

    first_break_pos = below_mask.idxmax()
    last_bar_idx = df.index[-1]
    if fresh_window is None:
        fresh_window = pivot_right
    is_fresh = bool((last_bar_idx - first_break_pos) <= fresh_window)

    rsi_at_break = float(rsi.loc[first_break_pos])
    if rsi_min is not None and (rsi_at_break < rsi_min or info["rsi_low"] < rsi_min):
        return None

    return {
        "direction": "bearish",
        "fresh_breakout": is_fresh,
        "signal_date": df.loc[first_break_pos, "Date"] if "Date" in df.columns else str(first_break_pos),
        "price": float(close.loc[first_break_pos]),
        "rsi_value": rsi_at_break,
        "rsi_low": info["rsi_low"],
        "peak_level": info["neckline"],
    }


def get_divergence_zone(df: pd.DataFrame, direction: str, rsi_period: int = 14,
                         pivot_left: int = 3, pivot_right: int = 3, tolerance: int = 3):
    """
    يستخرج 'منطقة الطلب/العرض' من فريم أعلى: نطاق الشمعة (Low..High) عند آخر
    قاع/قمة تأرجحية شكّلت الانفراج، بغض النظر عن حدوث الاختراق فعلياً في هذا الفريم.
    direction: 'bullish' (منطقة طلب) أو 'bearish' (منطقة عرض)
    يرجع dict {zone_low, zone_high, zone_date} أو None.
    """
    info = _find_last_divergence(df, direction, rsi_period, pivot_left, pivot_right, tolerance)
    if info is None:
        return None

    idx = info["cur_pos"]
    if "Low" not in df.columns or "High" not in df.columns:
        return None

    zone_low = float(df["Low"].iloc[idx])
    zone_high = float(df["High"].iloc[idx])
    zone_date = df.loc[idx, "Date"] if "Date" in df.columns else str(idx)

    return {"zone_low": zone_low, "zone_high": zone_high, "zone_date": zone_date}
