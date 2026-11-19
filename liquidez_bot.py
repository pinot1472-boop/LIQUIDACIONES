"""
BOT DE LIQUIDEZ (independiente) - BingX -> Telegram
Entra solo cuando el precio esta sobre una zona de liquidez IMPORTANTE y muestra fuerza:
  - SOUP   : Turtle Soup (barrido de la zona + cierre de vuelta dentro, en la misma vela)
  - SOUP+1 : Turtle Soup Plus One (barrido, cierra fuera, y la vela siguiente recupera el nivel)
  - REBOTE : rechazo en la zona sin barrido (mecha larga + volumen)
Zonas: cumulos de pivotes (maximos/minimos iguales) puntuados 0-100, con confluencia de
liquidaciones estimadas (modelo de apalancamiento x volumen). Sirve arriba, abajo e interna (medio).
"""
import asyncio
import os
import re
import time
import logging
import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiohttp
import ccxt.async_support as ccxt


def env_bool(name, default):
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "si", "on")


def env_floats(name, default):
    return [float(x) for x in os.getenv(name, default).split(",") if x.strip()]


# ======================= CONFIG =======================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TIMEFRAMES = [t.strip() for t in os.getenv("TIMEFRAMES", "5m,15m").split(",") if t.strip()]
CANDLES = int(os.getenv("CANDLES", "500"))
EMA_PERIOD = int(os.getenv("EMA_PERIOD", "200"))
MIN_VOLUME_USDT = float(os.getenv("MIN_VOLUME_USDT", "1000000"))   # sin liquidez real no hay "zonas"
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))
CLOSE_DELAY_SEC = int(os.getenv("CLOSE_DELAY_SEC", "3"))
MARKET_TYPE = os.getenv("MARKET_TYPE", "swap")

# Solo cripto
EXCLUDE_NON_CRYPTO = env_bool("EXCLUDE_NON_CRYPTO", True)
_DEFAULT_EXCLUDE = ("PAXG,XAUT,AAPLX,TSLAX,NVDAX,MSFTX,GOOGLX,AMZNX,METAX,COINX,MSTRX,HOODX,CRCLX,"
                    "SPYX,QQQX,NFLXX,AVGOX,JPMX,LLYX,WMTX,XOMX,ORCLX,AMDX,PLTRX,INTCX,IBMX,MCDX,GMEX")
EXCLUDE_BASES = {x.strip().upper() for x in os.getenv("EXCLUDE_BASES", _DEFAULT_EXCLUDE).split(",") if x.strip()}
NON_CRYPTO_RE = re.compile(r"^NC(CO|SK|SI|FX)")
EXCLUDED = []

# Zonas de liquidez
LQ_PIVOT = int(os.getenv("LQ_PIVOT", "5"))                 # velas a cada lado para un pivote
LQ_LOOKBACK = int(os.getenv("LQ_LOOKBACK", "300"))         # velas analizadas para zonas
LQ_TOL_ATR = float(os.getenv("LQ_TOL_ATR", "0.25"))        # tolerancia de "maximos iguales" en ATR
LQ_EXTREME_N = int(os.getenv("LQ_EXTREME_N", "20"))        # extremo de N velas (Turtle Soup original = 20)
LQ_MIN_ZONE_SCORE = float(os.getenv("LQ_MIN_ZONE_SCORE", "50"))   # importancia minima de la zona (0-100)

# Liquidaciones estimadas (modelo tipo heatmap)
LQ_LEVERAGES = env_floats("LQ_LEVERAGES", "10,25,50,100")
LQ_LEV_WEIGHTS = env_floats("LQ_LEV_WEIGHTS", "0.3,0.3,0.25,0.15")
LQ_LONG_SHARE = float(os.getenv("LQ_LONG_SHARE", "0.6"))   # % de longs en velas alcistas (y shorts en bajistas)
LQ_LIQ_RANGE_PCT = float(os.getenv("LQ_LIQ_RANGE_PCT", "10"))

# Senal / fuerza
LQ_ENABLE_SOUP = env_bool("LQ_ENABLE_SOUP", True)
LQ_ENABLE_PLUS1 = env_bool("LQ_ENABLE_PLUS1", True)
LQ_ENABLE_BOUNCE = env_bool("LQ_ENABLE_BOUNCE", True)
LQ_MIN_SWEEP_ATR = float(os.getenv("LQ_MIN_SWEEP_ATR", "0.05"))   # penetracion minima
LQ_MAX_SWEEP_ATR = float(os.getenv("LQ_MAX_SWEEP_ATR", "3.0"))    # mas que esto = ruptura real, no barrido
LQ_VOL_MULT = float(os.getenv("LQ_VOL_MULT", "1.5"))              # volumen minimo vs media 50
LQ_MIN_WICK = float(os.getenv("LQ_MIN_WICK", "0.35"))             # mecha de rechazo minima (% del rango)
LQ_MIN_QUALITY = float(os.getenv("LQ_MIN_QUALITY", "65"))
LQ_USE_EMA_FILTER = env_bool("LQ_USE_EMA_FILTER", False)          # exigir tendencia EMA200 a favor

# Gestion (SL / TP / salida)
LQ_SL_ATR_BUFFER = float(os.getenv("LQ_SL_ATR_BUFFER", "0.1"))
LQ_MIN_RR = float(os.getenv("LQ_MIN_RR", "1.5"))
LQ_DEFAULT_RR = float(os.getenv("LQ_DEFAULT_RR", "2.0"))
LQ_MAX_RR = float(os.getenv("LQ_MAX_RR", "4.0"))
LQ_MIN_TP_ZONE_SCORE = float(os.getenv("LQ_MIN_TP_ZONE_SCORE", "40"))
LQ_MAX_RISK_PCT = float(os.getenv("LQ_MAX_RISK_PCT", "3.0"))
LQ_MAX_HOLD_BARS = int(os.getenv("LQ_MAX_HOLD_BARS", "48"))
LQ_COOLDOWN_BARS = int(os.getenv("LQ_COOLDOWN_BARS", "6"))

# Resumen diario
REPORT_TZ = os.getenv("REPORT_TZ", "Europe/Madrid")
REPORT_TIME = os.getenv("REPORT_TIME", "23:59")
FEE_PCT_ROUNDTRIP = float(os.getenv("FEE_PCT_ROUNDTRIP", "0.1"))

# Extras traidos del indicador de TradingView (UT Bot + LinReg + S/R + panel)
LQ_USE_UT = env_bool("LQ_USE_UT", True)                    # UT Bot (trailing stop ATR) como confirmacion
UT_ATR_PERIOD = int(os.getenv("UT_ATR_PERIOD", "10"))
UT_MULT = float(os.getenv("UT_MULT", "2"))
LQ_UT_REQUIRE = env_bool("LQ_UT_REQUIRE", False)           # exigir UT a favor (filtro duro)
LQ_USE_LINREG = env_bool("LQ_USE_LINREG", True)            # velas LinReg + linea de senal
LINREG_LEN = int(os.getenv("LINREG_LEN", "11"))
LINREG_SIGNAL = int(os.getenv("LINREG_SIGNAL", "7"))
LQ_USE_SR = env_bool("LQ_USE_SR", True)                    # coincidencia con maximos/minimos de 100-500 velas
LQ_USE_HTF = env_bool("LQ_USE_HTF", True)                  # tendencia EMA9/21 en TF superiores + max/min de ayer
LQ_HTF_MIN_ALIGNED = int(os.getenv("LQ_HTF_MIN_ALIGNED", "0"))   # TF superiores a favor minimos (0 = solo puntua)
LQ_SHOW_PANEL = env_bool("LQ_SHOW_PANEL", True)            # panel de mercado en el mensaje

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("liquidez-bot")

POSITIONS = {}   # (symbol, tf) -> posicion virtual abierta
COOLDOWN = {}    # (symbol, tf) -> ts de vela hasta la que no se vuelve a operar
CLOSED = []      # operaciones cerradas desde el ultimo resumen


# ======================= UTILIDADES =======================
def tf_seconds(tf):
    return int(tf[:-1]) * {"m": 60, "h": 3600, "d": 86400, "w": 604800}[tf[-1]]


def calc_ema_last(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    prev = sum(values[:period]) / period
    for v in values[period:]:
        prev = v * k + prev * (1 - k)
    return prev


def is_non_crypto(m):
    base = (m.get("base") or "").upper()
    mid = (m.get("id") or "").upper()
    return bool(NON_CRYPTO_RE.match(base) or NON_CRYPTO_RE.match(mid) or base in EXCLUDE_BASES)


def fmt_link(symbol):
    base = symbol.split("/")[0]
    if MARKET_TYPE == "swap":
        return f"https://bingx.com/en/perpetual/{base}-USDT/"
    return f"https://bingx.com/en/spot/{base}USDT/"


def pretty(x):
    return f"{x:.8g}"


def fmt_age(bars, tf):
    secs = bars * tf_seconds(tf)
    if secs >= 86400:
        return f"{secs / 86400:.1f} d"
    if secs >= 3600:
        return f"{secs / 3600:.0f} h"
    return f"{secs / 60:.0f} min"


# ======================= INDICADORES (del Pine: UT Bot, LinReg, S/R, panel) =======================
def atr_series(c, period):
    """ATR de Wilder (como ta.atr). Lista alineada con c; None mientras no hay datos."""
    out = [None] * len(c)
    if len(c) <= period:
        return out
    trs = [c[0][2] - c[0][3]]
    for i in range(1, len(c)):
        trs.append(max(c[i][2] - c[i][3], abs(c[i][2] - c[i - 1][4]), abs(c[i][3] - c[i - 1][4])))
    prev = sum(trs[:period]) / period
    out[period - 1] = prev
    for i in range(period, len(c)):
        prev = (prev * (period - 1) + trs[i]) / period
        out[i] = prev
    return out


def ut_bot(c, mult, period):
    """UT Bot: trailing stop por ATR sobre el cierre. state=1 alcista, -1 bajista."""
    atr = atr_series(c, period)
    trail = prev_src = None
    state, flip_i = 0, None
    for i, x in enumerate(c):
        if atr[i] is None:
            continue
        src, nl = x[4], mult * atr[i]
        if trail is None:
            new = src - nl
        elif src > trail and prev_src > trail:
            new = max(trail, src - nl)
        elif src < trail and prev_src < trail:
            new = min(trail, src + nl)
        else:
            new = src - nl if src > trail else src + nl
        trail = new
        st = 1 if src > trail else (-1 if src < trail else state)
        if st != state:
            flip_i = i
        state, prev_src = st, src
    if trail is None:
        return None
    return {"state": state, "trail": trail,
            "since_flip": (len(c) - 1 - flip_i) if flip_i is not None else 999}


def linreg_last(vals, length):
    y = vals[-length:]
    n = len(y)
    if n < length or n < 2:
        return None
    xm, ym = (n - 1) / 2, sum(y) / n
    den = sum((i - xm) ** 2 for i in range(n))
    slope = sum((i - xm) * (v - ym) for i, v in enumerate(y)) / den
    return ym + slope * ((n - 1) - xm)


def linreg_signal(closes, length, sig_len):
    """(cierre LinReg actual, media de senal). Cierre > senal = impulso alcista."""
    pts = []
    for k in range(sig_len):
        end = len(closes) - (sig_len - 1 - k)
        v = linreg_last(closes[max(0, end - length):end], length)
        if v is None:
            return None
        pts.append(v)
    return pts[-1], sum(pts) / sig_len


def sr_levels(c):
    """Maximos/minimos de 10..1000 velas (lineas S/R del Pine): [(ventana, max, min)]."""
    n = len(c)
    return [(w, max(x[2] for x in c[-w:]), min(x[3] for x in c[-w:]))
            for w in (10, 50, 100, 250, 500, 1000) if n >= w]


def ema_series(vals, period):
    if len(vals) < period:
        return []
    k = 2 / (period + 1)
    prev = sum(vals[:period]) / period
    out = [prev]
    for v in vals[period:]:
        prev = v * k + prev * (1 - k)
        out.append(prev)
    return out


def rsi_last(closes, period=14):
    if len(closes) <= period:
        return None
    ag = sum(max(closes[i] - closes[i - 1], 0) for i in range(1, period + 1)) / period
    al = sum(max(closes[i - 1] - closes[i], 0) for i in range(1, period + 1)) / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (period - 1) + max(d, 0)) / period
        al = (al * (period - 1) + max(-d, 0)) / period
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)


def stoch_kd(c, length=14, sk=3, sd=3):
    raw = []
    for i in range(length - 1, len(c)):
        w = c[i - length + 1:i + 1]
        hh, ll = max(x[2] for x in w), min(x[3] for x in w)
        raw.append(50.0 if hh == ll else 100 * (c[i][4] - ll) / (hh - ll))
    ks = [sum(raw[j - sk + 1:j + 1]) / sk for j in range(sk - 1, len(raw))]
    ds = [sum(ks[j - sd + 1:j + 1]) / sd for j in range(sd - 1, len(ks))]
    return (ks[-1], ds[-1]) if ks and ds else None


# ======================= LIQUIDACIONES ESTIMADAS =======================
def liquidation_levels(c, price):
    """
    Modelo tipo heatmap: cada vela abre posiciones (peso = volumen en USDT) con reparto de
    apalancamientos; precio de liquidacion = entrada*(1 -/+ 0.95/L). Se descartan los niveles
    ya 'consumidos' (el precio paso por ellos despues). Devuelve niveles pendientes por lado:
    'S' = liquidacion de shorts (por encima), 'L' = liquidacion de longs (por debajo).
    """
    n = len(c)
    start = max(0, n - LQ_LOOKBACK)
    sufmin = [float("inf")] * (n + 1)
    sufmax = [float("-inf")] * (n + 1)
    for i in range(n - 1, -1, -1):
        sufmin[i] = min(c[i][3], sufmin[i + 1])
        sufmax[i] = max(c[i][2], sufmax[i + 1])
    lo_lim, hi_lim = price * (1 - LQ_LIQ_RANGE_PCT / 100), price * (1 + LQ_LIQ_RANGE_PCT / 100)
    out = {"S": [], "L": []}
    for i in range(start, n):
        o, h, l, cl, v = c[i][1], c[i][2], c[i][3], c[i][4], c[i][5] or 0
        entry = (h + l + cl) / 3
        w = v * entry
        long_share = LQ_LONG_SHARE if cl >= o else 1 - LQ_LONG_SHARE
        for lev, wt in zip(LQ_LEVERAGES, LQ_LEV_WEIGHTS):
            pl = entry * (1 - 0.95 / lev)
            if sufmin[i + 1] > pl and pl >= lo_lim:
                out["L"].append((pl, w * long_share * wt))
            ps = entry * (1 + 0.95 / lev)
            if sufmax[i + 1] < ps and ps <= hi_lim:
                out["S"].append((ps, w * (1 - long_share) * wt))
    stats = {}
    for side in ("S", "L"):
        lv = out[side]
        if lv:
            tot = sum(w for _, w in lv)
            span = max(p for p, _ in lv) - min(p for p, _ in lv)
        else:
            tot, span = 0.0, 0.0
        stats[side] = (lv, tot, span)
    return stats


# ======================= ZONAS DE LIQUIDEZ =======================
def analyze_zones(c):
    """Zonas de liquidez sin barrer, puntuadas 0-100, calculadas con las velas c (sin la vela de senal)."""
    n = len(c)
    k = LQ_PIVOT
    if n < 120:
        return None
    opens = [x[1] for x in c]
    highs = [x[2] for x in c]
    lows = [x[3] for x in c]
    closes = [x[4] for x in c]
    vols = [x[5] or 0 for x in c]
    trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
           for i in range(n - 14, n)]
    atr = sum(trs) / 14
    avgvol = sum(vols[-50:]) / 50
    if atr <= 0 or avgvol <= 0:
        return None
    tol = LQ_TOL_ATR * atr
    price = closes[-1]
    start = max(1, n - LQ_LOOKBACK)
    rng_hi, rng_lo = max(highs[-100:]), min(lows[-100:])
    span = max(rng_hi - rng_lo, 1e-12)
    ext_hi, ext_lo = max(highs[-LQ_EXTREME_N:]), min(lows[-LQ_EXTREME_N:])
    liq = liquidation_levels(c, price)

    zones = []
    for kind, series, extreme in (("BSL", highs, ext_hi), ("SSL", lows, ext_lo)):
        piv = []
        for i in range(start + k, n - k):
            w = series[i - k:i + k + 1]
            if (kind == "BSL" and series[i] == max(w)) or (kind == "SSL" and series[i] == min(w)):
                piv.append(i)
        piv.sort(key=lambda i: series[i])
        groups, cur = [], []
        for i in piv:
            if cur and abs(series[i] - series[cur[-1]]) > tol:
                groups.append(cur)
                cur = []
            cur.append(i)
        if cur:
            groups.append(cur)

        lv, tot, lspan = liq["S" if kind == "BSL" else "L"]
        for g in groups:
            last_p = max(g)
            level = max(series[i] for i in g) if kind == "BSL" else min(series[i] for i in g)
            after = series[last_p + 1:]
            if kind == "BSL" and after and max(after) > level + tol:
                continue    # ya barrida
            if kind == "SSL" and after and min(after) < level - tol:
                continue
            touches = len(g)
            age = n - last_p
            rvp = sum(vols[i] for i in g) / len(g) / avgvol
            dens = sum(w for p, w in lv if abs(p - level) <= tol)
            expected = tot / max(1.0, lspan / (2 * tol)) if tot > 0 else 0
            ratio = dens / expected if expected > 0 else 0.0
            pos = (level - rng_lo) / span
            ztype = "externa alta" if pos >= 0.67 else ("externa baja" if pos <= 0.33 else "interna")
            is_ext = abs(level - extreme) <= tol
            ext_pts = 15 if is_ext else (8 if ztype != "interna" else 0)
            score = (30 * min(touches, 4) / 4
                     + 10 * max(0.0, 1 - age / LQ_LOOKBACK)
                     + 15 * min(rvp / 3, 1)
                     + 30 * min(max(ratio - 1, 0) / 4, 1)
                     + ext_pts)
            zones.append({"kind": kind, "level": level, "score": score, "touches": touches,
                          "age": age, "ratio": ratio, "ext20": is_ext, "ztype": ztype})
    return zones, atr, avgvol, tol


# ======================= DETECCION DE SENALES =======================
def detect_signal(closed, ema200):
    """Evalua la ultima vela cerrada. Zonas calculadas SIN las dos ultimas velas (sin sesgo de futuro)."""
    n = len(closed)
    if n < 160:
        return None
    za = analyze_zones(closed[:-2])
    if not za:
        return None
    zones, atr, avgvol, tol = za
    c2, c1 = closed[-2], closed[-1]
    min_sw, max_sw = LQ_MIN_SWEEP_ATR * atr, LQ_MAX_SWEEP_ATR * atr
    best = None
    ut = ut_bot(closed, UT_MULT, UT_ATR_PERIOD) if LQ_USE_UT else None
    lrs = linreg_signal([x[4] for x in closed], LINREG_LEN, LINREG_SIGNAL) if LQ_USE_LINREG else None
    srl = sr_levels(closed[:-2]) if LQ_USE_SR else []
    late_max = 8 if LQ_USE_HTF else 0     # bonus maximo que se suma despues (TF superiores + ayer)

    for z in zones:
        if z["score"] < LQ_MIN_ZONE_SCORE:
            continue
        L = z["level"]
        bsl = z["kind"] == "BSL"
        side = "SHORT" if bsl else "LONG"

        def pen(c):   # penetracion de la vela mas alla del nivel
            return (c[2] - L) if bsl else (L - c[3])

        def wick(c):
            rng = c[2] - c[3]
            if rng <= 0:
                return 0.0
            return (c[2] - max(c[1], c[4])) / rng if bsl else (min(c[1], c[4]) - c[3]) / rng

        def back_inside(c):
            return c[4] < L if bsl else c[4] > L

        def against_dir(c):   # vela en la direccion de la operacion
            return c[4] < c[1] if bsl else c[4] > c[1]

        p2, p1 = pen(c2), pen(c1)
        kind = None
        rv = wk = depth = 0.0
        extreme = None
        if LQ_ENABLE_SOUP and p2 <= min_sw and min_sw <= p1 <= max_sw and back_inside(c1):
            kind, rv, wk, depth = "SOUP", c1[5] / avgvol, wick(c1), p1 / atr
            extreme = c1[2] if bsl else c1[3]
        elif (LQ_ENABLE_PLUS1 and min_sw <= p2 <= max_sw and p1 <= max_sw and back_inside(c1)
              and ((c2[4] >= L) if bsl else (c2[4] <= L)) and against_dir(c1)):
            kind, rv, wk, depth = "SOUP+1", max(c2[5], c1[5]) / avgvol, wick(c2), p2 / atr
            extreme = max(c2[2], c1[2]) if bsl else min(c2[3], c1[3])
        elif (LQ_ENABLE_BOUNCE and p2 <= min_sw and -tol <= p1 <= min_sw and back_inside(c1)
              and against_dir(c1) and wick(c1) >= max(LQ_MIN_WICK, 0.5)):
            kind, rv, wk, depth = "REBOTE", c1[5] / avgvol, wick(c1), 0.0
            extreme = c1[2] if bsl else c1[3]
        if not kind:
            continue
        if rv < LQ_VOL_MULT or (kind != "SOUP+1" and wk < LQ_MIN_WICK):
            continue

        entry = c1[4]
        buf = LQ_SL_ATR_BUFFER * atr
        sl = extreme + buf if bsl else extreme - buf
        risk = abs(sl - entry)
        if risk <= 0 or risk / entry * 100 > LQ_MAX_RISK_PCT:
            continue
        # objetivo: siguiente zona de liquidez importante en sentido contrario
        tp, tp_zone = None, None
        opp = [o for o in zones if o["kind"] == ("SSL" if bsl else "BSL") and o["score"] >= LQ_MIN_TP_ZONE_SCORE]
        if bsl:
            cand = [o for o in opp if o["level"] < entry - risk]
            if cand:
                tp_zone = max(cand, key=lambda o: o["level"])
                tp = tp_zone["level"] + 0.1 * atr
        else:
            cand = [o for o in opp if o["level"] > entry + risk]
            if cand:
                tp_zone = min(cand, key=lambda o: o["level"])
                tp = tp_zone["level"] - 0.1 * atr
        if tp is None:
            tp = entry - LQ_DEFAULT_RR * risk if bsl else entry + LQ_DEFAULT_RR * risk
        reward = abs(tp - entry)
        if reward > LQ_MAX_RR * risk:
            reward = LQ_MAX_RR * risk
            tp = entry - reward if bsl else entry + reward
        rr = reward / risk
        if rr < LQ_MIN_RR:
            continue
        aligned = (entry < ema200) if bsl else (entry > ema200)
        if LQ_USE_EMA_FILTER and not aligned:
            continue

        depth_pts = 0 if kind == "REBOTE" else (10 if 0.1 <= depth <= 1.0 else 5)
        quality = (40 * z["score"] / 100
                   + 20 * min(max(rv - 1, 0) / 2, 1)
                   + 15 * min(wk / 0.7, 1)
                   + depth_pts
                   + (10 if aligned else 0)
                   + (5 if rr >= 2 else 0)
                   - (10 if kind == "REBOTE" else 0))
        # --- extras importados del Pine ---
        want = 1 if side == "LONG" else -1
        ut_pts = lr_pts = sr_pts = 0
        sr_txt = None
        if ut:
            ut_ok = ut["state"] == want
            if LQ_UT_REQUIRE and not ut_ok:
                continue
            ut_pts = (4 if ut_ok else 0) + (2 if ut_ok and ut["since_flip"] <= 3 else 0)
        if lrs:
            lr_ok = (lrs[0] > lrs[1]) if want == 1 else (lrs[0] < lrs[1])
            lr_pts = 3 if lr_ok else 0
        for w, hi, lo in reversed(srl):
            lvl = hi if bsl else lo
            if w >= 100 and abs(lvl - L) <= tol:
                sr_pts = 3
                sr_txt = f"coincide con {'máx' if bsl else 'mín'} de {w} velas"
                break
        quality += ut_pts + lr_pts + sr_pts
        if quality < LQ_MIN_QUALITY - late_max:
            continue
        cand_sig = {"side": side, "kind": kind, "zone": z, "entry": entry, "sl": sl, "tp": tp,
                    "rr": rr, "quality": quality, "rv": rv, "wick": wk, "tp_zone": tp_zone,
                    "aligned": aligned, "atr": atr, "ts": c1[0], "tol": tol,
                    "ut": ut, "lr": lrs, "sr_txt": sr_txt,
                    "extras": {"UT": ut_pts, "LinReg": lr_pts, "S/R": sr_pts}}
        if best is None or quality > best["quality"]:
            best = cand_sig
    return best


def manage_position(pos, closed):
    """Revisa velas posteriores a la entrada. Devuelve (precio_salida, motivo, velas) o None."""
    bars = 0
    for c in closed:
        if c[0] <= pos["ts"]:
            continue
        bars += 1
        if pos["side"] == "LONG":
            hit_sl, hit_tp = c[3] <= pos["sl"], c[2] >= pos["tp"]
        else:
            hit_sl, hit_tp = c[2] >= pos["sl"], c[3] <= pos["tp"]
        if hit_sl:   # si SL y TP caen en la misma vela, se asume SL (conservador)
            return pos["sl"], "SL", bars
        if hit_tp:
            return pos["tp"], "TP", bars
        if bars >= LQ_MAX_HOLD_BARS:
            return c[4], "TIEMPO", bars
    return None


# ======================= TELEGRAM =======================
# Cola con ritmo controlado: Telegram permite ~1 msg/s por chat (20/min en grupos) y, si se excede,
# responde 429 con un "retry_after" que puede ser de HORAS. Seguir enviando durante el bloqueo lo alarga.
TG_MIN_INTERVAL = float(os.getenv("TG_MIN_INTERVAL", "3.2"))   # seg entre mensajes
TG_MAX_AGE = float(os.getenv("TG_MAX_AGE_SEC", "900"))         # una senal con mas de 15 min ya no sirve
TG_QUEUE_MAX = int(os.getenv("TG_QUEUE_MAX", "100"))
TG_QUEUE = None
TG_BLOCKED_UNTIL = 0.0


async def send_telegram(session, text):
    """Encola el mensaje (no bloquea el escaneo). Lo envia telegram_worker respetando limites."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Falta TELEGRAM_TOKEN o TELEGRAM_CHAT_ID")
        return
    if TG_QUEUE is None:
        return
    if TG_QUEUE.full():
        try:
            TG_QUEUE.get_nowait()  # descarta el mas antiguo
            log.warning("Cola de Telegram llena: descartado el mensaje mas antiguo")
        except asyncio.QueueEmpty:
            pass
    TG_QUEUE.put_nowait((time.time(), text))


async def telegram_worker(session):
    global TG_BLOCKED_UNTIL
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    while True:
        ts, text = await TG_QUEUE.get()
        plain = False
        while True:
            now = time.time()
            if now - ts > TG_MAX_AGE:
                log.warning("Mensaje descartado por antiguo (%.0f min en cola)", (now - ts) / 60)
                break
            if now < TG_BLOCKED_UNTIL:
                await asyncio.sleep(min(60.0, TG_BLOCKED_UNTIL - now))
                continue
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
            if not plain:
                payload["parse_mode"] = "HTML"
            try:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    body = await r.text()
                    if r.status == 200:
                        await asyncio.sleep(TG_MIN_INTERVAL)
                        break
                    if r.status == 429:
                        m = re.search(r'"retry_after"\s*:\s*(\d+)', body)
                        wait = int(m.group(1)) if m else 60
                        if time.time() >= TG_BLOCKED_UNTIL:  # loguea una sola vez por bloqueo
                            log.error("Telegram 429: bloqueado %d s (~%.1f h). Pauso envios.", wait, wait / 3600)
                        TG_BLOCKED_UNTIL = time.time() + wait + 1
                        continue
                    if r.status == 400 and "parse" in body.lower() and not plain:
                        plain = True  # HTML mal formado: reenviar como texto plano
                        text = re.sub(r"<[^>]+>", "", text)
                        continue
                    log.error("Telegram %s: %s", r.status, body[:200])
                    break
            except Exception as e:
                log.error("Error Telegram: %s", e)
                await asyncio.sleep(5)
                break


async def send_long(session, text):
    chunk = ""
    for line in text.split("\n"):
        if len(chunk) + len(line) + 1 > 3800:
            await send_telegram(session, chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk.strip():
        await send_telegram(session, chunk)


# ======================= MERCADOS =======================
async def get_symbols(ex):
    await ex.load_markets(True)
    symbols, excluded = [], []
    for s, m in ex.markets.items():
        if not m.get("active", True) or m.get("quote") != "USDT":
            continue
        if MARKET_TYPE == "swap" and not (m.get("swap") and m.get("linear")):
            continue
        if MARKET_TYPE == "spot" and not m.get("spot"):
            continue
        if EXCLUDE_NON_CRYPTO and is_non_crypto(m):
            excluded.append(m.get("base") or s)
            continue
        symbols.append(s)
    EXCLUDED[:] = sorted(excluded)
    if MIN_VOLUME_USDT > 0:
        try:
            tickers = await ex.fetch_tickers(symbols)
            symbols = [s for s in symbols if (tickers.get(s, {}).get("quoteVolume") or 0) >= MIN_VOLUME_USDT]
        except Exception as e:
            log.warning("No se pudo filtrar por volumen: %s", e)
    return symbols


# ======================= ANALISIS POR MONEDA =======================
async def analyze(ex, session, sem, symbol, tf):
    async with sem:
        try:
            ohlcv = await ex.fetch_ohlcv(symbol, tf, limit=CANDLES)
        except Exception as e:
            log.debug("%s %s error: %s", symbol, tf, e)
            return 0
    now_ms = int(time.time() * 1000)
    tf_ms = tf_seconds(tf) * 1000
    closed = [c for c in ohlcv if c[0] + tf_ms <= now_ms]
    if len(closed) < max(EMA_PERIOD + 2, 160):
        return 0
    last = closed[-1]
    key = (symbol, tf)
    link = f'<a href="{fmt_link(symbol)}">Abrir en BingX</a>'
    sent = 0

    # 1) gestionar posicion virtual abierta (cuando salir)
    pos = POSITIONS.get(key)
    if pos:
        res = manage_position(pos, closed)
        if res:
            exit_price, reason, bars = res
            pnl = ((exit_price - pos["entry"]) if pos["side"] == "LONG"
                   else (pos["entry"] - exit_price)) / pos["entry"] * 100 - FEE_PCT_ROUNDTRIP
            icon = "✅ GANADA" if pnl > 0 else "❌ PERDIDA"
            motivo = {"TP": "Objetivo alcanzado 🎯", "SL": "Stop alcanzado 🛑",
                      "TIEMPO": f"Salida por tiempo ({LQ_MAX_HOLD_BARS} velas) ⏱"}[reason]
            await send_telegram(session,
                f"🏁 <b>SALIR {pos['side']}</b> · {tf} · {pos['kind']}\n<b>{symbol}</b>\n{motivo}\n"
                f"Entrada: <code>{pretty(pos['entry'])}</code> → Salida: <code>{pretty(exit_price)}</code>\n"
                f"{icon}: <b>{pnl:+.2f}%</b> (neto de comisiones) · {bars} velas\n{link}")
            CLOSED.append({"symbol": symbol, "tf": tf, "side": pos["side"], "kind": pos["kind"],
                           "pnl": pnl, "reason": reason, "ts": time.time()})
            del POSITIONS[key]
            COOLDOWN[key] = last[0] + LQ_COOLDOWN_BARS * tf_ms
            sent += 1
        return sent

    # 2) buscar senal nueva
    if last[0] < COOLDOWN.get(key, 0):
        return 0
    ema200 = calc_ema_last([c[4] for c in closed], EMA_PERIOD)
    if ema200 is None:
        return 0
    sig = detect_signal(closed, ema200)
    if not sig:
        return 0
    htf = None
    if LQ_USE_HTF:
        try:
            htf = await get_htf_context(ex, symbol, tf, sig)
        except Exception as e:
            log.debug("%s HTF error: %s", symbol, e)
        if htf:
            sig["extras"]["HTF"] = htf["bonus"]
            sig["extras"]["Ayer"] = htf["pd_bonus"]
            sig["quality"] += htf["bonus"] + htf["pd_bonus"]
    sig["quality"] = min(100.0, sig["quality"])
    if sig["quality"] < LQ_MIN_QUALITY:
        return 0
    if htf and htf["aligned"] < LQ_HTF_MIN_ALIGNED:
        return 0
    z = sig["zone"]
    risk_pct = abs(sig["sl"] - sig["entry"]) / sig["entry"] * 100
    rew_pct = abs(sig["tp"] - sig["entry"]) / sig["entry"] * 100
    zname = "BSL (liquidez arriba)" if z["kind"] == "BSL" else "SSL (liquidez abajo)"
    extra = " · extremo 20 velas" if z["ext20"] else ""
    tp_txt = (f"{z_name(sig['tp_zone'])}" if sig["tp_zone"] else f"R:R fijo {LQ_DEFAULT_RR:g}")
    kind_txt = {"SOUP": "TURTLE SOUP", "SOUP+1": "TURTLE SOUP PLUS ONE", "REBOTE": "REBOTE EN LIQUIDEZ"}[sig["kind"]]
    emoji = "🟢" if sig["side"] == "LONG" else "🔴"
    hora = datetime.fromtimestamp(last[0] / 1000, tz=timezone.utc).strftime("%H:%M UTC")
    extras_txt = " · ".join(f"{k} {int(v):+d}" for k, v in sig["extras"].items() if v)
    extras_line = f"Extras: {extras_txt}\n" if extras_txt else ""
    sr_extra = f" · {sig['sr_txt']}" if sig["sr_txt"] else ""
    age_line = f"Longevidad: sin barrer hace {fmt_age(z['age'], tf)} ({z['age']} velas){sr_extra}\n"
    panel = build_panel(closed, sig, htf) if LQ_SHOW_PANEL else ""
    await send_telegram(session,
        f"{emoji} <b>{sig['side']} · {kind_txt}</b> · {tf}\n<b>{symbol}</b>\n"
        f"Zona: {zname} <code>{pretty(z['level'])}</code>{extra}\n"
        f"Zona {z['ztype']} · importancia <b>{z['score']:.0f}/100</b> "
        f"(toques {z['touches']} · liquidaciones x{z['ratio']:.1f})\n"
        f"{age_line}"
        f"Calidad: <b>{sig['quality']:.0f}/100</b> · RVOL {sig['rv']:.1f}x · mecha {sig['wick'] * 100:.0f}%\n"
        f"{extras_line}"
        f"Entrada: <code>{pretty(sig['entry'])}</code>\n"
        f"SL: <code>{pretty(sig['sl'])}</code> (-{risk_pct:.2f}%)\n"
        f"TP: <code>{pretty(sig['tp'])}</code> (+{rew_pct:.2f}%) → R:R <b>{sig['rr']:.1f}</b>\n"
        f"Objetivo: {tp_txt}\n"
        f"EMA{EMA_PERIOD}: {'a favor ✅' if sig['aligned'] else 'en contra ⚠️'}\n"
        f"{panel}"
        f"Salir: TP, SL o a las {LQ_MAX_HOLD_BARS} velas · Vela {hora}\n{link}")
    POSITIONS[key] = {"side": sig["side"], "kind": sig["kind"], "entry": sig["entry"],
                      "sl": sig["sl"], "tp": sig["tp"], "ts": sig["ts"]}
    return sent + 1


def z_name(z):
    return f"{'BSL' if z['kind'] == 'BSL' else 'SSL'} {pretty(z['level'])} ({z['ztype']}, {z['score']:.0f} pts)"


HTF_MAP = {"1m": ["5m", "15m", "1h"], "3m": ["15m", "1h", "4h"], "5m": ["15m", "1h", "4h"],
           "15m": ["1h", "4h", "1d"], "30m": ["1h", "4h", "1d"], "1h": ["4h", "1d", "1w"]}


def htf_list(tf):
    if tf in HTF_MAP:
        return HTF_MAP[tf]
    base = tf_seconds(tf)
    return [t for t in ("15m", "1h", "4h", "1d", "1w") if tf_seconds(t) > base][:3] or ["1d"]


async def get_htf_context(ex, symbol, tf, sig):
    """Tendencia EMA9/21 en 3 TF superiores (como el panel del Pine) + max/min de ayer."""
    tfs = htf_list(tf)
    need = list(dict.fromkeys(tfs + ["1d"]))
    res = await asyncio.gather(*[ex.fetch_ohlcv(symbol, t, limit=60) for t in need], return_exceptions=True)
    data = {t: r for t, r in zip(need, res) if not isinstance(r, Exception) and r}
    want = 1 if sig["side"] == "LONG" else -1
    trends, aligned, opposed = [], 0, 0
    for t in tfs:
        d = data.get(t)
        if not d or len(d) < 25:
            continue
        cl = [x[4] for x in d]
        e9, e21 = calc_ema_last(cl, 9), calc_ema_last(cl, 21)
        tr = 1 if e9 > e21 else (-1 if e9 < e21 else 0)
        trends.append((t, tr))
        aligned += tr == want
        opposed += tr == -want
    pdh = pdl = None
    pd_bonus = 0
    d1 = data.get("1d")
    if d1 and len(d1) >= 2:
        pdh, pdl = d1[-2][2], d1[-2][3]
        lvl = pdh if sig["zone"]["kind"] == "BSL" else pdl
        if abs(lvl - sig["zone"]["level"]) <= sig["tol"]:
            pd_bonus = 2
    if not trends and pdh is None:
        return None
    return {"trends": trends, "aligned": aligned, "bonus": max(-4, min(6, 2 * aligned - opposed)),
            "pd_bonus": pd_bonus, "pdh": pdh, "pdl": pdl}


def build_panel(closed, sig, htf):
    """Panel de mercado (version Telegram del dashboard del Pine). Termina en salto de linea."""
    ic = lambda ok: "🟢" if ok else "🔴"
    cl = [x[4] for x in closed]
    out = ["", "📊 <b>Panel</b>"]
    a = [v for v in atr_series(closed[-250:], 14) if v is not None]
    if len(a) >= 20:
        w = a[-20:]
        sma = sum(w) / 20
        sd = math.sqrt(sum((v - sma) ** 2 for v in w) / 20)
        top, bot = sma + 2 * sd, sma - 2 * sd
        vol = 30 + 40 * ((a[-1] - bot) / max(1e-10, top - bot))
        st = "alta 🟢" if a[-1] > sma * 1.2 else ("baja 🔴" if a[-1] < sma * 0.8 else "media ➖")
        out.append(f"Volatilidad {vol:.0f}% · ATR {st}")
    vols = [x[5] or 0 for x in closed]
    vma = sum(vols[-20:]) / 20
    if vma > 0:
        out.append(f"Volumen {vols[-1] / vma:.1f}x media20 {ic(vols[-1] > vma)}")
    day0 = closed[-1][0] - closed[-1][0] % 86400000
    dv = [(x[4], x[5] or 0) for x in closed if x[0] >= day0]
    tv = sum(v for _, v in dv)
    if tv > 0 and len(dv) >= 3:
        vw = sum(p * v for p, v in dv) / tv
        out.append(f"VWAP del día: precio {'por encima' if cl[-1] > vw else 'por debajo'} {ic(cl[-1] > vw)}")
    parts = []
    c3 = cl[-300:]
    e12, e26 = ema_series(c3, 12), ema_series(c3, 26)
    if e26:
        m = [e12[j + 14] - e26[j] for j in range(len(e26))]
        s9 = ema_series(m, 9)
        if s9:
            parts.append(f"MACD {ic(m[-1] > s9[-1])} {'alcista' if m[-1] > s9[-1] else 'bajista'}")
    kd = stoch_kd(closed[-80:])
    if kd:
        k, d = kd
        parts.append(f"Stoch {ic(k > d)} {'sobrecompra' if k > 80 else 'sobreventa' if k < 20 else 'neutral'}")
    rsi = rsi_last(cl[-200:])
    if rsi is not None:
        parts.append(f"RSI {rsi:.0f}")
    if parts:
        out.append(" · ".join(parts))
    ut = sig.get("ut")
    if ut:
        flip = f" (giro hace {ut['since_flip']} velas)" if ut["since_flip"] <= 3 else ""
        out.append(f"UT Bot: {'🟢 alcista' if ut['state'] == 1 else '🔴 bajista'} · trail <code>{pretty(ut['trail'])}</code>{flip}")
    lr = sig.get("lr")
    if lr:
        out.append(f"LinReg: {ic(lr[0] > lr[1])} {'alcista' if lr[0] > lr[1] else 'bajista'}")
    if htf and htf["trends"]:
        sym = {1: "🟢", -1: "🔴", 0: "➖"}
        out.append("Tendencia EMA9/21: " + " · ".join(f"{t} {sym[v]}" for t, v in htf["trends"]))
    if htf and htf["pdh"] is not None:
        hit = " ← la zona coincide" if htf["pd_bonus"] else ""
        out.append(f"Ayer: máx <code>{pretty(htf['pdh'])}</code> · mín <code>{pretty(htf['pdl'])}</code>{hit}")
    return "\n".join(out) + "\n"


async def scan(ex, session, tf, symbols):
    sem = asyncio.Semaphore(CONCURRENCY)
    t0 = time.time()
    res = await asyncio.gather(*[analyze(ex, session, sem, s, tf) for s in symbols], return_exceptions=True)
    errs = [r for r in res if isinstance(r, Exception)]
    if errs:
        log.error("Errores en scan %s: %s", tf, errs[0])
    log.info("[%s] %d monedas, %d mensajes, %d posiciones virtuales, %.1fs",
             tf, len(symbols), sum(r for r in res if isinstance(r, int)), len(POSITIONS), time.time() - t0)


# ======================= RESUMEN DIARIO =======================
def build_report(trades, date_str):
    open_n = len(POSITIONS)
    if not trades:
        return (f"📊 <b>Liquidez · resumen {date_str}</b>\nSin operaciones cerradas hoy.\n"
                f"Posiciones abiertas: {open_n}")
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    n = len(trades)
    total = sum(t["pnl"] for t in trades)
    gw = sum(t["pnl"] for t in wins)
    gl = -sum(t["pnl"] for t in losses)
    pf = (gw / gl) if gl > 0 else float("inf")
    avg_w = gw / len(wins) if wins else 0
    avg_l = -gl / len(losses) if losses else 0
    expect = total / n
    eq = peak = dd = 0.0
    for t in sorted(trades, key=lambda t: t["ts"]):
        eq += t["pnl"]
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
    pf_txt = "∞" if pf == float("inf") else f"{pf:.2f}"
    lines = [
        f"📊 <b>Liquidez · resumen {date_str}</b> (neto de comisiones)",
        f"Operaciones: <b>{n}</b> · ✅ {len(wins)} ({len(wins) / n * 100:.0f}%) · ❌ {len(losses)}",
        f"Media ganadora {avg_w:+.2f}% · media perdedora {avg_l:+.2f}%",
        f"Esperanza por operación: <b>{expect:+.2f}%</b> · Profit factor: <b>{pf_txt}</b>",
        f"Resultado total (suma de %): <b>{total:+.2f}%</b> · Drawdown máx: {dd:.2f}%",
        f"Posiciones abiertas: {open_n}", "", "<b>Por tipo</b>"]
    for k in ("SOUP", "SOUP+1", "REBOTE"):
        tt = [t for t in trades if t["kind"] == k]
        if tt:
            w = sum(1 for t in tt if t["pnl"] > 0)
            lines.append(f"{k}: {len(tt)} ops · {w}G/{len(tt) - w}P · {sum(t['pnl'] for t in tt):+.2f}%")
    lines += ["", "<b>Por timeframe y lado</b>"]
    for tf in sorted({t["tf"] for t in trades}, key=tf_seconds):
        for sd in ("LONG", "SHORT"):
            tt = [t for t in trades if t["tf"] == tf and t["side"] == sd]
            if tt:
                w = sum(1 for t in tt if t["pnl"] > 0)
                lines.append(f"{tf} {sd}: {len(tt)} ops · {w}G/{len(tt) - w}P · {sum(t['pnl'] for t in tt):+.2f}%")
    best = sorted(trades, key=lambda t: -t["pnl"])[:5]
    worst = sorted(trades, key=lambda t: t["pnl"])[:5]
    fmt = lambda t: f"{'✅' if t['pnl'] > 0 else '❌'} {t['symbol'].split(':')[0]} {t['side']} {t['tf']} {t['kind']} <b>{t['pnl']:+.2f}%</b>"
    lines += ["", "<b>Mejores</b>"] + [fmt(t) for t in best] + ["", "<b>Peores</b>"] + [fmt(t) for t in worst]
    return "\n".join(lines)


async def daily_report_loop(session):
    try:
        tz = ZoneInfo(REPORT_TZ)
    except Exception:
        tz = timezone.utc
    hh, mm = (int(x) for x in REPORT_TIME.split(":"))
    while True:
        now = datetime.now(tz)
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        trades = CLOSED[:]
        CLOSED.clear()
        try:
            await send_long(session, build_report(trades, target.strftime("%d/%m/%Y")))
        except Exception as e:
            log.error("Error resumen: %s", e)


# ======================= BUCLES =======================
async def tf_loop(ex, session, tf, symbols_ref):
    secs = tf_seconds(tf)
    while True:
        now = time.time()
        await asyncio.sleep((now // secs + 1) * secs + CLOSE_DELAY_SEC - now)
        try:
            await scan(ex, session, tf, symbols_ref["list"])
        except Exception as e:
            log.error("Error en scan %s: %s", tf, e)


async def refresh_symbols(ex, symbols_ref):
    while True:
        await asyncio.sleep(3600)
        try:
            symbols_ref["list"] = await get_symbols(ex)
            log.info("Lista actualizada: %d monedas", len(symbols_ref["list"]))
        except Exception as e:
            log.error("Error actualizando lista: %s", e)


async def main():
    ex = ccxt.bingx({"enableRateLimit": True,
                     "options": {"defaultType": "swap" if MARKET_TYPE == "swap" else "spot"}})
    global TG_QUEUE
    TG_QUEUE = asyncio.Queue(maxsize=TG_QUEUE_MAX)
    async with aiohttp.ClientSession() as session:
        tg_task = asyncio.create_task(telegram_worker(session))
        try:
            symbols = await get_symbols(ex)
            log.info("Monedas cripto: %d (excluidas %d)", len(symbols), len(EXCLUDED))
            symbols_ref = {"list": symbols}
            tipos = [x for x, on in (("Turtle Soup", LQ_ENABLE_SOUP), ("Soup+1", LQ_ENABLE_PLUS1),
                                     ("Rebote", LQ_ENABLE_BOUNCE)) if on]
            await send_telegram(session,
                f"✅ <b>Bot de Liquidez</b> iniciado\nTF: {', '.join(TIMEFRAMES)} · Monedas: {len(symbols)}\n"
                f"Setups: {', '.join(tipos)}\n"
                f"Zona mín. {LQ_MIN_ZONE_SCORE:.0f}/100 · Calidad mín. {LQ_MIN_QUALITY:.0f}/100 · R:R mín. {LQ_MIN_RR:g}\n"
                f"Resumen diario: {REPORT_TIME} ({REPORT_TZ})")
            await asyncio.gather(refresh_symbols(ex, symbols_ref), daily_report_loop(session),
                                 *[tf_loop(ex, session, tf, symbols_ref) for tf in TIMEFRAMES])
        finally:
            tg_task.cancel()
            await ex.close()


if __name__ == "__main__":
    asyncio.run(main())
