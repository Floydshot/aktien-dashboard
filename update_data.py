#!/usr/bin/env python3
"""Refresh the dashboard, retaining the provenance of every fallback.

Run ``python update_data.py`` for Yahoo Finance data, or add ``--seed`` for
the exact supplied starting values without any network or third-party import.
Historical multiples and buy_trigger strings below are user-supplied fixed
references, not independently verified five/ten-year historical calculations.
Stock tranches implement the requested forward-P/E thresholds, not the free-text
company-specific triggers. ETFs are assessed separately without stock thresholds.
All missing prices/technicals remain JSON null.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import sys
import tempfile
import time
from typing import Any, Callable
from zoneinfo import ZoneInfo


BERLIN = ZoneInfo("Europe/Berlin")
REFERENCE_WARNING = (
    "Historische Multiples, Dividendenreferenzen und Nachkauf-Indikatoren sind "
    "vorgegebene, nicht unabhängig geprüfte Referenzwerte; der 5J/10J-Zeitraum "
    "ist nicht je Aktie belegt. Tranchen folgen nur den Forward-KGV-Schwellen."
)
INITIAL_WARNING = "Startwerte aus dem Briefing; keine aktuellen Yahoo-Finance-Daten."
NUMERIC_FIELDS = (
    "current_pe", "forward_pe", "ps_ratio", "div_yield", "roe", "price",
    "sma200", "rsi14",
)
ETF_NUMERIC_FIELDS = ("distribution_yield_ttm", "distributions_per_share_ttm")
ETF_SOURCE = "https://am.jpmorgan.com/content/dam/jpm-am-aem/americas/us/en/literature/fact-sheet/etfs/FS-JEPQ.PDF"


def reference(name: str, ticker: str, cluster: str, hist_pe: float,
              buy_trigger: str, current_pe: float, forward_pe: float,
              *, hist_ps: float | None = None, ps_ratio: float | None = None,
              hist_div: float | None = None, div_yield: float | None = None,
              roe: float | None = None) -> dict[str, Any]:
    return dict(name=name, ticker=ticker, cluster=cluster, hist_pe=hist_pe,
                hist_ps=hist_ps, hist_div=hist_div, buy_trigger=buy_trigger,
                initial=dict(current_pe=current_pe, forward_pe=forward_pe,
                             ps_ratio=ps_ratio, div_yield=div_yield, roe=roe))


BIG_TECH = "Big Tech & Halbleiter"
SAAS = "High-Growth SaaS & Cyber"
EU_INSURANCE = "Erstversicherer Europa"
ASIA = "Asien & Japan Finanzen"
REINSURANCE = "Rückversicherer"
PORTFOLIOS = [
    dict(id="tech", name="Tech Stonks", stocks=[
        reference("Alphabet", "GOOGL", BIG_TECH, 24.3, "Forward P/E < 17x oder Wochen-EMA 50", 16.9, 16.8, hist_ps=6.2, ps_ratio=6.5),
        reference("Meta", "META", BIG_TECH, 25.9, "Forward P/E 18x–19x & Tages-RSI < 40", 27.7, 19.7, hist_ps=7.8, ps_ratio=9.2),
        reference("Broadcom", "AVGO", BIG_TECH, 65.7, "Forward P/E 19x–21x oder Tages-SMA 100/200", 41.4, 21.0, hist_ps=8.1, ps_ratio=18.3),
        reference("Amazon", "AMZN", BIG_TECH, 63.0, "Forward P/E < 24x oder P/OCF < 18x", 21.1, 23.6, hist_ps=3.5, ps_ratio=3.8),
        reference("Nvidia", "NVDA", BIG_TECH, 54.9, "Forward P/E 22x–24x oder Tages-SMA 200", 27.4, 24.9, hist_ps=20.5, ps_ratio=24.0),
        reference("Microsoft", "MSFT", BIG_TECH, 33.0, "Forward P/E 23x–24x oder Test SMA 200", 29.6, 25.1, hist_ps=11.2, ps_ratio=13.5),
        reference("ServiceNow", "NOW", SAAS, 147.5, "Forward P/E < 28x oder P/FCF < 30x", 87.5, 27.8, hist_ps=15.8, ps_ratio=16.5),
        reference("Apple", "AAPL", BIG_TECH, 33.0, "Abwarten auf Forward P/E < 28x", 39.3, 35.2, hist_ps=7.4, ps_ratio=9.8),
        reference("AMD", "AMD", BIG_TECH, 134.5, "Forward P/E 28x–32x oder Wochen-SMA 50", 137.1, 39.5, hist_ps=8.5, ps_ratio=11.5),
        reference("Arista Networks", "ANET", BIG_TECH, 46.5, "Rücksetzer auf Forward P/E < 38x–40x", 67.9, 48.5, hist_ps=14.5, ps_ratio=22.0),
        reference("Palo Alto Networks", "PANW", SAAS, 173.8, "EV/FCF < 30x–32x (Earnings-Dip nutzen)", 271.9, 93.5, hist_ps=11.5, ps_ratio=18.2),
        reference("CrowdStrike", "CRWD", SAAS, 200.0, "Strikte Disziplin: P/S < 25x oder P/FCF < 35x", 250.0, 208.3, hist_ps=27.1, ps_ratio=43.2),
        reference("Cloudflare", "NET", SAAS, 210.0, "Kompression auf P/S 20x–25x abwarten", 260.0, 226.9, hist_ps=31.1, ps_ratio=41.7),
    ]),
    dict(id="value", name="Value Stonks", stocks=[
        reference("AXA SA", "CS.PA", EU_INSURANCE, 11.2, "Forward P/E < 9,5x & Div.-Rendite > 5,7%", 8.5, 8.3, hist_div=5.4, div_yield=6.0, roe=15.0),
        reference("Sompo Holdings", "8630.T", ASIA, 11.5, "Forward P/E < 10x oder P/B < 1,25x", 8.9, 9.5, hist_div=3.1, div_yield=3.4, roe=14.0),
        reference("Zurich Insurance", "ZURN.SW", EU_INSURANCE, 14.9, "P/E < 11,5x & Div.-Rendite > 5,0%", 9.0, 9.8, hist_div=5.1, div_yield=5.2, roe=36.0),
        reference("Swiss Re", "SREN.SW", REINSURANCE, 13.8, "Forward P/E < 10x oder Div.-Rendite > 4,5%", 10.4, 10.1, hist_div=5.5, div_yield=4.2, roe=17.0),
        reference("Munich Re", "MUV2.DE", REINSURANCE, 13.5, "Forward P/E < 11x oder Test Tages-SMA 200", 11.2, 10.5, hist_div=4.3, div_yield=4.1, roe=19.0),
        reference("Hannover Rück", "HNR1.DE", REINSURANCE, 14.3, "Forward P/E < 11,5x & Div.-Rendite > 4,5%", 11.7, 11.2, hist_div=4.2, div_yield=4.6, roe=22.0),
        reference("MS&AD Insurance", "8725.T", ASIA, 9.0, "Forward P/E < 10,5x oder Div.-Rendite > 3,6%", 8.8, 12.3, hist_div=3.5, div_yield=3.2, roe=17.5),
        reference("Allianz", "ALV.DE", EU_INSURANCE, 12.5, "Forward P/E < 11,5x oder Div.-Rendite > 5,5%", 13.9, 12.4, hist_div=5.2, div_yield=5.2, roe=16.0),
        reference("Tokio Marine", "8766.T", ASIA, 12.6, "Rücksetzer auf Forward P/E 12x–13x abwarten", 18.3, 15.4, hist_div=3.2, div_yield=2.7, roe=13.0),
        reference("DBS Group Holdings", "D05.SI", ASIA, 11.8, "Disziplin: Warten auf Forward P/E < 12,5x", 17.8, 16.6, hist_div=4.9, div_yield=4.3, roe=16.2),
        reference("Swiss Life", "SLHN.SW", EU_INSURANCE, 14.2, "Forward P/E < 14,5x oder Div.-Rendite > 5,2%", 19.5, 17.2, hist_div=5.1, div_yield=4.8, roe=15.0),
        dict(name="JPMorgan Nasdaq Equity Premium Income ETF", ticker="JEPQ",
             cluster="Income-ETFs", asset_type="etf", fund_kind="equity_income_options",
             fund_inception="2022-05-03", fund_source=ETF_SOURCE,
             hist_pe=None, hist_ps=None, hist_div=None,
             buy_trigger="ETF separat bewerten; kein automatisches Kaufsignal.",
             distribution_yield_label="Ausschüttungsrendite (12M)",
             distribution_yield_method="Ausschüttungen je Anteil der letzten 12 Monate nach Ex-Tag / aktueller Kurs × 100; keine Gesamtrendite.",
             initial={}),
    ]),
]
EXPECTED_STOCK_COUNT = sum(len(group["stocks"]) for group in PORTFOLIOS)
ETF_TICKERS = frozenset(stock["ticker"] for group in PORTFOLIOS for stock in group["stocks"] if stock.get("asset_type") == "etf")


def finite_number(value: Any) -> float | None:
    """Reject bool, NaN and infinity before any JSON serialization."""
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def iso_now(now: datetime | None = None) -> str:
    return (now or datetime.now(timezone.utc)).astimezone(BERLIN).isoformat(timespec="seconds")


def tranche_for(portfolio_id: str, forward_pe: Any) -> tuple[str, str, str]:
    pe = finite_number(forward_pe)
    if pe is None or pe <= 0:
        return "Keine Bewertung verfügbar", "slate", "unavailable"
    immediate = pe < 24.0 if portfolio_id == "tech" else pe <= 11.3
    dip = pe <= (29.0 if portfolio_id == "tech" else 13.0)
    if immediate:
        return "Tranche 1 (Sofortkauf)", "emerald", "buy"
    if dip:
        return "Tranche 2 (Rücksetzer kaufen)", "sky", "dip"
    return "Hold / Watchlist", "amber", "watch"


def add_derived(stock: dict[str, Any], portfolio_id: str) -> None:
    raw = finite_number(stock.get("current_pe"))
    stock["chart_pe"] = 260.0 if raw is not None and stock["cluster"] == SAAS and (raw < 0 or raw > 280) else raw
    hist_pe = finite_number(stock.get("hist_pe"))
    spread = finite_number((raw / hist_pe - 1) * 100) if raw is not None and raw > 0 and hist_pe is not None and hist_pe > 0 else None
    stock["spread_pe_pct"] = round(spread, 4) if spread is not None else None
    price, sma = stock.get("price"), stock.get("sma200")
    distance = finite_number((price / sma - 1) * 100) if price and sma else None
    stock["sma200_distance_pct"] = round(distance, 4) if distance is not None else None
    if stock.get("asset_type") == "etf":
        stock["tranche"], stock["tranche_color"], stock["tranche_key"] = "ETF · separat bewerten", "slate", "unavailable"
        return
    stock["tranche"], stock["tranche_color"], stock["tranche_key"] = tranche_for(portfolio_id, stock.get("forward_pe"))
    if raw is not None and raw <= 0:
        stock["warnings"].append("TTM-KGV ist nicht positiv; ein Bewertungsabschlag ist nicht sinnvoll interpretierbar.")
    if raw is not None and stock["chart_pe"] != raw:
        stock["warnings"].append("Nur der TTM-Chartbalken ist auf 260x begrenzt; Kachel und Tooltip zeigen den Rohwert.")
    if stock["tranche_key"] == "unavailable":
        stock["warnings"].append("Forward-KGV fehlt oder ist nicht positiv; keine Tranche ableitbar.")


def build_seed_data(now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    stamp = iso_now(now)
    result = dict(schema_version=1,
                  last_updated=now.astimezone(BERLIN).strftime("%d.%m.%Y, %H:%M %Z"),
                  updated_at=stamp, last_attempted_at=None, data_mode="initial",
                  expected_stock_count=EXPECTED_STOCK_COUNT,
                  warnings=[INITIAL_WARNING, REFERENCE_WARNING], portfolios=[])
    for portfolio in PORTFOLIOS:
        group = dict(id=portfolio["id"], name=portfolio["name"], stocks=[])
        for ref in portfolio["stocks"]:
            stock = {key: value for key, value in ref.items() if key != "initial"}
            stock.update({field: ref["initial"].get(field) for field in NUMERIC_FIELDS})
            stock.update(currency=None, source_status="initial", as_of=None,
                         field_sources={field: "initial" if stock[field] is not None else "unavailable" for field in NUMERIC_FIELDS},
                         field_updated_at={field: None for field in NUMERIC_FIELDS}, warnings=[INITIAL_WARNING])
            stock["field_sources"]["currency"] = "unavailable"
            stock["field_updated_at"]["currency"] = None
            if stock.get("asset_type") == "etf":
                for field in ETF_NUMERIC_FIELDS:
                    stock[field] = None
                    stock["field_sources"][field] = "unavailable"
                    stock["field_updated_at"][field] = None
                stock["distribution_as_of"] = None
            add_derived(stock, portfolio["id"])
            group["stocks"].append(stock)
        result["portfolios"].append(group)
    return result


def normalize_dividend_yield(info: dict[str, Any], price: float | None) -> tuple[float | None, list[str]]:
    """Resolve Yahoo's fraction/percent variants against independent fields.

    dividendRate / price and trailingAnnualDividendYield (Yahoo fraction)
    provide unit checks. With contradictory or absent checks, do not guess.
    A missing dividendYield may be reconstructed from a valid annual rate.
    """
    raw = finite_number(info.get("dividendYield"))
    annual_rate = finite_number(info.get("dividendRate"))
    trailing = finite_number(info.get("trailingAnnualDividendYield"))
    annual = finite_number(annual_rate / price * 100) if annual_rate is not None and annual_rate >= 0 and price and price > 0 else None
    trailing_pct = finite_number(trailing * 100) if trailing is not None and trailing >= 0 else None
    anchors = [value for value in (annual, trailing_pct) if value is not None]
    if raw is None:
        if annual is not None:
            return annual, ["Dividendenrendite aus dividendRate / aktuellem Kurs berechnet."]
        if trailing_pct is not None:
            return trailing_pct, ["Nur die nachlaufende Jahresdividendenrendite ist verfügbar."]
        return None, []
    if raw < 0:
        return None, ["Negative Dividendenrendite verworfen."]
    if raw == 0:
        if any(value > 0.000001 for value in anchors):
            return None, ["Dividendenfelder widersprechen sich (Nullwert trotz positiver Referenz)."]
        return 0.0, []
    if not anchors:
        return None, ["Einheit von dividendYield ohne dividendRate/Kurs oder nachlaufende Rendite nicht sicher bestimmbar."]

    def matches(candidate: float, anchor: float) -> bool:
        return math.isfinite(candidate) and abs(candidate - anchor) <= max(0.000001, abs(anchor) * 0.25)

    candidates = [raw, raw * 100]
    matches_by_anchor = [{index for index, candidate in enumerate(candidates) if matches(candidate, anchor)} for anchor in anchors if anchor > 0]
    all_matches = set().union(*matches_by_anchor) if matches_by_anchor else set()
    if len(all_matches) == 1:
        chosen = all_matches.pop()
        warnings = []
        if any(not match for match in matches_by_anchor):
            warnings.append("Aktuelle und nachlaufende Dividendenreferenz weichen ab; Einheit über die passende Referenz geprüft.")
        return candidates[chosen], warnings
    return None, ["Dividenden-Einheit oder Referenzfelder sind widersprüchlich; bisherigen Wert als gekennzeichneten Fallback behalten."]


def wilder_rsi(close: Any, period: int = 14) -> float | None:
    """Wilder's RSI with the arithmetic first 14 changes and recursive RMA."""
    import pandas as pd
    values = pd.to_numeric(pd.Series(close), errors="coerce").replace([float("inf"), -float("inf")], float("nan")).dropna()
    if len(values) <= period:
        return None
    changes = values.diff().iloc[1:]
    gains, losses = changes.clip(lower=0), -changes.clip(upper=0)
    # Seed explicitly: pandas ewm's default first observation is not Wilder's seed.
    gain_seed = pd.Series([float(gains.iloc[:period].mean()), *gains.iloc[period:].tolist()])
    loss_seed = pd.Series([float(losses.iloc[:period].mean()), *losses.iloc[period:].tolist()])
    avg_gain = float(gain_seed.ewm(alpha=1 / period, adjust=False).mean().iloc[-1])
    avg_loss = float(loss_seed.ewm(alpha=1 / period, adjust=False).mean().iloc[-1])
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    if avg_gain == 0:
        return 0.0
    return finite_number(100 - 100 / (1 + avg_gain / avg_loss))


def calculate_technicals(history: Any) -> tuple[dict[str, Any], list[str]]:
    import pandas as pd
    if history is None or history.empty or "Close" not in history:
        raise ValueError("Keine gültigen historischen Tageskurse verfügbar")
    history = history.sort_index()
    history = history.loc[~history.index.duplicated(keep="last")]
    close = pd.to_numeric(history["Close"], errors="coerce").replace([float("inf"), -float("inf")], float("nan")).dropna()
    close = close[close > 0]
    if close.empty:
        raise ValueError("Historische Schlusskurse fehlen")
    last_index = close.index[-1]
    as_of = last_index.isoformat() if hasattr(last_index, "isoformat") else str(last_index)
    sma = finite_number(close.rolling(window=200, min_periods=200).mean().iloc[-1])
    rsi = wilder_rsi(close)
    notes = []
    if sma is None:
        notes.append(f"SMA 200 nicht berechenbar: nur {len(close)} gültige Tageskurse.")
    if rsi is None:
        notes.append("RSI 14 nicht berechenbar: weniger als 15 gültige Tageskurse.")
    return dict(sma200=sma, rsi14=rsi, history_price=float(close.iloc[-1]), history_as_of=as_of), notes


def calculate_distributions(history: Any) -> tuple[dict[str, Any], list[str]]:
    """Sum actual cash distributions over one complete rolling calendar year.

    Yahoo calls ETF distributions "Dividends". They may include option income,
    so the dashboard deliberately does not call this corporate dividend yield.
    The open left boundary prevents counting 13 monthly payments when the same
    calendar date occurs at both ends of the annual history.
    """
    import pandas as pd
    if history is None or history.empty or "Dividends" not in history:
        return {}, ["ETF-Ausschüttungshistorie fehlt; keine 12M-Rendite berechnet."]
    history = history.sort_index()
    history = history.loc[~history.index.duplicated(keep="last")]
    if not isinstance(history.index, pd.DatetimeIndex):
        return {}, ["ETF-Ausschüttungshistorie hat keine gültigen Tagesdaten."]
    end = history.index[-1].normalize()
    cutoff = end - pd.DateOffset(years=1)
    # Trading-day sampling can begin a few days after the exact calendar cutoff.
    if history.index[0].normalize() > cutoff + pd.Timedelta(days=5):
        return {}, ["ETF-Ausschüttungshistorie deckt keine vollständigen 12 Monate ab."]
    in_window = (history.index.normalize() > cutoff) & (history.index.normalize() <= end)
    distributions = pd.to_numeric(history.loc[in_window, "Dividends"], errors="coerce")
    if distributions.empty or not distributions.map(lambda value: finite_number(value) is not None and value >= 0).all():
        return {}, ["ETF-Ausschüttungshistorie enthält ungültige Werte; keine 12M-Rendite berechnet."]
    total = finite_number(distributions.sum())
    return dict(distributions_per_share_ttm=total, distribution_as_of=end.date().isoformat()), []


def _fetch_worker(ticker: str, pipe: Any, attempts: int) -> None:
    """Separate process gives even Ticker.info a hard overall time limit."""
    try:
        import yfinance as yf
        # Each endpoint also has its own bounded retry, in addition to watchdog.
        for endpoint in ("info", "history"):
            for attempt in range(attempts):
                try:
                    security = yf.Ticker(ticker)
                    if endpoint == "info":
                        info = security.info
                        if not isinstance(info, dict) or not any(info.get(key) is not None for key in ("trailingPE", "forwardPE", "currentPrice", "regularMarketPrice")):
                            raise ValueError("Leere oder unvollständige Yahoo-Stammdaten")
                        pipe.send(dict(info=info, info_retrieved_at=iso_now()))
                    else:
                        # Unadjusted Close shares the raw quote's price basis.
                        is_etf = ticker in ETF_TICKERS
                        history = security.history(period="1y", interval="1d", auto_adjust=False, actions=is_etf, timeout=12)
                        technicals, notes = calculate_technicals(history)
                        if is_etf:
                            distributions, distribution_notes = calculate_distributions(history)
                            technicals.update(distributions)
                            notes.extend(distribution_notes)
                        pipe.send(dict(technicals=technicals, history_retrieved_at=iso_now(), warnings=notes))
                    break
                except Exception as exc:
                    if attempt + 1 == attempts:
                        pipe.send(dict(warnings=[f"{endpoint}: {type(exc).__name__}: {str(exc)[:180]}"]))
                    else:
                        time.sleep(min(2 ** attempt, 4))
        pipe.send(dict(done=True))
    except Exception as exc:
        pipe.send(dict(warnings=[f"Abruf: {type(exc).__name__}: {str(exc)[:180]}"]))
    finally:
        pipe.close()


def fetch_ticker(ticker: str, timeout: float = 45.0, attempts: int = 2) -> dict[str, Any]:
    context = mp.get_context("spawn")
    parent_pipe, child_pipe = context.Pipe(duplex=False)
    worker = context.Process(target=_fetch_worker, args=(ticker, child_pipe, attempts), daemon=True)
    result: dict[str, Any] = dict(warnings=[])
    worker.start()
    child_pipe.close()
    deadline = time.monotonic() + timeout
    completed = False
    try:
        while time.monotonic() < deadline:
            if parent_pipe.poll(min(0.25, max(0, deadline - time.monotonic()))):
                try:
                    message = parent_pipe.recv()
                except EOFError:
                    break
                result["warnings"].extend(message.pop("warnings", []))
                completed = message.pop("done", False)
                result.update(message)
                if completed:
                    break
            elif not worker.is_alive():
                break
        if not completed and worker.is_alive():
            result["warnings"].append(f"Ticker-Zeitlimit von {timeout:g} Sekunden erreicht; bereits geladene Felder bleiben nutzbar.")
    finally:
        if worker.is_alive():
            worker.terminate()
        worker.join(timeout=3)
        if worker.is_alive():
            worker.kill()
            worker.join(timeout=1)
        parent_pipe.close()
    return result


def _fallback(stock: dict[str, Any], previous: dict[str, Any], field: str) -> None:
    previous_value = previous.get(field)
    previous_source = previous.get("field_sources", {}).get(field)
    if field == "currency":
        good = isinstance(previous_value, str) and bool(previous_value.strip())
    else:
        previous_value = finite_number(previous_value)
        good = previous_value is not None
    if good and previous_source in {"live", "previous"}:
        stock[field] = previous_value
        stock["field_sources"][field] = "previous"
        stock["field_updated_at"][field] = previous.get("field_updated_at", {}).get(field)


def merge_stock(seed: dict[str, Any], previous: dict[str, Any], payload: dict[str, Any], portfolio_id: str, stamp: str) -> dict[str, Any]:
    stock = {**seed, "field_sources": dict(seed["field_sources"]), "field_updated_at": dict(seed["field_updated_at"]), "warnings": list(payload.get("warnings", []))}
    info = payload.get("info") or {}
    technicals = payload.get("technicals") or {}
    quote_price = next((value for key in ("currentPrice", "regularMarketPrice") if (value := finite_number(info.get(key))) is not None and value > 0), None)
    history_price = finite_number(technicals.get("history_price"))
    price = quote_price or history_price
    is_etf = seed.get("asset_type") == "etf"
    dividend, dividend_warnings = (None, []) if is_etf else normalize_dividend_yield(info, price)
    stock["warnings"].extend(dividend_warnings)
    roe = finite_number(info.get("returnOnEquity"))
    values = dict(current_pe=finite_number(info.get("trailingPE")), forward_pe=finite_number(info.get("forwardPE")),
                  ps_ratio=finite_number(info.get("priceToSalesTrailing12Months")), div_yield=dividend,
                  roe=finite_number(roe * 100) if roe is not None else None, price=price,
                  sma200=finite_number(technicals.get("sma200")), rsi14=finite_number(technicals.get("rsi14")),
                  currency=info.get("currency") if isinstance(info.get("currency"), str) and info["currency"].strip() else None)
    fields = (*NUMERIC_FIELDS, "currency")
    if is_etf:
        # Funds have no company earnings forecast, ROE or sales valuation.
        for field in ("forward_pe", "ps_ratio", "roe", "div_yield"):
            stock[field] = values[field] = None
            stock["field_sources"][field] = "unavailable"
            stock["field_updated_at"][field] = None
        if values["current_pe"] is not None and values["current_pe"] <= 0:
            values["current_pe"] = None
        distributions = finite_number(technicals.get("distributions_per_share_ttm"))
        if distributions is not None and distributions < 0:
            distributions = None
        values["distributions_per_share_ttm"] = distributions
        values["distribution_yield_ttm"] = finite_number(distributions / price * 100) if distributions is not None and price and price > 0 else None
        fields = (*fields, *ETF_NUMERIC_FIELDS)
    for field in fields:
        if is_etf and field in {"forward_pe", "ps_ratio", "roe", "div_yield"}:
            continue
        value = values[field]
        if field in {"price", "sma200"} and value is not None and value <= 0:
            value = None
        if field in {"ps_ratio", "div_yield"} and value is not None and value < 0:
            value = None
        if field == "rsi14" and value is not None and not 0 <= value <= 100:
            value = None
        if value is None:
            _fallback(stock, previous, field)
        else:
            stock[field] = round(value, 6) if field != "currency" else value
            stock["field_sources"][field] = "live"
            history_field = field in {"sma200", "rsi14", *ETF_NUMERIC_FIELDS} or (field == "price" and quote_price is None)
            stock["field_updated_at"][field] = payload.get("history_retrieved_at" if history_field else "info_retrieved_at") or stamp
    if is_etf:
        if stock.get("current_pe") is not None and stock["current_pe"] <= 0:
            stock["current_pe"] = None
            stock["field_sources"]["current_pe"] = "unavailable"
            stock["field_updated_at"]["current_pe"] = None
        stock["distribution_as_of"] = technicals.get("distribution_as_of") if stock["field_sources"]["distribution_yield_ttm"] == "live" else previous.get("distribution_as_of")
    quote_epoch = finite_number(info.get("regularMarketTime"))
    if quote_price and quote_epoch:
        try:
            stock["as_of"] = datetime.fromtimestamp(quote_epoch, timezone.utc).isoformat(timespec="seconds")
        except (OverflowError, OSError, ValueError):
            stock["as_of"] = payload.get("info_retrieved_at") or stamp
    elif history_price is not None:
        stock["as_of"] = technicals.get("history_as_of")
    elif stock["field_sources"].get("price") == "previous":
        stock["as_of"] = previous.get("as_of")
    else:
        stock["as_of"] = None
    required = ["price", "sma200", "rsi14", "currency", "distribution_yield_ttm"] if is_etf else ["current_pe", "forward_pe", "price", "sma200", "rsi14", "currency", *( ["ps_ratio"] if portfolio_id == "tech" else ["div_yield", "roe"])]
    sources = [stock["field_sources"][field] for field in required]
    if all(source == "live" for source in sources):
        stock["source_status"] = "live"
    elif "live" in sources:
        stock["source_status"] = "partial"
    elif "previous" in sources:
        stock["source_status"] = "stale"
    else:
        stock["source_status"] = "initial"
    fallback_fields = [field for field in required if stock["field_sources"][field] != "live"]
    if fallback_fields:
        stock["warnings"].append("Nicht frisch verfügbar (Quelle je Feld gekennzeichnet): " + ", ".join(fallback_fields) + ".")
    if stock["field_sources"]["price"] != stock["field_sources"]["sma200"] and stock["price"] and stock["sma200"]:
        stock["warnings"].append("SMA-Abstand kombiniert unterschiedlich aktuelle Kurs-/SMA-Daten; Feldzeitpunkte beachten.")
    add_derived(stock, portfolio_id)
    return stock


def build_live_data(previous: dict[str, Any] | None = None,
                    fetcher: Callable[[str], dict[str, Any]] = fetch_ticker,
                    now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    result = build_seed_data(now)
    result["warnings"] = [REFERENCE_WARNING]
    result["last_attempted_at"] = iso_now(now)
    previous = previous or {}
    old_stocks = {stock["ticker"]: stock for group in previous.get("portfolios", []) for stock in group.get("stocks", []) if isinstance(stock, dict) and "ticker" in stock}
    for group in result["portfolios"]:
        for index, seed in enumerate(group["stocks"]):
            print(f"[{group['id']}] {seed['ticker']}: lade Daten …", flush=True)
            try:
                payload = fetcher(seed["ticker"])
                if not isinstance(payload, dict):
                    raise ValueError("Ungültige Abrufantwort")
                merged = merge_stock(seed, old_stocks.get(seed["ticker"], {}), payload, group["id"], result["last_attempted_at"])
            except Exception as exc:
                merged = merge_stock(seed, old_stocks.get(seed["ticker"], {}), dict(warnings=[f"Ticker-Fehler: {type(exc).__name__}: {str(exc)[:180]}"]), group["id"], result["last_attempted_at"])
            group["stocks"][index] = merged
            print(f"  {merged['source_status']}: {merged['tranche']}", flush=True)
    stocks = [stock for group in result["portfolios"] for stock in group["stocks"]]
    counts = Counter(stock["source_status"] for stock in stocks)
    result["status_counts"] = dict(counts)
    has_live = any("live" in stock["field_sources"].values() for stock in stocks)
    if counts["live"] == EXPECTED_STOCK_COUNT:
        result["data_mode"] = "live"
    elif has_live:
        result["data_mode"] = "partial"
        result["warnings"].append("Der Abruf ist teilweise erfolgreich; Fallbacks und fehlende Felder sind je Aktie ausgewiesen.")
    else:
        result["data_mode"] = "stale"
        result["warnings"].append("Keine neuen Yahoo-Finance-Daten; bisherige Werte bzw. Startwerte bleiben mit ursprünglicher Herkunft erhalten.")
        result["last_updated"] = previous.get("last_updated", result["last_updated"])
        result["updated_at"] = previous.get("updated_at", result["updated_at"])
    return result


def load_previous(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f"Ungültige JSON-Zahl: {value}")))
    if not isinstance(payload, dict) or payload.get("schema_version") != 1 or not isinstance(payload.get("portfolios"), list):
        raise ValueError("Vorhandene data.json hat kein unterstütztes Schema; Datei wird nicht überschrieben")
    return payload


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as handle:
            temporary_path = Path(handle.name)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", action="store_true", help="Nur die vorgegebenen Startwerte schreiben; kein Netzwerk")
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().with_name("data.json"), help="Ziel-JSON; standardmäßig neben diesem Skript")
    parser.add_argument("--ticker-timeout", type=float, default=45.0, help="Gesamtzeitlimit pro Ticker in Sekunden (max. 45)")
    parser.add_argument("--attempts", type=int, default=2, choices=(1, 2, 3), help="Abrufversuche je Endpoint")
    args = parser.parse_args(argv)
    if not 0 < args.ticker_timeout <= 45:
        parser.error("--ticker-timeout muss größer 0 und höchstens 45 sein")
    try:
        if args.seed:
            result = build_seed_data()
        else:
            # Fail early on missing dependencies, rather than failing per holding.
            import pandas  # noqa: F401
            import pytz  # noqa: F401
            import yfinance  # noqa: F401
            previous = load_previous(args.output)
            result = build_live_data(previous, fetcher=lambda ticker: fetch_ticker(ticker, args.ticker_timeout, args.attempts))
        atomic_write_json(args.output, result)
    except (OSError, ValueError, ImportError) as exc:
        print(f"Update fehlgeschlagen: {exc}", file=sys.stderr)
        return 1
    print(f"{EXPECTED_STOCK_COUNT} Werte → {args.output} | Modus: {result['data_mode']} | {result.get('status_counts', {'initial': EXPECTED_STOCK_COUNT})}")
    return 1 if result["data_mode"] == "stale" else 0


if __name__ == "__main__":
    mp.freeze_support()
    raise SystemExit(main())
