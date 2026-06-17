"""
AlphaGrid Intelligence Agent v2.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fully autonomous market intelligence, bot monitoring, and
pre-trade decision engine. No manual inputs required.

Capabilities:
- Auto-fetches 20 days of ES/NQ daily OHLC from Yahoo Finance on startup
- Refreshes market data daily at 6:00 PM ET (after close)
- Tracks ES/NQ correlation and divergence in real time
- Monitors VIX for volatility regime
- Auto-fetches high-impact economic news from FMP free API
- Tracks kill switch proximity and drawdown warnings per bot
- Detects degraded bot states (not just online/offline)
- Fires pre-market session summary at 7:45 AM ET
- Fires nightly performance + regime report at 9:00 PM ET
- 30-min health checks during trading hours
- Weekend-aware (no reports on Saturday/Sunday)
- All state survives restarts via yfinance auto-seed
"""

import asyncio, os, json, logging, aiohttp
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("alphagrid_agent")

EST = ZoneInfo("America/New_York")

# ── BOT REGISTRY ──────────────────────────────────────────────────────────────
BOTS = {
    "allnight": {
        "name":        "All Night Bot (FVG)",
        "url":         os.getenv("ALLNIGHT_URL", "https://alphagrid-allnight-production.up.railway.app"),
        "emoji":       "🧠",
        "strategy":    "FVG Sweep",
        "account":     "Builder $50K",
        "max_dd":      2000.0,
        "daily_cap":   1000.0,
    },
    "orb": {
        "name":        "ORB Bot",
        "url":         os.getenv("ORB_URL", "https://web-production-d58a3.up.railway.app"),
        "emoji":       "🏛️",
        "strategy":    "Opening Range Breakout",
        "account":     "$25K Eval",
        "max_dd":      1500.0,
        "daily_cap":   750.0,
    },
    "killzone": {
        "name":        "Kill Zone Bot",
        "url":         os.getenv("KILLZONE_URL", "https://web-production-1759f.up.railway.app"),
        "emoji":       "⚡",
        "strategy":    "ICT Kill Zone",
        "account":     "Old Eval",
        "max_dd":      1500.0,
        "daily_cap":   750.0,
    },
}

# ── TELEGRAM ──────────────────────────────────────────────────────────────────
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")

async def send_telegram(text: str):
    if not TG_TOKEN or not TG_CHAT:
        logger.warning("Telegram not configured")
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json={
                "chat_id":    TG_CHAT,
                "text":       text,
                "parse_mode": "Markdown"
            }, timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logger.warning(f"Telegram error: {e}")

# ── MARKET DATA ENGINE ────────────────────────────────────────────────────────
class MarketDataEngine:
    """
    Fetches and maintains ES and NQ daily OHLC history via yfinance.
    Also tracks VIX. Refreshes daily after market close.
    Provides: ADR, regime, volatility, ES/NQ correlation, divergence signals.
    """

    def __init__(self):
        self.es_history: list[dict] = []   # [{date, high, low, close, range}]
        self.nq_history: list[dict] = []
        self.vix_history: list[dict] = []

        # Intraday tracking
        self.es_day_high: float | None = None
        self.es_day_low:  float | None = None
        self.nq_day_high: float | None = None
        self.nq_day_low:  float | None = None
        self.es_last:     float | None = None
        self.nq_last:     float | None = None
        self.vix_last:    float | None = None

        self.last_fetch_date: date | None = None
        self.data_ready = False

    async def fetch_history(self):
        """Pull 20 days of ES, NQ, VIX daily OHLC from Yahoo Finance."""
        try:
            import yfinance as yf
            tickers = yf.download(
                ["ES=F", "NQ=F", "^VIX"],
                period="30d",
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )

            def extract(symbol):
                try:
                    df = tickers[symbol][["High", "Low", "Close"]].dropna()
                    df = df.tail(20)
                    return [
                        {
                            "date":  str(idx.date()),
                            "high":  round(float(row["High"]), 2),
                            "low":   round(float(row["Low"]), 2),
                            "close": round(float(row["Close"]), 2),
                            "range": round(float(row["High"]) - float(row["Low"]), 2),
                        }
                        for idx, row in df.iterrows()
                    ]
                except Exception as e:
                    logger.warning(f"Extract error {symbol}: {e}")
                    return []

            self.es_history  = extract("ES=F")
            self.nq_history  = extract("NQ=F")
            self.vix_history = extract("^VIX")

            if self.es_history:
                self.data_ready       = True
                self.last_fetch_date  = date.today()
                logger.info(f"✅ Market data loaded: {len(self.es_history)} ES days, "
                            f"{len(self.nq_history)} NQ days, {len(self.vix_history)} VIX days")
            else:
                logger.warning("⚠️ yfinance returned empty ES history")

        except ImportError:
            logger.error("yfinance not installed — run: pip install yfinance")
        except Exception as e:
            logger.error(f"Market data fetch failed: {e}")

    def update_price(self, symbol: str, price: float):
        """Called by /price-update to track intraday range."""
        today = date.today()
        if symbol in ("ES", "MES"):
            self.es_last = price
            if self.es_day_high is None or price > self.es_day_high:
                self.es_day_high = price
            if self.es_day_low is None or price < self.es_day_low:
                self.es_day_low = price
        elif symbol in ("NQ", "MNQ"):
            self.nq_last = price
            if self.nq_day_high is None or price > self.nq_day_high:
                self.nq_day_high = price
            if self.nq_day_low is None or price < self.nq_day_low:
                self.nq_day_low = price

    def reset_intraday(self):
        self.es_day_high = self.es_day_low = None
        self.nq_day_high = self.nq_day_low = None

    # ── ADR ───────────────────────────────────────────────────────────────────
    def adr(self, symbol="ES", days=20) -> float | None:
        hist = self.es_history if symbol == "ES" else self.nq_history
        if len(hist) < days:
            days = len(hist)
        if days < 3:
            return None
        ranges = [d["range"] for d in hist[-days:]]
        return round(sum(ranges) / len(ranges), 2)

    def adr_pct_used(self, symbol="ES") -> float:
        adr = self.adr(symbol)
        if not adr:
            return 0.0
        if symbol == "ES":
            rng = (self.es_day_high or 0) - (self.es_day_low or 0)
        else:
            rng = (self.nq_day_high or 0) - (self.nq_day_low or 0)
        return round(rng / adr * 100, 1) if adr else 0.0

    # ── REGIME ────────────────────────────────────────────────────────────────
    def regime(self, symbol="ES") -> str:
        hist = self.es_history if symbol == "ES" else self.nq_history
        if len(hist) < 5:
            return "UNKNOWN"
        closes = [d["close"] for d in hist[-10:]]
        # EMA-based trend: compare 5-day vs 10-day close average
        ema5  = sum(closes[-5:]) / 5
        ema10 = sum(closes) / len(closes)
        latest = closes[-1]
        prev   = closes[-2]
        if ema5 > ema10 * 1.002 and latest > prev:
            return "UPTREND"
        if ema5 < ema10 * 0.998 and latest < prev:
            return "DOWNTREND"
        return "RANGING"

    # ── VIX ───────────────────────────────────────────────────────────────────
    def vix_level(self) -> float | None:
        if self.vix_history:
            return self.vix_history[-1]["close"]
        return None

    def vix_regime(self) -> str:
        vix = self.vix_level()
        if vix is None:
            return "UNKNOWN"
        if vix > 30:
            return f"EXTREME ({vix:.1f}) — reduce size"
        if vix > 20:
            return f"ELEVATED ({vix:.1f}) — caution"
        if vix > 15:
            return f"NORMAL ({vix:.1f})"
        return f"LOW ({vix:.1f}) — trending conditions"

    # ── ES/NQ DIVERGENCE ──────────────────────────────────────────────────────
    def divergence_signal(self) -> tuple[bool, str]:
        """
        Detects ES vs NQ divergence — a key ICT confluence signal.
        If ES makes new high but NQ doesn't (or vice versa), flag it.
        """
        if len(self.es_history) < 3 or len(self.nq_history) < 3:
            return False, "insufficient data"

        es_closes = [d["close"] for d in self.es_history[-3:]]
        nq_closes = [d["close"] for d in self.nq_history[-3:]]

        es_up = es_closes[-1] > es_closes[-2] > es_closes[-3]
        es_dn = es_closes[-1] < es_closes[-2] < es_closes[-3]
        nq_up = nq_closes[-1] > nq_closes[-2] > nq_closes[-3]
        nq_dn = nq_closes[-1] < nq_closes[-2] < nq_closes[-3]

        if es_up and nq_dn:
            return True, "⚠️ DIVERGENCE: ES trending up, NQ trending down — proceed with caution"
        if es_dn and nq_up:
            return True, "⚠️ DIVERGENCE: NQ trending up, ES trending down — proceed with caution"
        return False, "ES/NQ correlated — no divergence"

    # ── GO/NO-GO ──────────────────────────────────────────────────────────────
    def go_no_go(self) -> tuple[bool, str]:
        vix = self.vix_level()
        if vix and vix > 35:
            return False, f"VIX {vix:.1f} — extreme volatility, skip today"
        pct = self.adr_pct_used("ES")
        if pct > 85:
            return False, f"ES ADR {pct:.0f}% used — over-extended, high reversal risk"
        div, div_msg = self.divergence_signal()
        reg = self.regime()
        return True, f"{reg} | ADR {pct:.0f}% used | {div_msg}"

    # ── STATUS DICT ───────────────────────────────────────────────────────────
    def status(self) -> dict:
        div, div_msg = self.divergence_signal()
        return {
            "es": {
                "regime":       self.regime("ES"),
                "adr_20day":    self.adr("ES", 20),
                "adr_5day":     self.adr("ES", 5),
                "adr_pct_used": self.adr_pct_used("ES"),
                "day_high":     self.es_day_high,
                "day_low":      self.es_day_low,
                "last_price":   self.es_last,
                "day_range":    round((self.es_day_high or 0) - (self.es_day_low or 0), 2),
            },
            "nq": {
                "regime":       self.regime("NQ"),
                "adr_20day":    self.adr("NQ", 20),
                "adr_5day":     self.adr("NQ", 5),
                "adr_pct_used": self.adr_pct_used("NQ"),
                "day_high":     self.nq_day_high,
                "day_low":      self.nq_day_low,
                "last_price":   self.nq_last,
                "day_range":    round((self.nq_day_high or 0) - (self.nq_day_low or 0), 2),
            },
            "vix":             self.vix_regime(),
            "divergence":      {"detected": div, "message": div_msg},
            "data_ready":      self.data_ready,
            "last_fetch_date": str(self.last_fetch_date) if self.last_fetch_date else None,
            "days_of_data":    len(self.es_history),
        }

market = MarketDataEngine()

# ── ECONOMIC NEWS ENGINE ──────────────────────────────────────────────────────
class NewsEngine:
    """
    Auto-fetches high-impact economic calendar from FMP (Financial Modeling Prep).
    Free tier supports economic calendar endpoint.
    Falls back to manual KNOWN_NEWS list if API unavailable.
    Refreshes daily at midnight ET.
    """

    FMP_KEY      = os.getenv("FMP_API_KEY", "UdHGYzs1jkVqoztL4azqVtqcPxMbImBM")
    FMP_BASE_URL = "https://financialmodelingprep.com/stable"

    def __init__(self):
        self.events: list[dict] = []
        self.last_fetch: date | None = None
        # Manual fallback list
        self.manual_events: list[dict] = []

    async def fetch_calendar(self):
        """Fetch next 7 days of high-impact events from FMP stable API."""
        try:
            today = date.today()
            end   = today + timedelta(days=7)
            url   = (
                f"{self.FMP_BASE_URL}/economic-calendar"
                f"?from={today}&to={end}&apikey={self.FMP_KEY}"
            )
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        data = await r.json()
                        if isinstance(data, list):
                            self.events = [
                                e for e in data
                                if e.get("impact") in ("High", "HIGH", "high")
                            ]
                            self.last_fetch = today
                            logger.info(f"📰 Fetched {len(self.events)} high-impact events (next 7 days)")
                        else:
                            logger.warning(f"FMP unexpected response: {str(data)[:100]}")
                    else:
                        logger.warning(f"FMP calendar HTTP {r.status}")
        except Exception as e:
            logger.warning(f"News fetch error: {e}")

    def todays_events(self) -> list[dict]:
        today = date.today().strftime("%Y-%m-%d")
        auto  = [e for e in self.events if e.get("date", "").startswith(today)]
        manual = [e for e in self.manual_events if e.get("date") == today]
        return auto + manual

    def blocks_trading(self, now: datetime) -> tuple[bool, str]:
        for e in self.todays_events():
            try:
                t = e.get("date", e.get("time", ""))
                # Parse time from event
                if "T" in t:
                    et_str = t.split("T")[1][:5]
                else:
                    et_str = e.get("time", "")
                if not et_str:
                    continue
                nh, nm = map(int, et_str.split(":")[:2])
                news_mins = nh * 60 + nm
                curr_mins = now.hour * 60 + now.minute
                if abs(curr_mins - news_mins) <= 30:
                    name = e.get("event", e.get("name", "Unknown"))
                    return True, f"{name} at {et_str} ET"
            except Exception:
                continue
        return False, ""

    def add_manual(self, event: dict):
        self.manual_events.append(event)

news_engine = NewsEngine()

# ── BOT HEALTH FETCHER ────────────────────────────────────────────────────────
async def fetch_bot(key: str) -> dict:
    bot = BOTS[key]
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{bot['url']}/health",
                             timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json()
                    return {"key": key, "ok": True, "data": data}
                return {"key": key, "ok": False, "error": f"HTTP {r.status}"}
    except Exception as e:
        return {"key": key, "ok": False, "error": str(e)}

async def fetch_all_bots() -> dict:
    results = await asyncio.gather(*[fetch_bot(k) for k in BOTS])
    return {r["key"]: r for r in results}

# ── BOT INTELLIGENCE ANALYZER ─────────────────────────────────────────────────
def analyze_bot(key: str, result: dict) -> dict:
    """
    Deep analysis of bot health beyond online/offline.
    Detects: drawdown proximity, kill switch status, price feed health,
    stale data, degraded state.
    """
    bot = BOTS[key]
    analysis = {
        "key":         key,
        "name":        bot["name"],
        "emoji":       bot["emoji"],
        "online":      result["ok"],
        "state":       "OFFLINE",
        "warnings":    [],
        "summary":     "",
    }

    if not result["ok"]:
        analysis["summary"] = f"❌ OFFLINE — {result['error'][:60]}"
        return analysis

    d = result["data"]
    warnings = []

    # Trading state
    trading  = d.get("trading", False)
    analysis["state"] = "ARMED" if trading else "PAUSED"

    # P&L
    total_pnl = d.get("total_pnl", 0)
    day_pnl   = d.get("day_pnl", 0)
    win_rate  = d.get("win_rate", 0)
    trades    = d.get("total_trades", 0)

    # Drawdown proximity warning
    dd_remaining = d.get("drawdown_remaining", None)
    if dd_remaining is not None:
        max_dd = bot["max_dd"]
        dd_used_pct = (max_dd - dd_remaining) / max_dd * 100
        if dd_used_pct >= 75:
            warnings.append(f"🚨 DD {dd_used_pct:.0f}% used — {dd_remaining:.0f} remaining")
        elif dd_used_pct >= 50:
            warnings.append(f"⚠️ DD {dd_used_pct:.0f}% used — monitor closely")

    # Daily loss proximity
    day_loss_remaining = d.get("day_loss_remaining", None)
    if day_loss_remaining is not None and day_loss_remaining < 0:
        daily_cap = bot["daily_cap"]
        loss_used_pct = abs(day_loss_remaining) / daily_cap * 100 if daily_cap else 0
        if loss_used_pct >= 75:
            warnings.append(f"⚠️ Daily loss {loss_used_pct:.0f}% of cap used")

    # Kill switch status
    ks = d.get("kill_switches", {})
    if ks:
        active_ks = [k for k, v in ks.items() if v]
        if active_ks:
            warnings.append(f"🔴 Kill switches active: {', '.join(active_ks)}")

    # Price feed health — check last price
    prices = d.get("prices", {})
    stale_feeds = [sym for sym, px in prices.items() if px == 0.0]
    if stale_feeds:
        warnings.append(f"⚠️ No price data: {', '.join(stale_feeds)}")

    # Consistency rule risk (no single day > 50% of total profits)
    if total_pnl > 0 and day_pnl > 0:
        consistency_pct = day_pnl / total_pnl * 100
        if consistency_pct > 40:
            warnings.append(f"⚠️ Consistency risk: today is {consistency_pct:.0f}% of total P&L")

    analysis["warnings"]  = warnings
    analysis["total_pnl"] = total_pnl
    analysis["day_pnl"]   = day_pnl
    analysis["win_rate"]  = win_rate
    analysis["trades"]    = trades
    analysis["trading"]   = trading

    status_icon = "✅" if trading else "🚫"
    pnl_icon    = "🟢" if day_pnl > 0 else "🔴" if day_pnl < 0 else "⚪"
    analysis["summary"] = (
        f"{status_icon} {bot['emoji']} *{bot['name']}*: "
        f"{pnl_icon} `${day_pnl:+.0f}` today | `${total_pnl:+.0f}` total | "
        f"`{win_rate:.0f}%` WR ({trades} trades)"
    )

    return analysis

# ── PERFORMANCE TRACKER ───────────────────────────────────────────────────────
class PerformanceTracker:
    def __init__(self):
        self.snapshots: list[dict] = []

    def record(self, analyses: list[dict]):
        snap = {
            "date": date.today().isoformat(),
            "ts":   datetime.now(EST).strftime("%H:%M ET"),
            "bots": {a["key"]: {
                "day_pnl":   a.get("day_pnl", 0),
                "total_pnl": a.get("total_pnl", 0),
                "win_rate":  a.get("win_rate", 0),
                "trades":    a.get("trades", 0),
            } for a in analyses if a["online"]},
        }
        self.snapshots.append(snap)
        if len(self.snapshots) > 200:
            self.snapshots = self.snapshots[-200:]

    def best_bot(self) -> str:
        scores = {}
        for key in ["allnight", "orb"]:
            recent = [
                s["bots"][key]
                for s in self.snapshots[-20:]
                if key in s.get("bots", {}) and s["bots"][key].get("trades", 0) > 0
            ]
            if recent:
                avg_wr  = sum(r["win_rate"] for r in recent) / len(recent)
                avg_pnl = sum(r["day_pnl"]  for r in recent) / len(recent)
                scores[key] = avg_wr * 0.6 + (avg_pnl / 10) * 0.4
        if not scores:
            return "insufficient data"
        return max(scores, key=scores.get)

    def sizing_rec(self) -> str:
        best = self.best_bot()
        if best == "insufficient data":
            return "Equal sizing — building performance history"
        return f"Favor {BOTS[best]['name']} — highest blended score (WR + P&L)"

perf = PerformanceTracker()

# ── MORNING INTELLIGENCE REPORT ───────────────────────────────────────────────
async def morning_report():
    """7:45 AM ET — Full pre-market intelligence brief."""
    now = datetime.now(EST)

    # Skip weekends
    if now.weekday() >= 5:
        return

    bot_data  = await fetch_all_bots()
    analyses  = [analyze_bot(k, bot_data[k]) for k in BOTS]
    perf.record(analyses)

    blocked, news_reason = news_engine.blocks_trading(now)
    go, regime_reason    = market.go_no_go()
    ms = market.status()

    overall_go = go and not blocked
    verdict    = "🟢 *GO — Trade today*" if overall_go else "🔴 *NO-GO — Skip today*"
    if blocked:
        verdict += f"\n⚠️ News block: {news_reason}"
    if not go:
        verdict += f"\n⚠️ {regime_reason}"

    # Market section
    es = ms["es"]
    nq = ms["nq"]
    market_block = (
        f"📊 *Market Intelligence*\n"
        f"  ES: `{es['regime']}` | ADR20 `{es['adr_20day'] or '—'}pts` | "
        f"ADR5 `{es['adr_5day'] or '—'}pts` | Today `{es['adr_pct_used']:.0f}%` used\n"
        f"  NQ: `{nq['regime']}` | ADR20 `{nq['adr_20day'] or '—'}pts` | "
        f"ADR5 `{nq['adr_5day'] or '—'}pts` | Today `{nq['adr_pct_used']:.0f}%` used\n"
        f"  VIX: `{ms['vix']}`\n"
        f"  {ms['divergence']['message']}"
    )

    # News section
    todays_news = news_engine.todays_events()
    if todays_news:
        news_lines = []
        for e in todays_news:
            name = e.get("event", e.get("name", "Unknown"))
            t    = e.get("time", e.get("date", ""))
            news_lines.append(f"  ⚠️ {t} — {name}")
        news_block = "📰 *High Impact News*\n" + "\n".join(news_lines)
    else:
        news_block = "📰 *High Impact News*: None today ✅"

    # Bot section
    bot_lines = []
    all_warnings = []
    for a in analyses:
        bot_lines.append(a["summary"])
        for w in a.get("warnings", []):
            all_warnings.append(f"  {w} ({a['name']})")

    warning_block = ""
    if all_warnings:
        warning_block = "\n\n🚨 *Active Warnings*\n" + "\n".join(all_warnings)

    text = (
        f"🤖 *AlphaGrid Intelligence Report*\n"
        f"{now.strftime('%A, %B %d — %I:%M %p ET')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{verdict}\n\n"
        f"{market_block}\n\n"
        f"{news_block}\n\n"
        f"🤖 *Bot Status*\n" + "\n".join(bot_lines) +
        warning_block + "\n\n"
        f"💡 *Sizing*: {perf.sizing_rec()}"
    )

    await send_telegram(text)
    logger.info("📬 Morning intelligence report sent")

# ── NIGHTLY PERFORMANCE REPORT ────────────────────────────────────────────────
async def nightly_report():
    """9:00 PM ET — End of day summary."""
    now = datetime.now(EST)
    if now.weekday() >= 5:
        return

    bot_data = await fetch_all_bots()
    analyses = [analyze_bot(k, bot_data[k]) for k in BOTS]
    perf.record(analyses)
    ms = market.status()
    es = ms["es"]

    total_day_pnl = sum(a.get("day_pnl", 0) for a in analyses if a["online"])
    day_emoji = "🟢" if total_day_pnl > 0 else "🔴" if total_day_pnl < 0 else "⚪"

    bot_lines = [a["summary"] for a in analyses]

    # Warnings summary
    all_warnings = []
    for a in analyses:
        for w in a.get("warnings", []):
            all_warnings.append(f"  {w} ({a['name']})")

    warning_block = ("\n\n🚨 *Warnings*\n" + "\n".join(all_warnings)) if all_warnings else ""

    best = perf.best_bot()
    best_name = (
        BOTS[best]["name"] if best in BOTS
        else "Building data..."
    )

    text = "\n".join([
        f"🌙 *AlphaGrid Nightly Report* — {date.today().strftime('%b %d')}",
        "━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"📊 *Market Close*",
        f"  ES: `{es['regime']}` | ADR20 `{es['adr_20day'] or '—'}pts` | "
        f"Range today `{es['day_range']:.1f}pts` (`{es['adr_pct_used']:.0f}%` of ADR)",
        f"  VIX: `{ms['vix']}`",
        f"  {ms['divergence']['message']}",
        "",
        "*Bot Performance Today:*",
        *bot_lines,
        warning_block,
        "",
        f"{day_emoji} *Combined Day P&L: `${total_day_pnl:+.0f}`*",
        "",
        f"💡 *Best strategy*: {best_name}",
        f"📈 *Sizing rec*: {perf.sizing_rec()}",
        "",
        f"🔄 Tomorrow: Morning report at 7:45 AM ET",
    ])

    await send_telegram(text)

    # Refresh market data after close
    await market.fetch_history()
    logger.info("📬 Nightly report sent + market data refreshed")

# ── HEALTH CHECK ──────────────────────────────────────────────────────────────
async def health_check():
    """Every 30 mins during trading hours — deep bot analysis."""
    now = datetime.now(EST)
    if now.weekday() >= 5:
        return

    bot_data = await fetch_all_bots()
    analyses = [analyze_bot(k, bot_data[k]) for k in BOTS]

    # Alert on offline bots
    offline = [a for a in analyses if not a["online"]]
    if offline:
        names = ", ".join(a["name"] for a in offline)
        await send_telegram(
            f"🚨 *BOT OFFLINE ALERT*\n"
            f"{names} not responding!\n"
            f"Check Railway immediately."
        )

    # Alert on critical warnings
    for a in analyses:
        critical = [w for w in a.get("warnings", []) if "🚨" in w]
        if critical:
            await send_telegram(
                f"🚨 *CRITICAL WARNING — {a['name']}*\n" +
                "\n".join(critical)
            )

# ── SCHEDULER ─────────────────────────────────────────────────────────────────
async def scheduler():
    sent_morning  = False
    sent_nightly  = False
    last_health   = None
    last_date     = date.today()
    last_news_fetch = None

    while True:
        await asyncio.sleep(30)
        now   = datetime.now(EST)
        today = now.date()
        h, m  = now.hour, now.minute

        # Day rollover
        if today != last_date:
            sent_morning = False
            sent_nightly = False
            market.reset_intraday()
            last_date = today

        # Auto-fetch news daily at midnight ET
        if last_news_fetch != today:
            await news_engine.fetch_calendar()
            last_news_fetch = today

        # Morning report — 7:45 AM ET (weekdays only)
        if h == 7 and m == 45 and not sent_morning:
            sent_morning = True
            await morning_report()

        # Nightly report — 9:00 PM ET (weekdays only)
        if h == 21 and m == 0 and not sent_nightly:
            sent_nightly = True
            await nightly_report()

        # Health checks every 30 mins, 7 AM – 5 PM ET weekdays
        if 7 <= h <= 17 and now.weekday() < 5:
            slot = (h, m // 30)
            if slot != last_health:
                last_health = slot
                await health_check()

# ── LIFESPAN ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🤖 AlphaGrid Intelligence Agent v2.0 starting...")

    # Auto-seed market data on every startup
    await market.fetch_history()

    # Fetch today's news
    await news_engine.fetch_calendar()

    task = asyncio.create_task(scheduler())
    logger.info("✅ Agent online — monitoring all systems")
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status":   "ok",
        "agent":    "AlphaGrid Intelligence Agent v2.0",
        "market":   market.status(),
        "bots":     list(BOTS.keys()),
        "time_et":  datetime.now(EST).strftime("%Y-%m-%d %H:%M ET"),
    }

@app.get("/status")
async def full_status():
    bot_data = await fetch_all_bots()
    analyses = [analyze_bot(k, bot_data[k]) for k in BOTS]
    perf.record(analyses)
    blocked, news_reason = news_engine.blocks_trading(datetime.now(EST))
    go, reason           = market.go_no_go()
    return {
        "time_et":    datetime.now(EST).strftime("%Y-%m-%d %H:%M ET"),
        "go_no_go":   {"go": go and not blocked, "reason": news_reason or reason},
        "market":     market.status(),
        "news_block": {"blocked": blocked, "reason": news_reason},
        "bots":       {a["key"]: a for a in analyses},
        "sizing":     perf.sizing_rec(),
        "best_bot":   perf.best_bot(),
    }

@app.post("/price-update")
async def price_update(req: dict):
    """
    Receive price ticks to track intraday regime.
    Accepts both bot format: {"ticker": "MES1!", "price": 5000.0}
    and direct format:       {"symbol": "ES",   "price": 5000.0}
    """
    price = float(req.get("price", 0))
    if price <= 0:
        return {"ok": False, "reason": "invalid price"}

    # Accept TradingView/bot ticker format or direct symbol
    raw = req.get("ticker", req.get("symbol", "ES"))
    symbol = raw.upper().replace("1!", "").replace("!", "")

    market.update_price(symbol, price)
    return {"ok": True, "symbol": symbol, "price": price}

@app.post("/report/morning")
async def trigger_morning():
    await morning_report()
    return {"ok": True}

@app.get("/report/morning")
async def trigger_morning_get():
    await morning_report()
    return {"ok": True}

@app.post("/report/nightly")
async def trigger_nightly():
    await nightly_report()
    return {"ok": True}

@app.get("/report/nightly")
async def trigger_nightly_get():
    await nightly_report()
    return {"ok": True}

@app.post("/report/health")
async def trigger_health():
    await health_check()
    return {"ok": True}

@app.post("/refresh-market")
async def refresh_market():
    """Manually trigger a market data refresh from Yahoo Finance."""
    await market.fetch_history()
    return {"ok": True, "market": market.status()}

@app.get("/refresh-market")
async def refresh_market_get():
    await market.fetch_history()
    return {"ok": True, "market": market.status()}

@app.post("/news")
async def add_news(event: dict):
    """Manually add a high-impact news event: {date, time, event, impact}"""
    news_engine.add_manual(event)
    return {"ok": True}

@app.get("/news")
async def get_news():
    return {
        "today":       news_engine.todays_events(),
        "all_auto":    news_engine.events,
        "all_manual":  news_engine.manual_events,
        "last_fetch":  str(news_engine.last_fetch),
    }

@app.get("/market")
async def get_market():
    return market.status()
