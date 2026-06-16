import asyncio, os, json, logging, aiohttp
from contextlib import asynccontextmanager
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("alphagrid_agent")

EST = ZoneInfo("America/New_York")

# ── BOT ENDPOINTS ─────────────────────────────────────────────────────────────
BOTS = {
    "allnight": {
        "name":     "All Night Bot (FVG)",
        "url":      os.getenv("ALLNIGHT_URL", "https://alphagrid-allnight-production.up.railway.app"),
        "emoji":    "🧠",
        "strategy": "FVG Sweep",
        "account":  "Builder",
    },
    "orb": {
        "name":     "ORB Bot",
        "url":      os.getenv("ORB_URL", "https://web-production-d58a3.up.railway.app"),
        "emoji":    "🏛️",
        "strategy": "Opening Range Breakout",
        "account":  "$25K Eval",
    },
    "killzone": {
        "name":     "Kill Zone Bot",
        "url":      os.getenv("KILLZONE_URL", "https://web-production-1759f.up.railway.app"),
        "emoji":    "⚡",
        "strategy": "ICT Levels",
        "account":  "Old Eval",
    },
}

# ── TELEGRAM ──────────────────────────────────────────────────────────────────
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID",   "")

async def send_telegram(text: str):
    if not TG_TOKEN or not TG_CHAT:
        logger.warning("Telegram not configured")
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json={"chat_id": TG_CHAT, "text": text,
                                    "parse_mode": "Markdown"},
                         timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logger.warning(f"Telegram error: {e}")

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

# ── ECONOMIC CALENDAR ─────────────────────────────────────────────────────────
# High impact news times ET — updated weekly manually or via API
# Format: {"date": "YYYY-MM-DD", "time": "HH:MM", "event": "name", "impact": "HIGH"}
KNOWN_NEWS: list[dict] = [
    # Add upcoming high-impact events here
    # {"date": "2026-06-16", "time": "08:30", "event": "Retail Sales", "impact": "HIGH"},
]

def get_todays_news() -> list[dict]:
    today = date.today().strftime("%Y-%m-%d")
    return [n for n in KNOWN_NEWS if n["date"] == today]

def news_blocks_trading(now: datetime) -> tuple[bool, str]:
    news = get_todays_news()
    for n in news:
        if n["impact"] != "HIGH":
            continue
        nh, nm = map(int, n["time"].split(":"))
        news_mins = nh * 60 + nm
        curr_mins = now.hour * 60 + now.minute
        if abs(curr_mins - news_mins) <= 30:
            return True, f"{n['event']} at {n['time']} ET"
    return False, ""

# ── MARKET REGIME ANALYZER ────────────────────────────────────────────────────
class RegimeAnalyzer:
    """
    Tracks daily price action to determine market regime.
    Fed by price updates from bots via /price-update.
    Determines: trending vs ranging, high vs low volatility.
    """
    def __init__(self):
        self.daily_highs: list[float] = []
        self.daily_lows:  list[float] = []
        self.daily_closes: list[float] = []
        self.current_day_high = None
        self.current_day_low  = None
        self.last_price       = None
        self.day_date         = date.today()
        self.adr_5day         = None   # 5-day Average Daily Range

    def update(self, price: float):
        today = date.today()
        if today != self.day_date:
            # New day — save yesterday
            if self.current_day_high and self.current_day_low:
                self.daily_highs.append(self.current_day_high)
                self.daily_lows.append(self.current_day_low)
                if self.last_price:
                    self.daily_closes.append(self.last_price)
                if len(self.daily_highs) > 20:
                    self.daily_highs  = self.daily_highs[-20:]
                    self.daily_lows   = self.daily_lows[-20:]
                    self.daily_closes = self.daily_closes[-20:]
                # Compute 5-day ADR
                if len(self.daily_highs) >= 5:
                    ranges = [h - l for h, l in zip(self.daily_highs[-5:], self.daily_lows[-5:])]
                    self.adr_5day = sum(ranges) / len(ranges)
            self.current_day_high = price
            self.current_day_low  = price
            self.day_date         = today
        else:
            if self.current_day_high is None or price > self.current_day_high:
                self.current_day_high = price
            if self.current_day_low is None or price < self.current_day_low:
                self.current_day_low = price
        self.last_price = price

    def current_day_range(self) -> float:
        if self.current_day_high and self.current_day_low:
            return self.current_day_high - self.current_day_low
        return 0.0

    def adr_pct_used(self) -> float:
        if self.adr_5day and self.adr_5day > 0:
            return self.current_day_range() / self.adr_5day * 100
        return 0.0

    def regime(self) -> str:
        if len(self.daily_closes) < 3:
            return "UNKNOWN"
        # Simple trend detection: 3 consecutive higher closes = uptrend
        c = self.daily_closes[-3:]
        if c[2] > c[1] > c[0]:
            return "UPTREND"
        if c[2] < c[1] < c[0]:
            return "DOWNTREND"
        return "RANGING"

    def volatility(self) -> str:
        if not self.adr_5day:
            return "UNKNOWN"
        pct = self.adr_pct_used()
        if pct > 80:
            return "HIGH — ADR nearly exhausted"
        if pct > 50:
            return "MODERATE"
        return "LOW — plenty of range left"

    def go_no_go(self) -> tuple[bool, str]:
        """Returns (go, reason)."""
        pct = self.adr_pct_used()
        if pct > 85:
            return False, f"ADR {pct:.0f}% used — over-extended, high reversal risk"
        reg = self.regime()
        if reg == "RANGING" and pct < 20:
            return True, "Ranging market, low ADR used — good sweep conditions"
        return True, f"{reg} | ADR {pct:.0f}% used"

    def status(self) -> dict:
        return {
            "regime":          self.regime(),
            "volatility":      self.volatility(),
            "adr_5day":        round(self.adr_5day, 2) if self.adr_5day else None,
            "day_range":       round(self.current_day_range(), 2),
            "adr_pct_used":    round(self.adr_pct_used(), 1),
            "current_high":    self.current_day_high,
            "current_low":     self.current_day_low,
            "days_of_data":    len(self.daily_highs),
        }

regime = RegimeAnalyzer()

# ── PERFORMANCE TRACKER ───────────────────────────────────────────────────────
class PerformanceTracker:
    """
    Tracks cross-bot performance to identify which strategy is winning.
    Shifts sizing recommendations toward the winner.
    """
    def __init__(self):
        self.bot_records: dict[str, list] = {k: [] for k in BOTS}
        self.daily_snapshots: list[dict]  = []

    def record_snapshot(self, bot_data: dict):
        today = date.today().isoformat()
        snap = {"date": today, "ts": datetime.now(EST).strftime("%H:%M ET")}
        for key, result in bot_data.items():
            if result["ok"]:
                d = result["data"]
                snap[key] = {
                    "total_pnl":   d.get("total_pnl", 0),
                    "day_pnl":     d.get("day_pnl", 0),
                    "win_rate":    d.get("win_rate", 0),
                    "total_trades": d.get("total_trades", 0),
                    "trading":     d.get("trading", False),
                }
            else:
                snap[key] = {"error": result["error"]}
        self.daily_snapshots.append(snap)
        if len(self.daily_snapshots) > 100:
            self.daily_snapshots = self.daily_snapshots[-100:]

    def best_bot(self) -> str:
        best = None
        best_wr = 0
        for key in ["allnight", "orb"]:
            snaps = [s[key] for s in self.daily_snapshots[-10:] if key in s and "win_rate" in s.get(key, {})]
            if snaps:
                avg_wr = sum(s["win_rate"] for s in snaps) / len(snaps)
                if avg_wr > best_wr:
                    best_wr = avg_wr
                    best = key
        return best or "insufficient data"

    def sizing_recommendation(self) -> str:
        best = self.best_bot()
        if best == "insufficient data":
            return "Equal sizing — insufficient data"
        bot_name = BOTS[best]["name"]
        return f"Favor {bot_name} — highest recent win rate"

perf = PerformanceTracker()

# ── MORNING INTELLIGENCE REPORT ───────────────────────────────────────────────
async def morning_report():
    """7:45 AM ET — Go/No-Go decision before market opens."""
    now       = datetime.now(EST)
    bot_data  = await fetch_all_bots()
    perf.record_snapshot(bot_data)
    news      = get_todays_news()
    blocked, news_reason = news_blocks_trading(now)
    go, regime_reason    = regime.go_no_go()
    rs = regime.status()

    # Overall Go/No-Go
    overall_go = go and not blocked
    verdict    = "🟢 GO — Trade today" if overall_go else "🔴 NO-GO — Skip today"
    if blocked:
        verdict += f"\n⚠️ News block: {news_reason}"
    if not go:
        verdict += f"\n⚠️ Market: {regime_reason}"

    # Bot status summary
    bot_lines = []
    for key, result in bot_data.items():
        b = BOTS[key]
        if result["ok"]:
            d = result["data"]
            trading = "✅ ARMED" if d.get("trading") else "🚫 PAUSED"
            pnl     = d.get("total_pnl", 0)
            wr      = d.get("win_rate", 0)
            trades  = d.get("total_trades", 0)
            bot_lines.append(
                f"{b['emoji']} *{b['name']}*: {trading} | P&L `${pnl:+.0f}` | WR `{wr:.0f}%` ({trades} trades)"
            )
        else:
            bot_lines.append(f"{b['emoji']} *{b['name']}*: ❌ OFFLINE — {result['error'][:50]}")

    # News
    news_str = "\n".join(f"  ⚠️ {n['time']} ET — {n['event']}" for n in news) if news else "  None today"

    text = (
        f"🤖 *AlphaGrid Intelligence Report*\n"
        f"{now.strftime('%A, %B %d, %Y — %I:%M %p ET')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"*{verdict}*\n\n"
        f"📊 *Market Regime*\n"
        f"  Trend: `{rs['regime']}` | Volatility: `{rs['volatility']}`\n"
        f"  ADR 5-day: `{rs['adr_5day'] or 'building...'}pts`\n"
        f"  Today's range: `{rs['day_range']:.1f}pts` (`{rs['adr_pct_used']:.0f}%` of ADR)\n\n"
        f"📰 *High Impact News*\n{news_str}\n\n"
        f"🤖 *Bot Status*\n" + "\n".join(bot_lines) + "\n\n"
        f"💡 *Sizing*: {perf.sizing_recommendation()}"
    )
    await send_telegram(text)
    logger.info("📬 Morning intelligence report sent")

# ── NIGHTLY PERFORMANCE REPORT ────────────────────────────────────────────────
async def nightly_report():
    """9:00 PM ET — End of day cross-bot analysis."""
    bot_data = await fetch_all_bots()
    perf.record_snapshot(bot_data)
    rs = regime.status()

    lines = [
        f"🌙 *AlphaGrid Nightly Report* — {date.today().strftime('%b %d')}",
        "━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"📊 *Market* — {rs['regime']} | ADR {rs['adr_pct_used']:.0f}% used",
        "",
        "*Bot Performance Today:*",
    ]

    total_day_pnl = 0
    for key, result in bot_data.items():
        b = BOTS[key]
        if result["ok"]:
            d = result["data"]
            day_pnl = d.get("day_pnl", 0)
            total_pnl = d.get("total_pnl", 0)
            wr = d.get("win_rate", 0)
            trades = d.get("total_trades", 0)
            emoji = "🟢" if day_pnl > 0 else "🔴" if day_pnl < 0 else "⚪"
            total_day_pnl += day_pnl
            lines.append(
                f"{emoji} {b['emoji']} {b['name']}: `${day_pnl:+.0f}` today | "
                f"`${total_pnl:+.0f}` total | `{wr:.0f}%` WR ({trades} trades)"
            )
        else:
            lines.append(f"❌ {b['emoji']} {b['name']}: OFFLINE")

    day_emoji = "🟢" if total_day_pnl > 0 else "🔴" if total_day_pnl < 0 else "⚪"
    lines += [
        "",
        f"{day_emoji} *Combined Day P&L: `${total_day_pnl:+.0f}`*",
        "",
        f"💡 *Best strategy*: {perf.best_bot().replace('allnight','All Night FVG').replace('orb','ORB').replace('insufficient data','Building data...')}",
        f"📈 *Sizing rec*: {perf.sizing_recommendation()}",
        "",
        f"🔄 Tomorrow: Morning report at 7:45 AM ET",
    ]

    await send_telegram("\n".join(lines))
    logger.info("📬 Nightly report sent")

# ── HOURLY HEALTH CHECK ───────────────────────────────────────────────────────
async def hourly_health_check():
    """Check all bots every hour during trading hours. Alert if any go offline."""
    bot_data = await fetch_all_bots()
    offline  = [k for k, r in bot_data.items() if not r["ok"]]
    if offline:
        names = ", ".join(BOTS[k]["name"] for k in offline)
        await send_telegram(
            f"🚨 *BOT OFFLINE ALERT*\n"
            f"{names} not responding!\n"
            f"Check Railway immediately."
        )
        logger.warning(f"Offline bots: {offline}")

# ── SCHEDULER ─────────────────────────────────────────────────────────────────
async def scheduler():
    sent_morning = False
    sent_nightly = False
    last_health  = None
    last_date    = date.today()

    while True:
        await asyncio.sleep(30)
        now   = datetime.now(EST)
        today = now.date()
        h, m  = now.hour, now.minute

        if today != last_date:
            sent_morning = False
            sent_nightly = False
            last_date    = today

        # Morning report — 7:45 AM ET
        if h == 7 and m == 45 and not sent_morning:
            sent_morning = True
            await morning_report()

        # Nightly report — 9:00 PM ET
        if h == 21 and m == 0 and not sent_nightly:
            sent_nightly = True
            await nightly_report()

        # Hourly health check during trading hours (7 AM – 5 PM ET)
        if 7 <= h <= 17:
            curr_hour = (h, m // 30)   # check every 30 mins
            if curr_hour != last_health:
                last_health = curr_hour
                await hourly_health_check()

# ── LIFESPAN ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(scheduler())
    logger.info("🤖 AlphaGrid Intelligence Agent — Phase 1 online")
    logger.info("Monitoring: All Night Bot | ORB Bot | Kill Zone Bot")
    logger.info("Reports: 7:45 AM ET morning | 9:00 PM ET nightly | 30-min health checks")
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status":  "ok",
        "agent":   "AlphaGrid Intelligence Agent v1",
        "regime":  regime.status(),
        "bots":    list(BOTS.keys()),
        "time_et": datetime.now(EST).strftime("%Y-%m-%d %H:%M ET"),
    }

@app.get("/status")
async def full_status():
    """Full cross-bot status snapshot."""
    bot_data = await fetch_all_bots()
    perf.record_snapshot(bot_data)
    blocked, news_reason = news_blocks_trading(datetime.now(EST))
    go, regime_reason    = regime.go_no_go()
    return {
        "time_et":     datetime.now(EST).strftime("%Y-%m-%d %H:%M ET"),
        "go_no_go":    {"go": go and not blocked, "reason": news_reason or regime_reason},
        "regime":      regime.status(),
        "news_block":  {"blocked": blocked, "reason": news_reason},
        "bots":        {k: v for k, v in bot_data.items()},
        "sizing":      perf.sizing_recommendation(),
        "best_bot":    perf.best_bot(),
    }

@app.post("/report/morning")
async def trigger_morning():
    await morning_report()
    return {"ok": True}

@app.post("/report/nightly")
async def trigger_nightly():
    await nightly_report()
    return {"ok": True}

@app.post("/report/health")
async def trigger_health():
    await hourly_health_check()
    return {"ok": True}

@app.post("/news")
async def add_news(event: dict):
    """Add a high-impact news event. Format: {date, time, event, impact}"""
    KNOWN_NEWS.append(event)
    return {"ok": True, "news": KNOWN_NEWS}

@app.get("/news")
async def get_news():
    return {"news": KNOWN_NEWS, "today": get_todays_news()}

@app.post("/price-update")
async def price_update(req: dict):
    """Receive price updates to track regime."""
    price = float(req.get("price", 0))
    if price > 0:
        regime.update(price)
    return {"ok": True}
