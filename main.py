import asyncio, json, os, time
from datetime import date, datetime, timedelta, timezone
from collections import Counter
import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

HKT = timezone(timedelta(hours=8))
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

CACHE_FILE = "cache.json"
CACHE_TTL  = 6 * 3600
HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://bet.hkjc.com/",
}
# 官方 last30draw.json（一次拿 30 期）
MS_LAST30 = "https://bet.hkjc.com/contentserver/jcbw/cmc/last30draw.json"
# 單期查詢（用嚟補舊數據）
MS_BY_DATE = "https://bet.hkjc.com/ch/marksix/getdrawresult?lang=ch&date={d}"

RED  = {1,2,7,8,12,13,18,19,23,24,29,30,34,35,40,45,46}
BLUE = {3,4,9,10,14,15,20,25,26,31,36,37,41,42,47,48}
def color_of(n): return "red" if n in RED else "blue" if n in BLUE else "green"

# ============ 快取 ============
def load_cache():
    if os.path.exists(CACHE_FILE):
        try: return json.load(open(CACHE_FILE, encoding="utf-8"))
        except Exception: pass
    return {}

def save_cache(c):
    try: json.dump(c, open(CACHE_FILE, "w", encoding="utf-8"), ensure_ascii=False)
    except Exception: pass

# ============ 日期 ============
def is_ms_day(d: date) -> bool:
    return d.weekday() in (1, 3, 5)   # 二、四、六

def next_ms_dates(n=5):
    out, d = [], date.today()
    while len(out) < n:
        if is_ms_day(d): out.append(d)
        d += timedelta(days=1)
    return out

# ============ 解析 ============
def parse_last30_item(item):
    """解析 last30draw.json 單條記錄"""
    try:
        # 格式範例：{"id":"24/064","date":"04/06/2024","no":"6+11+19+21+27+43","sno":"8"}
        date_raw = str(item.get("date", ""))
        parts = date_raw.split("/")
        if len(parts) == 3:
            iso = f"{parts[2]}-{parts[1]}-{parts[0]}"
        else:
            iso = date_raw

        no_str = str(item.get("no", "")).replace(" ", "")
        nums = [int(x) for x in no_str.split("+") if x.strip().isdigit()]
        if len(nums) < 6: return None

        special = None
        for k in ("sno", "special", "specialNo"):
            v = item.get(k)
            if v not in (None, "", "0"):
                try: special = int(v); break
                except: pass
        if special is None: return None

        return {
            "id": str(item.get("id", iso)),
            "date": iso,
            "main": sorted(nums[:6]),
            "special": special,
        }
    except Exception:
        return None

def parse_by_date(obj):
    if isinstance(obj, list):
        if not obj: return None
        obj = obj[0]
    if not isinstance(obj, dict): return None
    def pick(*ks):
        for k in ks:
            v = obj.get(k)
            if v not in (None, "", "0"): return v
        return None
    try:
        main = sorted(int(pick(f"no{i}")) for i in range(1, 7))
        sp = int(pick("sno", "special"))
    except (TypeError, ValueError):
        return None
    raw = str(pick("date", "drawDate") or "")
    iso = raw
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try: iso = datetime.strptime(raw, fmt).strftime("%Y-%m-%d"); break
        except ValueError: pass
    return {"id": str(pick("drawno", "drawNo") or iso),
            "date": iso, "main": main, "special": sp}

# ============ 抓取 ============
async def fetch_last30(client):
    for _ in range(3):
        try:
            r = await client.get(MS_LAST30, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                # 關鍵：呢個 endpoint 係 utf-8-sig
                r.encoding = "utf-8-sig"
                return r.json()
        except Exception:
            await asyncio.sleep(1)
    return None

async def fetch_by_date(client, sem, d):
    async with sem:
        for _ in range(2):
            try:
                r = await client.get(MS_BY_DATE.format(d=d.isoformat()),
                                     headers=HEADERS, timeout=12)
                if r.status_code == 200:
                    return parse_by_date(r.json())
            except Exception:
                await asyncio.sleep(0.8)
        return None

async def get_draws(years=1):
    cache = load_cache()
    ms = cache.get("ms", {})
    now = time.time()

    # 快取有效就唔使抓
    if now - ms.get("updated", 0) < CACHE_TTL and ms.get("draws"):
        return ms["draws"], False, ms.get("updated", 0), ms.get("source", "cache")

    draws = dict(ms.get("draws", {}))
    source = "hkjc"
    errors = []

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # 1. 先抓 last30draw.json（快、準）
        data = await fetch_last30(client)
        got30 = 0
        if data and isinstance(data, list):
            for item in data:
                p = parse_last30_item(item)
                if p:
                    draws[p["date"]] = p
                    got30 += 1
        else:
            errors.append("last30draw 抓取失敗")

        # 2. 如果想補更多歷史，逐期抓（可選，較慢）
        if years > 0 and got30 > 0:
            today = date.today()
            start = today - timedelta(days=365 * years)
            days, d = [], start
            while d <= today:
                if is_ms_day(d):
                    iso = d.isoformat()
                    if iso not in draws:
                        days.append(d)
                d += timedelta(days=1)

            # 只補最多 60 期，避免太慢
            days = days[-60:] if len(days) > 60 else days
            if days:
                sem = asyncio.Semaphore(6)
                results = await asyncio.gather(*[fetch_by_date(client, sem, x) for x in days])
                for x, r in zip(days, results):
                    if r: draws[x.isoformat()] = r

    if not draws:
        if ms.get("draws"):
            return ms["draws"], True, ms.get("updated", 0), "stale"
        return {}, True, 0, "empty"

    cache["ms"] = {"updated": now, "draws": draws, "source": source}
    save_cache(cache)
    return draws, False, now, source

# ============ 統計評分 ============
def score_numbers(draws_list):
    if not draws_list: return {}
    total = len(draws_list)
    freq, last_seen, recent20 = Counter(), {}, Counter()

    for idx, d in enumerate(draws_list):
        for n in list(d["main"]) + [d["special"]]:
            freq[n] += 1
            if n not in last_seen: last_seen[n] = idx
        if idx < 20:
            for n in d["main"]: recent20[n] += 1

    max_freq = max(freq.values()) if freq else 1
    out = {}
    for n in range(1, 50):
        f  = freq.get(n, 0)
        ls = last_seen.get(n, total)
        r  = recent20.get(n, 0)
        s = 0.40 * (f / max_freq) \
          + 0.35 * min(ls / max(total, 1), 1.0) \
          + 0.25 * min(r / 6, 1.0)
        out[n] = {"n": n, "score": round(s, 4), "freq": f,
                  "gap": ls, "recent20": r, "color": color_of(n)}
    return out

def build_dantuo(scores, n_dan=3, n_leg=6):
    ranked = sorted(scores.values(), key=lambda x: -x["score"])
    dan = [x["n"] for x in ranked[:n_dan]]
    leg = [x["n"] for x in ranked[n_dan:n_dan + n_leg]]
    tickets = []
    for i in range(len(leg)):
        for j in range(i + 1, len(leg)):
            for k in range(j + 1, len(leg)):
                tickets.append(sorted(dan + [leg[i], leg[j], leg[k]]))
    return {
        "dan": dan, "leg": leg, "tickets": tickets,
        "n_tickets": len(tickets), "cost_hkd": len(tickets) * 10,
    }

# ============ API ============
@app.get("/api/marksix/next")
async def api_next():
    nxt = next_ms_dates(5)
    wd = ["一","二","三","四","五","六","日"]
    return {"today": date.today().isoformat(),
            "next_draws": [{"date": d.isoformat(), "weekday": wd[d.weekday()]} for d in nxt],
            "note": "預設二、四、六搞珠；馬會如有特別安排以官網為準"}

@app.get("/api/marksix/analyze")
async def api_analyze(years: int = 1):
    draws, stale, ts, src = await get_draws(years)
    if not draws:
        return {"ok": False, "msg": "抓唔到馬會數據，請稍後再試"}

    items = sorted(draws.values(), key=lambda x: x["date"], reverse=True)
    scores = score_numbers(items)
    dt = build_dantuo(scores, 3, 6)
    nxt = next_ms_dates(1)[0]
    wd = ["一","二","三","四","五","六","日"]

    return {
        "ok": True,
        "data_source": "香港賽馬會 bet.hkjc.com",
        "draw_count": len(items),
        "latest_date": items[0]["date"],
        "latest_draw": items[0],
        "updated": datetime.fromtimestamp(ts, HKT).strftime("%Y-%m-%d %H:%M HKT") if ts else "",
        "stale": stale,
        "source": src,
        "next_draw": {"date": nxt.isoformat(), "weekday": wd[nxt.weekday()]},
        "dantuo": dt,
        "ranked": sorted(scores.values(), key=lambda x: -x["score"])[:20],
        "disclaimer": "統計評分並非預測，每注中獎概率相同，不能提高勝率。",
    }

@app.get("/health")
def health(): return {"status": "ok", "time": datetime.now(HKT).isoformat()}

@app.get("/")
def root(): return FileResponse("index.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
