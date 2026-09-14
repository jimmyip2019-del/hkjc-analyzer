import asyncio, json, os, time
from datetime import date, datetime, timedelta, timezone
from collections import Counter
import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

HKT = timezone(timedelta(hours=8))
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

CACHE_FILE = "cache.json"
CACHE_TTL = 6 * 3600

HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
    "Accept": "application/json, text/plain, */*",
}

# ========== 多個數據源（GitHub 優先，馬會後備）==========
DATA_SOURCES = [
    # 1. GitHub 公開六合彩數據庫（每日更新）
    "https://raw.githubusercontent.com/icelam/mark-six-data-visualization/master/public/data/latest.json",
    # 2. 另一個 GitHub 備份
    "https://raw.githubusercontent.com/kenchudigital/ML-SixMark-Lab/main/data/draws.json",
    # 3. 馬會官方（可能被封，但試下）
    "https://bet.hkjc.com/contentserver/jcbw/cmc/last30draw.json",
]

RED = {1,2,7,8,12,13,18,19,23,24,29,30,34,35,40,45,46}
BLUE = {3,4,9,10,14,15,20,25,26,31,36,37,41,42,47,48}
def color_of(n): return "red" if n in RED else "blue" if n in BLUE else "green"

# ========== 快取 ==========
def load_cache():
    if os.path.exists(CACHE_FILE):
        try: return json.load(open(CACHE_FILE, encoding="utf-8"))
        except Exception: pass
    return {}

def save_cache(c):
    try: json.dump(c, open(CACHE_FILE, "w", encoding="utf-8"), ensure_ascii=False)
    except Exception: pass

# ========== 日期 ==========
def is_ms_day(d: date) -> bool:
    return d.weekday() in (1, 3, 5)

def next_ms_dates(n=5):
    out, d = [], date.today()
    while len(out) < n:
        if is_ms_day(d): out.append(d)
        d += timedelta(days=1)
    return out

# ========== 通用解析器（處理多種格式）==========
def parse_any_item(item):
    """嘗試解析任何格式嘅開獎記錄"""
    if not isinstance(item, dict): return None

    # 搵日期
    date_val = None
    for k in ("date", "drawDate", "draw_date", "開獎日期"):
        v = item.get(k)
        if v: date_val = str(v); break
    if not date_val: return None

    # 日期轉 ISO
    iso = date_val
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
        try: iso = datetime.strptime(date_val, fmt).strftime("%Y-%m-%d"); break
        except ValueError: pass

    # 搵號碼
    nums = None
    for k in ("no", "numbers", "nums", "開獎號碼"):
        v = item.get(k)
        if v:
            if isinstance(v, str):
                nums = [int(x) for x in v.replace(" ", "").replace(",", "+").split("+") if x.strip().isdigit()]
            elif isinstance(v, list):
                nums = [int(x) for x in v if str(x).isdigit()]
            if nums and len(nums) >= 6: break

    # 如果冇直接欄位，試 no1~no6
    if not nums or len(nums) < 6:
        nums = []
        for i in range(1, 7):
            v = item.get(f"no{i}") or item.get(f"n{i}")
            if v and str(v).isdigit(): nums.append(int(v))
        if len(nums) < 6: return None

    # 特別號
    special = None
    for k in ("sno", "special", "specialNumber", "特別號"):
        v = item.get(k)
        if v not in (None, "", "0"):
            try: special = int(v); break
            except: pass

    if special is None:
        # 如果 nums 有 7 個，最後一個做特別號
        if len(nums) >= 7:
            special = nums[6]; nums = nums[:6]
        else:
            return None

    return {
        "id": str(item.get("id") or item.get("drawNumber") or iso),
        "date": iso,
        "main": sorted(nums[:6]),
        "special": special,
    }

async def fetch_source(client, url):
    """嘗試從單一數據源抓取"""
    try:
        r = await client.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200: return None
        # 處理 utf-8-sig
        text = r.text
        if text.startswith("\ufeff"): text = text[1:]
        data = json.loads(text)
        return data
    except Exception:
        return None

async def get_draws(years=1):
    cache = load_cache()
    ms = cache.get("ms", {})
    now = time.time()

    if now - ms.get("updated", 0) < CACHE_TTL and ms.get("draws"):
        return ms["draws"], False, ms.get("updated", 0), ms.get("source", "cache")

    draws = dict(ms.get("draws", {}))
    source = "unknown"
    errors = []

    async with httpx.AsyncClient(follow_redirects=True) as client:
        for url in DATA_SOURCES:
            data = await fetch_source(client, url)
            if not data: continue

            # 數據可能是 list 或 dict
            items = data if isinstance(data, list) else data.get("draws") or data.get("data") or data.get("results") or []
            got = 0
            for item in items:
                p = parse_any_item(item)
                if p:
                    draws[p["date"]] = p
                    got += 1

            if got > 0:
                source = url.split("/")[2]  # 域名
                break
            else:
                errors.append(f"{url.split('/')[2]} 格式唔啱")

    if not draws:
        if ms.get("draws"):
            return ms["draws"], True, ms.get("updated", 0), "stale"
        return {}, True, 0, "empty"

    cache["ms"] = {"updated": now, "draws": draws, "source": source}
    save_cache(cache)
    return draws, False, now, source

# ========== 統計 ==========
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
        f, ls, r = freq.get(n, 0), last_seen.get(n, total), recent20.get(n, 0)
        s = 0.40*(f/max_freq) + 0.35*min(ls/max(total,1),1) + 0.25*min(r/6,1)
        out[n] = {"n": n, "score": round(s,4), "freq": f, "gap": ls,
                  "recent20": r, "color": color_of(n)}
    return out

def build_dantuo(scores, n_dan=3, n_leg=6):
    ranked = sorted(scores.values(), key=lambda x: -x["score"])
    dan = [x["n"] for x in ranked[:n_dan]]
    leg = [x["n"] for x in ranked[n_dan:n_dan+n_leg]]
    tickets = []
    for i in range(len(leg)):
        for j in range(i+1, len(leg)):
            for k in range(j+1, len(leg)):
                tickets.append(sorted(dan + [leg[i], leg[j], leg[k]]))
    return {"dan": dan, "leg": leg, "tickets": tickets,
            "n_tickets": len(tickets), "cost_hkd": len(tickets)*10}

# ========== API ==========
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
        return {"ok": False, "msg": "抓唔到數據，請稍後再試"}
    items = sorted(draws.values(), key=lambda x: x["date"], reverse=True)
    scores = score_numbers(items)
    dt = build_dantuo(scores, 3, 6)
    nxt = next_ms_dates(1)[0]
    wd = ["一","二","三","四","五","六","日"]
    return {
        "ok": True,
        "data_source": f"數據源：{src}",
        "draw_count": len(items),
        "latest_date": items[0]["date"],
        "latest_draw": items[0],
        "updated": datetime.fromtimestamp(ts, HKT).strftime("%Y-%m-%d %H:%M HKT") if ts else "",
        "stale": stale, "source": src,
        "next_draw": {"date": nxt.isoformat(), "weekday": wd[nxt.weekday()]},
        "dantuo": dt,
        "ranked": sorted(scores.values(), key=lambda x: -x["score"])[:20],
        "disclaimer": "統計評分並非預測，每注中獎概率相同。",
    }

@app.get("/health")
def health(): return {"status": "ok"}

@app.get("/")
def root(): return FileResponse("index.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
