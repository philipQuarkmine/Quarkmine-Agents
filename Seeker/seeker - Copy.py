import argparse, json, os, re, requests, time
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse

DEFAULT_MODEL = "llama3:8b"
OLLAMA_API = "http://127.0.0.1:11434/api/generate"

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "data")
MASTER_PATH = os.path.join(DATA_DIR, "seeker_master.json")
LOG_PATH = os.path.join(ROOT_DIR, "seeker.log")

# -------------------------
# UTILS
# -------------------------
def now_iso(): return datetime.now().isoformat()

def log(msg: str):
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

def load_json(path, default):
    if not os.path.exists(path): return default
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return default

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f: json.dump(data, f, indent=2, ensure_ascii=False)

# -------------------------
# Ollama API
# -------------------------
def ask_ollama(prompt: str, model: str) -> str:
    try:
        r = requests.post(OLLAMA_API, json={"model": model, "prompt": prompt, "stream": False})
        r.raise_for_status()
        raw = r.json().get("response","").strip()
        m = re.search(r"(\{.*\}|\[.*\])", raw, flags=re.DOTALL)
        return m.group(1).strip() if m else raw
    except Exception as e:
        log(f"Ollama API error: {e}")
        return "[]"

# -------------------------
# DuckDuckGo scrape
# -------------------------
def duckduckgo_search(query, max_results=10):
    url = "https://html.duckduckgo.com/html/"
    try:
        r = requests.post(url, data={"q": query}, headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        results = []
        for a in soup.select(".result__a")[:max_results]:
            href, title = a.get("href"), a.get_text()
            if href and title:
                results.append({"url": href, "title": title, "confidence": 0.0, "category": "Unknown"})
        return results
    except Exception as e:
        log(f"DuckDuckGo search error: {e}")
        return []

# -------------------------
# Strict-mode verification
# -------------------------
DENY = ["facebook.com","niche.com","greatschools.org","publicschoolsk12.com",
        "wikipedia.org","zipdatamaps.com","schoolcalendarguide.com","mapquest.com"]

def strict_classify(result, district, state):
    host = (urlparse(result["url"]).hostname or "").lower()
    title = result.get("title","").lower()
    accepted = []
    score = 0

    if any(bad in host for bad in DENY):
        return {**result, "category":"External Reference","confidence":0.2,"strict_score":5,"accepted_by":["denylist"]}

    if host.endswith(".k12.oh.us") or host.endswith(".edu") or host.endswith(".gov") or host.endswith(".us"):
        score += 40; accepted.append("domain")

    if district.lower().split()[0] in title:
        score += 30; accepted.append("name")

    if "district" in title or "board" in title:
        score += 20; accepted.append("keyword")

    category, conf = "Unknown", 0.4
    if score >= 70:
        category, conf = "Official District Page", 0.95
    elif score >= 50:
        category, conf = "School-Level Page", 0.8
    else:
        category, conf = "External Reference", 0.5

    return {**result,"category":category,"confidence":round(conf,2),"strict_score":score,"accepted_by":accepted}

def strict_reclassify(results, district, state):
    return [strict_classify(r, district, state) for r in results]

# -------------------------
# Dedupe & save
# -------------------------
def normalize_query(q: str) -> str:
    return re.sub(r"\s+"," ", q.strip().lower())

def upsert_query(master: dict, query: str, results: list):
    norm = normalize_query(query)
    for qrec in master.get("queries", []):
        if normalize_query(qrec.get("query","")) == norm:
            qrec["results"], qrec["last_seen_at"] = results, now_iso()
            return master
    master.setdefault("queries", []).append({
        "query": query, "results": results,
        "created_at": now_iso(), "last_seen_at": now_iso()
    })
    return master

# -------------------------
# MAIN
# -------------------------
def main():
    parser = argparse.ArgumentParser(description="Seeker (DuckDuckGo + Strict Mode)")
    parser.add_argument("--query", required=True)
    parser.add_argument("--district", required=False, default="")
    parser.add_argument("--state", required=False, default="")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    args = parser.parse_args()

    log(f"Running Seeker query: {args.query}")
    results = duckduckgo_search(args.query)

    if args.verify and results:
        results = strict_reclassify(results, args.district or args.query, args.state or "")

    master = load_json(MASTER_PATH, {"queries":[]})
    master = upsert_query(master, args.query, results)
    save_json(MASTER_PATH, master)

    print(f"✅ Seeker finished. Results saved/updated in {MASTER_PATH}")

if __name__ == "__main__":
    main()
