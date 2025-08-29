import argparse, json, os, re, requests, hashlib
from datetime import datetime
from difflib import get_close_matches

# =========================
# CONFIG & PATHS
# =========================
DEFAULT_MODEL = "llama3:8b"
OLLAMA_API = "http://127.0.0.1:11434/api/generate"

ROOT_DIR    = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR  = os.path.join(ROOT_DIR, "data")
STATE_DIR   = os.path.join(OUTPUT_DIR, "states")
SEEDS_DIR   = os.path.join(OUTPUT_DIR, "seeds")
OVR_DIR     = os.path.join(OUTPUT_DIR, "overrides")

MASTER_PATH = os.path.join(OUTPUT_DIR, "scout_master.json")
COUNTY_SEEDS_PATH = os.path.join(SEEDS_DIR, "counties.json")
ALIASES_PATH      = os.path.join(OVR_DIR,   "district_aliases.json")

LOG_PATH   = os.path.join(ROOT_DIR, "scout.log")

# =========================
# UTILITIES
# =========================
def now_iso(): return datetime.now().isoformat()

def log(msg: str):
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

def ensure_dirs():
    for p in (OUTPUT_DIR, STATE_DIR, SEEDS_DIR, OVR_DIR):
        os.makedirs(p, exist_ok=True)

def load_json(path, default):
    if not os.path.exists(path): return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def safe_filename(part: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", (part or "").strip())

def slugify(name: str) -> str:
    if not name: return ""
    n = name.lower()
    n = re.sub(r"\b(exempted\s+village|city|local|village|public|school\s+district|schools|public\s+schools|csd|lsd|esd)\b", "", n)
    n = re.sub(r"\bschool(s)?\b", "", n)
    n = re.sub(r"[^a-z0-9]+", "-", n)
    return n.strip("-")

# =========================
# MASTER DB
# =========================
def load_master():
    return load_json(MASTER_PATH, {"states": {}})

def save_master(master):
    save_json(MASTER_PATH, master)

def load_alias_overrides():
    return load_json(ALIASES_PATH, {})

def save_alias_overrides(overrides):
    save_json(ALIASES_PATH, overrides)

def upsert_districts(master, overrides, state: str, county: str, districts: list) -> int:
    """Insert/update districts with canonical IDs + aliases"""
    states = master.setdefault("states", {})
    st = states.setdefault(state, {"districts": []})

    can_idx, alias_idx = {}, {}
    for i, d in enumerate(st["districts"]):
        can_idx[slugify(d.get("district",""))] = i
        for a in d.get("aliases", []):
            alias_idx[slugify(a)] = i

    ovr_state = overrides.get(state.lower(), {})
    def stable_id(state, name):
        base = f"{state}|{name}"
        h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
        return f"{state[:2].lower()}-{h}"

    added = 0
    for d in districts:
        nm = (d.get("district") or "").strip()
        if not nm: continue
        s = slugify(nm)
        j = can_idx.get(s) or alias_idx.get(s)
        if j is None:
            rec = {
                "id": stable_id(state, nm),
                "district": nm,
                "aliases": [],
                "state": state,
                "source_county": county,
                "source_counties": [county],
                "district_type": d.get("district_type"),
                "size": d.get("size"),
                "grades": d.get("grades"),
                "STEM_presence": d.get("STEM_presence"),
                "funding_signals": d.get("funding_signals"),
                "fit_score": d.get("fit_score"),
                "first_seen_at": now_iso(),
                "last_seen_at": now_iso()
            }
            st["districts"].append(rec)
            can_idx[s] = len(st["districts"]) - 1
            added += 1
        else:
            rec = st["districts"][j]
            rec["last_seen_at"] = now_iso()
    overrides[state.lower()] = ovr_state
    save_alias_overrides(overrides)
    master["states"][state] = st
    return added

# =========================
# OLLAMA API (WSL2)
# =========================
def ask_ollama(prompt: str, model: str) -> str:
    try:
        r = requests.post(OLLAMA_API, json={"model": model, "prompt": prompt, "stream": False})
        r.raise_for_status()
        raw = r.json().get("response","").strip()
        if not raw:
            return "[]"
        # Extract first JSON block
        m = re.search(r"(\{.*\}|\[.*\])", raw, flags=re.DOTALL)
        if m:
            return m.group(1).strip()
        return raw
    except Exception as e:
        log(f"Ollama API error: {e}")
        return "[]"

# =========================
# MAIN
# =========================
def main():
    parser = argparse.ArgumentParser(description="Scout (WSL2 + Ollama API)")
    parser.add_argument("--state", type=str, default="Ohio")
    parser.add_argument("--county", type=str, required=True)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    args = parser.parse_args()

    ensure_dirs()

    # build strict prompt
    schema = """[
      {
        "district": "Name",
        "district_type": "Public | Private | Charter",
        "size": "student count or estimate",
        "grades": "K-12, etc.",
        "STEM_presence": "Yes/No + notes",
        "funding_signals": "brief notes",
        "fit_score": 0-100
      }
    ]"""
    prompt = f"""
You are Scout, a market researcher.
Respond ONLY with a pure JSON array exactly matching this schema:
{schema}

Task:
- State: {args.state}
- County: {args.county}
- Return up to {args.limit} districts in this county.
- If none found, return [].

IMPORTANT:
- Do NOT include commentary, headers, or explanations.
- Output must be a valid JSON array and nothing else.
"""

    log(f"Running Scout for {args.state}/{args.county} with {args.model}")
    raw = ask_ollama(prompt, model=args.model)

    try:
        data = json.loads(raw) if raw else []
        if not isinstance(data, list): raise ValueError("Expected JSON array")
    except Exception as e:
        log(f"Parse error: {e}, raw={raw[:200]}")
        print("❌ Error: see scout.log")
        return

    # update master
    master = load_master()
    overrides = load_alias_overrides()
    added = upsert_districts(master, overrides, args.state, args.county, data)
    save_master(master)

    # save latest run
    latest_path = os.path.join(OUTPUT_DIR, f"latest_{safe_filename(args.state)}_{safe_filename(args.county)}.json")
    save_json(latest_path, data)

    print(f"✅ Scout finished: {added} new district(s) added → {MASTER_PATH}")
    print(f"Latest results saved to {latest_path}")

    # --- Scout → Seeker handoff (only if new districts found)
    if added > 0:
        seeker_dir = os.path.join(os.path.dirname(ROOT_DIR), "Seeker")
        seeker_py  = os.path.join(seeker_dir, "seeker.py")
        seeker_master_path = os.path.join(seeker_dir, "data", "seeker_master.json")

        existing = load_json(seeker_master_path, {"queries":[]})
        seen = { (q.get("query") or "").lower().strip() for q in existing.get("queries",[]) }

        confirmed = 0
        for d in data:
            nm = (d.get("district") or "").strip()
            if not nm: continue
            query = f"{nm} {args.state} site"
            if query.lower().strip() in seen: continue
            log(f"[handoff] Running Seeker for {nm} ({args.state}/{args.county})")
            os.system(f'py "{seeker_py}" --query "{query}" --district "{nm}" --state "{args.state}" --verify --model "{args.model}"')
            confirmed += 1

        print(f"[handoff] Sent {confirmed} districts to Seeker for validation")

if __name__ == "__main__":
    main()
