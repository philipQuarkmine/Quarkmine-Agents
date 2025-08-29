import json, re, requests, sys
from pathlib import Path
from urllib.parse import urlparse

# --- Config ---
JSON_PATH = r"C:\Quarkmine\Agents\Scout\data\scout_master.json"
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "llama3.2:1b"   # lightweight model for fuzzy name matching

def load_json():
    with Path(JSON_PATH).open("r", encoding="utf-8") as f:
        return json.load(f)

def get_all_district_records(data):
    """Return a flat list of district dicts from schema: states -> <State> -> districts -> [...]"""
    out = []
    states = data.get("states", {}) if isinstance(data, dict) else {}
    for st_obj in states.values():
        if isinstance(st_obj, dict):
            districts = st_obj.get("districts", [])
            if isinstance(districts, list):
                for d in districts:
                    if isinstance(d, dict):
                        out.append(d)
    return out

def collect_district_names(recs):
    names = set()
    keys = ("district","district_name","name","lea_name","organization","school_district")
    for rec in recs:
        for k in keys:
            v = rec.get(k)
            if isinstance(v, str) and v.strip():
                names.add(v.strip())
    # fallback: any key containing "name"
    if not names:
        for rec in recs:
            for k, v in rec.items():
                if "name" in k.lower() and isinstance(v, str) and v.strip():
                    names.add(v.strip())
    return sorted(names)

def ask_llm_pick_name(query, candidates):
    prompt = f"""You are a helper that selects the single best matching district name from a list, given a user's query.

User query: "{query}"

Candidates:
{chr(10).join(f"- {c}" for c in candidates)}

Strictly return ONLY the exact candidate text (one line, no extra words). If no good match, return "NONE".
"""
    resp = requests.post(OLLAMA_URL, json={
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.0}
    })
    resp.raise_for_status()
    out = resp.json().get("response", "").strip()
    return out.splitlines()[0].strip('"').strip()

URL_RE = re.compile(r"""(?i)\bhttps?://[^\s"'<>()]+""")

def extract_urls_anywhere(obj):
    urls = set()
    def walk(x):
        if isinstance(x, dict):
            for v in x.values(): walk(v)
        elif isinstance(x, list):
            for v in x: walk(v)
        elif isinstance(x, str):
            for m in URL_RE.findall(x):
                urls.add(m.rstrip('.,);]'))
    walk(obj)
    return sorted(urls)

def find_records_by_exact_name(recs, picked_name):
    keys = ("district","district_name","name","lea_name","organization","school_district")
    matches = []
    for rec in recs:
        for k in keys:
            v = rec.get(k)
            if isinstance(v, str) and v.strip() == picked_name:
                matches.append(rec); break
    return matches

def main():
    if len(sys.argv) < 2:
        print('Usage: py query_district_website.py "<District Name>"'); sys.exit(1)
    user_query = sys.argv[1]

    data = load_json()
    records = get_all_district_records(data)
    candidates = collect_district_names(records)
    if not candidates:
        print("No district names found in scout_master.json."); sys.exit(2)

    picked = ask_llm_pick_name(user_query, candidates)
    if picked.upper() == "NONE":
        print(f"No good match found for: {user_query}"); sys.exit(3)

    matches = find_records_by_exact_name(records, picked)
    if not matches:
        print(f"Matched district '{picked}', but could not locate its record."); sys.exit(4)

    urls = set()
    for rec in matches:
        for u in extract_urls_anywhere(rec):
            urls.add(u)

    print(f"Matched district: {picked}")
    if urls:
        print("Websites found:")
        for u in sorted(urls):
            host = urlparse(u).netloc
            print(f"- {u}  (domain: {host})")
    else:
        print("No website links found for this district.")

if __name__ == "__main__":
    main()
