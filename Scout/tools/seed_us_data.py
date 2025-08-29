# C:\Quarkmine\Agents\Scout\tools\seed_us_data.py
import os, json, re, argparse, urllib.request, urllib.error
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
SEEDS = os.path.join(DATA, "seeds")
STATES_DIR = os.path.join(DATA, "states")
LOG_PATH = os.path.join(ROOT, "tools", "seed_us_data.log")

# 50 states + DC (names -> USPS -> FIPS)
STATE_ROWS = [
    ("Alabama","AL","01"),("Alaska","AK","02"),("Arizona","AZ","04"),("Arkansas","AR","05"),
    ("California","CA","06"),("Colorado","CO","08"),("Connecticut","CT","09"),("Delaware","DE","10"),
    ("District of Columbia","DC","11"),("Florida","FL","12"),("Georgia","GA","13"),("Hawaii","HI","15"),
    ("Idaho","ID","16"),("Illinois","IL","17"),("Indiana","IN","18"),("Iowa","IA","19"),
    ("Kansas","KS","20"),("Kentucky","KY","21"),("Louisiana","LA","22"),("Maine","ME","23"),
    ("Maryland","MD","24"),("Massachusetts","MA","25"),("Michigan","MI","26"),("Minnesota","MN","27"),
    ("Mississippi","MS","28"),("Missouri","MO","29"),("Montana","MT","30"),("Nebraska","NE","31"),
    ("Nevada","NV","32"),("New Hampshire","NH","33"),("New Jersey","NJ","34"),("New Mexico","NM","35"),
    ("New York","NY","36"),("North Carolina","NC","37"),("North Dakota","ND","38"),("Ohio","OH","39"),
    ("Oklahoma","OK","40"),("Oregon","OR","41"),("Pennsylvania","PA","42"),("Rhode Island","RI","44"),
    ("South Carolina","SC","45"),("South Dakota","SD","46"),("Tennessee","TN","47"),("Texas","TX","48"),
    ("Utah","UT","49"),("Vermont","VT","50"),("Virginia","VA","51"),("Washington","WA","53"),
    ("West Virginia","WV","54"),("Wisconsin","WI","55"),("Wyoming","WY","56")
]

SUFFIXES = [
    r"\s+County$", r"\s+Parish$", r"\s+Borough$", r"\s+City and Borough$", r"\s+Census Area$",
    r"\s+Municipality$", r"\s+City$", r"\s+Municipio$"
]

def log(msg):
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

def normalize_county(name: str) -> str:
    n = (name or "").strip()
    for sx in SUFFIXES:
        n = re.sub(sx, "", n, flags=re.I)
    n = n.replace("–","-").replace("’","'").strip()
    n = re.sub(r"\s+", " ", n)
    # title case (keeps Mc/Mac etc reasonably)
    return n.title()

def census_url_dec(state_fips: str) -> str:
    # 2020 Decennial PL dataset
    return f"https://api.census.gov/data/2020/dec/pl?get=NAME&for=county:*&in=state:{state_fips}"

def census_url_acs(state_fips: str) -> str:
    # 2022 ACS 5-year, basic NAME field (cross-check)
    return f"https://api.census.gov/data/2022/acs/acs5?get=NAME&for=county:*&in=state:{state_fips}"

def fetch_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent":"Scout-US-Seed/1.0"})
    with urllib.request.urlopen(req, timeout=45) as r:
        return json.loads(r.read().decode("utf-8"))

def extract_counties(rows, state_name: str):
    # rows like: [["NAME","state","county"], ["Franklin County, Ohio","39","049"], ...]
    out = []
    for i, row in enumerate(rows):
        if i == 0:  # header
            continue
        name = row[0]
        # Trim trailing ", State"
        name = re.sub(rf",\s*{re.escape(state_name)}$", "", name)
        out.append(normalize_county(name))
    # de-dup preserve order
    seen, res = set(), []
    for c in out:
        if c and c not in seen:
            res.append(c); seen.add(c)
    return res

def write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def write_states_files(states, abbr_map, counties_map, site_bias_map):
    write_json(os.path.join(SEEDS, "states.json"), states)
    write_json(os.path.join(SEEDS, "state_abbr.json"), abbr_map)
    write_json(os.path.join(SEEDS, "counties.json"), counties_map)
    write_json(os.path.join(SEEDS, "site_bias.json"), site_bias_map)

def init_progress(state: str, counties: list, mode: str):
    os.makedirs(STATES_DIR, exist_ok=True)
    path = os.path.join(STATES_DIR, re.sub(r"[^A-Za-z0-9_-]+","_", state) + ".json")
    if os.path.exists(path) and mode == "merge":
        try:
            cur = json.load(open(path, "r", encoding="utf-8"))
        except Exception:
            cur = None
        if cur and isinstance(cur, dict):
            cur.setdefault("state", state)
            cur.setdefault("counties", {})
            for c in counties:
                cur["counties"].setdefault(c, {"status":"pending","runs":0,"last_run":None,"notes":""})
            cur.setdefault("created", datetime.now().isoformat())
            write_json(path, cur); return path
    # overwrite or fallback
    doc = {
        "state": state,
        "counties": { c: {"status":"pending","runs":0,"last_run":None,"notes":""} for c in counties },
        "created": datetime.now().isoformat()
    }
    write_json(path, doc); return path

def main():
    ap = argparse.ArgumentParser(description="Populate full US seeds (states, abbr, counties, site_bias) from Census")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing seeds instead of merging")
    ap.add_argument("--init-progress", choices=["ALL"], help='Also create per-state progress files. Use "ALL" to initialize every state.')
    ap.add_argument("--progress-mode", choices=["merge","overwrite"], default="merge", help="How to write progress files")
    args = ap.parse_args()

    states = [row[0] for row in STATE_ROWS]
    abbr   = {row[0]: row[1] for row in STATE_ROWS}
    fips   = {row[0]: row[2] for row in STATE_ROWS}

    counties_map = {}
    site_bias_map = {}  # e.g., { "Ohio": [".k12.oh.us"] }

    # fetch per state
    for state in states:
        sf = fips[state]
        try:
            dec = fetch_json(census_url_dec(sf))
            acs = fetch_json(census_url_acs(sf))
        except urllib.error.URLError as e:
            log(f"{state}: network error {e}")
            raise
        except Exception as e:
            log(f"{state}: unexpected error {e}")
            raise

        dec_list = extract_counties(dec, state)
        acs_list = extract_counties(acs, state)

        # prefer intersection if it's close; else union
        set_dec, set_acs = set(dec_list), set(acs_list)
        agree = sorted(set_dec & set_acs)
        if len(agree) >= 0.9 * max(len(set_dec), len(set_acs)):
            final = agree
        else:
            final = sorted(set_dec | set_acs)

        counties_map[state] = final
        site_bias_map[state] = [f".k12.{abbr[state].lower()}.us"]

        log(f"{state}: counties={len(final)} (dec={len(dec_list)} acs={len(acs_list)} agree={len(agree)})")

        # optional progress init
        if args.init_progress == "ALL":
            init_progress(state, final, mode=args.progress_mode)

    # merge or overwrite seeds on disk
    if not args.overwrite:
        # merge into existing if present
        old_states   = []
        old_abbr     = {}
        old_counties = {}
        old_bias     = {}
        try: old_states   = json.load(open(os.path.join(SEEDS,"states.json"), "r", encoding="utf-8"))
        except: pass
        try: old_abbr     = json.load(open(os.path.join(SEEDS,"state_abbr.json"), "r", encoding="utf-8"))
        except: pass
        try: old_counties = json.load(open(os.path.join(SEEDS,"counties.json"), "r", encoding="utf-8"))
        except: pass
        try: old_bias     = json.load(open(os.path.join(SEEDS,"site_bias.json"), "r", encoding="utf-8"))
        except: pass

        # merge logic: unions & updates
        merged_states = sorted(set(old_states or []) | set(states))
        merged_abbr   = {**old_abbr, **abbr}
        merged_counties = {**old_counties, **counties_map}
        merged_bias     = {**old_bias, **site_bias_map}

        write_states_files(merged_states, merged_abbr, merged_counties, merged_bias)
    else:
        write_states_files(states, abbr, counties_map, site_bias_map)

    print(f"Seeds written to: {SEEDS}")
    print(f"- states.json: {len(states)} states")
    print(f"- state_abbr.json: {len(states)} entries")
    print(f"- counties.json: {sum(len(v) for v in counties_map.values())} county-equivalents total")
    print(f"- site_bias.json: {len(site_bias_map)} entries")
    if args.init_progress == "ALL":
        print(f"Initialized ALL state progress files in {STATES_DIR} ({len(states)} files).")

if __name__ == "__main__":
    main()
