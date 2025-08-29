import argparse, json, os, re
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode, urlparse, quote_plus
from urllib.request import urlopen, Request
from xml.etree import ElementTree as ET
from difflib import get_close_matches

# ---------- DEFAULT PATHS (Radar runs in its own folder) ----------
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR     = os.path.join(SCRIPT_DIR, "data")  # Radar's own data here
# Read Scout’s watchlist by default from ../Scout/data/scout_master.json
DEFAULT_SCOUT_DB = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "Scout", "data", "scout_master.json"))

# Output files (can be overridden via --data-dir)
RADAR_DB_NAME  = "radar_master.json"
INTAKE_NAME    = "intake_signals.json"
REPORT_NAME    = "radar_report.md"
LOG_NAME       = "radar.log"

# ---------- SCORING / CONFIG ----------
DEFAULT_THRESHOLD = 70  # handoff to Scout if score >= threshold
W_RECENCY_MAX = 25
W_BUDGET_MAX  = 20
W_STEM_MAX    = 20
W_FIT_MAX     = 20
W_SRC_MAX     = 15

QUERY_TERMS = [
    'robotics', 'STEM', 'engineering', 'makerspace', 'CTE', '"career technical education"',
    'levy', 'bond', 'budget', 'millage', 'RFP', '"request for proposal"', 'grant'
]
HIGH_TRUST_TLDS   = ('.k12.', '.gov', '.edu')
KNOWN_NEWS_HINTS  = ('news', 'chronicle', 'gazette', 'press', 'daily', 'times', 'dispatch', 'beacon', 'abc', 'nbc', 'cbs', 'fox', 'mlive', 'freep')

# ---------- Built-in fallbacks (used only if seeds files are missing or invalid) ----------
BUILTIN_US_STATES = {
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","Florida","Georgia",
    "Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts",
    "Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey",
    "New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
    "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia","Washington","West Virginia",
    "Wisconsin","Wyoming","District of Columbia"
}
BUILTIN_STATE_ABBR = {
    "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA","Colorado":"CO","Connecticut":"CT",
    "Delaware":"DE","Florida":"FL","Georgia":"GA","Hawaii":"HI","Idaho":"ID","Illinois":"IL","Indiana":"IN","Iowa":"IA",
    "Kansas":"KS","Kentucky":"KY","Louisiana":"LA","Maine":"ME","Maryland":"MD","Massachusetts":"MA","Michigan":"MI",
    "Minnesota":"MN","Mississippi":"MS","Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV",
    "New Hampshire":"NH","New Jersey":"NJ","New Mexico":"NM","New York":"NY","North Carolina":"NC","North Dakota":"ND",
    "Ohio":"OH","Oklahoma":"OK","Oregon":"OR","Pennsylvania":"PA","Rhode Island":"RI","South Carolina":"SC",
    "South Dakota":"SD","Tennessee":"TN","Texas":"TX","Utah":"UT","Vermont":"VT","Virginia":"VA","Washington":"WA",
    "West Virginia":"WV","Wisconsin":"WI","Wyoming":"WY","District of Columbia":"DC"
}

# ---------- UTIL ----------
def paths(data_dir):
    return {
        "data_dir": data_dir,
        "radar_db": os.path.join(data_dir, RADAR_DB_NAME),
        "intake":   os.path.join(data_dir, INTAKE_NAME),
        "report":   os.path.join(data_dir, REPORT_NAME),
        "log":      os.path.join(SCRIPT_DIR, LOG_NAME),
    }

def ensure_dirs(data_dir):
    os.makedirs(data_dir, exist_ok=True)

def log(msg, log_path):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def slugify(text: str) -> str:
    t = (text or "").lower()
    t = re.sub(r"[^a-z0-9]+", "-", t)
    return t.strip("-")

def domain_of(link: str) -> str:
    try:
        return urlparse(link).netloc.lower()
    except Exception:
        return ""

def now_utc():
    return datetime.now(timezone.utc)

def parse_rfc822(dt_str: str):
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z",
                "%a, %d %b %Y %H:%M:%S %z",
                "%a, %d %b %Y %H:%M:%S GMT"):
        try:
            return datetime.strptime(dt_str, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(dt_str.replace("Z","")).replace(tzinfo=timezone.utc)
    except Exception:
        return None

def normalize_county(name: str) -> str:
    return re.sub(r"\s+county$", "", (name or "").strip(), flags=re.I).title()

def suggest_from_list(term: str, options: list, n=3, cutoff=0.72):
    return get_close_matches(term, options, n=n, cutoff=cutoff)

def confirm(prompt: str) -> bool:
    try:
        ans = input(prompt).strip().lower()
        return ans in ("y", "yes", "")
    except EOFError:
        return False

# ---------- SEEDS INTEGRATION (OPTION 2) ----------
def seeds_dir_for(scout_db_path: str) -> str:
    # typically: ...\Scout\data\seeds\
    base_dir = os.path.dirname(scout_db_path or "")
    return os.path.join(base_dir, "seeds")

def load_states_from_seeds(scout_db_path: str):
    sd = seeds_dir_for(scout_db_path)
    states = load_json(os.path.join(sd, "states.json"), None)
    counties = load_json(os.path.join(sd, "counties.json"), None)
    # Build states list from states.json; if missing, try counties.json keys; else fallback
    if isinstance(states, list) and states:
        return set([str(s) for s in states])
    if isinstance(counties, dict) and counties:
        return set([str(k) for k in counties.keys()])
    return set(BUILTIN_US_STATES)

def load_state_abbr_from_seeds(scout_db_path: str):
    sd = seeds_dir_for(scout_db_path)
    abbr = load_json(os.path.join(sd, "state_abbr.json"), None)
    if isinstance(abbr, dict) and abbr:
        # normalize keys to Title Case to match common input
        return {str(k): str(v) for k, v in abbr.items()}
    return dict(BUILTIN_STATE_ABBR)

def load_site_bias_from_seeds(scout_db_path: str):
    sd = seeds_dir_for(scout_db_path)
    bias = load_json(os.path.join(sd, "site_bias.json"), None)
    if isinstance(bias, dict):
        # ensure lists of strings
        out = {}
        for k, v in bias.items():
            if isinstance(v, list):
                out[str(k)] = [str(x) for x in v if isinstance(x, str)]
        return out
    return {}

def seeds_path_for(scout_db_path: str) -> str:
    # typically: ...\Scout\data\seeds\counties.json
    return os.path.join(seeds_dir_for(scout_db_path), "counties.json")

def load_county_seeds(scout_db_path: str):
    return load_json(seeds_path_for(scout_db_path), {})  # {"State": ["County", ...]}

def known_counties_for_state(scout_master: dict, seeds: dict, state: str):
    seeded = seeds.get(state, [])
    derived = []
    st = scout_master.get("states", {}).get(state, {})
    for d in st.get("districts", []):
        sc = d.get("source_county")
        if sc:
            derived.append(sc)
        for c in d.get("source_counties", []):
            if isinstance(c, str):
                derived.append(c)
    # de-dup, preserve capitalization
    seen, out = set(), []
    for c in seeded + derived:
        if c not in seen:
            out.append(c); seen.add(c)
    return out

# ---------- WATCHLIST FROM SCOUT ----------
def load_watchlist(scout_db_path, state=None, county=None, district=None):
    """
    Build a list of watch items. Ensures 'source_county' is present by falling back to
    the first element of 'source_counties' when needed. County filtering matches either
    source_county OR any entry in source_counties.
    """
    sm = load_json(scout_db_path, {"states": {}})
    items = []

    # Direct district run (bypass watchlist)
    if district:
        items.append({"state": state or "", "district": district, "source_county": county or ""})
        return items

    def county_of(record):
        sc = record.get("source_county")
        if not sc:
            scs = record.get("source_counties", [])
            sc = scs[0] if scs else ""
        return sc

    def row_for(st_name, record):
        sc = county_of(record)
        return {"state": st_name, **record, "source_county": sc}

    if not state:
        for st_name, st in sm.get("states", {}).items():
            for d in st.get("districts", []):
                items.append(row_for(st_name, d))
        return items

    st = sm.get("states", {}).get(state, {})
    for d in st.get("districts", []):
        if county:
            c_low = county.lower()
            sc = (d.get("source_county") or "").lower()
            scs = [c.lower() for c in d.get("source_counties", []) if isinstance(c, str)]
            if not (sc == c_low or c_low in scs):
                continue
        items.append(row_for(state, d))
    return items

def fit_score_for(scout_db_path, state, district):
    sm = load_json(scout_db_path, {"states": {}})
    st = sm.get("states", {}).get(state, {})
    for d in st.get("districts", []):
        if d.get("district","").lower() == (district or "").lower():
            try:
                return int(d.get("fit_score", 50))
            except Exception:
                return 50
    return 50

# ---------- TRIGGERS ----------
TRIGGERS = {
    "Funding & Facilities": re.compile(r"\b(levy|bond|millage|capital|facility|facilities|construction|maker(space)?)\b", re.I),
    "Policy & Strategy":    re.compile(r"\b(strategic plan|curriculum|policy|computer science requirement|plan)\b", re.I),
    "People Moves":         re.compile(r"\b(superintendent|cte director|stem coordinator|hired|appoint(ed|ment))\b", re.I),
    "Programs & Press":     re.compile(r"\b(robotics|vex|first robotics|stem night|engineering|makerspace|duckbowl)\b", re.I),
    "Procurement":          re.compile(r"\b(rfp|request for proposal|quote|bid|solicitation|purchase)\b", re.I),
}

def classify_trigger(text):
    for name, rx in TRIGGERS.items():
        if rx.search(text or ""):
            return name
    return "Other"

# ---------- SCORING ----------
def score_recency(published_iso):
    try:
        dt = datetime.fromisoformat(published_iso.replace("Z","")).astimezone(timezone.utc)
    except Exception:
        return 0
    days = (now_utc() - dt).days
    if days <= 3:  return W_RECENCY_MAX
    if days <= 7:  return 20
    if days <= 30: return 12
    if days <= 90: return 6
    return 0

def score_budget(text):
    s = 0
    if re.search(r"\b(levy|bond|millage|budget|appropriation)\b", text, re.I): s += 12
    if re.search(r"\b(rfp|request for proposal|bid|solicitation|quote)\b", text, re.I): s += 8
    if re.search(r"\$\s?\d|million|m\b", text, re.I): s += 6
    return min(s, W_BUDGET_MAX)

def score_stem(text):
    s = 0
    if re.search(r"\brobot(ic|ics)\b", text, re.I): s += 12
    if re.search(r"\bstem\b", text, re.I): s += 8
    if re.search(r"\b(engineering|makerspace|cte|career technical education)\b", text, re.I): s += 6
    return min(s, W_STEM_MAX)

def score_fit(scout_db_path, state, district):
    fs = fit_score_for(scout_db_path, state, district)
    return int(round(max(0, min(100, fs)) * (W_FIT_MAX/100.0)))

def score_source(link):
    dom = domain_of(link)
    if not dom: return 0
    if dom.endswith(HIGH_TRUST_TLDS) or any(tld in dom for tld in HIGH_TRUST_TLDS):
        return W_SRC_MAX
    if any(h in dom for h in KNOWN_NEWS_HINTS):
        return 12
    if re.search(r"\b(blog|medium|substack)\b", dom):
        return 4
    return 8

def compute_signal_score(scout_db_path, state, district, title, link, published):
    text = f"{title} {link}"
    rec = score_recency(published)
    bud = score_budget(text)
    stm = score_stem(text)
    fit = score_fit(scout_db_path, state, district)
    src = score_source(link)
    total = rec + bud + stm + fit + src
    return total, {"recency": rec, "budget": bud, "stem": stm, "fit": fit, "source": src}

# ---------- SITE BIAS (from seeds with safe fallback) ----------
def state_bias_sites(scout_db_path: str, state: str) -> str:
    """
    Build site bias using seeds/site_bias.json if present.
    Always include generic (.k12.us OR .gov OR .edu).
    If a state-specific bias is not found, try .k12.<abbr>.us from state_abbr.json.
    """
    bias_map = load_site_bias_from_seeds(scout_db_path)  # { "Ohio": [".k12.oh.us"], ... }
    abbr_map = load_state_abbr_from_seeds(scout_db_path)
    generic = ['site:.k12.us', 'site:.gov', 'site:.edu']
    extras = []

    if state:
        st = state.strip()
        if st in bias_map:
            extras.extend([f"site:{tld.lstrip('.')}" if tld.startswith('.') else f"site:{tld}" for tld in bias_map[st] if isinstance(tld, str)])
        else:
            abbr = abbr_map.get(st)
            if abbr:
                extras.append(f"site:.k12.{abbr.lower()}.us")
    # De-dup while preserving order
    seen = set()
    parts = []
    for token in generic + extras:
        tok = token if token.startswith("site:") else f"site:{token}"
        if tok not in seen:
            seen.add(tok); parts.append(tok)
    return " OR ".join(parts)

# ---------- QUERIES / FETCH ----------
def build_queries(scout_db_path, state, district):
    name = f'"{district}"'
    tail = f'("{state}" OR {name})'
    terms = " OR ".join(QUERY_TERMS)
    q = f'{name} ({terms}) {tail}'
    site_bias = f'({state_bias_sites(scout_db_path, state)})'
    q_full = f"{q} {site_bias}"
    return [
        ("gnews", f"https://news.google.com/rss/search?{urlencode({'q': q_full, 'hl':'en-US','gl':'US','ceid':'US:en'})}"),
        ("bing",  f"https://www.bing.com/news/search?q={quote_plus(q_full)}&format=rss"),
    ]

def fetch_rss(url, timeout=20):
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (Radar/1.0)"})
    with urlopen(req, timeout=timeout) as r:
        return r.read()

def parse_rss_items(xml_bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []
    items = []
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        link  = (it.findtext("link") or "").strip()
        pub   = (it.findtext("pubDate") or "").strip()
        pubdt = parse_rfc822(pub) or now_utc()
        items.append({"title": title, "link": link, "published": pubdt.isoformat()})
    return items

# ---------- PIPELINE ----------
def run_radar(scout_db_path, data_dir, state=None, county=None, district=None,
              max_items_per=6, since_days=120, threshold=DEFAULT_THRESHOLD,
              limit_districts=None, log_path=None):
    ensure_dirs(data_dir)
    P = paths(data_dir)
    if not log_path:
        log_path = P["log"]

    db     = load_json(P["radar_db"], {"signals": []})
    intake = load_json(P["intake"],   {"to_scout": []})

    # --- migrate old records that predate 'breakdown' / 'trigger' fields ---
    migrated = False
    for s in db.get("signals", []):
        if "trigger" not in s:
            s["trigger"] = classify_trigger(f"{s.get('title','')} {s.get('link','')}")
            migrated = True
        if "breakdown" not in s:
            _, br = compute_signal_score(
                scout_db_path,
                s.get("state",""),
                s.get("district",""),
                s.get("title",""),
                s.get("link",""),
                s.get("published",""),
            )
            s["breakdown"] = br
            migrated = True
    if migrated:
        save_json(P["radar_db"], db)

    existing_ids = { s.get("id") for s in db.get("signals", []) }

    watch = load_watchlist(scout_db_path, state=state, county=county, district=district)
    if limit_districts:
        watch = watch[:limit_districts]

    since_cutoff = now_utc() - timedelta(days=since_days)
    new_signals = []
    trigger_counts = {}

    for w in watch:
        st   = w.get("state","")
        dist = w.get("district","")
        src_county = w.get("source_county","")
        if not dist:
            continue

        for engine, url in build_queries(scout_db_path, st, dist):
            try:
                xml = fetch_rss(url)
            except Exception as e:
                log(f"Fetch error for {dist} [{engine}]: {e}", log_path)
                continue

            items = parse_rss_items(xml)
            for it in items[:max_items_per]:
                title = it["title"]
                link  = it["link"]
                pub   = it["published"]

                try:
                    pdt = datetime.fromisoformat(pub.replace("Z","")).astimezone(timezone.utc)
                except Exception:
                    pdt = now_utc()
                if pdt < since_cutoff:
                    continue

                trig = classify_trigger(f"{title} {link}")
                score, breakdown = compute_signal_score(scout_db_path, st, dist, title, link, pub)
                sig_id = slugify(f"{st}-{dist}-{title}-{link}")[:128]
                if sig_id in existing_ids:
                    continue

                rec = {
                    "id": sig_id,
                    "state": st,
                    "district": dist,
                    "source_county": src_county,
                    "title": title,
                    "link": link,
                    "published": pub,
                    "trigger": trig,
                    "score": score,
                    "breakdown": breakdown,
                    "created_at": now_utc().isoformat()
                }
                db["signals"].append(rec)
                new_signals.append(rec)
                existing_ids.add(sig_id)

                trigger_counts[trig] = trigger_counts.get(trig, 0) + 1

                if score >= threshold:
                    intake["to_scout"].append({
                        "state": st,
                        "district": dist,
                        "source_county": src_county,
                        "title": title,
                        "link": link,
                        "trigger": trig,
                        "score": score,
                        "published": pub,
                        "created_at": now_utc().isoformat()
                    })

    save_json(P["radar_db"], db)
    save_json(P["intake"], intake)
    write_report(P["report"], db, threshold)

    # Enhanced logging: per-trigger counts + top-5 signals by score
    if new_signals:
        top5 = sorted(new_signals, key=lambda s: s["score"], reverse=True)[:5]
        log("— Trigger counts this run —", log_path)
        for k in sorted(trigger_counts.keys()):
            log(f"  {k}: {trigger_counts[k]}", log_path)
        log("— Top 5 signals this run —", log_path)
        for s in top5:
            log(f'  [{s["score"]}] {s["district"]} — {s["title"]}', log_path)

    return new_signals, P

def write_report(report_path, db, threshold):
    lines = []
    lines.append(f"# Radar Report — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"_Threshold for handoff: {threshold}_")
    lines.append("")
    for s in sorted(db.get("signals", []), key=lambda x: x.get("created_at",""), reverse=True)[:500]:
        dt = s.get("published","")[:16]
        lines.append(f"- **{s['district']} ({s['state']})** — {s['title']}  ")
        lines.append(
            f"  - {s.get('trigger','?')} • score **{s['score']}** "
            f"(r{s['breakdown']['recency']}, b{s['breakdown']['budget']}, s{s['breakdown']['stem']}, "
            f"f{s['breakdown']['fit']}, c{s['breakdown']['source']}) • {dt}  "
        )
        lines.append(f"  - {s['link']}")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Radar — News & Signals watcher (reads seeds from Scout)")
    # paths
    ap.add_argument("--scout-db", type=str, default=DEFAULT_SCOUT_DB, help="Path to Scout's scout_master.json")
    ap.add_argument("--data-dir", type=str, default=DATA_DIR, help="Where Radar should write its own DB/report")
    # filters
    ap.add_argument("--state", type=str, help="Filter by state (e.g., Ohio)")
    ap.add_argument("--county", type=str, help="Filter by county (matches scout_master source_county)")
    ap.add_argument("--district", type=str, help='Run for a single district name (exact match)')
    # behavior
    ap.add_argument("--max-items-per", type=int, default=6, help="Max RSS items per engine per district")
    ap.add_argument("--since-days", type=int, default=120, help="Only include items newer than N days")
    ap.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD, help="Signal score to hand off to Scout")
    ap.add_argument("--limit-districts", type=int, help="Limit number of watchlist districts (for testing)")
    ap.add_argument("--open-report", action="store_true", help="Open Markdown report in Notepad after run")
    ap.add_argument("--no-spellcheck", action="store_true", help="Disable interactive spell-check")
    args = ap.parse_args()

    ensure_dirs(args.data_dir)
    P = paths(args.data_dir)

    # ---- Load seeds + master for spell-check / validation
    sm     = load_json(args.scout_db, {"states": {}})
    seeds  = load_county_seeds(args.scout_db)
    states_set = load_states_from_seeds(args.scout_db)  # Option 2
    # Fallback hard-coded if something goes weird
    if not states_set:
        states_set = set(BUILTIN_US_STATES)

    # State spell-check
    if args.state:
        st_raw = args.state.strip()
        if st_raw not in states_set:
            tc = st_raw.title()
            if tc in states_set:
                args.state = tc
            else:
                suggestion = get_close_matches(st_raw, sorted(list(states_set)), n=1, cutoff=0.75)
                if suggestion and not args.no_spellcheck:
                    if confirm(f"State '{st_raw}' not recognized. Did you mean '{suggestion[0]}'? [Y/n] "):
                        args.state = suggestion[0]
                    else:
                        print("Aborted due to unknown state."); return

    # County spell-check (only if state provided)
    if args.state and args.county:
        def _all_seed_counties(seeds_map: dict):
            pool = set()
            for lst in (seeds_map or {}).values():
                for c in (lst or []):
                    pool.add(normalize_county(c))
            return sorted(pool)

        known = known_counties_for_state(sm, seeds, args.state)
        # Build primary pool from the selected state
        norm_map = {normalize_county(k): k for k in known}
        c_norm = normalize_county(args.county)

        # If we have no counties for this state, use a cross-state fallback pool
        candidate_keys = list(norm_map.keys())
        fallback_used = False
        if not candidate_keys:
            candidate_keys = _all_seed_counties(seeds)
            # map to itself since we don't know the original casing from another state
            norm_map = {k: k for k in candidate_keys}
            fallback_used = True

        if candidate_keys and c_norm not in norm_map:
            sugg = suggest_from_list(c_norm, candidate_keys, n=1, cutoff=0.72)
            if sugg and not args.no_spellcheck:
                pretty = norm_map[sugg[0]]
                pool_label = "all-seeds" if fallback_used else args.state
                if confirm(f"County '{args.county}' not found for {args.state} (pool: {pool_label}). Did you mean '{pretty}'? [Y/n] "):
                    args.county = pretty


    # District helper (optional)
    if args.state and args.district:
        st = sm.get("states", {}).get(args.state, {})
        known_dists = [d.get("district","") for d in st.get("districts", []) if d.get("district")]
        if known_dists:
            dist_map = {d.lower(): d for d in known_dists}
            d_low = args.district.lower()
            if d_low not in dist_map:
                sugg = get_close_matches(d_low, list(dist_map.keys()), n=1, cutoff=0.8)
                if sugg and not args.no_spellcheck:
                    pretty = dist_map[sugg[0]]
                    if confirm(f"District '{args.district}' not found. Did you mean '{pretty}'? [Y/n] "):
                        args.district = pretty

    log(f"Radar start | scout_db={args.scout_db} state={args.state} county={args.county} district={args.district}", P["log"])

    new_items, P = run_radar(
        scout_db_path=args.scout_db,
        data_dir=args.data_dir,
        state=args.state,
        county=args.county,
        district=args.district,
        max_items_per=args.max_items_per,
        since_days=args.since_days,
        threshold=args.threshold,
        limit_districts=args.limit_districts,
        log_path=P["log"]
    )

    log(f"Radar done | new_signals={len(new_items)}", P["log"])
    print(f"Radar finished. New signals: {len(new_items)}")
    print(f"- DB: {P['radar_db']}\n- Intake for Scout: {P['intake']}\n- Report: {P['report']}")
    if args.open_report and os.path.exists(P["report"]):
        os.system('start "" notepad "{}"'.format(P["report"]))

if __name__ == "__main__":
    main()
