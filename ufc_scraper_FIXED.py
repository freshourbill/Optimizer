import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import re
from collections import defaultdict, Counter
from datetime import datetime, date
from typing import Optional, Tuple, Dict, List

# === CONFIG ===
# Updated to match your OneDrive path
CSV_PATH = r"C:\Users\fresh\OneDrive\Desktop\Real Website\fight data scraper\ufc_fight_data.csv"
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_REQUESTS = 1
UPDATE_EXISTING = False   # False = skip fights already in CSV; True = refresh fights on the latest date
BACKFILL_ALL = False      # False = only events newer than latest CSV date; True = scan all events to fill any gaps

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

# ---------- Normalization helpers ----------

def norm_key(k: str) -> str:
    """Normalize CSV header keys: strip BOM/ZW chars, trim, lowercase, spaces->underscore."""
    if k is None:
        return ""
    k = k.replace("\ufeff", "").replace("\u200b", "").replace("\u200c", "").replace("\u200d", "")
    k = k.strip().lower()
    k = re.sub(r"\s+", " ", k).replace(" ", "_")
    return k

def norm_name(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"\s+", " ", name).strip().casefold()

DATE_FORMATS = [
    "%B %d, %Y",   # August 23, 2025
    "%b %d, %Y",   # Aug 23, 2025
    "%Y-%m-%d",    # 2025-08-23
    "%m/%d/%Y",    # 08/23/2025
    "%d %B %Y",
    "%d %b %Y",
    "%b-%d-%Y",
    "%B-%d-%Y",
]

def parse_date_to_obj(s: str) -> Optional[date]:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s.strip())
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    s2 = s.replace(",", "")
    for fmt in [f.replace(",", "") for f in DATE_FORMATS]:
        try:
            return datetime.strptime(s2, fmt).date()
        except Exception:
            pass
    return None

def to_iso(d: Optional[date]) -> str:
    return d.isoformat() if isinstance(d, date) else ""

def legacy_key(date_str: str, f1: str, f2: str) -> str:
    """Order-insensitive key using ISO date + normalized names."""
    iso = to_iso(parse_date_to_obj(date_str))
    a, b = norm_name(f1), norm_name(f2)
    if a <= b:
        return f"{iso}|{a}|{b}"
    else:
        return f"{iso}|{b}|{a}"

# ---------- HTTP / scraping helpers ----------

def get_event_links():
    url = "http://www.ufcstats.com/statistics/events/completed?page=all"
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')
    return [a['href'] for a in soup.select('.b-statistics__table-events a[href*="/event-details/"]')]

def split_stat(td):
    """FIXED: Get stats from individual <p> tags, not combined text"""
    try:
        # Each td has 2 <p> tags: one for fighter 1, one for fighter 2
        p_tags = td.find_all('p')
        if len(p_tags) >= 2:
            val1 = p_tags[0].text.strip()
            val2 = p_tags[1].text.strip()
            # Extract just the first number from "X of Y" format
            match1 = re.match(r'(\d+)', val1)
            match2 = re.match(r'(\d+)', val2)
            return (match1.group(1) if match1 else "0", match2.group(1) if match2 else "0")
        if len(p_tags) == 1:
            val = p_tags[0].text.strip()
            match = re.match(r'(\d+)', val)
            num = match.group(1) if match else "0"
            return num, num
        return "0", "0"
    except Exception:
        return "0", "0"

def scrape_career_stats(soup):
    stats = {k:'Unknown' for k in [
        'SLpM','Str_Acc','SApM','Str_Def','TD_Avg','TD_Acc','TD_Def','Sub_Avg'
    ]}
    labels = {
        'SLpM:':'SLpM','Str. Acc.:':'Str_Acc','SApM:':'SApM','Str. Def:':'Str_Def',
        'TD Avg.:':'TD_Avg','TD Acc.:':'TD_Acc','TD Def.:':'TD_Def','Sub. Avg.:':'Sub_Avg'
    }
    for li in soup.select('.b-list__box-list.b-list__box-list_margin-top li'):
        it = li.find('i')
        if not it: continue
        key = labels.get(it.text.strip())
        if key:
            stats[key] = li.text.replace(it.text, '').strip().lstrip(':').strip()
    return stats

def fetch_with_retries(url: str, tries: int = 3, delay: float = 1.0) -> Optional[requests.Response]:
    for attempt in range(1, tries+1):
        try:
            r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == tries:
                print(f"    !! Failed GET {url}: {e}")
                return None
            time.sleep(delay)

def scrape_fighter_details(url) -> dict:
    r = fetch_with_retries(url)
    if r is None:
        return { 'record':'Unknown','height':'Unknown','weight':'Unknown','reach':'Unknown',
                 'stance':'Unknown','dob':'Unknown','SLpM':'Unknown','Str_Acc':'Unknown',
                 'SApM':'Unknown','Str_Def':'Unknown','TD_Avg':'Unknown','TD_Acc':'Unknown',
                 'TD_Def':'Unknown','Sub_Avg':'Unknown' }

    soup = BeautifulSoup(r.text, 'html.parser')
    info = { 'record':'Unknown','height':'Unknown','weight':'Unknown',
             'reach':'Unknown','stance':'Unknown','dob':'Unknown' }

    rec = soup.select_one('.b-content__title-record')
    if rec: info['record'] = rec.text.replace('Record:', '').strip()

    bio = soup.select_one('.b-list__info-box')
    if bio:
        for li in bio.select('.b-list__box-list li'):
            it = li.select_one('i')
            if not it: continue
            label = it.text.strip().lower()
            value = li.text.replace(it.text, '').replace(':','').strip()
            if   'height' in label: info['height'] = value
            elif 'weight' in label: info['weight'] = value
            elif 'reach'  in label: info['reach']  = value
            elif 'stance' in label: info['stance'] = value
            elif 'dob'    in label: info['dob']    = value

    return {**info, **scrape_career_stats(soup)}

def scrape_fight_details(fight_url: str) -> dict:
    """NEW: Scrape detailed stats from individual fight page (totals only)"""
    r = fetch_with_retries(fight_url)
    if r is None:
        return {}
    
    soup = BeautifulSoup(r.text, 'html.parser')
    stats = {}
    
    # Find the "Totals" section (not per-round)
    sections = soup.find_all('section', class_='b-fight-details__section')
    
    totals_table = None
    sig_strikes_table = None
    
    for i, section in enumerate(sections):
        # Look for "Totals" heading
        heading = section.find('p', class_='b-fight-details__collapse-link_tot')
        if heading:
            heading_text = heading.text.strip()
            
            # First "Totals" = main stats table
            if 'Totals' in heading_text and not totals_table:
                # Get the next table after this section
                totals_table = section.find_next('table')
            
            # "Significant Strikes" heading = detailed strikes table
            elif 'Significant Strikes' in heading_text and not sig_strikes_table:
                sig_strikes_table = section.find_next('table')
    
    # Extract from totals table (KD, Sig Str, TD, Sub, etc.)
    if totals_table:
        tbody = totals_table.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')
            # Skip rows with "Round" headers, get only the totals row
            for row in rows:
                # Skip round header rows
                th = row.find('th')
                if th and 'Round' in th.text:
                    continue
                
                cols = row.find_all('td')
                if len(cols) >= 10:
                    # This is the totals row!
                    # Columns: 0=fighters, 1=KD, 2=Sig.str, 3=Sig.str%, 4=Total.str, 5=Td, 6=Td%, 7=Sub, 8=Rev, 9=Ctrl
                    
                    f1_kd, f2_kd = split_stat(cols[1])
                    stats['f1_total_kd'] = f1_kd
                    stats['f2_total_kd'] = f2_kd
                    
                    # Sig str - need landed AND attempted
                    p_tags = cols[2].find_all('p')
                    if len(p_tags) >= 2:
                        f1_sig = p_tags[0].text.strip()
                        f2_sig = p_tags[1].text.strip()
                        
                        f1_match = re.match(r'(\d+)\s+of\s+(\d+)', f1_sig)
                        f2_match = re.match(r'(\d+)\s+of\s+(\d+)', f2_sig)
                        
                        stats['f1_sig_str_landed'] = f1_match.group(1) if f1_match else '0'
                        stats['f1_sig_str_attempted'] = f1_match.group(2) if f1_match else '0'
                        stats['f2_sig_str_landed'] = f2_match.group(1) if f2_match else '0'
                        stats['f2_sig_str_attempted'] = f2_match.group(2) if f2_match else '0'
                    
                    # Sig str %
                    p_tags = cols[3].find_all('p')
                    if len(p_tags) >= 2:
                        f1_pct = p_tags[0].text.strip().replace('%', '')
                        f2_pct = p_tags[1].text.strip().replace('%', '')
                        stats['f1_sig_str_pct'] = f1_pct if f1_pct != '---' else '0'
                        stats['f2_sig_str_pct'] = f2_pct if f2_pct != '---' else '0'
                    
                    # Total str
                    p_tags = cols[4].find_all('p')
                    if len(p_tags) >= 2:
                        f1_total = p_tags[0].text.strip()
                        f2_total = p_tags[1].text.strip()
                        
                        f1_match = re.match(r'(\d+)\s+of\s+(\d+)', f1_total)
                        f2_match = re.match(r'(\d+)\s+of\s+(\d+)', f2_total)
                        
                        stats['f1_total_str_landed'] = f1_match.group(1) if f1_match else '0'
                        stats['f1_total_str_attempted'] = f1_match.group(2) if f1_match else '0'
                        stats['f2_total_str_landed'] = f2_match.group(1) if f2_match else '0'
                        stats['f2_total_str_attempted'] = f2_match.group(2) if f2_match else '0'
                    
                    # TD
                    p_tags = cols[5].find_all('p')
                    if len(p_tags) >= 2:
                        f1_td = p_tags[0].text.strip()
                        f2_td = p_tags[1].text.strip()
                        
                        f1_match = re.match(r'(\d+)\s+of\s+(\d+)', f1_td)
                        f2_match = re.match(r'(\d+)\s+of\s+(\d+)', f2_td)
                        
                        stats['f1_td_landed'] = f1_match.group(1) if f1_match else '0'
                        stats['f1_td_attempted'] = f1_match.group(2) if f1_match else '0'
                        stats['f2_td_landed'] = f2_match.group(1) if f2_match else '0'
                        stats['f2_td_attempted'] = f2_match.group(2) if f2_match else '0'
                    
                    # TD %
                    p_tags = cols[6].find_all('p')
                    if len(p_tags) >= 2:
                        f1_td_pct = p_tags[0].text.strip().replace('%', '')
                        f2_td_pct = p_tags[1].text.strip().replace('%', '')
                        stats['f1_td_pct'] = f1_td_pct if f1_td_pct != '---' else '0'
                        stats['f2_td_pct'] = f2_td_pct if f2_td_pct != '---' else '0'
                    
                    # Sub att
                    f1_sub, f2_sub = split_stat(cols[7])
                    stats['f1_sub_att'] = f1_sub
                    stats['f2_sub_att'] = f2_sub
                    
                    # Reversals
                    f1_rev, f2_rev = split_stat(cols[8])
                    stats['f1_reversals'] = f1_rev
                    stats['f2_reversals'] = f2_rev
                    
                    # Control time
                    p_tags = cols[9].find_all('p')
                    if len(p_tags) >= 2:
                        stats['f1_ctrl_time'] = p_tags[0].text.strip()
                        stats['f2_ctrl_time'] = p_tags[1].text.strip()
                    
                    break  # Found totals row, exit loop
    
    # Extract from significant strikes detail table (Head, Body, Leg, etc.)
    if sig_strikes_table:
        tbody = sig_strikes_table.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')
            # Again, skip round headers
            for row in rows:
                th = row.find('th')
                if th and 'Round' in th.text:
                    continue
                
                cols = row.find_all('td')
                if len(cols) >= 9:
                    # Columns: 0=fighters, 1=sig.str, 2=sig.str%, 3=head, 4=body, 5=leg, 6=distance, 7=clinch, 8=ground
                    
                    # Head
                    p_tags = cols[3].find_all('p')
                    if len(p_tags) >= 2:
                        f1_head = p_tags[0].text.strip()
                        f2_head = p_tags[1].text.strip()
                        
                        # Parse "X of Y" format - extract BOTH numbers
                        f1_match = re.match(r'(\d+)\s+of\s+(\d+)', f1_head)
                        f2_match = re.match(r'(\d+)\s+of\s+(\d+)', f2_head)
                        
                        if f1_match:
                            stats['f1_head_landed'] = f1_match.group(1)      # Just the landed (5)
                            stats['f1_head_attempted'] = f1_match.group(2)   # Just the attempted (24)
                        else:
                            stats['f1_head_landed'] = '0'
                            stats['f1_head_attempted'] = '0'
                        
                        if f2_match:
                            stats['f2_head_landed'] = f2_match.group(1)      # Just the landed
                            stats['f2_head_attempted'] = f2_match.group(2)   # Just the attempted
                        else:
                            stats['f2_head_landed'] = '0'
                            stats['f2_head_attempted'] = '0'
                    else:
                        stats['f1_head_landed'] = '0'
                        stats['f1_head_attempted'] = '0'
                        stats['f2_head_landed'] = '0'
                        stats['f2_head_attempted'] = '0'
                    
                    # Body
                    p_tags = cols[4].find_all('p')
                    if len(p_tags) >= 2:
                        f1_body = p_tags[0].text.strip()
                        f2_body = p_tags[1].text.strip()
                        
                        f1_match = re.match(r'(\d+)\s+of\s+(\d+)', f1_body)
                        f2_match = re.match(r'(\d+)\s+of\s+(\d+)', f2_body)
                        
                        stats['f1_body_landed'] = f1_match.group(1) if f1_match else '0'
                        stats['f1_body_attempted'] = f1_match.group(2) if f1_match else '0'
                        stats['f2_body_landed'] = f2_match.group(1) if f2_match else '0'
                        stats['f2_body_attempted'] = f2_match.group(2) if f2_match else '0'
                    
                    # Leg
                    p_tags = cols[5].find_all('p')
                    if len(p_tags) >= 2:
                        f1_leg = p_tags[0].text.strip()
                        f2_leg = p_tags[1].text.strip()
                        
                        f1_match = re.match(r'(\d+)\s+of\s+(\d+)', f1_leg)
                        f2_match = re.match(r'(\d+)\s+of\s+(\d+)', f2_leg)
                        
                        stats['f1_leg_landed'] = f1_match.group(1) if f1_match else '0'
                        stats['f1_leg_attempted'] = f1_match.group(2) if f1_match else '0'
                        stats['f2_leg_landed'] = f2_match.group(1) if f2_match else '0'
                        stats['f2_leg_attempted'] = f2_match.group(2) if f2_match else '0'
                    
                    # Distance
                    p_tags = cols[6].find_all('p')
                    if len(p_tags) >= 2:
                        f1_dist = p_tags[0].text.strip()
                        f2_dist = p_tags[1].text.strip()
                        
                        f1_match = re.match(r'(\d+)\s+of\s+(\d+)', f1_dist)
                        f2_match = re.match(r'(\d+)\s+of\s+(\d+)', f2_dist)
                        
                        stats['f1_distance_landed'] = f1_match.group(1) if f1_match else '0'
                        stats['f1_distance_attempted'] = f1_match.group(2) if f1_match else '0'
                        stats['f2_distance_landed'] = f2_match.group(1) if f2_match else '0'
                        stats['f2_distance_attempted'] = f2_match.group(2) if f2_match else '0'
                    
                    # Clinch
                    p_tags = cols[7].find_all('p')
                    if len(p_tags) >= 2:
                        f1_clinch = p_tags[0].text.strip()
                        f2_clinch = p_tags[1].text.strip()
                        
                        f1_match = re.match(r'(\d+)\s+of\s+(\d+)', f1_clinch)
                        f2_match = re.match(r'(\d+)\s+of\s+(\d+)', f2_clinch)
                        
                        stats['f1_clinch_landed'] = f1_match.group(1) if f1_match else '0'
                        stats['f1_clinch_attempted'] = f1_match.group(2) if f1_match else '0'
                        stats['f2_clinch_landed'] = f2_match.group(1) if f2_match else '0'
                        stats['f2_clinch_attempted'] = f2_match.group(2) if f2_match else '0'
                    
                    # Ground
                    p_tags = cols[8].find_all('p')
                    if len(p_tags) >= 2:
                        f1_ground = p_tags[0].text.strip()
                        f2_ground = p_tags[1].text.strip()
                        
                        f1_match = re.match(r'(\d+)\s+of\s+(\d+)', f1_ground)
                        f2_match = re.match(r'(\d+)\s+of\s+(\d+)', f2_ground)
                        
                        stats['f1_ground_landed'] = f1_match.group(1) if f1_match else '0'
                        stats['f1_ground_attempted'] = f1_match.group(2) if f1_match else '0'
                        stats['f2_ground_landed'] = f2_match.group(1) if f2_match else '0'
                        stats['f2_ground_attempted'] = f2_match.group(2) if f2_match else '0'
                    
                    break  # Found totals row, exit loop
    
    return stats

def extract_event_meta(event_url) -> Tuple[BeautifulSoup, Optional[date], str, str]:
    r = fetch_with_retries(event_url)
    if r is None:
        return BeautifulSoup("", 'html.parser'), None, "", ""
    soup = BeautifulSoup(r.text, 'html.parser')
    details = soup.find('div', class_='b-fight-details')
    lis = details.find_all('li') if details else []
    date_str = lis[0].text.strip().replace("Date:", "").strip() if lis else "Unknown"
    loc_str  = lis[1].text.strip().replace("Location:", "").strip() if len(lis) > 1 else "Unknown"
    d_obj = parse_date_to_obj(date_str)
    return soup, d_obj, date_str, loc_str

# ---------- CSV helpers ----------

def map_stats(prefix: str, stats: dict) -> dict:
    """Map UFCStats keys to your CSV's lower_snake_case columns."""
    return {
        f"{prefix}_slpm":     stats.get("SLpM", "Unknown"),
        f"{prefix}_str_acc":  stats.get("Str_Acc", "Unknown"),
        f"{prefix}_sapm":     stats.get("SApM", "Unknown"),
        f"{prefix}_str_def":  stats.get("Str_Def", "Unknown"),
        f"{prefix}_td_avg":   stats.get("TD_Avg", "Unknown"),
        f"{prefix}_td_acc":   stats.get("TD_Acc", "Unknown"),
        f"{prefix}_td_def":   stats.get("TD_Def", "Unknown"),
        f"{prefix}_sub_avg":  stats.get("Sub_Avg", "Unknown"),
    }

def merge_rows(old_row, new_row):
    merged = dict(old_row) if old_row else {}
    for k, v in new_row.items():
        if v is None: continue
        sv = str(v).strip()
        if (not sv) or (sv.lower() == 'unknown'):
            if k in merged and str(merged[k]).strip():
                continue
        merged[k] = sv
    return merged

def load_existing_csv(csv_path):
    """Read CSV, normalize headers, build {iso|f1|f2 -> row}, find max date."""
    rows, key_to_row, headers = [], {}, []
    if not os.path.exists(csv_path):
        return rows, key_to_row, headers, None

    def _read(enc):
        with open(csv_path, 'r', encoding=enc, newline='') as f:
            reader = csv.reader(f)
            raw_headers = next(reader, [])
            norm_headers = [norm_key(h) for h in raw_headers]
            dict_rows = []
            for raw in reader:
                row = {}
                for i, val in enumerate(raw):
                    if i < len(norm_headers):
                        row[norm_headers[i]] = val
                dict_rows.append(row)
            return dict_rows, norm_headers

    try:
        rows, headers = _read('utf-8')
    except UnicodeDecodeError:
        for enc in ('cp1252', 'latin-1'):
            try:
                rows, headers = _read(enc); break
            except UnicodeDecodeError:
                continue
        else:
            raise

    candidate_date_cols = ['event_date', 'date']
    date_col = next((c for c in candidate_date_cols if c in headers), None)

    max_date = None
    for r in rows:
        r_norm = {norm_key(k): v for k, v in r.items()}
        date_val = r_norm.get(date_col, '') if date_col else (r_norm.get('event_date', '') or r_norm.get('date', ''))
        f1 = r_norm.get('fighter_1', '')
        f2 = r_norm.get('fighter_2', '')
        k = legacy_key(date_val, f1, f2)
        key_to_row[k] = r  # keep row as loaded
        d = parse_date_to_obj(date_val)
        if d and (max_date is None or d > max_date):
            max_date = d

    return rows, key_to_row, headers, max_date

def write_csv(csv_path, rows, headers_priority):
    # Create directory if it doesn't exist
    csv_dir = os.path.dirname(csv_path)
    if csv_dir and not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
        print(f"Created directory: {csv_dir}")
    
    header_set = set()
    for r in rows:
        header_set.update(r.keys())
    seen = set()
    ordered = []
    for h in headers_priority:
        if h not in seen:
            ordered.append(h); seen.add(h)
    for h in sorted(header_set):
        if h not in seen:
            ordered.append(h); seen.add(h)

    with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=ordered)
        w.writeheader()
        for r in rows:
            w.writerow({h: r.get(h, '') for h in ordered})

# ---------- Enrichment: born/gym backfill, UFC counters, active flags ----------

BLANK_TOKENS = {"", "unknown", "n/a", "na", "null", "none", "-", "—"}

def is_blank(v: Optional[str]) -> bool:
    if v is None:
        return True
    return str(v).strip().lower() in BLANK_TOKENS

PROFILE_ATTRS = [
    'record','height','weight','reach','stance','dob',
    'slpm','str_acc','sapm','str_def','td_avg','td_acc','td_def','sub_avg'
]

def build_fighter_kb(rows: List[dict]) -> Dict[str, dict]:
    """
    Build a per-fighter KB:
      - born_counts: Counter of 'born'
      - born_timeline: list[(date, born)]
      - gym_counts: Counter of 'gym'
      - gym_timeline: list[(date, gym)]
      - last_seen_date: most recent event date (for active flag)
    """
    kb: Dict[str, dict] = {}
    for r in rows:
        d = parse_date_to_obj(r.get('event_date', ''))
        if not d:
            continue
        for side in ('fighter_1', 'fighter_2'):
            name = r.get(side, '')
            if not name:
                continue
            key = norm_name(name)
            kbe = kb.setdefault(key, {
                'born_counts': Counter(),
                'born_timeline': [],
                'gym_counts': Counter(),
                'gym_timeline': [],
                'last_seen_date': date.min,
            })
            born_val = r.get(f'{side}_born', '')
            if not is_blank(born_val):
                kbe['born_counts'][born_val] += 1
                kbe['born_timeline'].append((d, born_val))
            gym_val = r.get(f'{side}_gym', '')
            if not is_blank(gym_val):
                kbe['gym_counts'][gym_val] += 1
                kbe['gym_timeline'].append((d, gym_val))
            if d > kbe['last_seen_date']:
                kbe['last_seen_date'] = d

    # sort timelines
    for kbe in kb.values():
        kbe['born_timeline'].sort(key=lambda x: x[0])
        kbe['gym_timeline'].sort(key=lambda x: x[0])
    return kb

def choose_mode_with_recent_fallback(counter: Counter, timeline: List[Tuple[date, str]]) -> Optional[str]:
    if counter:
        most_common = counter.most_common()
        top_count = most_common[0][1]
        candidates = [val for val, cnt in most_common if cnt == top_count]
        if len(candidates) == 1:
            return candidates[0]
        # tie: pick the most recent in timeline among candidates
        for d, val in reversed(timeline):  # latest first
            if val in candidates:
                return val
    # fallback: most recent non-blank in timeline
    for d, val in reversed(timeline):
        if not is_blank(val):
            return val
    return None

def find_gym_for_date(timeline: List[Tuple[date, str]], target: date, mode_value: Optional[str]) -> Optional[str]:
    # last <= target
    prev_val = None
    for d, val in timeline:
        if d <= target and not is_blank(val):
            prev_val = val
        if d > target:
            break
    if prev_val:
        return prev_val
    # earliest >= target
    for d, val in timeline:
        if d >= target and not is_blank(val):
            return val
    # fallback to mode
    return mode_value

def backfill_born_gym(rows: List[dict], kb: Dict[str, dict]) -> None:
    for r in rows:
        d = parse_date_to_obj(r.get('event_date', '')) or date.min
        for side in ('fighter_1', 'fighter_2'):
            name = r.get(side, '')
            if not name:
                continue
            key = norm_name(name)
            info = kb.get(key)
            if not info:
                continue

            # born
            col_born = f'{side}_born'
            if is_blank(r.get(col_born, '')):
                born_best = choose_mode_with_recent_fallback(info['born_counts'], info['born_timeline'])
                if born_best and not is_blank(born_best):
                    r[col_born] = born_best

            # gym
            col_gym = f'{side}_gym'
            if is_blank(r.get(col_gym, '')):
                gym_mode = info['gym_counts'].most_common(1)[0][0] if info['gym_counts'] else None
                gym_best = find_gym_for_date(info['gym_timeline'], d, gym_mode)
                if gym_best and not is_blank(gym_best):
                    r[col_gym] = gym_best

def compute_ufc_counters(rows: List[dict]) -> None:
    """
    For each fighter, sort fights by date and set:
      - fighter_[1|2]_ufcwins / fighter_[1|2]_ufcloss BEFORE that fight.
    Assumes every row is a UFC bout (true for UFCStats).
    """
    timeline = defaultdict(list)  # fighter -> [(date, side, row)]
    for r in rows:
        d = parse_date_to_obj(r.get('event_date', '')) or date.min
        n1 = norm_name(r.get('fighter_1', ''))
        n2 = norm_name(r.get('fighter_2', ''))
        if n1: timeline[n1].append((d, 'f1', r))
        if n2: timeline[n2].append((d, 'f2', r))

    for fighter, items in timeline.items():
        items.sort(key=lambda x: x[0])  # chronological
        wins = losses = 0
        for d, side, r in items:
            # write counters BEFORE the fight
            if side == 'f1':
                r['fighter_1_ufcwins'] = str(wins)
                r['fighter_1_ufcloss'] = str(losses)
            else:
                r['fighter_2_ufcwins'] = str(wins)
                r['fighter_2_ufcloss'] = str(losses)

            # update AFTER the fight based on fighter_1-centric "result"
            res = (r.get('result', '') or '').strip().lower()
            # treat draw/NC as no change
            if res.startswith('win'):
                if side == 'f1':
                    wins += 1
                else:
                    losses += 1
            elif res.startswith('loss') or res.startswith('l'):
                if side == 'f1':
                    losses += 1
                else:
                    wins += 1
            # else: draw/NC -> no change

def apply_active_flags(rows: List[dict], years: int = 3) -> None:
    """fighter_[1|2]_active = TRUE if last fight ≤ 3 years ago (relative to today)."""
    last_seen: Dict[str, date] = {}
    for r in rows:
        d = parse_date_to_obj(r.get('event_date', ''))
        if not d:
            continue
        for side in ('fighter_1', 'fighter_2'):
            n = norm_name(r.get(side, ''))
            if not n:
                continue
            if (n not in last_seen) or (d > last_seen[n]):
                last_seen[n] = d

    today = date.today()
    cutoff_days = int(365.25 * years)

    for r in rows:
        for side in ('fighter_1', 'fighter_2'):
            n = norm_name(r.get(side, ''))
            active = False
            last = last_seen.get(n)
            if last:
                active = (today - last).days <= cutoff_days
            r[f'{side}_active'] = 'TRUE' if active else 'FALSE'

# ---------- Main ----------

def main():
    # Priority columns (includes requested new columns)
    fieldnames_priority = [
        'event_date', 'event_location', 'fighter_1', 'fighter_2', 'result',
        'method_main', 'method_detail', 'round', 'time',
        'fighter_1_kd', 'fighter_1_str', 'fighter_1_td', 'fighter_1_sub',
        'fighter_2_kd', 'fighter_2_str', 'fighter_2_td', 'fighter_2_sub',
        'weight_class',
        'fighter_1_record', 'fighter_1_height', 'fighter_1_weight', 'fighter_1_reach',
        'fighter_1_stance', 'fighter_1_dob',
        'fighter_1_slpm','fighter_1_str_acc','fighter_1_sapm','fighter_1_str_def',
        'fighter_1_td_avg','fighter_1_td_acc','fighter_1_td_def','fighter_1_sub_avg',
        'fighter_2_record', 'fighter_2_height', 'fighter_2_weight', 'fighter_2_reach',
        'fighter_2_stance', 'fighter_2_dob',
        'fighter_2_slpm','fighter_2_str_acc','fighter_2_sapm','fighter_2_str_def',
        'fighter_2_td_avg','fighter_2_td_acc','fighter_2_td_def','fighter_2_sub_avg',
        # New/derived columns you asked for:
        'fighter_1_born','fighter_1_gym','fighter_2_born','fighter_2_gym',
        'fighter_1_ufcwins','fighter_1_ufcloss','fighter_2_ufcwins','fighter_2_ufcloss',
        'fighter_1_active','fighter_2_active',
        # Fight details (NEW):
        'f1_total_kd','f2_total_kd',
        'f1_sig_str_landed','f1_sig_str_attempted','f1_sig_str_pct',
        'f2_sig_str_landed','f2_sig_str_attempted','f2_sig_str_pct',
        'f1_total_str_landed','f1_total_str_attempted',
        'f2_total_str_landed','f2_total_str_attempted',
        'f1_td_landed','f1_td_attempted','f1_td_pct',
        'f2_td_landed','f2_td_attempted','f2_td_pct',
        'f1_sub_att','f2_sub_att',
        'f1_reversals','f2_reversals',
        'f1_ctrl_time','f2_ctrl_time',
        'f1_head_landed','f1_head_attempted',
        'f2_head_landed','f2_head_attempted',
        'f1_body_landed','f1_body_attempted',
        'f2_body_landed','f2_body_attempted',
        'f1_leg_landed','f1_leg_attempted',
        'f2_leg_landed','f2_leg_attempted',
        'f1_distance_landed','f1_distance_attempted',
        'f2_distance_landed','f2_distance_attempted',
        'f1_clinch_landed','f1_clinch_attempted',
        'f2_clinch_landed','f2_clinch_attempted',
        'f1_ground_landed','f1_ground_attempted',
        'f2_ground_landed','f2_ground_attempted',
        # If you already have these in your CSV, keeping them here ensures stable ordering:
        'fighter_1_wins','fighter_2_wins',
    ]

    _, key_to_row, _, max_date = load_existing_csv(CSV_PATH)
    print(f"Latest date in CSV: {max_date if max_date else 'None'}")

    event_links = get_event_links()
    print(f"\n📊 Found {len(event_links)} total events")
        
    # LIMIT TO FIRST 5 FOR TESTING
  testr    
    total_new = total_updated = 0

    for i, event_link in enumerate(event_links, 1):
        soup, event_date_obj, event_date_str, event_loc = extract_event_meta(event_link)

        # Date gating (list is newest -> oldest)
        if not BACKFILL_ALL and (max_date is not None):
            if UPDATE_EXISTING:
                if (event_date_obj is None) or (event_date_obj < max_date):
                    break
            else:
                if (event_date_obj is None) or (event_date_obj <= max_date):
                    break

        print(f"\n[{i}/{len(event_links)}] {event_date_str} | {event_loc}")

        table = soup.find('table', class_='b-fight-details__table')
        if not table:
            print("  (No fight table)")
            continue

        new_count = updated_count = 0

        for row in table.find_all('tr')[1:]:
            cols = row.find_all('td')
            if len(cols) < 10:
                continue

            # Get fighter names and their detail links
            fighter_links = cols[1].find_all('a')
            if len(fighter_links) < 2:
                continue
            f1_tag, f2_tag = fighter_links[0], fighter_links[1]
            f1_name, f2_name = f1_tag.text.strip(), f2_tag.text.strip()

            k = legacy_key(event_date_str, f1_name, f2_name)
            have_already = k in key_to_row

            # Skip heavy requests if already in CSV and not refreshing
            if have_already and not UPDATE_EXISTING:
                continue

            result = cols[0].text.strip()
            method_ps = cols[7].find_all("p")
            method_main = method_ps[0].text.strip() if method_ps else ""
            method_detail = method_ps[1].text.strip() if len(method_ps) > 1 else ""
            round_ = cols[8].text.strip()
            time_ = cols[9].text.strip()

            f1_kd, f2_kd = split_stat(cols[2])
            f1_str, f2_str = split_stat(cols[3])
            f1_td,  f2_td  = split_stat(cols[4])
            f1_sub, f2_sub = split_stat(cols[5])
            weight_class = ' '.join(cols[6].text.split())

            # Heavy only when needed (new or updating)
            f1_stats = scrape_fighter_details(f1_tag['href'])
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            f2_stats = scrape_fighter_details(f2_tag['href'])
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            
            # NEW: Get detailed fight stats from individual fight page
            # The fight details link is in the fighter name column (cols[1])
            fight_details = {}
            fight_url = None
            
            # Try to find the fight details link
            # It's usually an onclick or data attribute on the row, or we construct it
            # For UFCStats, we need to construct the URL from the event page
            # The fight links are actually embedded in each row's data-link attribute or similar
            
            # Let's try finding it in the row itself
            parent_tr = cols[0].find_parent('tr')
            if parent_tr:
                # Look for data-link attribute or any link in the row
                for a_tag in parent_tr.find_all('a'):
                    href = a_tag.get('href', '')
                    if '/fight-details/' in href:
                        fight_url = href
                        break
            
            if not fight_url:
                # If we can't find it in the row, try the result column
                result_links = cols[0].find_all('a')
                for link in result_links:
                    href = link.get('href', '')
                    if '/fight-details/' in href:
                        fight_url = href
                        break
            
            # If we found a fight URL, scrape details
            if fight_url:
                print(f"    📥 {f1_name} vs {f2_name}")
                print(f"       Getting details from: {fight_url}")
                fight_details = scrape_fight_details(fight_url)
                
                # DEBUG: Show what we got
                if fight_details:
                    print(f"       ✅ Got {len(fight_details)} stats:")
                    print(f"          F1 Sig Strikes: {fight_details.get('f1_sig_str_landed', 'N/A')} of {fight_details.get('f1_sig_str_attempted', 'N/A')}")
                    print(f"          F2 Sig Strikes: {fight_details.get('f2_sig_str_landed', 'N/A')} of {fight_details.get('f2_sig_str_attempted', 'N/A')}")
                    print(f"          F1 Head: {fight_details.get('f1_head_landed', 'N/A')} of {fight_details.get('f1_head_attempted', 'N/A')}")
                    print(f"          F2 Head: {fight_details.get('f2_head_landed', 'N/A')} of {fight_details.get('f2_head_attempted', 'N/A')}")
                else:
                    print(f"       ⚠️  No detailed stats found")
                
                time.sleep(SLEEP_BETWEEN_REQUESTS)
            else:
                print(f"    ⚠️  {f1_name} vs {f2_name} - No fight details URL found")

            rowdict = {
                'event_date': event_date_str,
                'event_location': event_loc,
                'fighter_1': f1_name,
                'fighter_2': f2_name,
                'result': result,
                'method_main': method_main,
                'method_detail': method_detail,
                'round': round_,
                'time': time_,
                'fighter_1_kd': f1_kd,
                'fighter_1_str': f1_str,
                'fighter_1_td': f1_td,
                'fighter_1_sub': f1_sub,
                'fighter_2_kd': f2_kd,
                'fighter_2_str': f2_str,
                'fighter_2_td': f2_td,
                'fighter_2_sub': f2_sub,
                'weight_class': weight_class,
                'fighter_1_record': f"'{f1_stats['record']}",
                'fighter_1_height': f1_stats['height'],
                'fighter_1_weight': f1_stats['weight'],
                'fighter_1_reach': f1_stats['reach'],
                'fighter_1_stance': f1_stats['stance'],
                'fighter_1_dob': f1_stats['dob'],
                'fighter_2_record': f"'{f2_stats['record']}",
                'fighter_2_height': f2_stats['height'],
                'fighter_2_weight': f2_stats['weight'],
                'fighter_2_reach': f2_stats['reach'],
                'fighter_2_stance': f2_stats['stance'],
                'fighter_2_dob': f2_stats['dob'],
            }

            # Map career stats to your lowercase schema
            rowdict.update(map_stats('fighter_1', f1_stats))
            rowdict.update(map_stats('fighter_2', f2_stats))
            
            # Add detailed fight stats
            rowdict.update(fight_details)

            if have_already:
                merged = merge_rows(key_to_row[k], rowdict)
                if merged != key_to_row[k]:
                    key_to_row[k] = merged
                    updated_count += 1
            else:
                key_to_row[k] = rowdict
                new_count += 1

        total_new += new_count
        total_updated += updated_count
        print(f"  Added: {new_count} | Updated: {updated_count} | Total so far: {len(key_to_row)}")
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    # -------- After scraping: enrich from existing data --------
    final_rows = list(key_to_row.values())

    # 1) Build fighter knowledge base and backfill born/gym
    kb = build_fighter_kb(final_rows)
    backfill_born_gym(final_rows, kb)

    # 2) Compute UFC wins/losses at time of fight
    compute_ufc_counters(final_rows)

    # 3) Apply active flags (last 3 years)
    apply_active_flags(final_rows, years=3)

    # Write out everything (existing + new)
    write_csv(CSV_PATH, final_rows, fieldnames_priority)
    print(f"\n✅ Done. {len(final_rows)} total unique fights saved to '{CSV_PATH}'")
    print(f"New this run: {total_new} | Updated: {total_updated}")
    
    # TEST MODE SUMMARY
    print("\n" + "="*60)
    print("="*60)
    print(f"Events scraped: 5")
    print(f"Total fights in CSV: {len(final_rows)}")
    print(f"New fights added: {total_new}")
    print(f"Fights updated: {total_updated}")
    print("\n💡 Check your CSV to verify the data looks correct!")
    print("   Look for columns like:")
    print("   - f1_sig_str_landed, f2_sig_str_landed")
    print("   - f1_head_landed, f2_head_landed")
    print("   - f1_body_landed, f2_body_landed")
    print("\n   Values should be clean numbers (not 'X of Y' format)")
    print("   Each fighter should have separate columns")
    print("="*60)

if __name__ == "__main__":
    main()
