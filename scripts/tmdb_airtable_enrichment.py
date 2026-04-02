#!/usr/bin/env python3
"""
enrich_tmdb_airtable.py

Enriches Airtable records with data from The Movie Database (TMDb).
Identifies the TMDb ID from 'Soc TMDb Id' or 'Soc IMDb Id'.
Fetches full biographical, social, and movie credit info.

Usage:
    python3 enrich_tmdb_airtable.py --limit 10
    python3 enrich_tmdb_airtable.py --all
"""

import os
import sys
import argparse
import requests
import json
import time
import re
from datetime import date
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Loads from .env or environment variables
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
BASE_ID          = os.environ.get("AIRTABLE_BASE_ID")
TABLE_ID         = os.environ.get("AIRTABLE_TABLE_ID")
VIEW_NAME        = os.environ.get("AIRTABLE_VIEW_NAME", "To Process")

# TMDb Read Access Token (v4)
TMDB_READ_TOKEN = os.environ.get("TMDB_READ_TOKEN")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

TMDB_HEADERS = {
    "Authorization": f"Bearer {TMDB_READ_TOKEN}",
    "accept": "application/json"
}

TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/original"

# ── TMDb API Helpers ──────────────────────────────────────────────────────────

def extract_tmdb_id(id_string):
    """Extract numeric ID from TMDb URL or string."""
    if not id_string:
        return None
    # If it's a URL like https://www.themoviedb.org/person/1549008
    match = re.search(r'person/(\d+)', str(id_string))
    if match:
        return match.group(1)
    # If it's just a number
    match = re.search(r'(\d+)', str(id_string))
    if match:
        return match.group(1)
    return None

def find_person_by_imdb(imdb_id):
    """Use TMDb /find endpoint to get TMDb ID from IMDb ID."""
    if not imdb_id:
        return None
    
    # Try searching for the IMDb ID regardless of prefix
    url = f"{TMDB_BASE_URL}/find/{imdb_id}?external_source=imdb_id&language=en-US"
    try:
        resp = requests.get(url, headers=TMDB_HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            # Check for person results first
            person_results = data.get('person_results', [])
            if person_results:
                return person_results[0].get('id')
            
            # If it's a company or other entity, TMDb might not return it as a 'person'
    except Exception as e:
        print(f"    [ERROR] TMDb Find failed: {e}")
    return None

def search_person_by_name(name):
    """Search TMDb for a person by name as a last fallback."""
    if not name or name == "Unknown":
        return None
    
    url = f"{TMDB_BASE_URL}/search/person?query={requests.utils.quote(name)}&language=en-US&page=1"
    try:
        resp = requests.get(url, headers=TMDB_HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            results = data.get('results', [])
            if results:
                # Use the first exact name match if possible, otherwise first result
                for r in results:
                    if r.get('name', '').lower() == name.lower():
                        return r.get('id')
                return results[0].get('id')
    except Exception as e:
        print(f"    [ERROR] TMDb Name Search failed: {e}")
    return None

def fetch_tmdb_person_details(tmdb_id):
    """Fetch full person details with appended social, images, and credits."""
    url = f"{TMDB_BASE_URL}/person/{tmdb_id}?append_to_response=images,external_ids,combined_credits&language=en-US"
    try:
        resp = requests.get(url, headers=TMDB_HEADERS, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"    [WARN] TMDb API returned {resp.status_code}")
    except Exception as e:
        print(f"    [ERROR] TMDb Fetch failed: {e}")
    return None

# ── Airtable Helpers ──────────────────────────────────────────────────────────

def update_records_bulk(records_batch: list):
    """PATCH up to 10 records at once in Airtable."""
    if not records_batch:
        return True, {}
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    try:
        r = requests.patch(url, headers=AIRTABLE_HEADERS, json={"records": records_batch}, timeout=20)
        return r.status_code == 200, r.json()
    except Exception as e:
        return False, str(e)

def update_run_details(existing_json_str, status, message, date_str):
    """Maintain a JSON array of run details history."""
    try:
        arr = json.loads(existing_json_str) if existing_json_str else []
    except Exception:
        arr = []
    
    # Add new entry
    arr.append({
        "date": date_str,
        "status": status,
        "details": message
    })
    
    # Keep only last 10 entries to avoid hitting Airtable cell limits
    return json.dumps(arr[-10:])

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Enrich TMDb profiles in Airtable.")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N records")
    parser.add_argument("--all",   action="store_true",   help="Process all records in the view")
    args = parser.parse_args()

    if not args.all and args.limit is None:
        args.limit = 10

    print(f"🎬 Starting TMDb enrichment (limit={args.limit or 'ALL'})...")

    ok_count    = 0
    err_count   = 0
    skip_count  = 0
    processed   = 0
    batch_queue = []
    today_str   = date.today().isoformat()

    airtable_url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    # Fields we need to read: Name, Soc TMDb Id, Soc IMDb Id, etc.
    params = {
        "pageSize": 100, 
        "view": VIEW_NAME,
        "fields[]": ["Name", "Soc TMDb Id", "Soc IMDb Id", "Run Details"]
    }

    while True:
        try:
            r = requests.get(airtable_url, headers=AIRTABLE_HEADERS, params=params, timeout=15)
            data = r.json()
        except Exception as e:
            print(f"[ERROR] Airtable fetch failed: {e}")
            break

        if "error" in data:
            print(f"[ERROR] Airtable: {data}")
            break

        page_records = data.get("records", [])
        if not page_records:
            break

        print(f"\n--- Processing page of {len(page_records)} records ---")

        for record in page_records:
            if args.limit and processed >= args.limit:
                break

            rec_id = record["id"]
            fields = record.get("fields", {})
            name   = fields.get("Name", "Unknown")
            
            raw_tmdb_id = fields.get("Soc TMDb Id", "")
            imdb_id     = fields.get("Soc IMDb Id", "").strip()

            processed += 1
            print(f"[{processed}] {name}")

            # 1. Determine TMDb ID
            tmdb_id = extract_tmdb_id(raw_tmdb_id)
            
            if not tmdb_id and imdb_id:
                print(f"  🔍 Searching by IMDb ID: {imdb_id}")
                tmdb_id = find_person_by_imdb(imdb_id)
            
            if not tmdb_id:
                print(f"  🔍 Final fallback: Searching by Name: {name}")
                tmdb_id = search_person_by_name(name)
            
            if not tmdb_id:
                print("  ⏭  Skipping — No TMDb or IMDb ID available")
                update_fields = {
                    "Last Check": today_str,
                    "Run Status": "Processed",
                    "Run Details": update_run_details(fields.get("Run Details"), "Skipped", "No TMDb or IMDb ID found", today_str)
                }
                batch_queue.append({"id": rec_id, "fields": update_fields})
                skip_count += 1
                
                # Check for batch flush
                if len(batch_queue) >= 10:
                    print(f"  📤 Sending bulk update ({len(batch_queue)} records - including skipped)...")
                    success, resp = update_records_bulk(batch_queue)
                    if success:
                        print("  ✅ Batch saved successfully")
                    else:
                        print(f"  ❌ Batch update failed: {resp}")
                    batch_queue.clear()
                    time.sleep(0.5)
                continue

            print(f"  📥 Fetching TMDb details for ID: {tmdb_id}")
            person = fetch_tmdb_person_details(tmdb_id)

            if not person:
                print("  ❌ Failed to fetch data from TMDb")
                err_count += 1
                continue

            # 2. Map fields as requested
            # Airtable Field Name - TMDb Variable
            update_fields = {}
            
            if person.get("name"): 
                update_fields["Name"] = person["name"]
            
            if person.get("id"):
                update_fields["Soc TMDb Id"] = str(person["id"])
                update_fields["Soc TMDb"] = f"https://www.themoviedb.org/person/{person['id']}"
                
            if person.get("biography"):
                update_fields["Description"] = person["biography"]
                
            if person.get("birthday"):
                update_fields["TMDB Birthday"] = person["birthday"]
                
            if person.get("deathday"):
                update_fields["TMDB Deathday"] = person["deathday"]
                
            if person.get("place_of_birth"):
                update_fields["TMDB Place of Birth"] = person["place_of_birth"]
                
            if person.get("gender"):
                # 1 = Female, 2 = Male, 3 = Non-binary
                gender_map = {1: "Female", 2: "Male", 3: "Non-binary"}
                gender_val = gender_map.get(person["gender"], str(person["gender"]))
                update_fields["Gender"] = gender_val
                update_fields["TMDB Gender"] = gender_val
                
            if person.get("known_for_department"):
                update_fields["TMDB Known For"] = person["known_for_department"]
                
            if person.get("popularity"):
                update_fields["TMDB Popularity"] = str(person["popularity"])
                
            if person.get("profile_path"):
                img_url = f"{TMDB_IMAGE_BASE}{person['profile_path']}"
                update_fields["Profile Image"] = img_url
                update_fields["TMDB Profile Image"] = img_url

            # Socials Mapping (Full URLs)
            ext_ids = person.get("external_ids", {})
            social_urls = []
            if ext_ids.get("imdb_id"): social_urls.append(f"https://www.imdb.com/name/{ext_ids['imdb_id']}/")
            if ext_ids.get("instagram_id"): social_urls.append(f"https://www.instagram.com/{ext_ids['instagram_id']}/")
            if ext_ids.get("twitter_id"): social_urls.append(f"https://twitter.com/{ext_ids['twitter_id']}")
            if ext_ids.get("facebook_id"): social_urls.append(f"https://www.facebook.com/{ext_ids['facebook_id']}")
            if ext_ids.get("tiktok_id"): social_urls.append(f"https://www.tiktok.com/@{ext_ids['tiktok_id']}")
            if ext_ids.get("wikidata_id"): social_urls.append(f"https://www.wikidata.org/wiki/{ext_ids['wikidata_id']}")
            
            if social_urls:
                update_fields["TMDB Socials"] = ", ".join(social_urls)

            # Mark as updated
            update_fields["Last Check"] = today_str
            update_fields["Run Status"] = "Processed"
            update_fields["Run Details"] = update_run_details(
                fields.get("Run Details"), 
                "Processed", 
                f"Successfully enriched from TMDb ID {tmdb_id}", 
                today_str
            )

            batch_queue.append({"id": rec_id, "fields": update_fields})

            # 3. Handle Batch Updates
            if len(batch_queue) >= 10:
                print(f"  📤 Sending bulk update ({len(batch_queue)} records)...")
                success, resp = update_records_bulk(batch_queue)
                if success:
                    print("  ✅ Batch saved successfully")
                    ok_count += len(batch_queue)
                else:
                    print(f"  ❌ Batch update failed: {resp}")
                    err_count += len(batch_queue)
                batch_queue.clear()
                time.sleep(0.5)

            # Respect TMDb rate limits (40 requests per 10 seconds, light delay)
            time.sleep(0.3)

        # Final flush
        if batch_queue:
            print(f"  📤 Final flush ({len(batch_queue)} records)...")
            success, resp = update_records_bulk(batch_queue)
            if success:
                print("  ✅ Batch saved successfully")
                ok_count += len(batch_queue)
            else:
                print(f"  ❌ Batch update failed: {resp}")
                err_count += len(batch_queue)
            batch_queue.clear()
            time.sleep(0.5)

        if args.limit and processed >= args.limit:
            break
            
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset

    print(f"\n{'='*55}")
    print(f"🎬 TMDb Enrichment Complete!")
    print(f"   ✅ Updated : {ok_count}")
    print(f"   ⏭  Skipped : {skip_count}")
    print(f"   ❌ Failed  : {err_count}")
    print(f"{'='*55}")

if __name__ == "__main__":
    main()
