import os
import requests
import logging

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def fetch_supabase_table(table_name: str, id_list: list, select_cols: str = "*") -> dict:
    """Fetch rows from PostgREST with guards against empty filter strings."""
    if not id_list:
        return {}

    unique_ids = list(set(str(i) for i in id_list if i))
    if not unique_ids:
        return {}

    formatted_ids = ",".join(f'"{i}"' for i in unique_ids)
    url = f"{SUPABASE_URL}/rest/v1/{table_name}?id=in.({formatted_ids})&select={select_cols}"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=5)
        response.raise_for_status()
        data = response.json()
        return {str(item["id"]): item for item in data if "id" in item}
    except Exception as e:
        logger.warning(f"Failed to fetch {table_name} metadata from Supabase: {e}")
        return {}


def enrich_cache_payload(payload: dict) -> dict:
    """Hydrate leaderboard user IDs, event IDs, and post IDs with Supabase metadata."""
    if not payload:
        return payload

    # 1. Gather all IDs needing metadata lookup
    leaderboard = payload.get("weekly_leaderboard", [])
    user_ids = [entry["user_id"] for entry in leaderboard if "user_id" in entry]
    
    event_ids = []
    post_ids = []
    for entry in leaderboard:
        event_ids.extend(entry.get("attended_event_ids", []))
        post_ids.extend(entry.get("post_ids", []))

    # 2. Query Supabase endpoints safely (tries 'users', falls back to 'profiles' if empty)
    users_meta = fetch_supabase_table("users", user_ids, select_cols="id,full_name,display_name")
    if not users_meta:
        users_meta = fetch_supabase_table("profiles", user_ids, select_cols="id,full_name,display_name")
        
    events_meta = fetch_supabase_table("events", event_ids, select_cols="id,title")
    posts_meta = fetch_supabase_table("posts", post_ids, select_cols="id,content,title")

    # 3. Attach enriched names/titles to leaderboard entries
    for entry in leaderboard:
        uid = str(entry.get("user_id"))
        user_info = users_meta.get(uid, {})
        entry["user_name"] = user_info.get("full_name") or user_info.get("display_name") or "Unknown Student"

        # Enrich event names
        entry["attended_events"] = [
            events_meta.get(str(eid), {}).get("title", f"Event {eid[:6]}")
            for eid in entry.get("attended_event_ids", [])
        ]

        # Enrich post snippets
        entry["posts"] = [
            posts_meta.get(str(pid), {}).get("title") or posts_meta.get(str(pid), {}).get("content", "Post Content")[:30]
            for pid in entry.get("post_ids", [])
        ]

    payload["weekly_leaderboard"] = leaderboard
    return payload