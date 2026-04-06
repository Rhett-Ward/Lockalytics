# fetch_and_store.py  (optimized – concurrent, batched uploads, lockfile)
# pipeline: Analytics General -> Analytics per hero -> Builds -> Matches ->
#           Bulk metadata -> metadata -> Scoreboards -> Players
#
# uses ThreadPoolExecutor (10 workers) for concurrent API calls,
# doesn't retry 403/400/404 (permanent errors), uploads match history
# in batches to avoid OOM, and uses a flock lockfile so cron runs
# don't overlap. single BlobServiceClient + pooled HTTP session.
#
# Cron (every 5 min):
#   */5 * * * * /root/lockalytics/venv/bin/python /root/lockalytics/fetch_and_store.py \
#       >> /root/lockalytics/logs/ingest.log 2>&1

import fcntl
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests as req_lib
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# --- Config ---
AZURE_CONN_STR = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "deadlock-data")

if not AZURE_CONN_STR:
    print("ERROR: AZURE_CONNECTION_STRING not set in .env", file=sys.stderr)
    sys.exit(1)

BLOB_SVC = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
MAX_RETRIES = 3
RETRY_DELAY = 2
API_HOST = "https://api.deadlock-api.com"
MAX_WORKERS = 10
UPLOAD_BATCH = 500   # upload match_history in batches of N accounts
LOCK_FILE = "/tmp/lockalytics_ingest.lock"

# reuse one session so we get connection pooling
HTTP = req_lib.Session()
adapter = req_lib.adapters.HTTPAdapter(
    pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS
)
HTTP.mount("https://", adapter)
HTTP.mount("http://", adapter)


# --- Helpers ---

def upload_blob(data, endpoint, timestamp, suffix=""):
    """Push JSON payload to azure blob."""
    date_path = timestamp.strftime("%Y/%m/%d/%H%M%S")
    path = f"raw/{endpoint}/{date_path}{suffix}.json"
    payload = {
        "fetched_at": timestamp.isoformat(),
        "endpoint": endpoint,
        "record_count": len(data),
        "data": data,
    }
    body = json.dumps(payload, default=str)
    err = None
    for i in range(MAX_RETRIES):
        try:
            client = BLOB_SVC.get_blob_client(
                container=CONTAINER_NAME, blob=path
            )
            client.upload_blob(body, overwrite=True)
            return path
        except Exception as e:
            err = e
            wait = RETRY_DELAY * (2 ** i)
            print(f"WARN: blob upload for {endpoint} failed, "
                  f"retry {i+1}/{MAX_RETRIES} in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"blob upload failed for {endpoint}: {err}")


def api_get(path, label, params=None):
    """Hit an API endpoint with retries. 403/400/404/422 are permanent so we skip those."""
    url = f"{API_HOST}{path}"
    for attempt in range(MAX_RETRIES):
        try:
            resp = HTTP.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = float(resp.headers.get("Retry-After", 2))
                print(f"WARN:\t{label} rate-limited, waiting {wait}s")
                time.sleep(wait)
            elif resp.status_code in [400, 403, 404, 422]:
                # permanent — protected user, bad request, etc
                return None
            else:
                print(f"WARN:\t{label} HTTP {resp.status_code}, "
                      f"retry {attempt+1}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY * (2 ** attempt))
        except Exception as e:
            print(f"WARN:\t{label} {e}, "
                  f"retry {attempt+1}/{MAX_RETRIES}")
            time.sleep(RETRY_DELAY * (2 ** attempt))
    print(f"ERROR:\t{label} gave up after {MAX_RETRIES} tries",
          file=sys.stderr)
    return None


def normalize(resp):
    if resp is None:
        return []
    if isinstance(resp, dict):
        return [resp]
    if isinstance(resp, list):
        return resp
    return [resp]


# --- Fetch patterns ---

def fetch_endpoint(path, blob_name, ts, label=None, params=None):
    """Fetch a single endpoint and upload to blob."""
    label = label or blob_name
    data = api_get(path, label, params)
    records = normalize(data)
    if not records:
        print(f"SKIP\t{label} — empty")
        return False
    try:
        p = upload_blob(records, blob_name, ts)
        print(f"OK\t{label}: {len(records)} records -> {p}")
        return True
    except Exception as e:
        print(f"FAIL\t{label}: {e}", file=sys.stderr)
        return False


def fetch_many_concurrent(items, worker_fn, blob_name, ts, id_key="id"):
    """Run worker_fn for each item in parallel, bundle results, upload."""
    all_recs = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(worker_fn, item): item for item in items}
        for future in as_completed(futures):
            try:
                recs = future.result()
                all_recs.extend(recs)
            except Exception as e:
                item = futures[future]
                print(f"WARN:\t{blob_name}({id_key}={item}) exception: {e}")
    if not all_recs:
        print(f"SKIP\t{blob_name} — no data")
        return False
    try:
        p = upload_blob(all_recs, blob_name, ts)
        print(f"OK\t{blob_name}: {len(all_recs)} records "
              f"({len(items)} {id_key}s) -> {p}")
        return True
    except Exception as e:
        print(f"FAIL\t{blob_name}: {e}", file=sys.stderr)
        return False


def fetch_many_batched(items, worker_fn, blob_name, ts, id_key="id",
                       batch_size=UPLOAD_BATCH):
    """Same as fetch_many_concurrent but uploads in chunks to avoid OOM.
    Each batch gets its own blob (_batchN suffix), then we free the memory
    before starting the next one. Needed because match history for ~7500
    accounts can easily blow past 2GB."""
    total_recs = 0
    batch_num = 0
    success = False

    for start in range(0, len(items), batch_size):
        batch = items[start : start + batch_size]
        batch_num += 1
        batch_recs = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(worker_fn, item): item for item in batch}
            for future in as_completed(futures):
                try:
                    recs = future.result()
                    batch_recs.extend(recs)
                except Exception as e:
                    item = futures[future]
                    print(f"WARN:\t{blob_name}({id_key}={item}) exception: "
                          f"{e}")

        if batch_recs:
            try:
                suffix = f"_batch{batch_num}" if len(items) > batch_size else ""
                p = upload_blob(batch_recs, blob_name, ts, suffix=suffix)
                count = len(batch_recs)
                total_recs += count
                success = True
                print(f"OK\t{blob_name} batch {batch_num}: {count} records "
                      f"({len(batch)} {id_key}s) -> {p}")
            except Exception as e:
                print(f"FAIL\t{blob_name} batch {batch_num}: {e}",
                      file=sys.stderr)
        else:
            print(f"SKIP\t{blob_name} batch {batch_num} — no data "
                  f"(0/{len(batch)} {id_key}s returned results)")

    print(f"  {blob_name} totals: {total_recs} records across "
          f"{batch_num} batch(es)")
    return success


# --- Endpoint paths ---

ANALYTICS_ENDPOINTS = [
    ("/v1/analytics/hero-stats", "hero_stats"),
    ("/v1/analytics/item-stats", "item_stats"),
    ("/v1/analytics/hero-counter-stats", "hero_counter_stats"),
    ("/v1/analytics/hero-synergy-stats", "hero_synergy_stats"),
    ("/v1/analytics/hero-comb-stats", "hero_comb_stats"),
    ("/v1/analytics/badge-distribution", "badge_distribution"),
    ("/v1/analytics/kill-death-stats", "kill_death_stats"),
    ("/v1/analytics/player-performance-curve", "player_performance_curve"),
    ("/v1/analytics/player-stats/metrics", "player_stat_metrics"),
]


# --- Main pipeline ---

def main():
    start = datetime.now(timezone.utc)
    print(f"[{start.isoformat()}] Starting full ingestion run (concurrent)...")

    ts = start
    ok = 0
    fail = 0

    # step 1: analytics (general) - fire them all at once
    print("\n  Step 1: Analytics (general)")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(fetch_endpoint, path, name, ts): name
            for path, name in ANALYTICS_ENDPOINTS
        }
        for f in as_completed(futures):
            try:
                if f.result():
                    ok += 1
                else:
                    fail += 1
            except Exception as e:
                print(f"FAIL\t{futures[f]}: {e}", file=sys.stderr)
                fail += 1

    # step 2: per-hero analytics
    print("\n  Step 2: Analytics (per-hero)")
    hero_data = api_get("/v1/analytics/hero-stats", "hero_id_discovery")
    hero_ids = []
    if hero_data:
        seen = set()
        for h in normalize(hero_data):
            hid = h.get("hero_id") if isinstance(h, dict) else None
            if hid is not None and hid not in seen:
                hero_ids.append(hid)
                seen.add(hid)
    print(f"  Found {len(hero_ids)} heroes")

    if hero_ids:
        for api_path, blob_name in [
            ("/v1/analytics/ability-order-stats", "ability_order_stats"),
            ("/v1/analytics/build-item-stats",    "build_item_stats"),
        ]:
            def hero_worker(hid, _path=api_path, _blob=blob_name):
                label = f"{_blob}(hero={hid})"
                data = api_get(_path, label, params={"hero_id": hid})
                recs = normalize(data)
                for r in recs:
                    r["hero_id"] = hid
                return recs

            if fetch_many_concurrent(hero_ids, hero_worker, blob_name, ts,
                                     id_key="hero"):
                ok += 1
            else:
                fail += 1
    else:
        print("  No hero IDs found, skipping")
        fail += 2

    # step 3: builds
    print("\n  Step 3: Builds")
    if fetch_endpoint("/v1/builds", "search_builds", ts):
        ok += 1
    else:
        fail += 1

    # step 4: matches
    print("\n  Step 4: Matches")

    match_ids = set()
    account_ids = set()

    # 4a - bulk metadata (recent matches list, no player info)
    bulk_data = api_get("/v1/matches/metadata", "bulk_metadata")
    if bulk_data:
        bulk_recs = normalize(bulk_data)
        for match in bulk_recs:
            mid = match.get("match_id")
            if mid is not None:
                match_ids.add(mid)
        try:
            path = upload_blob(bulk_recs, "bulk_metadata", ts)
            print(f"OK\tbulk_metadata: {len(bulk_recs)} matches -> {path}")
            ok += 1
        except Exception as e:
            print(f"FAIL\tbulk_metadata: {e}", file=sys.stderr)
            fail += 1
        print(f"  Discovered {len(match_ids)} unique match IDs")
    else:
        print("SKIP\tbulk_metadata — no response")
        fail += 1

    # 4b - per-match metadata (concurrent), also grabs account_ids from players
    if match_ids:
        print(f"  Fetching metadata for {len(match_ids)} matches "
              f"concurrently...")

        def match_worker(mid):
            data = api_get(f"/v1/matches/{mid}/metadata",
                           f"metadata(match={mid})")
            if data is None:
                return [], set()
            recs = normalize(data)
            aids = set()
            for r in recs:
                r["match_id"] = mid
                # players are nested under match_info
                match_info = r.get("match_info", {})
                for p in (match_info.get("players") or []):
                    aid = (p.get("account_id") if isinstance(p, dict)
                           else None)
                    if aid is not None and aid > 0:
                        aids.add(aid)
                # also check top-level players in case the response is flattened
                for p in (r.get("players") or []):
                    aid = (p.get("account_id") if isinstance(p, dict)
                           else None)
                    if aid is not None and aid > 0:
                        aids.add(aid)
            return recs, aids

        all_meta = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(match_worker, mid): mid
                       for mid in match_ids}
            for f in as_completed(futures):
                try:
                    recs, aids = f.result()
                    all_meta.extend(recs)
                    account_ids.update(aids)
                except Exception as e:
                    mid = futures[f]
                    print(f"WARN:\tmetadata(match={mid}) exception: {e}")

        if all_meta:
            try:
                path = upload_blob(all_meta, "match_metadata", ts)
                print(f"OK\tmatch_metadata: {len(all_meta)} matches "
                      f"-> {path}")
                ok += 1
            except Exception as e:
                print(f"FAIL\tmatch_metadata: {e}", file=sys.stderr)
                fail += 1
        else:
            print("SKIP\tmatch_metadata — no data")
            fail += 1

        print(f"  Total unique account IDs after match drill-down: "
              f"{len(account_ids)}")
    else:
        print("  No match IDs to fetch metadata for")
        fail += 1

    # step 5: scoreboards
    print("\n  Step 5: Scoreboards")
    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(
            fetch_endpoint, "/v1/analytics/scoreboards/heroes",
            "hero_scoreboard", ts, params={"sort_by": "wins"})
        f2 = pool.submit(
            fetch_endpoint, "/v1/analytics/scoreboards/players",
            "player_scoreboard", ts, params={"sort_by": "wins"})
        for f in [f1, f2]:
            try:
                if f.result():
                    ok += 1
                else:
                    fail += 1
            except Exception as e:
                print(f"FAIL\tscoreboard: {e}", file=sys.stderr)
                fail += 1

    # step 6: players (batched so we don't OOM)
    accounts = [aid for aid in account_ids if aid and aid > 0]
    print(f"\n  Step 6: Players ({len(accounts)} accounts, "
          f"batch size {UPLOAD_BATCH})")

    if accounts:
        def history_worker(aid):
            data = api_get(
                f"/v1/players/{aid}/match-history",
                f"match_history(acct={aid})",
                params={"only_stored_history": "true"},
            )
            recs = normalize(data)
            for r in recs:
                r["account_id"] = aid
            return recs

        if fetch_many_batched(accounts, history_worker, "match_history",
                              ts, id_key="account"):
            ok += 1
        else:
            fail += 1
    else:
        print("  No account IDs discovered, skipping player endpoints")
        fail += 1

    # summary
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    total = ok + fail
    print(f"\n[{datetime.now(timezone.utc).isoformat()}] Done. "
          f"{ok}/{total} endpoints succeeded, {fail} failed. "
          f"Took {elapsed:.1f}s")
    print(f"Matches tracked: {len(match_ids)}")
    print(f"Players tracked: {len(account_ids)}")
    if ok == 0 and fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    # lockfile so cron doesn't overlap runs
    lock_fp = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("Another instance is already running — exiting.",
              file=sys.stderr)
        sys.exit(0)

    try:
        main()
    except Exception as e:
        print(f"FATAL: unhandled exception: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        fcntl.flock(lock_fp, fcntl.LOCK_UN)
        lock_fp.close()
