import csv
import asyncio
import aiohttp
import random
import time
from datetime import datetime
import json  # Changed from ast to json

###############################################################################
# CONFIG
###############################################################################

RPC_URL = "https://fullnode.mainnet.sui.io:443"

# Adjust these to tune concurrency and retries
MAX_CONCURRENT_REQUESTS = 5  # max number of simultaneous requests
MAX_RETRIES = 5              # how many times to retry a request if 429 or network error

# Pagination defaults
DEFAULT_PAGES = 10
DEFAULT_LIMIT_PER_PAGE = 50
DEFAULT_DESCENDING = True

###############################################################################
# GLOBAL CACHES
###############################################################################
_pool_cache = {}         # pool_id => (coinA, coinB)
_metadata_cache = {}     # coin_type => (decimals, symbol)

###############################################################################
# 1) SEMAPHORE + SAFE POST WITH RETRIES
###############################################################################
request_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

async def safe_post(session: aiohttp.ClientSession, url: str, json_data: dict, retries=MAX_RETRIES):
    """
    Send a POST request with concurrency-limiting and exponential backoff on 429 or network errors.
    """
    backoff = 1.0
    for attempt in range(retries):
        async with request_semaphore:
            try:
                async with session.post(url, json=json_data) as resp:
                    if resp.status == 429:
                        # Rate-limited; wait and retry
                        print(f"Rate limited. Retrying after {backoff} seconds...")
                        await asyncio.sleep(backoff + random.random())
                        backoff *= 2
                        continue
                    resp.raise_for_status()
                    response_json = await resp.json()
                    return response_json

            except aiohttp.ClientError as e:
                print(f"Client error on attempt {attempt+1}: {e}")
                await asyncio.sleep(backoff + random.random())
                backoff *= 2

    raise Exception(f"Failed after {retries} retries. Last payload: {json_data}")

###############################################################################
# 2) GET POOL COINS (ASYNC) WITH CACHE
###############################################################################

async def get_pool_coins(session: aiohttp.ClientSession, pool_id: str):
    """
    Fetch coin A/B for a given pool_id using sui_getObject.
    Caches results to avoid repeated network calls for the same pool_id.
    """
    if pool_id in _pool_cache:
        return _pool_cache[pool_id]

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sui_getObject",
        "params": [
            pool_id,
            {
                "showType": True,
                "showOwner": True,
                "showPreviousTransaction": True,
                "showDisplay": False,
                "showContent": True,
                "showBcs": False,
                "showStorageRebate": True
            }
        ]
    }

    try:
        result = await safe_post(session, RPC_URL, payload)
    except Exception as e:
        print(f"Error fetching pool {pool_id}: {e}")
        _pool_cache[pool_id] = ("Unknown", "Unknown")
        return ("Unknown", "Unknown")

    result = result.get("result", {})

    if "error" in result:
        print(f"Error in response for pool {pool_id}: {result.get('error')}")
        _pool_cache[pool_id] = ("Unknown", "Unknown")
        return ("Unknown", "Unknown")

    obj_data = result.get("data")
    if not obj_data:
        print(f"No data found for pool {pool_id}.")
        _pool_cache[pool_id] = ("Unknown", "Unknown")
        return ("Unknown", "Unknown")

    coin_type_a = None
    coin_type_b = None

    # Attempt parsing from obj_data["type"] first
    type_str = obj_data.get("type", "")
    if "Pool<" in type_str:
        inside = type_str.split("Pool<", 1)[1].rstrip(">")
        parts = [p.strip() for p in inside.split(",")]
        if len(parts) == 2:
            coin_type_a, coin_type_b = parts

    # Or fallback to content.fields
    content = obj_data.get("content", {})
    fields = content.get("fields", {})

    # Attempt to extract from 'coin_type_a' and 'coin_type_b' fields
    if not coin_type_a:
        coin_type_a = fields.get("coin_type_a")
    if not coin_type_b:
        coin_type_b = fields.get("coin_type_b")

    # If still not found, attempt to extract from 'type_names'
    if not coin_type_a or not coin_type_b:
        type_names = fields.get("type_names", [])
        if isinstance(type_names, list) and len(type_names) == 2:
            coin_type_a, coin_type_b = type_names
            print(f"Extracted coin types from 'type_names' for pool {pool_id}:")
            print(f"  Coin A: {coin_type_a}")
            print(f"  Coin B: {coin_type_b}")
        else:
            print(f"Failed to parse coin types for pool {pool_id}.")
            print(f"Type String: {type_str}")
            print(f"Content Fields: {json.dumps(fields, indent=2)}")
            _pool_cache[pool_id] = ("Unknown", "Unknown")
            return ("Unknown", "Unknown")

    if not coin_type_a:
        coin_type_a = "Unknown"
    if not coin_type_b:
        coin_type_b = "Unknown"

    # Debugging lines to verify fetched pool coins
    print(f"Fetched pool {pool_id}: Coin A = {coin_type_a}, Coin B = {coin_type_b}")

    _pool_cache[pool_id] = (coin_type_a, coin_type_b)
    return (coin_type_a, coin_type_b)


###############################################################################
# 3) GET COIN METADATA (ASYNC) WITH CACHE
###############################################################################

async def get_coin_metadata(session: aiohttp.ClientSession, coin_type: str):
    """
    Fetch metadata (decimals, symbol) for a given coin_type.
    Caches results so that subsequent lookups skip the network call.
    """
    if coin_type in _metadata_cache:
        return _metadata_cache[coin_type]

    if coin_type == "Unknown" or not coin_type:
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN")

    # Removed the '0x' prefix addition as coin_type should already be correctly formatted
    # Ensure coin_type is a valid Sui coin type
    if not isinstance(coin_type, str):
        print(f"Invalid coin_type format: {coin_type}")
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN")

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_getCoinMetadata",
        "params": [coin_type]
    }

    try:
        data = await safe_post(session, RPC_URL, payload)
    except Exception as e:
        print(f"Error fetching metadata for {coin_type}: {e}")
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN")

    if "error" in data:
        print(f"Error in metadata response for {coin_type}: {data.get('error')}")
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN")

    result = data.get("result", {})
    if not result:
        print(f"No metadata found for {coin_type}.")
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN")

    decimals = result.get("decimals", 0)
    symbol = result.get("symbol", "UNKNOWN")

    # Debugging line to verify fetched metadata
    print(f"Fetched metadata for {coin_type}: Decimals={decimals}, Symbol={symbol}")

    _metadata_cache[coin_type] = (decimals, symbol)
    return (decimals, symbol)

###############################################################################
# 4) FETCH A SINGLE PAGE OF EVENTS
###############################################################################

async def fetch_page(session: aiohttp.ClientSession, event_filter: dict, cursor, limit_per_page, descending):
    """
    Fetch a single page of events from suix_queryEvents using the given cursor.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_queryEvents",
        "params": [
            event_filter,
            cursor,
            limit_per_page,
            descending
        ]
    }
    try:
        data = await safe_post(session, RPC_URL, payload)
        return data["result"]
    except Exception as e:
        print(f"Error fetching page: {e}")
        return {"data": [], "hasNextPage": False}

###############################################################################
# 5) PAGINATED EVENT FETCH
###############################################################################

async def fetch_aftermathv2_swap_events_paginated(
    session: aiohttp.ClientSession,
    pages=DEFAULT_PAGES,
    limit_per_page=DEFAULT_LIMIT_PER_PAGE,
    descending=DEFAULT_DESCENDING
):
    """
    Asynchronously fetch up to `pages` pages of aftermathv2 SwapEvent objects.
    """
    event_filter = {
        "MoveEventType": "0xc4049b2d1cc0f6e017fda8260e4377cecd236bd7f56a54fee120816e72e2e0dd::events::SwapEventV2"
    }

    all_events = []
    cursor = None

    for page_index in range(pages):
        result = await fetch_page(session, event_filter, cursor, limit_per_page, descending)
        page_events = result.get("data", [])
        all_events.extend(page_events)

        print(f"Page {page_index+1}: fetched {len(page_events)} events (total {len(all_events)})")

        if not result.get("hasNextPage", False):
            break
        cursor = result.get("nextCursor")

    return all_events

###############################################################################
# 6) BATCHED ENRICHMENT: PRE-FETCH POOL AND COIN METADATA
###############################################################################

async def enrich_events_with_pool_and_coin_data(session, events):
    """
    Faster approach:
      - Gather all unique pools, fetch them in parallel
      - Gather all unique coin types from those pools, fetch them in parallel
      - Final pass to fill event data from the now-cached data
    """
    # 1) Gather unique pool IDs
    unique_pool_ids = set()
    for e in events:
        pjson = e.get("parsedJson", {})
        if isinstance(pjson, str):
            try:
                pjson = json.loads(pjson)  # Changed to json.loads
            except json.JSONDecodeError:
                pjson = {}
        pool_id = pjson.get("pool_id", "")
        if pool_id:
            unique_pool_ids.add(pool_id)

    print(f"Unique pools to fetch: {len(unique_pool_ids)}")

    # 2) Fetch each pool (in parallel)
    pool_tasks = [asyncio.create_task(get_pool_coins(session, pid)) for pid in unique_pool_ids]
    await asyncio.gather(*pool_tasks)

    # 3) Gather all unique coin types from those pools
    unique_coin_types = set()
    for pid in unique_pool_ids:
        coin_a, coin_b = _pool_cache.get(pid, ("Unknown", "Unknown"))
        if coin_a and coin_a != "Unknown":
            unique_coin_types.add(coin_a)
        if coin_b and coin_b != "Unknown":
            unique_coin_types.add(coin_b)

    print(f"Unique coin types to fetch: {len(unique_coin_types)}")

    # 4) Fetch each coin metadata (in parallel)
    coin_tasks = [asyncio.create_task(get_coin_metadata(session, ctype)) for ctype in unique_coin_types]
    await asyncio.gather(*coin_tasks)

    # 5) Final pass: fill each event using the cached data
    enrichment_tasks = [asyncio.create_task(_fill_single_event_from_cache(e)) for e in events]
    await asyncio.gather(*enrichment_tasks)

    return events

async def _fill_single_event_from_cache(event: dict):
    """
    Fill event['token_in'], etc. from pool cache and coin cache (no new network calls).
    Updated to match the new SwapEvent structure.
    """
    pjson = event.get("parsedJson", {})
    if isinstance(pjson, str):
        try:
            pjson = json.loads(pjson)  # Changed to json.loads
        except json.JSONDecodeError:
            pjson = {}
    pool_id = pjson.get("pool_id", "")  # Updated key

    # Fetch pool coins from cache
    coin_a, coin_b = _pool_cache.get(pool_id, ("Unknown", "Unknown"))

    # Extract types and amounts
    types_in = pjson.get("types_in", [])
    types_out = pjson.get("types_out", [])
    amounts_in = pjson.get("amounts_in", [])
    amounts_out = pjson.get("amounts_out", [])

    # Handle multiple tokens if necessary
    # For simplicity, assume single token in and out
    token_in = types_in[0] if types_in else "Unknown"
    token_out = types_out[0] if types_out else "Unknown"

    token_in_raw = amounts_in[0] if amounts_in else "0"
    token_out_raw = amounts_out[0] if amounts_out else "0"

    # Fetch metadata from cache
    in_decimals, in_symbol = _metadata_cache.get(token_in, (0, "UNKNOWN"))
    out_decimals, out_symbol = _metadata_cache.get(token_out, (0, "UNKNOWN"))

    # Convert raw amount to decimal
    def parse_decimal(amount_str, decimals):
        try:
            raw_int = int(amount_str)
        except:
            raw_int = 0
        return raw_int / (10 ** decimals) if decimals > 0 else raw_int

    in_decimal_amt = parse_decimal(token_in_raw, in_decimals)
    out_decimal_amt = parse_decimal(token_out_raw, out_decimals)

    # Update the event dictionary with enriched data
    event["token_in"] = token_in
    event["token_in_symbol"] = in_symbol
    event["token_in_decimals"] = in_decimals
    event["token_in_raw_amount"] = token_in_raw
    event["token_in_decimal_amount"] = in_decimal_amt
    event["token_out"] = token_out
    event["token_out_symbol"] = out_symbol
    event["token_out_decimals"] = out_decimals
    event["token_out_raw_amount"] = token_out_raw
    event["token_out_decimal_amount"] = out_decimal_amt

###############################################################################
# 7) WRITE EVENTS TO CSV
###############################################################################

def write_events_to_csv_with_enrichment(events, output_csv="aftermathv2_swap_events.csv"):
    """
    Write the enriched events to CSV.
    """
    fieldnames = [
        "txDigest",
        "eventSeq",
        "timestampIso",
        "sender",
        "type",
        "poolId",
        "token_in",
        "token_in_symbol",
        "token_in_decimals",
        "token_in_raw_amount",
        "token_in_decimal_amount",
        "token_out",
        "token_out_symbol",
        "token_out_decimals",
        "token_out_raw_amount",
        "token_out_decimal_amount",
        "parsedJson",
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for event in events:
            tx_digest = event["id"]["txDigest"]
            seq = event["id"]["eventSeq"]

            # Convert timestampMs to ISO8601 if present
            timestamp_iso = ""
            timestamp_ms_str = event.get("timestampMs", "")
            if timestamp_ms_str:
                try:
                    ms_val = int(timestamp_ms_str)
                    dt = datetime.utcfromtimestamp(ms_val / 1000.0)
                    timestamp_iso = dt.isoformat(timespec="seconds")
                except:
                    pass

            sender = event["sender"]
            etype = event["type"]
            pjson = event.get("parsedJson", {})
            if isinstance(pjson, str):
                try:
                    pjson = json.loads(pjson)  # Changed to json.loads
                except json.JSONDecodeError:
                    pjson = {}
            pool_id = pjson.get("pool_id", "")  # Updated key

            row = {
                "txDigest": tx_digest,
                "eventSeq": seq,
                "timestampIso": timestamp_iso,
                "sender": sender,
                "type": etype,
                "poolId": pool_id,
                "token_in": event.get("token_in", ""),
                "token_in_symbol": event.get("token_in_symbol", ""),
                "token_in_decimals": event.get("token_in_decimals", 0),
                "token_in_raw_amount": event.get("token_in_raw_amount", ""),
                "token_in_decimal_amount": event.get("token_in_decimal_amount", 0),
                "token_out": event.get("token_out", ""),
                "token_out_symbol": event.get("token_out_symbol", ""),
                "token_out_decimals": event.get("token_out_decimals", 0),
                "token_out_raw_amount": event.get("token_out_raw_amount", ""),
                "token_out_decimal_amount": event.get("token_out_decimal_amount", 0),
                "parsedJson": json.dumps(pjson),  # Ensure JSON is stringified
            }
            writer.writerow(row)

###############################################################################
# 8) ASYNC MAIN
###############################################################################

async def main():
    async with aiohttp.ClientSession() as session:
        # 1) Fetch events (paginated)
        print("Starting to fetch events...")
        events = await fetch_aftermathv2_swap_events_paginated(
            session,
            pages=DEFAULT_PAGES,
            limit_per_page=DEFAULT_LIMIT_PER_PAGE,
            descending=DEFAULT_DESCENDING
        )
        print(f"Total events fetched: {len(events)}")

        if not events:
            print("No events fetched. Exiting.")
            return

        # 2) Enrich events in a batched way
        print("Starting to enrich events...")
        start_enrich = time.time()
        await enrich_events_with_pool_and_coin_data(session, events)
        end_enrich = time.time()
        print(f"Enrichment took {end_enrich - start_enrich:.2f} seconds.")

    # 3) Write them to CSV
    print("Starting to write events to CSV...")
    start_csv = time.time()
    output_csv = "aftermathv2_swap_events.csv"
    write_events_to_csv_with_enrichment(events, output_csv)
    end_csv = time.time()
    print(f"Wrote events to {output_csv} in {end_csv - start_csv:.2f} seconds.")

###############################################################################
# 9) DRIVER
###############################################################################

if __name__ == "__main__":
    asyncio.run(main())
