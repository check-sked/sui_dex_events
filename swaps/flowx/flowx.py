import csv
import asyncio
import aiohttp
import random
import time
from datetime import datetime

###############################################################################
# CONFIG
###############################################################################

RPC_URL = "https://fullnode.mainnet.sui.io:443"
MAX_CONCURRENT_REQUESTS = 5
MAX_RETRIES = 5
DEFAULT_PAGES = 100
DEFAULT_LIMIT_PER_PAGE = 50
DEFAULT_DESCENDING = True

###############################################################################
# GLOBAL CACHES
###############################################################################
_pool_cache = {}         # pool_id => (coinA, coinB)
_metadata_cache = {}     # coin_type => (decimals, symbol)
_tx_cache = {}          # tx_digest => (checkpoint, timestamp)

###############################################################################
# 1) SEMAPHORE + SAFE POST WITH RETRIES
###############################################################################
request_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

async def safe_post(session: aiohttp.ClientSession, url: str, json_data: dict, retries=MAX_RETRIES):
    backoff = 1.0
    for attempt in range(retries):
        async with request_semaphore:
            try:
                async with session.post(url, json=json_data) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(backoff + random.random())
                        backoff *= 2
                        continue
                    resp.raise_for_status()
                    return await resp.json()
            except aiohttp.ClientError as e:
                print(f"Client error on attempt {attempt+1}: {e}")
                await asyncio.sleep(backoff + random.random())
                backoff *= 2
    raise Exception(f"Failed after {retries} retries. Last payload: {json_data}")

###############################################################################
# 2) GET POOL COINS (ASYNC) WITH CACHE
###############################################################################

async def get_pool_coins(session: aiohttp.ClientSession, pool_id: str):
    """Modified to handle the Flowx pool structure"""
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
                "showContent": True
            }
        ]
    }

    result = await safe_post(session, RPC_URL, payload)
    result = result.get("result", {})

    if "error" in result:
        _pool_cache[pool_id] = ("Unknown", "Unknown")
        return ("Unknown", "Unknown")

    obj_data = result.get("data")
    if not obj_data:
        _pool_cache[pool_id] = ("Unknown", "Unknown")
        return ("Unknown", "Unknown")

    # Updated parsing logic for Flowx pool structure
    type_str = obj_data.get("type", "")
    coin_type_a = "Unknown"
    coin_type_b = "Unknown"

    if "Pair<" in type_str:
        inside = type_str.split("Pair<", 1)[1].rstrip(">")
        parts = [p.strip() for p in inside.split(",")]
        if len(parts) == 2:
            coin_type_a, coin_type_b = parts

    _pool_cache[pool_id] = (coin_type_a, coin_type_b)
    return (coin_type_a, coin_type_b)

###############################################################################
# 3) GET COIN METADATA (ASYNC) WITH CACHE
###############################################################################

async def get_coin_metadata(session: aiohttp.ClientSession, coin_type: str):
    """Fetches coin metadata via RPC"""
    if coin_type in _metadata_cache:
        return _metadata_cache[coin_type]

    if not coin_type or coin_type == "Unknown":
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN")

    try:
        # Normalize the coin type format
        normalized_type = coin_type
        if not coin_type.startswith("0x") and len(coin_type) >= 64:
            hex_addr = coin_type[:64]
            remaining = coin_type[64:]
            normalized_type = f"0x{hex_addr.lstrip('0')}{remaining}"

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "suix_getCoinMetadata",
            "params": [normalized_type]
        }

        print(f"Fetching metadata for {normalized_type}")
        data = await safe_post(session, RPC_URL, payload)
        
        if "error" not in data and "result" in data and data["result"]:
            result = data["result"]
            decimals = result.get("decimals", 0)
            fetched_symbol = result.get("symbol", "UNKNOWN")
            print(f"Got metadata for {fetched_symbol}: {decimals} decimals")
            _metadata_cache[coin_type] = (decimals, fetched_symbol)
            return (decimals, fetched_symbol)
        else:
            print(f"No metadata found for {normalized_type}. Using defaults.")
            _metadata_cache[coin_type] = (0, "UNKNOWN")
            return (0, "UNKNOWN")

    except Exception as e:
        print(f"Error fetching metadata for {coin_type}: {e}")
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN")

###############################################################################
# 4) GET TRANSACTION BLOCK INFO (ASYNC) WITH CACHE
###############################################################################

async def get_tx_block_info(session: aiohttp.ClientSession, tx_digest: str):
    """
    Fetch transaction block info including checkpoint number.
    Caches results to avoid repeated network calls.
    """
    if tx_digest in _tx_cache:
        return _tx_cache[tx_digest]

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sui_getTransactionBlock",
        "params": [
            tx_digest,
            {
                "showEffects": True,
                "showInput": False,
                "showRawInput": False,
                "showEvents": False
            }
        ]
    }

    try:
        result = await safe_post(session, RPC_URL, payload)
        data = result.get("result", {})
        
        checkpoint = data.get("checkpoint", None)
        timestamp = data.get("timestampMs", None)
        
        _tx_cache[tx_digest] = (checkpoint, timestamp)
        return (checkpoint, timestamp)
    except Exception as e:
        print(f"Error fetching tx block info for {tx_digest}: {e}")
        _tx_cache[tx_digest] = (None, None)
        return (None, None)

###############################################################################
# 5) FETCH EVENTS PAGINATION
###############################################################################

async def fetch_page(session: aiohttp.ClientSession, event_filter: dict, cursor, limit_per_page, descending):
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
    data = await safe_post(session, RPC_URL, payload)
    return data["result"]

async def fetch_swap_events_paginated(
    session: aiohttp.ClientSession,
    pages=DEFAULT_PAGES,
    limit_per_page=DEFAULT_LIMIT_PER_PAGE,
    descending=DEFAULT_DESCENDING
):
    """Fetches Flowx swap events"""
    event_filter = {
        "MoveEventType": "0xba153169476e8c3114962261d1edc70de5ad9781b83cc617ecc8c1923191cae0::pair::Swapped"
    }
    
    print("Fetching events with filter:", event_filter)

    all_events = []
    cursor = None

    for page_index in range(pages):
        result = await fetch_page(session, event_filter, cursor, limit_per_page, descending)
        
        if page_index == 0:
            print("\nFirst page response structure:")
            print("Has next page:", result.get("hasNextPage"))
            if "data" in result and len(result["data"]) > 0:
                print("\nSample event structure:")
                print(result["data"][0])
        
        page_events = result.get("data", [])
        all_events.extend(page_events)

        print(f"Page {page_index+1}: fetched {len(page_events)} events (total {len(all_events)})")

        if not result.get("hasNextPage", False):
            break
        cursor = result.get("nextCursor")

    return all_events

###############################################################################
# 6) EVENT ENRICHMENT
###############################################################################

async def _fill_single_event_from_cache(event: dict):
    """Modified to handle Flowx Swapped event structure"""
    pjson = event.get("parsedJson", {})
    
    # Determine which coin was swapped in/out
    amount_x_in = int(pjson.get("amount_x_in", "0"))
    amount_x_out = int(pjson.get("amount_x_out", "0"))
    amount_y_in = int(pjson.get("amount_y_in", "0"))
    amount_y_out = int(pjson.get("amount_y_out", "0"))
    
    coin_x = pjson.get("coin_x", "Unknown")
    coin_y = pjson.get("coin_y", "Unknown")
    
    # Determine direction based on non-zero amounts
    if amount_x_in > 0:
        token_in = coin_x
        token_out = coin_y
        raw_amt_in = str(amount_x_in)
        raw_amt_out = str(amount_y_out)
    else:
        token_in = coin_y
        token_out = coin_x
        raw_amt_in = str(amount_y_in)
        raw_amt_out = str(amount_x_out)

    in_decimals, in_symbol = _metadata_cache.get(token_in, (0, "UNKNOWN"))
    out_decimals, out_symbol = _metadata_cache.get(token_out, (0, "UNKNOWN"))

    def parse_decimal(amount_str, decimals):
        try:
            raw_int = int(amount_str)
        except:
            raw_int = 0
        return raw_int / (10 ** decimals) if decimals > 0 else raw_int

    in_decimal_amt = parse_decimal(raw_amt_in, in_decimals)
    out_decimal_amt = parse_decimal(raw_amt_out, out_decimals)

    event["token_in"] = token_in
    event["token_in_symbol"] = in_symbol
    event["token_in_decimals"] = in_decimals
    event["token_in_raw_amount"] = raw_amt_in
    event["token_in_decimal_amount"] = in_decimal_amt
    event["token_out"] = token_out
    event["token_out_symbol"] = out_symbol
    event["token_out_decimals"] = out_decimals
    event["token_out_raw_amount"] = raw_amt_out
    event["token_out_decimal_amount"] = out_decimal_amt

async def enrich_events_with_all_data(session, events):
    """Enhanced to include transaction block info"""
    # Gather unique tx digests and coin types
    unique_tx_digests = set()
    unique_coin_types = set()
    
    for e in events:
        tx_digest = e.get("id", {}).get("txDigest")
        if tx_digest:
            unique_tx_digests.add(tx_digest)
            
        pjson = e.get("parsedJson", {})
        coin_x = pjson.get("coin_x", "")
        coin_y = pjson.get("coin_y", "")
        if coin_x:
            unique_coin_types.add(coin_x)
        if coin_y:
            unique_coin_types.add(coin_y)

    print(f"Unique transactions to fetch: {len(unique_tx_digests)}")
    print(f"Unique coin types to fetch: {len(unique_coin_types)}")

    # Fetch transaction block info and coin metadata in parallel
    tx_tasks = [asyncio.create_task(get_tx_block_info(session, digest)) for digest in unique_tx_digests]
    coin_tasks = [asyncio.create_task(get_coin_metadata(session, ctype)) for ctype in unique_coin_types]
    
    await asyncio.gather(*tx_tasks, *coin_tasks)

    # Fill each event with enriched data
    for e in events:
        await _fill_single_event_from_cache(e)

    return events

###############################################################################
# 7) CSV EXPORT
###############################################################################

def write_events_to_csv_with_enrichment(events, output_csv="flowx_swaps.csv"):
    """Enhanced CSV writing with checkpoint and transaction block info"""
    fieldnames = [
        "txDigest",
        "eventSeq",
        "checkpoint",          # New field
        "timestampMs",         # New field from tx block
        "timestampIso",
        "sender",
        "type",
        "poolId",
        "token_in_type",
        "token_in_symbol",
        "token_in_decimals",
        "token_in_raw_amount",
        "token_in_decimal_amount",
        "token_out_type",
        "token_out_symbol",
        "token_out_decimals",
        "token_out_raw_amount",
        "token_out_decimal_amount",
        "parsed_json"
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for event in events:
            tx_digest = event["id"]["txDigest"]
            seq = event["id"]["eventSeq"]

            # Get checkpoint and tx block timestamp from cache
            checkpoint, tx_timestamp = _tx_cache.get(tx_digest, (None, None))
            
            # Convert timestampMs to ISO8601 if present
            timestamp_iso = ""
            timestamp_ms = tx_timestamp or event.get("timestampMs", "")
            if timestamp_ms:
                try:
                    ms_val = int(timestamp_ms)
                    dt = datetime.utcfromtimestamp(ms_val / 1000.0)
                    timestamp_iso = dt.isoformat(timespec="seconds")
                except:
                    pass

            sender = event["sender"]
            etype = event["type"]
            pjson = event.get("parsedJson", {})

            row = {
                "txDigest": tx_digest,
                "eventSeq": seq,
                "checkpoint": checkpoint,        # New field
                "timestampMs": timestamp_ms,     # New field
                "timestampIso": timestamp_iso,
                "sender": sender,
                "type": etype,
                "poolId": "",  # No pool ID for Flowx events
                "token_in_type": event.get("token_in", ""),
                "token_in_symbol": event.get("token_in_symbol", ""),
                "token_in_decimals": event.get("token_in_decimals", 0),
                "token_in_raw_amount": event.get("token_in_raw_amount", ""),
                "token_in_decimal_amount": event.get("token_in_decimal_amount", 0),
                "token_out_type": event.get("token_out", ""),
                "token_out_symbol": event.get("token_out_symbol", ""),
                "token_out_decimals": event.get("token_out_decimals", 0),
                "token_out_raw_amount": event.get("token_out_raw_amount", ""),
                "token_out_decimal_amount": event.get("token_out_decimal_amount", 0),
                "parsed_json": pjson
            }
            writer.writerow(row)

###############################################################################
# 8) ASYNC MAIN
###############################################################################

async def main():
    # Use a longer timeout for the session
    timeout = aiohttp.ClientTimeout(total=3600)  # 1 hour timeout
    async with aiohttp.ClientSession(timeout=timeout) as session:
        print("Starting to fetch events...")
        events = await fetch_swap_events_paginated(
            session,
            pages=DEFAULT_PAGES,
            limit_per_page=DEFAULT_LIMIT_PER_PAGE,
            descending=DEFAULT_DESCENDING
        )
        print(f"Total events fetched: {len(events)}")

        if not events:
            print("No events fetched. Exiting.")
            return

        # Enrich events with all data (including tx block info)
        print("Starting to enrich events...")
        start_enrich = time.time()
        await enrich_events_with_all_data(session, events)
        end_enrich = time.time()
        print(f"Enrichment took {end_enrich - start_enrich:.2f} seconds.")

        # Write them to CSV
        print("Starting to write events to CSV...")
        start_csv = time.time()
        output_csv = "flowx_swaps.csv"
        write_events_to_csv_with_enrichment(events, output_csv)
        end_csv = time.time()
        print(f"Wrote events to {output_csv} in {end_csv - start_csv:.2f} seconds.")

###############################################################################
# 9) DRIVER
###############################################################################

if __name__ == "__main__":
    asyncio.run(main())