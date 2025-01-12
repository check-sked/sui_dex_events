import csv
import asyncio
import aiohttp
import random
import time
from datetime import datetime
import json

###############################################################################
# CONFIG
###############################################################################

RPC_URL = "https://fullnode.mainnet.sui.io:443"

MAX_CONCURRENT_REQUESTS = 5
MAX_RETRIES = 5

# Pagination
DEFAULT_PAGES = 10
DEFAULT_LIMIT_PER_PAGE = 50
DEFAULT_DESCENDING = True

###############################################################################
# GLOBAL CACHES
###############################################################################
_pool_cache = {}         # pool_id => (coinA, coinB)
_metadata_cache = {}     # coin_type => (decimals, symbol)
_tx_cache = {}          # tx_digest => (checkpoint, timestamp)

# Hardcoded metadata
SPECIAL_COINS = {
    "d5fcf6a2947411e145a01e31cf97d43d13d9cd37b7cac2bb3296f7539ebaaf4a::rex::REX": (6, "REX"),
    "f6d855f2876cd5b5bfaa7fcbbb9721f447290606699a6207b4a7757e3eea0ae::sgf::SGF": (6, "SGF"),
    "f25a3d63032dbf7005453b831862a6831fadf8c501d9cde0e54f2da95b27b470::af_lp::AF_LP": (0,"AF_LP"),
    "02b8726cb7ddcd879d9e7228c7961ea5deaf5dd72bace57ad9b24f52ff857cd6::af_lp::AF_LP": (0,"AF_LP")
}

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

    type_str = obj_data.get("type", "")
    if "Pool<" in type_str:
        inside = type_str.split("Pool<", 1)[1].rstrip(">")
        parts = [p.strip() for p in inside.split(",")]
        if len(parts) == 2:
            coin_type_a, coin_type_b = parts

    content = obj_data.get("content", {})
    fields = content.get("fields", {})

    if not coin_type_a:
        coin_type_a = fields.get("coin_type_a")
    if not coin_type_b:
        coin_type_b = fields.get("coin_type_b")

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

    print(f"Fetched pool {pool_id}: Coin A = {coin_type_a}, Coin B = {coin_type_b}")

    _pool_cache[pool_id] = (coin_type_a, coin_type_b)
    return (coin_type_a, coin_type_b)

###############################################################################
# 3) GET COIN METADATA (ASYNC) WITH CACHE
###############################################################################

async def get_coin_metadata(session: aiohttp.ClientSession, coin_type: str):
    """
    Fetch metadata (decimals, symbol) for a given coin_type.
    Now returns tuple of (decimals, symbol, formatted_coin_type)
    """
    if coin_type in _metadata_cache:
        return _metadata_cache[coin_type] + (coin_type,)

    if coin_type == "Unknown" or not coin_type:
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN", coin_type)

    if coin_type in SPECIAL_COINS:
        decimals, symbol = SPECIAL_COINS[coin_type]
        _metadata_cache[coin_type] = (decimals, symbol)
        return (decimals, symbol, coin_type)

    formatted_coin_type = coin_type
    if not ('::0x' in coin_type or coin_type.startswith('0x')):
        formatted_coin_type = f'0x{coin_type}'

    print(f"Fetching metadata:")
    print(f"  Original coin_type: {coin_type}")
    print(f"  Formatted coin_type: {formatted_coin_type}")

    if not isinstance(formatted_coin_type, str):
        print(f"Invalid coin_type format: {formatted_coin_type}")
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN", formatted_coin_type)

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_getCoinMetadata",
        "params": [formatted_coin_type]
    }

    try:
        data = await safe_post(session, RPC_URL, payload)
    except Exception as e:
        print(f"Error fetching metadata for {formatted_coin_type}: {e}")
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN", formatted_coin_type)

    if "error" in data:
        print(f"Error in metadata response for {formatted_coin_type}: {data.get('error')}")
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN", formatted_coin_type)

    result = data.get("result", {})
    if not result:
        print(f"No metadata found for {formatted_coin_type}.")
        print(f"Full response: {json.dumps(data, indent=2)}")
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN", formatted_coin_type)

    decimals = result.get("decimals", 0)
    symbol = result.get("symbol", "UNKNOWN")

    if symbol == "UNKNOWN":
        print(f"Got UNKNOWN symbol for coin type:")
        print(f"  Original: {coin_type}")
        print(f"  Formatted: {formatted_coin_type}")
        print(f"  API Response: {json.dumps(data, indent=2)}")

    _metadata_cache[coin_type] = (decimals, symbol)
    return (decimals, symbol, formatted_coin_type)

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
# 5) FETCH A SINGLE PAGE OF EVENTS
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
# 6) PAGINATED EVENT FETCH
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
# 7) BATCHED ENRICHMENT: PRE-FETCH ALL DATA
###############################################################################

async def enrich_events_with_all_data(session, events):
    unique_pool_ids = set()
    unique_tx_digests = set()
    
    for e in events:
        pjson = e.get("parsedJson", {})
        if isinstance(pjson, str):
            try:
                pjson = json.loads(pjson)
            except json.JSONDecodeError:
                pjson = {}
        pool_id = pjson.get("pool_id", "")
        if pool_id:
            unique_pool_ids.add(pool_id)
            
        tx_digest = e.get("id", {}).get("txDigest")
        if tx_digest:
            unique_tx_digests.add(tx_digest)

    print(f"Unique pools to fetch: {len(unique_pool_ids)}")
    print(f"Unique transactions to fetch: {len(unique_tx_digests)}")

    pool_tasks = [asyncio.create_task(get_pool_coins(session, pid)) for pid in unique_pool_ids]
    tx_tasks = [asyncio.create_task(get_tx_block_info(session, digest)) for digest in unique_tx_digests]
    
    await asyncio.gather(*pool_tasks, *tx_tasks)

    unique_coin_types = set()
    for pid in unique_pool_ids:
        coin_a, coin_b = _pool_cache.get(pid, ("Unknown", "Unknown"))
        if coin_a and coin_a != "Unknown":
            unique_coin_types.add(coin_a)
        if coin_b and coin_b != "Unknown":
            unique_coin_types.add(coin_b)

    print(f"Unique coin types to fetch: {len(unique_coin_types)}")

    coin_tasks = [asyncio.create_task(get_coin_metadata(session, ctype)) for ctype in unique_coin_types]
    await asyncio.gather(*coin_tasks)

    enrichment_tasks = [asyncio.create_task(_fill_single_event_from_cache(session, e)) for e in events]
    await asyncio.gather(*enrichment_tasks)

    return events

###############################################################################
# 8) FILL SINGLE EVENT FROM CACHE
###############################################################################

async def _fill_single_event_from_cache(session, event: dict):
    """
    Fill event['token_in'], etc. from pool cache and coin cache (no new network calls).
    Updated to include formatted types in the event data.
    """
    pjson = event.get("parsedJson", {})
    if isinstance(pjson, str):
        try:
            pjson = json.loads(pjson)
        except json.JSONDecodeError:
            pjson = {}
    pool_id = pjson.get("pool_id", "")

    coin_a, coin_b = _pool_cache.get(pool_id, ("Unknown", "Unknown"))

    types_in = pjson.get("types_in", [])
    types_out = pjson.get("types_out", [])
    amounts_in = pjson.get("amounts_in", [])
    amounts_out = pjson.get("amounts_out", [])

    token_in = types_in[0] if types_in else "Unknown"
    token_out = types_out[0] if types_out else "Unknown"

    token_in_raw = amounts_in[0] if amounts_in else "0"
    token_out_raw = amounts_out[0] if amounts_out else "0"

    in_decimals, in_symbol, in_formatted_type = await get_coin_metadata(session, token_in)
    out_decimals, out_symbol, out_formatted_type = await get_coin_metadata(session, token_out)

    def parse_decimal(amount_str, decimals):
        try:
            raw_int = int(amount_str)
        except:
            raw_int = 0
        return raw_int / (10 ** decimals) if decimals > 0 else raw_int

    in_decimal_amt = parse_decimal(token_in_raw, in_decimals)
    out_decimal_amt = parse_decimal(token_out_raw, out_decimals)

    event["token_in"] = token_in
    event["token_in_formatted_type"] = in_formatted_type
    event["token_in_symbol"] = in_symbol
    event["token_in_decimals"] = in_decimals
    event["token_in_raw_amount"] = token_in_raw
    event["token_in_decimal_amount"] = in_decimal_amt
    event["token_out"] = token_out
    event["token_out_formatted_type"] = out_formatted_type
    event["token_out_symbol"] = out_symbol
    event["token_out_decimals"] = out_decimals
    event["token_out_raw_amount"] = token_out_raw
    event["token_out_decimal_amount"] = out_decimal_amt

###############################################################################
# 9) WRITE EVENTS TO CSV
###############################################################################

def write_events_to_csv_with_enrichment(events, output_csv="aftermathv2_swap_events.csv"):
    """
    Enhanced version that includes checkpoint and transaction block info
    """
    fieldnames = [
        "txDigest",
        "eventSeq",
        "checkpoint",
        "timestampMs",
        "timestampIso",
        "sender",
        "type",
        "poolId",
        "token_in",
        "token_in_formatted_type",
        "token_in_symbol",
        "token_in_decimals",
        "token_in_raw_amount",
        "token_in_decimal_amount",
        "token_out",
        "token_out_formatted_type",
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

            checkpoint, tx_timestamp = _tx_cache.get(tx_digest, (None, None))
            
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
            if isinstance(pjson, str):
                try:
                    pjson = json.loads(pjson)
                except json.JSONDecodeError:
                    pjson = {}
            pool_id = pjson.get("pool_id", "")

            row = {
                "txDigest": tx_digest,
                "eventSeq": seq,
                "checkpoint": checkpoint,
                "timestampMs": timestamp_ms,
                "timestampIso": timestamp_iso,
                "sender": sender,
                "type": etype,
                "poolId": pool_id,
                "token_in": event.get("token_in", ""),
                "token_in_formatted_type": event.get("token_in_formatted_type", ""),
                "token_in_symbol": event.get("token_in_symbol", ""),
                "token_in_decimals": event.get("token_in_decimals", 0),
                "token_in_raw_amount": event.get("token_in_raw_amount", ""),
                "token_in_decimal_amount": event.get("token_in_decimal_amount", 0),
                "token_out": event.get("token_out", ""),
                "token_out_formatted_type": event.get("token_out_formatted_type", ""),
                "token_out_symbol": event.get("token_out_symbol", ""),
                "token_out_decimals": event.get("token_out_decimals", 0),
                "token_out_raw_amount": event.get("token_out_raw_amount", ""),
                "token_out_decimal_amount": event.get("token_out_decimal_amount", 0),
                "parsedJson": json.dumps(pjson),
            }
            writer.writerow(row)

###############################################################################
# 10) ASYNC MAIN
###############################################################################

async def main():
    async with aiohttp.ClientSession() as session:
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

        print("Starting to enrich events...")
        start_enrich = time.time()
        await enrich_events_with_all_data(session, events)
        end_enrich = time.time()
        print(f"Enrichment took {end_enrich - start_enrich:.2f} seconds.")

        print("Starting to write events to CSV...")
        start_csv = time.time()
        output_csv = "aftermathv2_swap_events.csv"
        write_events_to_csv_with_enrichment(events, output_csv)
        end_csv = time.time()
        print(f"Wrote events to {output_csv} in {end_csv - start_csv:.2f} seconds.")

###############################################################################
# 11) DRIVER
###############################################################################

if __name__ == "__main__":
    asyncio.run(main())