import csv
import asyncio
import aiohttp
import random
import time
from datetime import datetime

# Configuration
RPC_URL = "https://fullnode.mainnet.sui.io:443"
MAX_CONCURRENT_REQUESTS = 5
MAX_RETRIES = 5
DEFAULT_PAGES = 100
DEFAULT_LIMIT_PER_PAGE = 50
DEFAULT_DESCENDING = True

_pool_cache = {}
_metadata_cache = {}

# Semaphore for rate limiting
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

async def get_pool_coins(session: aiohttp.ClientSession, pool_id: str):
    """Modified to handle the new pool structure"""
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

    # Updated parsing logic for the new pool structure
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

async def get_coin_metadata(session: aiohttp.ClientSession, coin_type: str):
    """Fetches coin metadata entirely via RPC without hardcoded values"""
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

        # Fetch metadata from RPC
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
            decimals = result.get("decimals", 0)  # Default to 0 if not provided
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

async def fetch_page(session: aiohttp.ClientSession, event_filter: dict, cursor, limit_per_page, descending):
    """Keep existing page fetching logic"""
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
    """Updated to use new event type with debugging"""
    event_filter = {
        "MoveEventType": "0xba153169476e8c3114962261d1edc70de5ad9781b83cc617ecc8c1923191cae0::pair::Swapped"
    }
    
    print("Fetching events with filter:", event_filter)

    all_events = []
    cursor = None

    for page_index in range(pages):
        result = await fetch_page(session, event_filter, cursor, limit_per_page, descending)
        
        # Debug first page results
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

async def _fill_single_event_from_cache(event: dict):
    """Modified to handle the Swapped event structure"""
    pjson = event.get("parsedJson", {})
    
    # For this event type, we need to determine which coin was swapped in/out
    amount_x_in = int(pjson.get("amount_x_in", "0"))
    amount_x_out = int(pjson.get("amount_x_out", "0"))
    amount_y_in = int(pjson.get("amount_y_in", "0"))
    amount_y_out = int(pjson.get("amount_y_out", "0"))
    
    coin_x = pjson.get("coin_x", "Unknown")
    coin_y = pjson.get("coin_y", "Unknown")
    
    # Determine which coin is being swapped in/out based on non-zero amounts
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

async def enrich_events_with_pool_and_coin_data(session, events):
    """Keep existing enrichment logic with updated field names"""
    # For this event type, we don't need pool_ids since coins are directly in the event
    unique_coin_types = set()
    for e in events:
        pjson = e.get("parsedJson", {})
        coin_x = pjson.get("coin_x", "")
        coin_y = pjson.get("coin_y", "")
        if coin_x:
            unique_coin_types.add(coin_x)
        if coin_y:
            unique_coin_types.add(coin_y)

    # Fetch metadata for all unique coin types
    coin_tasks = [asyncio.create_task(get_coin_metadata(session, ctype)) for ctype in unique_coin_types]
    await asyncio.gather(*coin_tasks)

    # Fill each event with the fetched metadata
    for e in events:
        await _fill_single_event_from_cache(e)

    return events

def write_events_to_csv_with_enrichment(events, output_csv="flowx_swaps.csv"):
    """Enhanced CSV writing logic with requested field names"""
    fieldnames = [
        "txDigest",
        "eventSeq",
        "timestampIso",
        "sender",
        "type",
        "poolId",
        "token_in_type",  # Full type
        "token_in_symbol",
        "token_in_decimals",
        "token_in_raw_amount",
        "token_in_decimal_amount",
        "token_out_type",  # Full type
        "token_out_symbol",
        "token_out_decimals",
        "token_out_raw_amount",
        "token_out_decimal_amount",
        "parsed_json"  # Changed from raw_data
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for event in events:
            tx_digest = event["id"]["txDigest"]
            seq = event["id"]["eventSeq"]

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
            # No pool_id in this event type
            pool_id = ""

            row = {
                "txDigest": tx_digest,
                "eventSeq": seq,
                "timestampIso": timestamp_iso,
                "sender": sender,
                "type": etype,
                "poolId": "",  # No pool ID for these events
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

async def main():
    # Use a longer timeout for the session
    timeout = aiohttp.ClientTimeout(total=3600)  # 1 hour timeout
    async with aiohttp.ClientSession(timeout=timeout) as session:
        events = await fetch_swap_events_paginated(
            session,
            #pages=1,  # Reduced number of pages for testing
            limit_per_page=50,
            descending=True
        )
        print(f"Total events fetched: {len(events)}")

        start_enrich = time.time()
        await enrich_events_with_pool_and_coin_data(session, events)
        end_enrich = time.time()
        print(f"Enrichment took {end_enrich - start_enrich:.2f} seconds.")

    start_csv = time.time()
    output_csv = "flowx_swaps.csv"
    write_events_to_csv_with_enrichment(events, output_csv)
    end_csv = time.time()
    print(f"Wrote events to {output_csv} in {end_csv - start_csv:.2f} seconds.")

if __name__ == "__main__":
    asyncio.run(main())
