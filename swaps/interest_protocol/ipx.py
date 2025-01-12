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
BASE_MODULE = "0x5c45d10c26c5fb53bfaff819666da6bc7053d2190dfa29fec311cc666ff1f4b0"

# Adjust these to tune concurrency and retries
MAX_CONCURRENT_REQUESTS = 5  
MAX_RETRIES = 5              

# Pagination defaults
DEFAULT_PAGES = 10
DEFAULT_LIMIT_PER_PAGE = 50
DEFAULT_DESCENDING = True

###############################################################################
# GLOBAL CACHES
###############################################################################
_metadata_cache = {}     # coin_type => (decimals, symbol)

###############################################################################
# EVENT TYPES TO TRACK
###############################################################################
SWAP_EVENTS = [
    f"{BASE_MODULE}::core::SwapTokenX<{BASE_MODULE}::curve::Volatile, 0x2::sui::SUI, 0x5d4b302506645c37ff133b98c4b50a5ae14841659738d6d733d59d0d217a93bf::coin::COIN>",
    f"{BASE_MODULE}::core::SwapTokenX<{BASE_MODULE}::curve::Volatile, 0x2::sui::SUI, 0x909cba62ce96d54de25bec9502de5ca7b4f28901747bbf96b76c2e63ec5f1cba::coin::COIN>",
    f"{BASE_MODULE}::core::SwapTokenX<{BASE_MODULE}::curve::Volatile, 0x2::sui::SUI, 0xaf8cd5edc19c4512f4259f0bee101a40d41ebed738ade5874359610ef8eeced5::coin::COIN>",
    f"{BASE_MODULE}::core::SwapTokenX<{BASE_MODULE}::curve::Volatile, 0x2::sui::SUI, 0xb848cce11ef3a8f62eccea6eb5b35a12c4c2b1ee1af7755d02d7bd6218e8226f::coin::COIN>",
    f"{BASE_MODULE}::core::SwapTokenX<{BASE_MODULE}::curve::Volatile, 0x2::sui::SUI, 0xc060006111016b8a020ad5b33834984a437aaa7d3c74c18e09a95d48aceab08c::coin::COIN>",
    f"{BASE_MODULE}::core::SwapTokenY<{BASE_MODULE}::curve::Volatile, 0x2::sui::SUI, 0x5d4b302506645c37ff133b98c4b50a5ae14841659738d6d733d59d0d217a93bf::coin::COIN>",
    f"{BASE_MODULE}::core::SwapTokenY<{BASE_MODULE}::curve::Volatile, 0x2::sui::SUI, 0x909cba62ce96d54de25bec9502de5ca7b4f28901747bbf96b76c2e63ec5f1cba::coin::COIN>",
    f"{BASE_MODULE}::core::SwapTokenY<{BASE_MODULE}::curve::Volatile, 0x2::sui::SUI, 0xaf8cd5edc19c4512f4259f0bee101a40d41ebed738ade5874359610ef8eeced5::coin::COIN>",
    f"{BASE_MODULE}::core::SwapTokenY<{BASE_MODULE}::curve::Volatile, 0x2::sui::SUI, 0xb848cce11ef3a8f62eccea6eb5b35a12c4c2b1ee1af7755d02d7bd6218e8226f::coin::COIN>",
    f"{BASE_MODULE}::core::SwapTokenY<{BASE_MODULE}::curve::Volatile, 0x2::sui::SUI, 0xc060006111016b8a020ad5b33834984a437aaa7d3c74c18e09a95d48aceab08c::coin::COIN>",
    f"{BASE_MODULE}::core::SwapTokenY<{BASE_MODULE}::curve::Volatile, 0x2::sui::SUI, 0xdbe380b13a6d0f5cdedd58de8f04625263f113b3f9db32b3e1983f49e2841676::coin::COIN>"
]

###############################################################################
# SAFE POST WITH RETRIES
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
# GET COIN METADATA (ASYNC) WITH CACHE
###############################################################################

async def get_coin_metadata(session: aiohttp.ClientSession, coin_type: str):
    if coin_type in _metadata_cache:
        return _metadata_cache[coin_type]

    if coin_type == "Unknown" or not coin_type:
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN")

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_getCoinMetadata",
        "params": [coin_type]
    }

    data = await safe_post(session, RPC_URL, payload)
    if "error" in data:
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN")

    result = data.get("result", {})
    if not result:
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN")

    decimals = result.get("decimals", 0)
    symbol = result.get("symbol", "UNKNOWN")

    _metadata_cache[coin_type] = (decimals, symbol)
    return (decimals, symbol)

###############################################################################
# FETCH EVENTS
###############################################################################

async def fetch_page(session: aiohttp.ClientSession, event_type: str, cursor, limit_per_page, descending):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_queryEvents",
        "params": [
            {"MoveEventType": event_type},
            cursor,
            limit_per_page,
            descending
        ]
    }
    data = await safe_post(session, RPC_URL, payload)
    return data["result"]

async def fetch_all_swap_events(
    session: aiohttp.ClientSession,
    pages=DEFAULT_PAGES,
    limit_per_page=DEFAULT_LIMIT_PER_PAGE,
    descending=DEFAULT_DESCENDING
):
    all_events = []
    
    for event_type in SWAP_EVENTS:
        print(f"\nFetching events for {event_type}")
        cursor = None
        event_count = 0

        for page_index in range(pages):
            result = await fetch_page(session, event_type, cursor, limit_per_page, descending)
            page_events = result["data"]
            all_events.extend(page_events)
            event_count += len(page_events)

            print(f"Page {page_index+1}: fetched {len(page_events)} events (total for this type: {event_count})")

            if not result["hasNextPage"]:
                break
            cursor = result["nextCursor"]

    print(f"\nTotal events fetched across all types: {len(all_events)}")
    return all_events

###############################################################################
# ENRICH EVENTS
###############################################################################

async def enrich_events(session, events):
    """Extract token types and amounts from the events"""
    enriched_events = []
    
    for event in events:
        event_type = event["type"]
        is_swap_x = "SwapTokenX" in event_type
        
        # Extract token types from the event type
        token_parts = event_type.split("<")[1].rstrip(">").split(", ")
        sui_token = "0x2::sui::SUI"
        other_token = token_parts[-1]
        
        # Get metadata for both tokens
        await get_coin_metadata(session, sui_token)
        await get_coin_metadata(session, other_token)
        
        # Parse amounts from parsedJson
        pjson = event.get("parsedJson", {})
        coin_x_in = pjson.get("coin_x_in", "0")
        coin_y_out = pjson.get("coin_y_out", "0")
        coin_x_out = pjson.get("coin_x_out", "0")
        coin_y_in = pjson.get("coin_y_in", "0")
        
        # For router swaps: 
        # SwapTokenX means we're swapping from token X (SUI) to token Y (other)
        # SwapTokenY means we're swapping from token Y (other) to token X (SUI)
        if is_swap_x:
            token_in = sui_token
            token_out = other_token
            amount_in = coin_x_in
            amount_out = coin_y_out
        else:
            token_in = other_token
            token_out = sui_token
            amount_in = coin_y_in
            amount_out = coin_x_out
            
        # Add debug logging for first few events
        if len(enriched_events) < 5:
            print(f"\nDebug Event {len(enriched_events)+1}:")
            print(f"Type: {'SwapTokenX' if is_swap_x else 'SwapTokenY'}")
            print(f"Token In: {token_in}")
            print(f"Amount In: {amount_in}")
            print(f"Token Out: {token_out}")
            print(f"Amount Out: {amount_out}")
            print("Raw JSON:", pjson)

        # Get decimals and symbols
        in_decimals, in_symbol = _metadata_cache.get(token_in, (0, "UNKNOWN"))
        out_decimals, out_symbol = _metadata_cache.get(token_out, (0, "UNKNOWN"))

        # Convert amounts to decimal
        def parse_decimal(amount_str, decimals):
            try:
                raw_int = int(amount_str)
            except:
                raw_int = 0
            return raw_int / (10 ** decimals) if decimals > 0 else raw_int

        in_decimal_amt = parse_decimal(amount_in, in_decimals)
        out_decimal_amt = parse_decimal(amount_out, out_decimals)

        # Add enriched data to event
        event.update({
            "token_in": token_in,
            "token_in_symbol": in_symbol,
            "token_in_decimals": in_decimals,
            "token_in_raw_amount": amount_in,
            "token_in_decimal_amount": in_decimal_amt,
            "token_out": token_out,
            "token_out_symbol": out_symbol,
            "token_out_decimals": out_decimals,
            "token_out_raw_amount": amount_out,
            "token_out_decimal_amount": out_decimal_amt
        })
        
        enriched_events.append(event)
    
    return enriched_events

###############################################################################
# WRITE TO CSV
###############################################################################

def write_events_to_csv(events, output_csv="ipx_swaps.csv"):
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

            timestamp_iso = ""
            timestamp_ms_str = event.get("timestampMs", "")
            if timestamp_ms_str:
                try:
                    ms_val = int(timestamp_ms_str)
                    dt = datetime.utcfromtimestamp(ms_val / 1000.0)
                    timestamp_iso = dt.isoformat(timespec="seconds")
                except:
                    pass

            row = {
                "txDigest": tx_digest,
                "eventSeq": seq,
                "timestampIso": timestamp_iso,
                "sender": event["sender"],
                "type": event["type"],
                "poolId": event.get("parsedJson", {}).get("id", ""),
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
                "parsedJson": event.get("parsedJson", {}),
            }
            writer.writerow(row)

###############################################################################
# MAIN
###############################################################################

async def main():
    async with aiohttp.ClientSession() as session:
        # 1) Fetch all swap events
        events = await fetch_all_swap_events(
            session,
            pages=DEFAULT_PAGES,
            limit_per_page=DEFAULT_LIMIT_PER_PAGE,
            descending=DEFAULT_DESCENDING
        )

        # 2) Enrich events with token data
        start_enrich = time.time()
        enriched_events = await enrich_events(session, events)
        end_enrich = time.time()
        print(f"Enrichment took {end_enrich - start_enrich:.2f} seconds.")

        # 3) Write to CSV
        start_csv = time.time()
        output_csv = "ipx_swaps.csv"
        write_events_to_csv(enriched_events, output_csv)
        end_csv = time.time()
        print(f"Wrote {len(enriched_events)} events to {output_csv} in {end_csv - start_csv:.2f} seconds.")

###############################################################################
# SCRIPT EXECUTION
###############################################################################

if __name__ == "__main__":
    print("\nStarting Router Swap Events Collection...")
    print(f"Will collect events for {len(SWAP_EVENTS)} different swap event types")
    print("Configuration:")
    print(f"  - Max concurrent requests: {MAX_CONCURRENT_REQUESTS}")
    print(f"  - Max retries per request: {MAX_RETRIES}")
    print(f"  - Pages per event type: {DEFAULT_PAGES}")
    print(f"  - Events per page: {DEFAULT_LIMIT_PER_PAGE}")
    print(f"  - Order: {'Descending' if DEFAULT_DESCENDING else 'Ascending'}\n")
    
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    
    print(f"\nTotal execution time: {end_time - start_time:.2f} seconds")