import csv
import asyncio
import aiohttp
import random
import time
from datetime import datetime

RPC_URL = "https://fullnode.mainnet.sui.io:443"

MAX_CONCURRENT_REQUESTS = 5
MAX_RETRIES = 5

DEFAULT_PAGES = 10
DEFAULT_LIMIT_PER_PAGE = 50
DEFAULT_DESCENDING = True

# We'll fetch all events from the "spot_dex" module in the Kriya package
#  (which you discovered by using "MoveEventModule": { package, module })
KRIYA_PACKAGE_ID = "0xa0eba10b173538c8fecca1dff298e488402cc9ff374f8a12ca7758eebe830b66"
KRIYA_MODULE = "spot_dex"

_pool_cache = {}           # pool_id -> (coinX, coinY)
_metadata_cache = {}       # coin_type -> (decimals, symbol)
_pool_reserves_cache = {}  # pool_id -> (previous_reserve_x, previous_reserve_y)

request_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

async def safe_post(session: aiohttp.ClientSession, url: str, json_data: dict):
    backoff = 1.0
    for attempt in range(MAX_RETRIES):
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
                print(f"[safe_post] Error attempt {attempt+1}: {e}")
                await asyncio.sleep(backoff + random.random())
                backoff *= 2
    raise Exception(f"[safe_post] Failed after {MAX_RETRIES} retries. Payload: {json_data}")

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


async def get_pool_coins(session: aiohttp.ClientSession, pool_id: str):
    """
    Similar to Cetus: fetch the object, see if it's a struct like 
    '...::Pool<coinX, coinY>'.
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
    resp_data = await safe_post(session, RPC_URL, payload)
    obj_res = resp_data.get("result", {})
    if "error" in obj_res:
        _pool_cache[pool_id] = ("Unknown", "Unknown")
        return ("Unknown", "Unknown")

    data = obj_res.get("data")
    if not data:
        _pool_cache[pool_id] = ("Unknown", "Unknown")
        return ("Unknown", "Unknown")

    type_str = data.get("type", "")
    coin_x = None
    coin_y = None

    # Suppose it's "0xa0eb...::spot_dex::Pool<coinX, coinY>"
    if "Pool<" in type_str:
        inside = type_str.split("Pool<", 1)[1].rstrip(">")
        # If it has 2 type params, they'll be separated by a comma
        parts = [p.strip() for p in inside.split(",")]
        if len(parts) == 2:
            coin_x, coin_y = parts

    # If we still didn't find them, see if the content has "fields" with coin_type_x, coin_type_y
    content = data.get("content", {})
    fields = content.get("fields", {})
    if not coin_x:
        coin_x = fields.get("coin_type_x")
    if not coin_y:
        coin_y = fields.get("coin_type_y")

    coin_x = coin_x or "Unknown"
    coin_y = coin_y or "Unknown"
    _pool_cache[pool_id] = (coin_x, coin_y)
    return (coin_x, coin_y)


async def fetch_events_spot_dex_module(
    session: aiohttp.ClientSession,
    package_id: str,
    module_name: str,
    pages=DEFAULT_PAGES,
    limit_per_page=DEFAULT_LIMIT_PER_PAGE,
    descending=DEFAULT_DESCENDING
):
    """
    Using "MoveEventModule": { package, module }
    to fetch all events in that module. 
    """
    all_events = []
    cursor = None

    for page_index in range(pages):
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "suix_queryEvents",
            "params": [
                {
                    "MoveEventModule": {
                        "package": package_id,
                        "module": module_name
                    }
                },
                cursor,
                limit_per_page,
                descending
            ]
        }
        data = await safe_post(session, RPC_URL, payload)
        result = data["result"]
        page_events = result["data"]
        all_events.extend(page_events)
        print(f"Page {page_index+1}: fetched {len(page_events)} events (total {len(all_events)})")

        if not result["hasNextPage"]:
            break
        cursor = result["nextCursor"]

    return all_events


def parse_decimal(amount_str, decimals):
    try:
        raw_int = int(amount_str)
    except:
        raw_int = 0
    return raw_int / (10 ** decimals) if decimals > 0 else raw_int


async def enrich_events_and_write_csv(session, events, output_csv="kriya_swap_events.csv"):
    """
    1) For each event, parse out the pool_id, fetch coin_x, coin_y from that pool,
       fetch coin metadata for both, figure out which coin was in/out based on
       the change in reserves (if we have a prior reserve state).
    2) Write them to CSV.
    """

    fieldnames = [
        "txDigest",
        "eventSeq",
        "timestampIso",
        "sender",
        "type",
        "poolId",
        "coin_in",
        "coin_in_symbol",
        "coin_in_decimals",
        "coin_in_raw_amount",
        "coin_in_decimal_amount",
        "coin_out",
        "coin_out_symbol",
        "coin_out_decimals",
        "coin_out_raw_amount",
        "coin_out_decimal_amount",
        "parsedJson"
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for ev in events:
            tx_digest = ev["id"]["txDigest"]
            seq = ev["id"]["eventSeq"]
            sender = ev["sender"]
            etype = ev["type"]
            timestamp_iso = ""

            # Convert timestampMs to ISO8601
            ts_ms = ev.get("timestampMs", "")
            if ts_ms:
                try:
                    ms_val = int(ts_ms)
                    dt = datetime.utcfromtimestamp(ms_val / 1000.0)
                    timestamp_iso = dt.isoformat(timespec="seconds")
                except:
                    pass

            pjson = ev.get("parsedJson", {})
            amount_in_str = pjson.get("amount_in", "0")
            amount_out_str = pjson.get("amount_out", "0")
            pool_id = pjson.get("pool_id", "")
            reserve_x_str = pjson.get("reserve_x", "0")
            reserve_y_str = pjson.get("reserve_y", "0")

            # 1) Identify the two coins from the pool
            coin_x, coin_y = await get_pool_coins(session, pool_id)

            # 2) Fetch metadata
            x_decimals, x_symbol = await get_coin_metadata(session, coin_x)
            y_decimals, y_symbol = await get_coin_metadata(session, coin_y)

            # 3) Decide which coin is "in" vs. "out"
            # We'll compare new reserves_x,y to old reserves_x,y for this pool
            # to see which one grew. If X grew, then X was coin_in, etc.
            # If we have no old record for this pool, we'll guess based on the event type param or just pick a default.
            old_rx, old_ry = _pool_reserves_cache.get(pool_id, (None, None))

            new_rx = int(reserve_x_str)
            new_ry = int(reserve_y_str)

            if old_rx is not None and old_ry is not None:
                diff_x = new_rx - old_rx
                diff_y = new_ry - old_ry
                # If diff_x >= 0, X is in. If diff_x < 0, X is out, etc.
                # It's possible both differ, especially if there's a protocol fee or something
                # but typically one side goes up, the other goes down.
                if diff_x >= 0 and diff_y <= 0:
                    # coin_x is in, coin_y out
                    coin_in = coin_x
                    coin_out = coin_y
                    coin_in_raw_str = amount_in_str
                    coin_out_raw_str = amount_out_str
                    in_decimals, in_symbol = x_decimals, x_symbol
                    out_decimals, out_symbol = y_decimals, y_symbol
                else:
                    # coin_x is out, coin_y in
                    coin_in = coin_y
                    coin_out = coin_x
                    coin_in_raw_str = amount_in_str
                    coin_out_raw_str = amount_out_str
                    in_decimals, in_symbol = y_decimals, y_symbol
                    out_decimals, out_symbol = x_decimals, x_symbol
            else:
                # No prior reserves to compare.
                # We can guess that the *event type param* might be coin_in or something else.
                # We'll just guess "coin_x is in, coin_y is out" to keep it simple.
                # Or you can do more logic if you know how Kriya encodes the type param.
                coin_in = coin_x
                coin_out = coin_y
                coin_in_raw_str = amount_in_str
                coin_out_raw_str = amount_out_str
                in_decimals, in_symbol = x_decimals, x_symbol
                out_decimals, out_symbol = y_decimals, y_symbol

            # Now store the new reserves in the cache for next time
            _pool_reserves_cache[pool_id] = (new_rx, new_ry)

            # Convert raw amounts
            in_decimal_amt = parse_decimal(coin_in_raw_str, in_decimals)
            out_decimal_amt = parse_decimal(coin_out_raw_str, out_decimals)

            row = {
                "txDigest": tx_digest,
                "eventSeq": seq,
                "timestampIso": timestamp_iso,
                "sender": sender,
                "type": etype,
                "poolId": pool_id,
                "coin_in": coin_in,
                "coin_in_symbol": in_symbol,
                "coin_in_decimals": in_decimals,
                "coin_in_raw_amount": coin_in_raw_str,
                "coin_in_decimal_amount": in_decimal_amt,
                "coin_out": coin_out,
                "coin_out_symbol": out_symbol,
                "coin_out_decimals": out_decimals,
                "coin_out_raw_amount": coin_out_raw_str,
                "coin_out_decimal_amount": out_decimal_amt,
                "parsedJson": pjson
            }
            writer.writerow(row)

    print(f"Wrote {len(events)} events to {output_csv}")

async def main():
    async with aiohttp.ClientSession() as session:
        # 1) Fetch up to 10 pages from the Kriya "spot_dex" module
        events = await fetch_events_spot_dex_module(
            session,
            package_id=KRIYA_PACKAGE_ID,
            module_name=KRIYA_MODULE,
            pages=DEFAULT_PAGES,
            limit_per_page=DEFAULT_LIMIT_PER_PAGE,
            descending=DEFAULT_DESCENDING
        )
        print(f"Total events fetched: {len(events)}")

        # 2) Enrich & write CSV
        await enrich_events_and_write_csv(session, events, "kriya_swap_events.csv")

if __name__ == "__main__":
    asyncio.run(main())
