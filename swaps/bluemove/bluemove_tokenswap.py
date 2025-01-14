import csv
import asyncio
import aiohttp
import random
import time
import json
from datetime import datetime

###############################################################################
# CONFIG
###############################################################################

RPC_URL = "https://fullnode.mainnet.sui.io:443"

MAX_CONCURRENT_REQUESTS = 5
MAX_RETRIES = 10

# Pagination
DEFAULT_PAGES = 2
DEFAULT_LIMIT_PER_PAGE = 50
DEFAULT_DESCENDING = True

###############################################################################
# GLOBAL CACHES
###############################################################################
_pool_cache = {}         # pool_id => (coin_x, coin_y)
_metadata_cache = {}     # coin_type => (decimals, symbol)
_tx_cache = {}           # tx_digest => (checkpoint, timestamp)

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
                        print(f"Received 429 Too Many Requests. Backing off for {backoff + random.random():.2f} seconds.")
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
    """
    Fetch coin X/Y for a given pool_id using sui_getObject.
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
        print(f"Error in response for pool {pool_id}: {result['error']}")
        _pool_cache[pool_id] = ("Unknown", "Unknown")
        return ("Unknown", "Unknown")

    obj_data = result.get("data")
    if not obj_data:
        print(f"No data found for pool {pool_id}.")
        _pool_cache[pool_id] = ("Unknown", "Unknown")
        return ("Unknown", "Unknown")

    coin_type_x = None
    coin_type_y = None

    type_str = obj_data.get("type", "")
    if "Pool<" in type_str:
        inside = type_str.split("Pool<", 1)[1].rstrip(">")
        parts = [p.strip() for p in inside.split(",")]
        if len(parts) == 2:
            coin_type_x, coin_type_y = parts
        else:
            print(f"Unexpected number of type parameters in pool {pool_id}: {parts}")

    content = obj_data.get("content", {})
    fields = content.get("fields", {})
    if not coin_type_x:
        coin_type_x = fields.get("coin_type_x")
    if not coin_type_y:
        coin_type_y = fields.get("coin_type_y")

    if not coin_type_x:
        coin_type_x = "Unknown"
    if not coin_type_y:
        coin_type_y = "Unknown"

    _pool_cache[pool_id] = (coin_type_x, coin_type_y)
    print(f"Pool {pool_id}: coin_x={coin_type_x}, coin_y={coin_type_y}")
    return (coin_type_x, coin_type_y)

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
        print(f"Error in metadata response for {coin_type}: {data['error']}")
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN")

    result = data.get("result", {})
    if not result:
        print(f"No metadata found for {coin_type}.")
        _metadata_cache[coin_type] = (0, "UNKNOWN")
        return (0, "UNKNOWN")

    decimals = result.get("decimals", 0)
    symbol = result.get("symbol", "UNKNOWN")

    _metadata_cache[coin_type] = (decimals, symbol)
    print(f"Metadata for {coin_type}: decimals={decimals}, symbol={symbol}")
    return (decimals, symbol)

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
    except Exception as e:
        print(f"Error fetching transaction block info for {tx_digest}: {e}")
        _tx_cache[tx_digest] = (None, None)
        return (None, None)

    data = result.get("result", {})
    
    checkpoint = data.get("checkpoint", None)
    timestamp = data.get("timestampMs", None)
    
    _tx_cache[tx_digest] = (checkpoint, timestamp)
    print(f"Transaction {tx_digest}: checkpoint={checkpoint}, timestampMs={timestamp}")
    return (checkpoint, timestamp)

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
        if "result" not in data:
            print(f"Unexpected response structure: {data}")
            raise KeyError("'result' key not found in response.")
        return data["result"]
    except Exception as e:
        print(f"Error fetching page: {e}")
        return {"data": [], "hasNextPage": False, "nextCursor": None}

###############################################################################
# 6) PAGINATED EVENT FETCH PER EVENT TYPE
###############################################################################

async def fetch_swap_events_for_type(
    session: aiohttp.ClientSession,
    event_type: str,
    pages=DEFAULT_PAGES,
    limit_per_page=DEFAULT_LIMIT_PER_PAGE,
    descending=DEFAULT_DESCENDING
):
    """
    Asynchronously fetch up to `pages` pages of SwapEvent objects for a specific event type.
    """
    event_filter = {
        "MoveEventType": event_type  # Single event type per request
    }

    all_events = []
    cursor = None

    for page_index in range(pages):
        result = await fetch_page(session, event_filter, cursor, limit_per_page, descending)
        page_events = result.get("data", [])
        all_events.extend(page_events)

        print(f"Event Type: {event_type} | Page {page_index+1}: fetched {len(page_events)} events (total {len(all_events)})")

        if not result.get("hasNextPage", False):
            print(f"Event Type: {event_type} | No more pages to fetch.")
            break
        cursor = result.get("nextCursor", None)

    return all_events

###############################################################################
# 7) FILL SINGLE EVENT FROM CACHE
###############################################################################

async def _fill_single_event_from_cache(event: dict):
    """
    Fill event['token_in'], etc. from pool cache and coin cache (no new network calls).
    """
    pjson = event.get("parsedJson", {})
    
    if isinstance(pjson, str):
        try:
            pjson = json.loads(pjson)
        except json.JSONDecodeError:
            print(f"Failed to parse parsedJson for event {event.get('id', {}).get('eventSeq', 'Unknown')}")
            pjson = {}

    pool_id = pjson.get("pool_id", "")
    
    if not pool_id:
        print(f"Event {event.get('id', {}).get('eventSeq', 'Unknown')} missing 'pool_id' field in parsedJson.")
        pool_id = "Unknown"

    coin_x, coin_y = _pool_cache.get(pool_id, ("Unknown", "Unknown"))

    amount_x_in = pjson.get("amount_x_in", pjson.get("amountXIn", "0"))
    amount_y_in = pjson.get("amount_y_in", pjson.get("amountYIn", "0"))
    amount_x_out = pjson.get("amount_x_out", pjson.get("amountXOut", "0"))
    amount_y_out = pjson.get("amount_y_out", pjson.get("amountYOut", "0"))

    token_in = "Unknown"
    token_out = "Unknown"
    token_in_raw = "0"
    token_out_raw = "0"

    try:
        if int(amount_x_in) > 0:
            token_in = coin_x
            token_out = coin_y
            token_in_raw = amount_x_in
            token_out_raw = amount_y_out
        elif int(amount_y_in) > 0:
            token_in = coin_y
            token_out = coin_x
            token_in_raw = amount_y_in
            token_out_raw = amount_x_out
        else:
            print(f"Swap direction undefined for event {event.get('id', {}).get('eventSeq', 'Unknown')}.")
    except ValueError:
        print(f"Invalid amount values for event {event.get('id', {}).get('eventSeq', 'Unknown')}.")

    in_decimals, in_symbol = _metadata_cache.get(token_in, (0, "UNKNOWN"))
    out_decimals, out_symbol = _metadata_cache.get(token_out, (0, "UNKNOWN"))

    def parse_decimal(amount_str, decimals):
        try:
            raw_int = int(amount_str)
        except (ValueError, TypeError):
            raw_int = 0
        return raw_int / (10 ** decimals) if decimals > 0 else raw_int

    in_decimal_amt = parse_decimal(token_in_raw, in_decimals)
    out_decimal_amt = parse_decimal(token_out_raw, out_decimals)

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
    event["poolId"] = pool_id

    print(f"Enriched event {event.get('id', {}).get('eventSeq', 'Unknown')}: token_in={token_in} ({in_symbol}), token_out={token_out} ({out_symbol})")

###############################################################################
# 8) BATCHED ENRICHMENT
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
                print(f"Failed to parse parsedJson for event {e.get('id', {}).get('eventSeq', 'Unknown')}")
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
    await asyncio.gather(*pool_tasks)

    tx_tasks = [asyncio.create_task(get_tx_block_info(session, digest)) for digest in unique_tx_digests]
    await asyncio.gather(*tx_tasks)
    
    unique_coin_types = set()
    for pid in unique_pool_ids:
        coin_x, coin_y = _pool_cache.get(pid, ("Unknown", "Unknown"))
        if coin_x and coin_x != "Unknown":
            unique_coin_types.add(coin_x)
        if coin_y and coin_y != "Unknown":
            unique_coin_types.add(coin_y)

    print(f"Unique coin types to fetch metadata for: {len(unique_coin_types)}")

    coin_tasks = [asyncio.create_task(get_coin_metadata(session, ctype)) for ctype in unique_coin_types]
    await asyncio.gather(*coin_tasks)

    enrichment_tasks = [asyncio.create_task(_fill_single_event_from_cache(e)) for e in events]
    await asyncio.gather(*enrichment_tasks)

    return events

###############################################################################
# 9) WRITE EVENTS TO CSV
###############################################################################

def write_events_to_csv_with_enrichment(events, output_csv="bluemove_swap_events.csv"):
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

    try:
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for event in events:
                tx_digest = event.get("id", {}).get("txDigest", "")
                seq = event.get("id", {}).get("eventSeq", "")

                checkpoint, tx_timestamp = _tx_cache.get(tx_digest, (None, None))
                
                timestamp_iso = ""
                timestamp_ms = tx_timestamp or event.get("timestampMs", "")
                if timestamp_ms:
                    try:
                        ms_val = int(timestamp_ms)
                        dt = datetime.utcfromtimestamp(ms_val / 1000.0)
                        timestamp_iso = dt.isoformat(timespec="seconds")
                    except (ValueError, TypeError):
                        print(f"Error parsing timestampMs {timestamp_ms} for tx {tx_digest}")
                        pass

                sender = event.get("sender", "")
                etype = event.get("type", "")
                pjson = event.get("parsedJson", {})
                if isinstance(pjson, dict):
                    pjson_str = json.dumps(pjson)
                else:
                    pjson_str = str(pjson)
                pool_id = event.get("poolId", "")

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
                    "token_in_symbol": event.get("token_in_symbol", ""),
                    "token_in_decimals": event.get("token_in_decimals", 0),
                    "token_in_raw_amount": event.get("token_in_raw_amount", ""),
                    "token_in_decimal_amount": event.get("token_in_decimal_amount", 0),
                    "token_out": event.get("token_out", ""),
                    "token_out_symbol": event.get("token_out_symbol", ""),
                    "token_out_decimals": event.get("token_out_decimals", 0),
                    "token_out_raw_amount": event.get("token_out_raw_amount", ""),
                    "token_out_decimal_amount": event.get("token_out_decimal_amount", 0),
                    "parsedJson": pjson_str,
                }
                writer.writerow(row)
        print(f"Wrote {len(events)} events to {output_csv}.")
    except Exception as e:
        print(f"Error writing to CSV: {e}")

###############################################################################
# 10) ASYNC MAIN
###############################################################################

async def main():
    swap_event_types = [
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0xd9f9b0b4f35276eecd1eea6985bfabe2a2bbd5575f9adb9162ccbdb4ddebde7f::smove::SMOVE>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0x19b68de6a17be1fac9043faa550150b558edb7ed27cddd044828723aebe6f7af::suimarket::SUIMARKET>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0x38c41caa99a1e767f9436b60a5ce7be73d6c70d4f4b1e6a6048b90277cb0cd85::suigma::SUIGMA>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0x57184d3106ea793e09506453ef61559fa74f34ea8d1b9db4d3b47c980a053f94::capys::CAPYS>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0x571b537313abc4d66d68a86d422cd7c7020b91fb73aa3c1069923e3acdd7307d::bluec::BLUEC>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0x578d7bf9e96ba1f811aece4006991a20e0af52f84e252732c098b7992d5bb10d::luna::LUNA>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0x5b11f0449f3aa1f91955e87f60f2493933a526946357cf408868d222fa47fd37::zappy::ZAPPY>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0x61a279cf60998ca93df93f186c117048e5351b88eccc31efef8fc15488ad85ee::baba::BABA>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0x7373a010c5a466cd4ad5c73f72b2cd6d8c61d34540dc401ff759c4bf938e7e33::luga::LUGA>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0x77ac2b6b73c4a87c253193944be712bc1799ed71070b8a70b0bf56f61bc51a7a::strump::STRUMP>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0x8120491d565ceacf20000e75b0829591ed3c9d2eb1d29c53dbe44947e3b5ae87::fuki::FUKI>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0x89886681b5b9400bdd63497681e7aad637e2f665007801915a5dffbe4938dd21::hippo2::HIPPO2>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0xa2c255f996396807545328bee5f4ae2d1e42758fa3068139fe9edcb43c587166::croco::CROCO>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0xb45bdfaa00776818bf9c58eda2987cbe9d4167a12844d7557c7adadc83698c92::sog::SOG>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0xd42570d78904f4e80a27852446b02b7dfb80d515f5b67399e8644e913c987748::punk::PUNK>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0xdf2be2807709fef8c7d38841ba54645eaf0048f4106e1a02239bf303971ff2da::dolf::DOLF>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0xe38babf1e0f7e549f9d4c0441c9d00c8ffc9dcd34dcc19f3bb12eafe0267e037::movepump::MOVEPUMP>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0xe6b43a6f4f220f7d454097195c12688e7dcb258ac1f5cf87219e2609cefe1346::nago::NAGO>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0xe9d72c853ddde90a5430217e78aff193245712d4c99d91d958a588db99858370::sui::SUI>",
        "0xb24b6789e088b876afabca733bed2299fbc9e2d6369be4d1acfa17d8145454d9::swap::Swap_Event<0x2::sui::SUI, 0xf5c71b5525a6c280fe4c8f20a4541179991ce96f4094f385c96377a1656ab2f7::popsui::POPSUI>",
    ]

    all_fetched_events = []

    async with aiohttp.ClientSession() as session:
        print("Fetching swap events...")

        fetch_tasks = [
            asyncio.create_task(
                fetch_swap_events_for_type(
                    session,
                    event_type=event_type,
                    pages=DEFAULT_PAGES,
                    limit_per_page=DEFAULT_LIMIT_PER_PAGE,
                    descending=DEFAULT_DESCENDING
                )
            )
            for event_type in swap_event_types
        ]

        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        for idx, result in enumerate(results):
            event_type = swap_event_types[idx]
            if isinstance(result, Exception):
                print(f"Failed to fetch events for {event_type}: {result}")
            else:
                all_fetched_events.extend(result)

        print(f"Total events fetched across all types: {len(all_fetched_events)}")

        if not all_fetched_events:
            print("No events to process. Exiting.")
            return

        start_enrich = time.time()
        await enrich_events_with_all_data(session, all_fetched_events)
        end_enrich = time.time()
        print(f"Enrichment took {end_enrich - start_enrich:.2f} seconds.")

    start_csv = time.time()
    output_csv = "bluemove_swap_events.csv"
    write_events_to_csv_with_enrichment(all_fetched_events, output_csv)
    end_csv = time.time()
    print(f"Wrote events to {output_csv} in {end_csv - start_csv:.2f} seconds.")

###############################################################################
# 11) DRIVER
###############################################################################

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"An error occurred during execution: {e}")