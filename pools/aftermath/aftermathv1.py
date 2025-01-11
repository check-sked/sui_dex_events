import csv
import asyncio
import aiohttp
import random
import time
from datetime import datetime
import json
from typing import Dict, List, Optional, Tuple

###############################################################################
# CONFIG
###############################################################################

RPC_URL = "https://fullnode.mainnet.sui.io:443"

# Concurrency and retry settings
MAX_CONCURRENT_REQUESTS = 5
MAX_RETRIES = 5

###############################################################################
# DEX Configurations
###############################################################################

DEX_CONFIGS = {
    "aftermathv1": {
        "name": "Aftermath v1",
        "package_id": "0xefe170ec0be4d762196bedecd7a065816576198a6527c99282a2551aaa7da38c",
        "event_query": {
            "MoveEventType": "0xefe170ec0be4d762196bedecd7a065816576198a6527c99282a2551aaa7da38c::events::CreatedPoolEvent"
        }
    }
}

###############################################################################
# Global Caches
###############################################################################

_pool_cache: Dict[str, Tuple[str, str]] = {}
_metadata_cache: Dict[str, Tuple[int, str]] = {}

###############################################################################
# Core Network Functions
###############################################################################

request_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

async def safe_post(session: aiohttp.ClientSession, url: str, json_data: dict, retries=MAX_RETRIES):
    """Enhanced safe_post with better error handling and logging"""
    backoff = 1.0
    last_error = None
    
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
                    return await resp.json()
            except Exception as e:
                last_error = e
                print(f"Error on attempt {attempt+1}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(backoff + random.random())
                    backoff *= 2
                    continue

    raise Exception(f"Failed after {retries} retries. Last error: {last_error}")

###############################################################################
# Pool Discovery Functions
###############################################################################

async def get_all_pools_from_events(session: aiohttp.ClientSession, event_filter: dict) -> List[str]:
    """Discover all pools by querying PoolCreatedEvents."""
    pools = set()
    cursor = None
    
    while True:
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "suix_queryEvents",
                "params": [
                    event_filter,
                    cursor,
                    100,
                    True
                ]
            }
            
            result = await safe_post(session, RPC_URL, payload)
            data = result.get("result", {})
            
            for event in data.get("data", []):
                try:
                    parsed_json = event.get("parsedJson", {})
                    
                    pool_id = parsed_json.get("pool_id")
                    if pool_id:
                        if not pool_id.startswith("0x"):
                            pool_id = "0x" + pool_id
                        pools.add(pool_id)
                        print(f"Successfully extracted pool ID: {pool_id}")
                        
                        # Also print coin information for debugging
                        coins = parsed_json.get("coins", [])
                        print(f"Pool coins: {coins}")
                except Exception as e:
                    print(f"Error parsing event: {e}")
                    continue
            
            if not data.get("hasNextPage"):
                break
                
            cursor = data.get("nextCursor")
            print(f"Found {len(pools)} pools so far, fetching next page...")
            
        except Exception as e:
            print(f"Error fetching events: {e}")
            break
    
    return list(pools)

###############################################################################
# Pool Analysis Functions
###############################################################################

async def get_pool_coins(session: aiohttp.ClientSession, pool_id: str) -> Tuple[str, str]:
    """Get coin types for a pool with enhanced debugging."""
    if pool_id in _pool_cache:
        return _pool_cache[pool_id]

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sui_getObject",
        "params": [
            pool_id,
            {
                "showContent": True,
                "showType": True,
                "showOwner": True
            }
        ]
    }

    try:
        result = await safe_post(session, RPC_URL, payload)
        print("\nRaw API response for pool:", json.dumps(result, indent=2)[:1000])
        
        data = result.get("result", {}).get("data", {})
        if not data:
            print(f"No data found for pool {pool_id}")
            return ("Unknown", "Unknown")

        content = data.get("content", {})
        fields = content.get("fields", {})
        
        # Check for type_names field first as it contains the actual coin types
        if "type_names" in fields and isinstance(fields["type_names"], list):
            coin_types = fields["type_names"]
            if len(coin_types) == 2:
                _pool_cache[pool_id] = tuple(coin_types)
                print(f"Successfully found coins from type_names: {coin_types}")
                return tuple(coin_types)

        # Fallback to previous methods if type_names not found
        type_str = data.get("type", "")
        if "Pool<" in type_str:
            inside = type_str.split("Pool<", 1)[1].rstrip(">")
            coin_types = [t.strip() for t in inside.split(",")]
            if len(coin_types) == 2:
                _pool_cache[pool_id] = tuple(coin_types)
                print(f"Successfully found coins from type string: {coin_types}")
                return tuple(coin_types)

        print("Could not find valid coin types")
        return ("Unknown", "Unknown")

    except Exception as e:
        print(f"Error fetching pool {pool_id}: {str(e)}")
        return ("Unknown", "Unknown")

async def get_coin_metadata(session: aiohttp.ClientSession, coin_type: str) -> Tuple[int, str]:
    """Get metadata for a coin type."""
    if coin_type in _metadata_cache:
        print(f"Using cached metadata for {coin_type}: {_metadata_cache[coin_type]}")
        return _metadata_cache[coin_type]
        
    if coin_type == "Unknown":
        return (0, "UNKNOWN")
    
    print(f"\nFetching metadata for coin: {coin_type}")
    
    # Ensure the coin type has 0x prefix
    parts = coin_type.split("::")
    if not parts[0].startswith("0x"):
        parts[0] = "0x" + parts[0]
        coin_type = "::".join(parts)
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_getCoinMetadata",
        "params": [coin_type]
    }
    
    try:
        result = await safe_post(session, RPC_URL, payload)
        print(f"Metadata API response: {json.dumps(result, indent=2)}")
        
        metadata = result.get("result")
        if metadata:
            decimals = metadata.get("decimals", 9)
            symbol = metadata.get("symbol")
            
            if symbol:
                _metadata_cache[coin_type] = (decimals, symbol)
                print(f"Successfully got metadata: decimals={decimals}, symbol={symbol}")
                return (decimals, symbol)
        
        # If we didn't get metadata, try to extract a reasonable symbol
        module_name = parts[-2].upper()  # Get the module name
        print(f"No metadata found, using module name as symbol: {module_name}")
        _metadata_cache[coin_type] = (9, module_name)
        return (9, module_name)
        
    except Exception as e:
        print(f"Error fetching metadata for {coin_type}: {e}")
        # Fallback to module name
        module_name = parts[-2].upper()
        print(f"Using fallback symbol: {module_name}")
        _metadata_cache[coin_type] = (9, module_name)
        return (9, module_name)

async def analyze_pool(session: aiohttp.ClientSession, pool_id: str, dex_name: str) -> Optional[dict]:
    """Analyze a single pool and return its details."""
    print(f"Analyzing pool: {pool_id}")
    
    try:
        coins = await get_pool_coins(session, pool_id)
        if coins == ("Unknown", "Unknown"):
            print(f"Could not get coins for pool {pool_id}")
            return None
            
        coin_a, coin_b = coins
        print(f"Found coins: {coin_a}, {coin_b}")
        
        meta_a = await get_coin_metadata(session, coin_a)
        meta_b = await get_coin_metadata(session, coin_b)
        
        pool_info = {
            "pool_id": pool_id,
            "dex": dex_name,
            "coin_a": coin_a,
            "coin_a_symbol": meta_a[1],
            "coin_a_decimals": meta_a[0],
            "coin_b": coin_b,
            "coin_b_symbol": meta_b[1],
            "coin_b_decimals": meta_b[0]
        }
        
        print(f"Successfully analyzed pool: {json.dumps(pool_info, indent=2)}")
        return pool_info
        
    except Exception as e:
        print(f"Error analyzing pool {pool_id}: {e}")
        return None

###############################################################################
# CSV Output Functions
###############################################################################

def write_pools_to_csv(pools: List[dict], filename: str = "sui_dex_pools.csv"):
    """Write discovered pools to CSV."""
    if not pools:
        print("No pools to write to CSV")
        return
        
    print(f"\nAttempting to write {len(pools)} pools to CSV")
    print("First pool data:", json.dumps(pools[0], indent=2) if pools else "No pools")
        
    fieldnames = [
        "pool_id",
        "dex",
        "coin_a",
        "coin_a_symbol",
        "coin_a_decimals",
        "coin_b",
        "coin_b_symbol",
        "coin_b_decimals"
    ]
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for pool in pools:
                print(f"Writing pool: {pool['pool_id']}")
                writer.writerow(pool)
        
        print(f"Successfully wrote {len(pools)} pools to {filename}")
    except Exception as e:
        print(f"Error writing to CSV: {e}")

###############################################################################
# Main Function
###############################################################################

async def main():
    """Main execution function"""
    async with aiohttp.ClientSession() as session:
        all_pools = []
        MAX_POOLS = 100  # Limit to 100 pools for testing
        
        for dex_name, config in DEX_CONFIGS.items():
            print(f"\nProcessing {config['name']}...")
            
            pool_ids = await get_all_pools_from_events(session, config['event_query'])
            print(f"Found {len(pool_ids)} total pools (limiting to {MAX_POOLS} for testing)")
            
            # Limit pool_ids to first 10
            pool_ids = pool_ids[:MAX_POOLS]
            
            for i, pool_id in enumerate(pool_ids):
                print(f"Analyzing pool {i+1}/{len(pool_ids)}: {pool_id}")
                pool_info = await analyze_pool(session, pool_id, dex_name)
                if pool_info:
                    all_pools.append(pool_info)
                    print(f"Added pool with coins: {pool_info['coin_a_symbol']}-{pool_info['coin_b_symbol']}")
                else:
                    print(f"Pool analysis returned None for pool_id: {pool_id}")
        
        print(f"\nTotal pools found: {len(all_pools)}")
        write_pools_to_csv(all_pools)

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")