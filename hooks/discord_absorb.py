#!/usr/bin/env python3
import aiohttp
import asyncio
import json
import os
import sys
import yaml


def load_config():
    with open(os.environ['CONFIG_PATH'], 'r') as f:
        return yaml.safe_load(f)


async def discord_blocks_farmed(absorbeb_coins):
    config = load_config()
    absorbeb_coins = json.loads(absorbeb_coins.strip())

    coins = []
    farmers = set()
    for coin, singleton_coin, farmer_record in absorbeb_coins:
        coins.append(coin)
        farmers.add(farmer_record['launcher_id'])

    coins_blocks = ', '.join([f'#{c["confirmed_block_index"]}' for c in coins])
    farmed_by = ', '.join(farmers)

    async with aiohttp.request('POST', config['hook_discord_absorb']['url'], json={
        'content': f"New block(s) farmed! {coins_blocks}. Farmed by {farmed_by}.",
        'username': config['hook_discord_absorb']['username'],
    }) as r:
        pass


if __name__ == '__main__':
    if sys.argv[1] != 'ABSORB':
        print('Not an ABSORB hook')
        sys.exit(1)
    asyncio.run(discord_blocks_farmed(
        sys.argv[2],
    ))
