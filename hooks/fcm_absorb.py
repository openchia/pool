#!/usr/bin/env python3
from pyfcm import FCMNotification
import asyncio
import json
import os
import sys
import yaml


def load_config():
    with open(os.environ['CONFIG_PATH'], 'r') as f:
        return yaml.safe_load(f)


async def fcm_blocks_farmed(absorbeb_coins):
    config = load_config()
    push_service = FCMNotification(api_key=config['hook_fcm_absorb']['api_key'])

    absorbeb_coins = json.loads(absorbeb_coins.strip())

    farmed_heights = []
    farmers = set()
    for coin, farmer_record in absorbeb_coins:
        farmed_height = str(int.from_bytes(bytes.fromhex(
            coin['coin']['parent_coin_info'][2:])[16:], 'big'
        ))
        farmed_heights.append(farmed_height)
        farmers.add(farmer_record['name'] or farmer_record['launcher_id'])
        if farmer_record['fcm_token'] and farmer_record['push_block_farmed']:
            push_service.notify_single_device(
                registration_id=farmer_record['fcm_token'],
                message_title='Farmed a new block!',
                message_body=f'You have farmed the block #{farmed_height}.'
            )

    coins_blocks = ', '.join(farmed_heights)
    farmed_by = ', '.join(farmers)

    push_service.notify_topic_subscribers(
        topic_name='blocks',
        message_body=f'New block(s) farmed! {coins_blocks}. Farmed by {farmed_by}.',
    )


if __name__ == '__main__':
    if sys.argv[1] != 'ABSORB':
        print('Not an ABSORB hook')
        sys.exit(1)
    asyncio.run(fcm_blocks_farmed(
        sys.argv[2],
    ))
