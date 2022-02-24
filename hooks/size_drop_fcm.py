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


async def size_drop_fcm(farmer_record, data):
    config = load_config()

    data = json.loads(data.strip())
    if 'PUSH' not in data['size_drop']:
        return

    farmer_record = json.loads(farmer_record.strip())
    if not farmer_record['fcm_token']:
        return

    push_service = FCMNotification(api_key=config['hook_fcm_absorb']['api_key'])
    push_service.notify_single_device(
        registration_id=farmer_record['fcm_token'],
        message_title='Farm dropped in size!',
        message_body=(
            f'Your farm of launcher id {farmer_record["launcher_id"]} has dropped '
            f'{int((1 - data["rate"]) * 100)}% of estimated size in the last '
            f'{data["size_drop_interval"]} minutes.'
        ),
    )

if __name__ == '__main__':
    if sys.argv[1] != 'SIZE_DROP':
        print('Not an SIZE_DROP hook')
        sys.exit(1)
    asyncio.run(size_drop_fcm(*sys.argv[2:]))
