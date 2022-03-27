#!/usr/bin/env python3
from pyfcm import FCMNotification
import asyncio
import json
import os
import sys
import yaml

from pool.store.pgsql_store import PgsqlPoolStore


def load_config():
    with open(os.environ['CONFIG_PATH'], 'r') as f:
        return yaml.safe_load(f)


async def main(payments):
    config = load_config()
    store = PgsqlPoolStore(config)
    await store.connect()

    push_service = FCMNotification(api_key=config['hook_fcm_absorb']['api_key'])

    payments = json.loads(payments.strip())
    launcher_ids = list(payments.keys())
    for launcher_id, notification in (await store.get_notifications(launcher_ids)).items():

        if 'PUSH' not in notification['payment'] or not notification['fcm_token']:
            print('No PUSH enabled or fcm token found for', launcher_id)
            continue

        print('Payment notification being sent for', launcher_id)
        push_service.notify_single_device(
            registration_id=notification['fcm_token'],
            message_title='Payment sent!',
            message_body=(
                f'Your launcher id {launcher_id} was just sent a payment of '
                f'{payments[launcher_id] / 10 ** 12} XCH.'
            ),
        )

    await store.close()

if __name__ == '__main__':
    if sys.argv[1] != 'PAYMENT':
        print('Not an PAYMENT hook')
        sys.exit(1)
    asyncio.run(main(*sys.argv[2:]))
