#!/usr/bin/env python3
import asyncio
import email
import json
import os
import smtplib
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

    with open(config['hook_payment']['message_path'], 'r') as f:
        message_string = f.read().strip()

    payments = json.loads(payments.strip())
    launcher_ids = list(payments.keys())
    for launcher_id, notification in (await store.get_notifications(launcher_ids)).items():

        if 'EMAIL' not in notification['payment'] or not notification['email']:
            continue

        try:
            with smtplib.SMTP('localhost') as smtp:
                msg = email.message_from_string(message_string % {
                    'launcher_id': launcher_id,
                    'amount': payments[launcher_id] / 10 ** 12,
                })
                smtp.send_message(msg)
        except Exception as e:
            print('Failed to send email to {}: {}'.format(launcher_id, str(e)))

    await store.close()


if __name__ == '__main__':
    if sys.argv[1] != 'PAYMENT':
        print('Not an PAYMENT hook')
        sys.exit(1)
    asyncio.run(main(*sys.argv[2:]))
