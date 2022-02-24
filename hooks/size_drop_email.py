#!/usr/bin/env python3
import asyncio
import email
import json
import os
import smtplib
import sys
import yaml


def load_config():
    with open(os.environ['CONFIG_PATH'], 'r') as f:
        return yaml.safe_load(f)


async def main(farmer_record, data):

    data = json.loads(data.strip())
    if 'EMAIL' not in data['size_drop']:
        return

    farmer_record = json.loads(farmer_record.strip())
    if not farmer_record['email']:
        return

    config = load_config()

    with open(config['hook_size_drop']['message_path'], 'r') as f:
        message_string = f.read().strip()
    with smtplib.SMTP('localhost') as smtp:
        msg = email.message_from_string(message_string % {
            'launcher_id': farmer_record['launcher_id'],
            'to': farmer_record['email'],
            'rate': int((1 - data['rate']) * 100),
            'interval': data['size_drop_interval'],
        })
        smtp.send_message(msg)


if __name__ == '__main__':
    if sys.argv[1] != 'SIZE_DROP':
        print('Not an SIZE_DROP hook')
        sys.exit(1)
    asyncio.run(main(*sys.argv[2:]))
