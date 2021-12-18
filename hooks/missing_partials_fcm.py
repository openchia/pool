#!/usr/bin/env python3
from pyfcm import FCMNotification
import json
import os
import sys
import yaml


def load_config():
    with open(os.environ['CONFIG_PATH'], 'r') as f:
        return yaml.safe_load(f)


def main(farmer_records):
    config = load_config()
    push_service = FCMNotification(api_key=config['hook_missing_partials']['fcm_api_key'])
    message_string = config['hook_missing_partials']['push_message']

    for launcher_id, rec in farmer_records.items():
        if rec['push_missing_partials_hours'] and rec['fcm_token']:
            push_service.notify_single_device(
                registration_id=rec['fcm_token'],
                message_title='Missing partials!',
                message_body=message_string % {
                    'launcher_id': launcher_id,
                },
            )


if __name__ == '__main__':
    assert sys.argv[1] == 'MISSING_PARTIALS'
    main(json.loads(sys.argv[2].strip()))
