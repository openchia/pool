#!/usr/bin/env python3
import email
import json
import os
import smtplib
import sys
import textwrap
import yaml


def load_config():
    with open(os.environ['CONFIG_PATH'], 'r') as f:
        return yaml.safe_load(f)


def main(farmer_records):
    config = load_config()
    with open(config['hook_missing_partials']['message_path'], 'r') as f:
        message_string = f.read().strip()
    with smtplib.SMTP('localhost') as smtp:
        for launcher_id, rec in farmer_records.items():
            msg = email.message_from_string(message_string % {
                'launcher_id': launcher_id,
                'to': rec['email'],
            })
            smtp.send_message(msg)


if __name__ == '__main__':
    assert sys.argv[1] == 'MISSING_PARTIALS'
    main(json.loads(sys.argv[2].strip()))
