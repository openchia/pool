#!/bin/bash
set -e

export CHIA_ROOT=/data/chia/mainnet

cd /root/pool
./venv/bin/python -m pool.pool_server -c /data/config.yaml | rotatelogs -D -e /data/pool_log/stdout 5M
