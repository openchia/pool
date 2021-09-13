#!/bin/bash
set -e

export CHIA_ROOT=/data/chia/${CHIA_NETWORK:=mainnet}

cd /root/pool
./venv/bin/python -m pool.pool_server -c /data/config.yaml | rotatelogs -L /data/pool_log/stdout -n 5 -D -e /data/pool_log/output 5M
