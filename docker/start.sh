#!/bin/bash
set -e

export CHIA_ROOT=/data/chia/${CHIA_NETWORK:=mainnet}

trap "killall python" TERM

simpleproxy -d -L 127.0.0.1:25 -R ${MAIL_HOSTNAME:=mail}:25

cd /root/pool
./venv/bin/python -m pool.pool_server -c /data/config.yaml | rotatelogs -L /data/pool_log/stdout -n 5 -D -e /data/pool_log/output 5M
