#!/bin/bash
set -e

export CHIA_ROOT=/data/chia/${CHIA_NETWORK:=mainnet}
loglevel=${LOGLEVEL:=INFO}

trap "killall python" TERM

simpleproxy -d -L 127.0.0.1:25 -R ${MAIL_HOSTNAME:=mail}:25

cd /root/pool
exec ./venv/bin/python -m pool.pool_server --log-level ${loglevel} --log-file /data/pool_log/stdout -c /data/config.yaml
