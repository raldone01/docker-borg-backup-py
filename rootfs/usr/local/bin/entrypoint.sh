#!/bin/ash

set -eu
set -o pipefail

chmod 700 /root/.ssh && chmod 600 /root/.ssh/*

. /usr/local/src/py_borg_back/venv/bin/activate
# We can use exec here since the docker container is launched with --init true
exec python /usr/local/src/py_borg_back/py_borg_back.py -c /etc/py_borg_back/config.toml $@
