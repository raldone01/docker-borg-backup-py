#!/bin/ash

chmod 700 /root/.ssh && chmod 600 /root/.ssh/*

. /usr/local/src/py_borg_back/venv/bin/activate
python /usr/local/src/py_borg_back/py_borg_back.py -c /etc/py_borg_back/config.toml $@
