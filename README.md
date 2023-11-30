# Docker Borg Backup

TODO: improve this readme
TODO: improve logging and add option to config to change log level
TODO: Forensics
TODO: _add for config keys to append
TODO: Prometheus metrics
TODO: Support more borg options
TODO: Support hooks (pre/post backup)
TODO: Handle ssh host keys better

`ssh-keyscan <id>.repo.borgbase.com` to get the host key.
`ssh-keygen -b 4069 -t rsa -C py_borg_back_1@borgbase -f backup_ssh_key_py_b
org_back_1` to generate a key.

Dont forget to intialize the repo before running the container.
export BORG_RSH="ssh -i /run/secrets/backup_ssh_key_py_borg_back_1"
export BORG_PASSPHRASE=$(cat /run/secrets/borg_pass)
borg init -e repokey-blake2 ssh://<id>@<id>.repo.borgbase.com/./repo

export BORG_RSH="ssh -i /run/secrets/backup_ssh_key_py_borg_back_2"
export BORG_PASSPHRASE=$(cat /run/secrets/borg_pass)
borg init -e repokey-blake2 ssh://<id>@<id>.repo.borgbase.com/./repo
