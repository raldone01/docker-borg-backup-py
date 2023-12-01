# Docker Borg Backup

## Todo

* improve this readme
* improve logging and add option to config to change log level
* Forensics
* `*_add/*_rem for config keys to append/remove`
* Prometheus metrics
* Support more borg options
* Support hooks (pre/post backup)
* Handle ssh host keys better
* Support handling SIGHUP to reload config
* Fix log encoding `b' and \n` proper utf decoding

## Information

`ssh-keyscan <id>.repo.borgbase.com` to get the host key.
`ssh-keygen -b 4069 -t rsa -C py_borg_back_1@borgbase -f backup_ssh_key_py_borg_back_1` to generate a key.

Don't forget to intialize the repo before running the container.

```
export BORG_RSH="ssh -i /run/secrets/backup_ssh_key_py_borg_back_1"
export BORG_PASSPHRASE=$(cat /run/secrets/borg_pass)
borg init -e repokey-blake2 ssh://<id>@<id>.repo.borgbase.com/./repo
```

```
export BORG_RSH="ssh -i /run/secrets/backup_ssh_key_py_borg_back_2"
export BORG_PASSPHRASE=$(cat /run/secrets/borg_pass)
borg init -e repokey-blake2 ssh://<id>@<id>.repo.borgbase.com/./repo
```
