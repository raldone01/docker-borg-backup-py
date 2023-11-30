FROM alpine:latest

RUN apk add --no-cache nano curl borgbackup rclone openssh-client python3 py3-pip \
  && pip3 install --upgrade pip

VOLUME ["/root/.ssh/", "/root/.cache/borg/", "/host/", "/var/log/"]

COPY etc /etc
COPY usr /usr

RUN cd /usr/local/src/py_borg_back \
  && pip3 install -r requirements.txt

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
