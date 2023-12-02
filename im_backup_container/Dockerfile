FROM alpine:latest

RUN --mount=type=cache,id=apk,sharing=locked,target=/var/cache/apk \
  --mount=type=cache,id=pip,sharing=locked,target=/root/.cache/pip \
  apk add nano curl borgbackup rclone openssh-client python3 py3-pip \
  && pip3 install --upgrade pip

VOLUME ["/root/.ssh/", "/root/.cache/borg/", "/host/", "/var/log/"]

COPY etc /etc
COPY usr /usr

RUN --mount=type=cache,id=pip,sharing=locked,target=/root/.cache/pip \
  cd /usr/local/src/py_borg_back \
  && pip3 install -r requirements.txt

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
