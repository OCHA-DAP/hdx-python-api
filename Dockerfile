FROM unocha/alpine-base-python3

MAINTAINER Michael Rans <rans@email.com>

RUN apk add --no-cache --upgrade python3 build-base musl-dev python3-dev libffi-dev openssl-dev libxml2-dev libxslt-dev && \
    pip --no-cache-dir install hdx-python-api && \
    rm -r /root/.cache && \
    rm -rf /var/lib/apk/*