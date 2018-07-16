FROM unocha/alpine-base:3.8

MAINTAINER Michael Rans <rans@email.com>

RUN apk add --no-cache --upgrade python3 build-base musl-dev python3-dev libffi-dev openssl-dev libxml2-dev libxslt-dev && \
    curl -so /root/get-pip.py https://bootstrap.pypa.io/get-pip.py && \
    python3 /root/get-pip.py && \
    pip --no-cache-dir install setuptools --upgrade && \
    pip --no-cache-dir install hdx-python-api && \
    apk del build-base musl-dev python3-dev libffi-dev openssl-dev libxml2-dev libxslt-dev && \
    apk add --no-cache --upgrade libstdc++ && \
    rm -r /root/.cache && \
    rm -rf /var/lib/apk/*
