FROM unocha/alpine-base:3.8

MAINTAINER Michael Rans <rans@email.com>

RUN apk add --no-cache --upgrade python3 && \
    apk add --no-cache --upgrade --virtual .build-deps build-base musl-dev python3-dev libffi-dev libressl-dev libxml2-dev libxslt-dev postgresql-dev && \
    pip3 --no-cache-dir install --upgrade pip && \
    pip --no-cache-dir install setuptools --upgrade && \
    pip --no-cache-dir install hdx-python-api && \
    apk del .build-deps && \
    apk add --no-cache --upgrade libstdc++ && \
    rm -r /root/.cache && \
    rm -rf /var/lib/apk/*
