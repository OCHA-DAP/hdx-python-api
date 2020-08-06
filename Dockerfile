FROM unocha/alpine-base:3.12

MAINTAINER Michael Rans <rans@email.com>

RUN apk add --no-cache --upgrade \
        python3 \
        py3-pip && \
    apk add --no-cache --upgrade --virtual .build-deps \
        build-base \
        libffi-dev \
        postgresql-dev \
        python3-dev && \
    pip3 --no-cache-dir install --upgrade pip && \
    pip --no-cache-dir install hdx-python-api && \
    apk del .build-deps && \
    apk add --no-cache --upgrade libstdc++ && \
    rm -rf /var/lib/apk/*
