FROM unocha/alpine-base:3.13

MAINTAINER Michael Rans <rans@email.com>

RUN apk add --no-cache --upgrade \
        python3 \
        py3-pip && \
    apk add --no-cache --upgrade -X http://dl-cdn.alpinelinux.org/alpine/edge/community --virtual .build-deps1 \
        py3-wheel \
        cargo && \
    apk add --no-cache --upgrade --virtual .build-deps2 \
        build-base \
        libffi-dev \
        openssl-dev \
        python3-dev && \
    pip3 --no-cache-dir install --upgrade pip && \
    pip --no-cache-dir install hdx-python-api && \
    apk del .build-deps1 && \
    apk del .build-deps2 && \
    apk add --no-cache --upgrade libstdc++ && \
    rm -rf ./target ~/.cargo /var/lib/apk/*
