FROM unocha/alpine-base-python3

MAINTAINER Michael Rans <rans@email.com>

RUN apk update && \
    apk upgrade python3 && \
    apk add build-base musl-dev python3-dev libffi-dev openssl-dev && \
    pip install hdx-python-api && \
    rm -rf /var/cache/apk/*
