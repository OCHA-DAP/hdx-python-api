FROM unocha/alpine-base:3.14

RUN apk add --no-cache --upgrade \
        python3 \
        py3-pip  \
        libxslt && \
    apk add --no-cache --upgrade --virtual .build-deps \
        build-base \
        cargo \
        libffi-dev \
        libxml2-dev \
        libxslt-dev \
        openssl-dev \
        python3-dev && \
    pip --no-cache-dir install --upgrade hdx-python-api --ignore-installed six && \
    apk del .build-deps && \
    apk add --no-cache --upgrade libstdc++ && \
    rm -rf ./target ~/.cargo /var/lib/apk/*
