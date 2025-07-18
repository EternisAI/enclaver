FROM rust:1.88.0-alpine3.22@sha256:63985230b69fbd90528857dabf261379eb47f285ccc69f577d17c3dfde721deb AS builder

RUN apk add --no-cache curl musl-dev

ARG ZIG_VERSION=0.14.1
RUN curl -L "https://ziglang.org/download/${ZIG_VERSION}/zig-$(uname -m)-linux-${ZIG_VERSION}.tar.xz" | tar -J -x -C /usr/local && \
    mv /usr/local/zig-$(uname -m)-linux-${ZIG_VERSION}/zig /usr/local/bin/zig && \
    mv /usr/local/zig-$(uname -m)-linux-${ZIG_VERSION}/lib/* /usr/local/lib/

RUN rustup target add x86_64-unknown-linux-musl && \
    rustup target add aarch64-unknown-linux-musl

RUN cargo install cargo-zigbuild

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
