FROM rust:latest AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    clang \
    cmake

COPY ./ ./
RUN cargo build --release


FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/* 

ENV TZ=Europe/Oslo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY --from=builder /build/target/release/fdk-mqa-url-checker /fdk-mqa-url-checker

CMD ["/fdk-mqa-url-checker"]
