FROM rust:latest AS builder

WORKDIR /opt/app

COPY src/ ./src/
COPY Cargo.toml ./
COPY Cargo.lock ./

RUN apt update && apt install -y cmake clang
RUN cargo build --release

FROM rust:latest

WORKDIR /opt/app
ENV TZ=Europe/Oslo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY --from=builder /opt/app/target/release/fdk-mqa-url-checker ./

CMD ./fdk-mqa-url-checker --brokers "$BROKERS" --schema-registry "$SCHEMA_REGISTRY"
