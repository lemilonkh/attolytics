FROM rustlang/rust:nightly-buster-slim as builder
WORKDIR /usr/src/attolytics
# ENV SYSTEMD_LIB_DIR=/lib/x86_64-linux-gnu
# RUN apt-get update & apt-get install -y libsystemd-dev & rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo install --path .

FROM debian:buster-slim
ENV PORT 8000
ENV DB_URL invalid
RUN apt-get update & apt-get install -y extra-runtime-dependencies & rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/attolytics /usr/local/bin/attolytics
WORKDIR /etc/attolytics
COPY ./schema-example.conf.yaml ./schema.conf.yaml
CMD attolytics --port ${PORT} --db_url ${DB_URL} --host 0.0.0.0 --schema /etc/attolytics/schema.conf.yaml
