FROM rust:1-trixie AS builder
WORKDIR /s3util
COPY . ./
RUN git config --global --add safe.directory /s3util \
&& cargo build --release

FROM debian:trixie-slim
RUN apt-get update \
&& apt-get install --no-install-recommends -y ca-certificates \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*

COPY --from=builder /s3util/target/release/s3util /usr/local/bin/s3util

RUN useradd -m -s /bin/bash s3util
USER s3util
WORKDIR /home/s3util/
ENTRYPOINT ["/usr/local/bin/s3util"]
