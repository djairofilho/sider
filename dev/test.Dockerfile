# Local development environment. This is not the Sider distribution image.
FROM docker:29.2.1-cli@sha256:cab69e2d0a1a2ea9a1ce1060252f439e83483ae41ec09317aecb33b08a0656a5 AS docker_cli
FROM ubuntu:24.04@sha256:1e0a86e57d247923571b75e0aaf48a1449cf8c543d51fb3e07a4a7d7bfa79316

ENV RUSTUP_HOME=/opt/rustup \
    CARGO_HOME=/opt/cargo \
    CARGO_TARGET_DIR=/tmp/sider-target \
    PATH=/opt/cargo/bin:$PATH \
    LANG=C.UTF-8

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
       build-essential ca-certificates curl git pkg-config \
    && apt-get clean

COPY rustup-init.sha256 /tmp/rustup-init.sha256
RUN curl --proto '=https' --tlsv1.2 --fail --show-error --silent \
        https://static.rust-lang.org/rustup/archive/1.28.2/x86_64-unknown-linux-gnu/rustup-init \
        --output /tmp/rustup-init \
    && sha256sum --check --strict /tmp/rustup-init.sha256 \
    && chmod 0755 /tmp/rustup-init \
    && /tmp/rustup-init -y --no-modify-path --profile minimal \
        --default-toolchain 1.97.1 --default-host x86_64-unknown-linux-gnu \
        --component rustfmt,clippy \
    && rustup set auto-self-update disable

COPY --from=docker_cli /usr/local/bin/docker /usr/local/bin/docker
RUN rustc --version --verbose \
    && cargo --version \
    && docker --version

WORKDIR /workspace
CMD ["cargo", "test", "--locked"]
