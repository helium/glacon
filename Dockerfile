# BASE: the toolchain CI runs fmt, clippy and the tests in. Nothing is
# published from it beyond CI's own cache (ghcr.io/<repo>/base:<hash>).
FROM rust:bookworm AS base

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    cmake \
    ca-certificates \
    curl \
    unzip \
    jq \
    && rm -rf /var/lib/apt/lists/*

# The toolchain rust-toolchain.toml names, with its components, made the
# default in place of the image's own.
COPY rust-toolchain.toml /tmp/toolchain/
RUN cd /tmp/toolchain \
    && image_toolchain="$(rustup default | cut -d' ' -f1)" \
    && rustup toolchain install \
    && toolchain="$(rustup show active-toolchain | cut -d' ' -f1)" \
    && rustup default "$toolchain" \
    && if [ "$image_toolchain" != "$toolchain" ]; then rustup toolchain uninstall "$image_toolchain"; fi \
    && chmod -R a+w "$RUSTUP_HOME" "$CARGO_HOME" \
    && rm -rf /tmp/toolchain

# cargo-nextest: the tests need it (see README, Development).
ARG NEXTEST_VERSION=0.9.146
RUN case "$(uname -m)" in aarch64) platform=linux-arm ;; *) platform=linux ;; esac \
    && curl -LsSf "https://get.nexte.st/${NEXTEST_VERSION}/${platform}" \
    | tar zxf - -C "${CARGO_HOME}/bin"

# cargo-deny (advisories, licenses, sources: deny.toml) and cargo-shear
# (unused dependencies), prebuilt.
ARG CARGO_DENY_VERSION=0.20.2
ARG CARGO_SHEAR_VERSION=1.14.0
RUN target="$(uname -m)-unknown-linux-musl" \
    && mkdir /tmp/tools \
    && curl -LsSf "https://github.com/EmbarkStudios/cargo-deny/releases/download/${CARGO_DENY_VERSION}/cargo-deny-${CARGO_DENY_VERSION}-${target}.tar.gz" \
    | tar zxf - -C /tmp/tools \
    && curl -LsSf "https://github.com/Boshen/cargo-shear/releases/download/v${CARGO_SHEAR_VERSION}/cargo-shear-${target}.tar.gz" \
    | tar zxf - -C /tmp/tools \
    && find /tmp/tools -type f \( -name cargo-deny -o -name cargo-shear \) -exec install -m 755 {} "${CARGO_HOME}/bin/" \; \
    && rm -rf /tmp/tools

# AWS CLI: creates the bucket the test catalogs store their tables in.
RUN curl -sSf "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o /tmp/awscliv2.zip \
    && unzip -q /tmp/awscliv2.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/aws /tmp/awscliv2.zip
