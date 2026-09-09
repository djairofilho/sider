# Imagem privada de distribuição: o contexto contém somente o pacote Linux validado.
# Não recompila o produto e não consulta repositórios de pacotes durante o build.
FROM ubuntu:24.04@sha256:1e0a86e57d247923571b75e0aaf48a1449cf8c543d51fb3e07a4a7d7bfa79316

ARG SIDER_VERSION
ARG SIDER_SOURCE_SHA
ARG SIDER_SHA256
ARG SIDER_MIGRATOR_SHA256
ARG SIDER_BACKUP_SHA256
ARG SIDER_REPLICA_SHA256

COPY sider sider-aof-migrate sider-backup sider-replica /usr/local/bin/
COPY README.md LICENSE /usr/share/doc/sider/
COPY licenses/ /usr/share/doc/sider/licenses/

RUN chmod 0755 /usr/local/bin/sider /usr/local/bin/sider-aof-migrate /usr/local/bin/sider-backup /usr/local/bin/sider-replica \
    && test -n "$SIDER_VERSION" \
    && test "${#SIDER_SOURCE_SHA}" = 40 \
    && printf '%s\n' "$SIDER_SOURCE_SHA" | grep -Eq '^[0-9a-f]{40}$' \
    && printf '%s  /usr/local/bin/sider\n%s  /usr/local/bin/sider-aof-migrate\n%s  /usr/local/bin/sider-backup\n%s  /usr/local/bin/sider-replica\n' \
        "$SIDER_SHA256" "$SIDER_MIGRATOR_SHA256" "$SIDER_BACKUP_SHA256" "$SIDER_REPLICA_SHA256" | sha256sum --check --strict \
    && test "$(/usr/local/bin/sider --version)" = "sider $SIDER_VERSION" \
    && /usr/local/bin/sider-aof-migrate --help >/dev/null \
    && test "$(/usr/local/bin/sider-backup --version)" = "sider-backup $SIDER_VERSION" \
    && /usr/local/bin/sider-replica --help >/dev/null \
    && install -d -m 0700 -o 10001 -g 10001 /var/lib/sider

LABEL org.opencontainers.image.title="Sider" \
      org.opencontainers.image.version="$SIDER_VERSION" \
      org.opencontainers.image.revision="$SIDER_SOURCE_SHA" \
      org.opencontainers.image.licenses="MIT" \
      io.sider.binary.sha256="$SIDER_SHA256" \
      io.sider.migrator.sha256="$SIDER_MIGRATOR_SHA256" \
      io.sider.backup.sha256="$SIDER_BACKUP_SHA256" \
      io.sider.replica.sha256="$SIDER_REPLICA_SHA256"

ENV SIDER_ADDR=0.0.0.0:6379 \
    SIDER_AOF_DIR=/var/lib/sider \
    SIDER_AOF_SYNC=always
VOLUME ["/var/lib/sider"]
EXPOSE 6379
STOPSIGNAL SIGTERM
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/sider"]
