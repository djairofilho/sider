#!/usr/bin/env bash
set -euo pipefail
trap 'echo "Provisioning failed at line $LINENO" >&2' ERR

# Create a fresh, restricted three-identity generation for one primary,
# one replica, and one backup/monitor tool on a Linux Docker host.
if [[ $# -ne 1 || ${EUID} -ne 0 || -z ${SIDER_IMAGE:-} ]]; then
  echo 'Usage: sudo SIDER_IMAGE=ghcr.io/djairofilho/sider@sha256:<digest> deploy/provision-v1.1-security.sh /absolute/new/directory' >&2
  exit 2
fi
if [[ ! $SIDER_IMAGE =~ ^ghcr\.io/djairofilho/sider@sha256:[0-9a-f]{64}$ && ${SIDER_PROVISION_ALLOW_LOCAL_IMAGE:-} != 1 ]]; then
  echo 'SIDER_IMAGE must be the approved immutable GHCR digest.' >&2
  exit 2
fi
root=$1
if [[ $root != /* || -e $root || -L $root ]]; then
  echo 'The destination must be an unused absolute path.' >&2
  exit 2
fi
for tool in docker openssl sha256sum date; do
  command -v "$tool" >/dev/null || { echo "Missing $tool" >&2; exit 2; }
done
umask 077
install -d -m 0700 "$root" "$root/ca" "$root/ca/newcerts"
for identity in primary replica backup; do
  install -d -m 0700 "$root/$identity"
done

read -r -s -p 'Sider admin password (16-1024 bytes): ' password
printf '\n'
verifier=$(printf %s "$password" | docker run --rm -i --network none \
  --entrypoint /usr/local/bin/sider "$SIDER_IMAGE" --hash-password)
unset password
start=$(date -u -d '-1 minute' +%Y-%m-%dT%H:%M:%SZ)
end=$(date -u -d '+90 days' +%Y-%m-%dT%H:%M:%SZ)

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -noenc -days 365 -subj /CN=sider-internal-ca \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign' \
  -keyout "$root/ca/ca-key.pem" -out "$root/ca/ca-cert.pem" >/dev/null 2>&1
touch "$root/ca/index.txt"
printf '1000\n' >"$root/ca/crlnumber"
cat >"$root/ca/ca.cnf" <<EOF
[ca]
default_ca = local
[local]
database = $root/ca/index.txt
new_certs_dir = $root/ca/newcerts
certificate = $root/ca/ca-cert.pem
private_key = $root/ca/ca-key.pem
crlnumber = $root/ca/crlnumber
default_md = sha256
default_crl_days = 30
crl_extensions = crl_ext
[crl_ext]
authorityKeyIdentifier = keyid:always
EOF
openssl ca -gencrl -config "$root/ca/ca.cnf" \
  -out "$root/ca/internal-crl.pem" >/dev/null 2>&1

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -noenc -days 90 -subj /CN=sider-client \
  -addext 'subjectAltName=IP:127.0.0.1' \
  -addext 'extendedKeyUsage=serverAuth' \
  -keyout "$root/primary/client-key.pem" \
  -out "$root/primary/client-cert.pem" >/dev/null 2>&1
cp "$root/primary/client-cert.pem" "$root/replica/client-cert.pem"
cp "$root/primary/client-key.pem" "$root/replica/client-key.pem"
cat >"$root/primary/client-policy.json" <<EOF
{"version":1,"users":[{"name":"admin","credentials":[{"verifier":"$verifier","not_before":"$start","expires_at":"$end"}],"commands":["GET","MGET","EXISTS","TTL","PTTL","HGET","HEXISTS","HLEN","HGETALL","LLEN","LRANGE","SISMEMBER","SCARD","SMEMBERS","ZCARD","ZSCORE","ZRANGE","SET","DEL","MSET","INCR","DECR","EXPIRE","PEXPIRE","PERSIST","HSET","HDEL","LPUSH","RPUSH","LPOP","RPOP","SADD","SREM","ZADD","ZREM","PUBLISH","SUBSCRIBE","MULTI","EXEC","DISCARD","UNWATCH","WATCH","INFO","PING","ECHO","MEM.INFO","MEM.GET","MEM.PTTL","MEM.SEARCH","MEM.CREATE","MEM.PUT","MEM.DEL","MEM.EXPIRE","MEM.PERSIST"],"read_keys":{"exact":[],"prefix":[],"all":true},"write_keys":{"exact":[],"prefix":[],"all":true},"publish_channels":{"exact":[],"prefix":[],"all":true},"subscribe_channels":{"exact":[],"prefix":[],"all":true},"info_sections":["server","clients","stats","memory","persistence","config","replication"]}]}
EOF
cp "$root/primary/client-policy.json" "$root/replica/client-policy.json"
unset verifier start end

make_leaf() {
  local identity=$1 uri=$2 serial=$3
  openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
    -noenc -subj "/CN=sider-$identity" \
    -keyout "$root/$identity/internal-key.pem" \
    -out "$root/ca/$identity.csr" >/dev/null 2>&1
  cat >"$root/ca/$identity.ext" <<EOF
[leaf]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = IP:127.0.0.1,URI:$uri
EOF
  openssl x509 -req -in "$root/ca/$identity.csr" \
    -CA "$root/ca/ca-cert.pem" -CAkey "$root/ca/ca-key.pem" \
    -set_serial "$serial" -days 90 -sha256 \
    -extfile "$root/ca/$identity.ext" -extensions leaf \
    -out "$root/$identity/internal-cert.pem" >/dev/null 2>&1
  cp "$root/ca/ca-cert.pem" "$root/$identity/internal-ca.pem"
  cp "$root/ca/internal-crl.pem" "$root/$identity/internal-crl.pem"
}
make_leaf primary spiffe://sider/primary-a/server 1
make_leaf replica spiffe://sider/holder-b/replica 2
make_leaf backup spiffe://sider/holder-c/backup 3
cat >"$root/primary/internal-policy.json" <<'EOF'
{"version":1,"instance_id":"primary-a","principals":[{"uri_san":"spiffe://sider/holder-b/replica","roles":["replica"],"upstream_instance":"primary-a"},{"uri_san":"spiffe://sider/holder-c/backup","roles":["backup","monitor"]}]}
EOF
cat >"$root/replica/internal-policy.json" <<'EOF'
{"version":1,"instance_id":"holder-b","principals":[],"upstream_principal":"spiffe://sider/primary-a/server"}
EOF
cat >"$root/backup/internal-policy.json" <<'EOF'
{"version":1,"instance_id":"holder-c","principals":[],"upstream_principal":"spiffe://sider/primary-a/server"}
EOF

write_manifest() {
  local identity=$1 generation=1 name hash separator=''
  {
    printf '{"version":1,"generation":%s,"files":{' "$generation"
    for name in "$root/$identity"/*.json "$root/$identity"/*.pem; do
      [[ ${name##*/} == security-manifest.json ]] && continue
      hash=$(sha256sum "$name")
      printf '%s"%s":"%s"' "$separator" "${name##*/}" "${hash%% *}"
      separator=,
    done
    printf '}}\n'
  } >"$root/$identity/security-manifest.json"
  chmod 0600 "$root/$identity"/*
  chown -R 10001:10001 "$root/$identity"
  chmod 0700 "$root/$identity"
}
for identity in primary replica backup; do
  write_manifest "$identity"
done
echo "Primary: $root/primary"
echo "Replica: $root/replica"
echo "Backup tool: $root/backup"
echo "Client trust anchor: $root/primary/client-cert.pem"
echo 'Keep the CA key private; renew the CRL and certificates before expiry.'
