# Network security boundary

This is the versioned target contract for
[issue #115](https://github.com/djairofilho/sider/issues/115) and implementation
follow-up [#93](https://github.com/djairofilho/sider/issues/93). It describes
required behavior; #93 tracks remaining implementation and release evidence.
The published 1.0.1 listeners have no peer authentication or transport
encryption.

Contract version: 2, versioned in source. Changes to
wire replies, profile defaults, role authority, or policy interpretation require
a new contract version and migration note.

Version 2 clarifies the Windows ancestor rule: an unrelated principal may create
a sibling directory when it has no right to modify, rename, or remove any existing
component of the protected path. Version 1's broad prohibition on ancestor write
access would also reject the default volume-root ACL on common Windows installs.
This clarification does not change the `security-manifest.json` schema version.

## Threat model and listener identities

An untrusted peer may send malformed or expensive frames, read or mutate data,
subscribe to channels, export a snapshot, or request promotion. A network observer
can read plaintext traffic. A stolen credential, key, backup, AOF directory, or
configuration file is a compromise. This contract protects network identity and
authorization; it does not encrypt data at rest or make a compromised host trustworthy.

| Listener | Identity proof | Authority |
| --- | --- | --- |
| Client data | Named user/password over TLS; client validates server certificate | Explicit command, key, and channel grants |
| Internal | Mutual TLS certificate with exact configured principal | Explicit replica, backup, monitor, or promotion roles |

Client passwords never authenticate to the internal listener. Internal certificates
never grant RESP access. Replication epoch and sequence establish continuity, not
identity. Source address and loopback origin are exposure controls, not identities.

## Profiles, defaults, and migration

`SIDER_SECURITY_PROFILE` accepts exactly `protected` or `legacy-loopback`. If unset,
it is `protected` for fresh installations and upgrades. Unknown values fail startup.
Profile choice never depends on bind address, AOF contents, or TLS file presence.
Protected mode requires `SIDER_SECURITY_DIR` to name an existing absolute directory;
the value is a path, never secret content. Setting `SIDER_SECURITY_DIR` with
`legacy-loopback` is a startup error.

Protected mode binds the client listener to loopback by default. The internal
listener remains disabled unless replication, backup, or administration is
configured; when enabled without an explicit address, it binds to loopback.
A non-loopback bind is
accepted only when that listener's TLS, authentication, and trust settings validate
before either listener opens or readiness is published. Internal always uses mutual
TLS, including on loopback. Client always uses server-authenticated TLS; client
certificates are not required. No plaintext fallback or downgrade is allowed.

`legacy-loopback` explicitly permits the 1.0.1 unauthenticated plaintext behavior
only when both effective binds and any outgoing internal target are loopback.
Internal tools apply the same profile selection and target restriction. It emits
one startup warning with the profile and effective addresses, never secrets.
A non-loopback bind fails startup;
changing only an address cannot select legacy mode. New installs and upgrades do not
select it automatically. Upgrade procedure: provision protected credentials and
certificates, update clients and internal tools to TLS/auth, then start in `protected`.
Operators may explicitly use `legacy-loopback` for a bounded cutover. Missing or
invalid protected configuration fails closed and never falls back to legacy. The
release guide must document the compatibility break, ordered migration, and recovery.

Both listeners use distinct addresses and readiness files. Internal port zero remains
supported. Readiness may report TLS mode and `authentication_required`; it never
reports principal names or secret values. Invalid protected configuration prevents
both listeners from opening and both readiness records from publishing.

Before a protected release, deployment, replication, and backup guides replace all
plaintext/private-network-only examples with explicit profile, TLS, credential, and
identity configuration, and document the legacy migration procedure.

## TLS and certificate identity

Protected mode uses TLS 1.2 or later, complete chain validation, and expiry checks.
Internal TLS additionally requires configured CRLs covering every certificate in
the verified chain except its trust anchor. Missing coverage, unknown revocation
status, revoked certificates, and expired CRLs fail closed. CRLs must have a valid
`nextUpdate`, which is enforced. CRLs are loaded with the trust roots in one
configuration generation; no live OCSP or CRL fetch occurs during handshake. Clients
validate the server certificate against the DNS name or IP used to connect, including
the matching SAN type. Verification cannot be disabled. Anonymous ciphers and TLS
compression are prohibited, and renegotiation is disabled. TLS session resumption
and early data are disabled in version 1 so a new connection validates current
trust, revocation, and certificate identity.

Client listener presents a server certificate and does not request a client
certificate. Internal listener requires a client certificate trusted by configured
internal roots. Its principal is an exact URI SAN:
`spiffe://sider/<instance>/<principal>`. Both components match
`[a-z0-9][a-z0-9._-]{0,62}`. `<instance>` identifies the certificate holder, not
the receiving server. An internal certificate has exactly one URI SAN; it must
match this grammar without escaping, query, fragment, or additional path segments.
Other DNS/IP SANs are permitted. The process's own internal certificate must name
its configured `instance_id`. Inbound policy lists each permitted remote URI
explicitly; remote instance IDs need not equal the receiving server's instance ID.
No CN fallback, wildcard, IP-based mapping, or implicit role mapping is allowed.
The internal policy maps each principal to explicit roles and, for replicas, an
allowed upstream instance. A certificate may have multiple roles only when listed.

The server's security directory contains `client-policy.json`, `client-cert.pem`,
and `client-key.pem`. When the internal listener or outgoing replica is enabled,
it also contains `internal-policy.json`, `internal-cert.pem`, `internal-key.pem`,
`internal-ca.pem`, and `internal-crl.pem`. Backup export and replica administration
tools read `SIDER_SECURITY_PROFILE` and `SIDER_SECURITY_DIR` and require these five
internal files in protected mode; they do not require the three client files.
Every protected process also requires the restricted `security-manifest.json`
described below. PEM files contain one certificate chain or private key,
except `internal-ca.pem` and `internal-crl.pem`, which may contain multiple trust
anchors or revocation lists. Missing, empty, or malformed required files fail
protected startup before either listener opens, or before a tool connects.
The server's internal certificate and key are used for both inbound internal TLS
and outgoing replica TLS and require both serverAuth and clientAuth extended key
usages. A tool's internal certificate requires clientAuth. An outgoing internal
connection validates the server against the literal IP SAN in `SIDER_REPLICA_OF`
or the tool's target address, and against the expected URI SAN in policy; both
checks must pass. Paths are fixed relative to the security directory; no TLS
material is read from environment variables.
The internal policy top-level object has `version: 1`, `instance_id`,
`principals`, and the conditionally required `upstream_principal` below; no other
fields are allowed. `instance_id` follows the URI component grammar above.
Each principal object has exactly `uri_san`, `roles`, and optional
`upstream_instance`. `roles` is a nonempty unique subset of `replica`, `backup`,
`monitor`, and `promotion`. `upstream_instance` is required exactly when `replica`
is present and must equal this policy's `instance_id`: the replica is allowed to
consume from this receiving instance. Every `uri_san` uses the holder grammar
above. Duplicate URI SANs or unknown fields reject the complete policy.
`upstream_principal` is required whenever this process opens an outgoing internal
connection, including replica sync, backup export, status, and promotion. It is
the target server's exact URI SAN and is absent for a server with no upstream.
Tools may use an empty `principals` array because they accept no inbound peers.

Certificate, trust, user, or role reload activates one complete configuration
generation. Invalid replacement leaves the old generation active and reports a
redacted error. Explicit user/credential removal or invalidated certificate trust
closes affected sessions before reload success. Changing an authenticated
credential's validity interval so it is no longer valid also closes that session.
Removing an internal principal
or any of its roles closes that principal's internal sessions. Reducing client
command/key/channel/section grants retains the authenticated identity and applies
the transaction, WATCH, and Pub/Sub cleanup rules below.
Replacing the process's own certificate/key pair closes sessions established
with the old pair before reload success. Certificate or CRL expiry closes affected
internal sessions and prevents new admission even without a reload; an invalid
replacement cannot extend the validity of the active generation.

## Client credentials and AUTH grammar

Protected mode has no default user/password. Administrators provision named users
in a restricted UTF-8 JSON policy file. User names match
`[A-Za-z0-9][A-Za-z0-9._-]{0,63}` and are case-sensitive. Passwords are sent only in
RESP AUTH or HELLO AUTH frames and are never accepted in config or environment
variables. The policy stores Argon2id PHC verifiers with version 19 and exactly
`m=65536,t=3,p=1` (64 MiB, three iterations, one lane), a 16-byte salt, and a
32-byte output. Salts are generated independently with a cryptographic random
source. The PHC prefix is exactly `$argon2id$v=19$m=65536,t=3,p=1$`; salt and output
use standard unpadded PHC Base64. Extra parameters, unsupported versions, other
lengths, malformed encoding, or noncanonical parameter order fail policy validation.
Passwords contain 16–1024 bytes. Verifiers have UTC RFC3339 `not_before` and
`expires_at`, with `not_before < expires_at`; the validity interval includes its
start and excludes its end. Rotation installs a
second verifier; overlap is allowed only with explicit expiry and a maximum 24-hour
overlap. A user has one or two credentials, never zero or more than two.

The client policy file is `client-policy.json` in `SIDER_SECURITY_DIR`. Its top-level
object has exactly `version: 1` and `users`. Each user
object has exactly `name`, `credentials`, `commands`,
`read_keys`, `write_keys`, `publish_channels`, `subscribe_channels`, and
`info_sections`. Each `credentials` entry has exactly `verifier`, `not_before`,
and `expires_at`; their validity intervals may overlap by at most 24 hours.
Unknown fields, duplicate names, malformed Base64, and unknown
commands/sections reject the complete file. Each scope requires `exact` (array
of Base64 strings) and `prefix` (array of Base64 strings), and permits `all`
(boolean) as its only optional field; absent `all` means false. `info_sections`
is an array drawn from `server`, `clients`,
`stats`, `memory`, `persistence`, `config`, and `replication`. Policy grants have
no wildcards or category aliases.

The request is a RESP array of non-null bulk strings. Command names and option
keywords are ASCII case-insensitive. User names and passwords are case-sensitive.
Accepted forms are:

```text
AUTH <username> <password>
HELLO 2
HELLO 3
HELLO 2 AUTH <username> <password>
HELLO 3 AUTH <username> <password>
```

HELLO without AUTH is accepted only after authentication. Before authentication,
only AUTH and HELLO with AUTH are accepted. HELLO AUTH authenticates and negotiates
atomically. A malformed, unsupported, or failed HELLO leaves the prior protocol and
identity unchanged. HELLO in MULTI is rejected and dirties the transaction.

Successful AUTH replies `+OK` in the negotiated protocol. Successful HELLO AUTH
returns the normal HELLO array/map in the requested protocol. Any unauthenticated
non-auth command, including PING, INFO, or HELLO without AUTH, returns
`-NOAUTH Authentication required`. Unknown, wrong, expired, or not-yet-valid
credentials all return the same `-WRONGPASS Authentication failed`. Forbidden
operations return `-NOPERM Permission denied`. Replies disclose no submitted name,
key, channel, or argument.

AUTH replaces identity only after successful verification. On success, transaction,
WATCH, and Pub/Sub state are cleared before replying. On failure, existing identity
and state remain if still valid under policy. A session is tied to the exact
verifier that authenticated it, not automatically to another overlapping verifier.
Natural expiry of that verifier clears identity, transaction, WATCH, and Pub/Sub
state before another operation or notification is admitted. Operations admitted
before expiry finish under their existing policy lease. The connection remains open for
reauthentication; its next ordinary command returns NOAUTH. Expiry does not reset
failure counters or start another first-authentication deadline. Explicit removal
or replacement of that verifier by reload closes the session instead.
One-argument AUTH is rejected; there is no default user.

## Authentication limits

TLS handshake and certificate validation have a 5-second deadline. The first valid
authentication must complete within 5 seconds after TLS. Each connection has at most
three authentication failures; the third gets `-WRONGPASS Authentication failed`
and then the connection closes. Success resets the connection count.
Argon2 runs in a bounded pool of
two workers plus a finite queue of 64 requests. Unknown user names use a fixed
dummy verifier with the same parameters and one verification pass. Full queue
fails closed with `-WRONGPASS Authentication failed` and counts as a failure.
Malformed AUTH or HELLO AUTH receives its controlled syntax/arity error and does
not count as a credential attempt; no Argon2 job is scheduled. A password outside
the permitted byte length instead gets WRONGPASS and counts as a failure.
An otherwise valid HELLO with an unsupported protocol is a negotiation error,
does not verify a password, and does not count as an authentication attempt.
Expired queued work is discarded before verification when it has not yet started;
completed work cannot authenticate a connection whose deadline has elapsed.

Reconnect budget is a token bucket keyed by remote IP after IPv4-mapped IPv6
normalization: capacity five, refilling one token every 12 seconds. Each
authentication failure consumes one available token; success removes the bucket.
Before scheduling verification, an exhausted bucket gets
`-WRONGPASS Authentication failed` and the connection closes without an Argon2 job.
This denial does not move the next refill time. At 65,536 tracked
addresses, evict the oldest inactive entry; when all entries were active in the last
12 seconds, untracked addresses receive `-WRONGPASS Authentication failed` and
close without insertion or an Argon2 job. Syntax/negotiation errors and
unauthenticated ordinary commands returning NOAUTH consume no tokens.
No username or IP labels are emitted. Limits are fixed, not configurable.

## Authorization model and complete command matrix

Every user has default-deny command, key, publish-channel, and subscribe-channel
grants. Policy command names are canonical uppercase names and match ASCII
case-insensitively. Keys and channels are arbitrary bytes. JSON represents exact
strings and prefixes as standard padded Base64 in separate fields. Matching is
bytewise, without globbing, Unicode normalization, or case folding. Empty exact
strings match only empty values. Empty prefixes are invalid. Universal scope requires
explicit `all: true`; absent or empty scope grants nothing. Overlapping rules are
additive; there are no deny rules.

Parsing is bounded and complete before authorization. Authorization must pass before
queue acceptance, subscription mutation, publication, response data access, or worker
acceptance. Every key and channel is checked; multi-key denial is all-or-nothing.
Unknown/future commands are denied until added to this contract.

| Commands | Required authority |
| --- | --- |
| `GET`, `MGET`, `EXISTS`, `TTL`, `PTTL`, `HGET`, `HEXISTS`, `HLEN`, `HGETALL`, `LLEN`, `LRANGE`, `SISMEMBER`, `SCARD`, `SMEMBERS`, `ZCARD`, `ZSCORE`, `ZRANGE` | Exact command grant and read scope for every key argument |
| `SET`, `DEL`, `MSET`, `INCR`, `DECR`, `EXPIRE`, `PEXPIRE`, `PERSIST`, `HSET`, `HDEL`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `SADD`, `SREM`, `ZADD`, `ZREM` | Exact command grant and write scope for every key argument; MSET checks keys, never values |
| `PUBLISH` | Exact command grant and publish scope for its channel; denial causes no delivery and returns no count |
| `SUBSCRIBE` | Exact command grant and subscribe scope for every channel; check all before adding any |
| `UNSUBSCRIBE` | Authenticated connection may remove its own subscriptions after grant reduction; cannot restore revoked subscriptions or receive messages from revoked channels |
| `MEM.INFO`, `MEM.GET`, `MEM.PTTL`, `MEM.SEARCH` when memory version 1 is installed | Exact command grant and read scope for the collection key; namespace and metadata filters do not grant authority |
| `MEM.CREATE`, `MEM.PUT`, `MEM.DEL`, `MEM.EXPIRE`, `MEM.PERSIST` when memory version 1 is installed | Exact command grant and write scope for the collection key; member IDs are data within that scope |
| `WATCH` | Exact command grant and read scope for every watched key; denial adds no watch |
| `MULTI`, `EXEC`, `DISCARD`, `UNWATCH` | Exact command grant. Each queued command independently needs its command and key/channel grants. WATCH is forbidden inside MULTI. DISCARD/UNWATCH only release local state. |
| `INFO` | Exact command grant plus grants for each requested diagnostic section. |
| `PING`, `ECHO` | Exact command grant after authentication |
| `HELLO`, `AUTH` | Authentication/protocol control only; excluded from ordinary command ACL |

Key positions are exactly those parsed by each command: GET/SET/INCR/DECR/expiry
commands and collection commands use their first key argument; DEL/EXISTS/MGET use
all arguments; MSET uses every even-position key; WATCH uses all arguments.
Memory commands use the collection key immediately after the command name.
PUBLISH uses its first argument as channel; SUBSCRIBE uses every argument. Values,
fields, members, scores, and messages are not keys. INFO section grants match the
canonical section names. RESP command names are case-insensitive; grants use their
canonical spelling. The current RESP3 subset changes reply encoding only and adds no
authority or commands. `writes_dataset()` is not an authorization source; Pub/Sub,
diagnostics, and administration use distinct authority.

For INFO, section authority is exact membership in `info_sections`. An omitted
section list uses the current default section set and requires every corresponding
grant. The response is assembled only from authorized sections; a request containing
any unauthorized section fails in full without returning other sections. Request
selectors `all`, `default`, and `everything` expand to all seven sections and require
all seven grants. Any other unknown section is a syntax error before authorization.

## Transactions, WATCH, reload, and Pub/Sub ordering

Each request is checked at acceptance and again at final execution admission.
MULTI queue acceptance checks its command and every relevant key/channel.
Immediately before EXEC submission, all queued grants are checked against one
policy snapshot. A policy read lease covers final authorization through the
complete worker operation, including queue waiting and all EXEC effects; it is
retained by an accepted operation even if its caller times out. Reload takes the
policy write lock, waits for those admitted operations, swaps the complete validated
generation, and cleans affected session state before releasing the lock. EXEC
either completes under its admitted generation before reload, or uses the new
generation and rejects the entire queue with NOPERM. Policy change cannot cause
partial EXEC.

WATCH requires read scope at registration. Revoked identity or key grant clears all
WATCH registrations. AUTH replacement, natural credential expiry, or explicit
identity revocation clears queue and watches. A grant reduction that invalidates
queued authority discards the queued commands and retains a denial marker until
EXEC or DISCARD releases the transaction. EXEC consumes that marker and returns
NOPERM; no prefix executes. Other retained queues are rechecked at EXEC admission.
AUTH and HELLO are never queueable. SUBSCRIBE, UNSUBSCRIBE, and PUBLISH are also
rejected inside MULTI and dirty the queue; Pub/Sub state changes are never part of
an EXEC batch.

PUBLISH authorization and local enqueue use the same policy read lease. SUBSCRIBE
checks all channels before adding any. Each protected notification is rechecked
against the subscriber's identity, credential validity, and channel grant, and a
read lease covers its complete bounded socket write. Reload removes revoked
subscriptions and drops their pending notifications under the write lock before
another notification is queued or written. It closes explicitly revoked identities
and waits for affected socket writers to finish or terminate before success.
Here delivery means the server's enqueue or completed socket write; bytes already
written to TLS/TCP may be read by the client after reload and cannot be recalled.
No new notification enqueue or write for removed authority begins after reload
completes. The rule applies to RESP2 arrays and RESP3 push frames. A grant-reduced
authenticated client may still UNSUBSCRIBE its own remaining subscriptions.

## Internal roles and message direction

Certificate validation and exact policy identity admission precede application
frames. For an admitted peer, read and validate only the bounded fixed frame header
first, then authorize its message kind and session direction before allocating or
decoding its payload. An untrusted, unlisted, wrong-upstream-instance, or wrong-role
peer is closed without protocol response.
Roles derive only from exact URI SAN and local policy, never loopback or cursor.

| Client-to-server request | Required role | Permitted response/action |
| --- | --- | --- |
| `Hello` carrying replica capabilities/cursor; later `Ack` | Replica principal allowed for this local instance | Hello, Continue/FullStart, snapshot entries, batches, heartbeat, and stream frames for that session |
| `Export` | Backup | One Hello and one immutable snapshot, then EOF; no stream subscription or ACK |
| `StatusRequest` | Monitor or promotion | Status only; promotion role explicitly includes status disclosure |
| `Promote` | Promotion | Durable role change and Promoted, only after operator fences former primary |

Hello is bidirectional: replica sends capabilities/cursor; primary replies with its
capabilities and stream position. Continue is a primary response; Ack is a replica
request within a replica session. Responses grant no authority. Backup cannot request
replication, status, or promotion; monitor cannot export or mutate; replica cannot
export unrelated snapshots or promote. A principal gets exactly its configured roles.

Internal sessions take short policy leases for each authorized frame and operation;
they do not retain one lease for the lifetime of a replication stream. Final snapshot
installation and durable promotion are admitted as complete operations under a lease.
Reload either waits for an admitted operation to finish, or revokes the session before
that operation begins. An interrupted snapshot cannot be partially installed.

The operator fences the former primary before promotion. This contract adds no
consensus, automatic fencing, or failover and does not replace AOF role, epoch,
sequence, or durable promotion checks. Role denial occurs before snapshot capture,
status disclosure, or AOF/role change. It closes TLS without a frame, distinct from
authenticated version/layout/resource Reject codes. Auth failures are logged as
authentication failures without certificate material.

## Secret files, diagnostics, and recovery

Client and internal policies, verifiers, private keys, and trust roots are restricted
UTF-8 JSON/PEM files. No secret is accepted in environment variables or CLI arguments.
On POSIX, required files are regular, owned by the effective service UID, mode
`0600`; the security directory is service-owned, mode `0700`. Ancestors through the
filesystem root must be owned by that UID or root and have no group/other write.
Symlinks in any checked path component are rejected.
On Windows, the service identity is the process token's user SID. Required files
and the security directory are owned by that SID and allow access only to it,
LocalSystem, and local Administrators; broad inherited access is rejected.
Ancestors through the volume root may also be owned by those trusted identities
or TrustedInstaller and may grant ordinary read/traverse access. They may grant
creation of sibling directories, but must not grant other identities file creation,
modification, child-delete, delete, ownership-change, or ACL-change rights on the
protected path. Inherit-only grants are evaluated on the child where they apply.
Reparse points in any checked component are rejected. Check permissions on the
objects whose bytes are read, not only path strings. If ownership, type, or mode/ACL
checks cannot be performed, protected startup or replacement validation fails closed.

### Configuration generations and reload trigger

`security-manifest.json` is restricted by the same file rules. Its top-level object
has exactly `version: 1`, `generation`, and `files`. `generation` is a positive
unsigned 64-bit integer. `files` maps each required fixed JSON/PEM basename for this
process to its SHA-256 digest as 64 lowercase hexadecimal characters. It excludes
the manifest itself. Missing, extra, duplicate, or invalid entries reject the
generation. No entry can name a path or redirect a read. A server and a tool use
separate directories if their required file sets differ.

Provision a complete generation before startup. For rotation, write restricted
replacement files and atomically rename each on the same volume, then atomically
replace the manifest last with a generation greater than the active one. The loader
reads the manifest, validates and loads every required file, verifies its digest,
then rereads the manifest and requires identical bytes. Validation and activation
use those same loaded bytes; mutable paths are not reopened after validation.
An interrupted replacement cannot activate a mixture of generations. Startup with
an incomplete or invalid generation fails before listeners or outgoing connections.

A running server polls the manifest every second and serializes reload attempts.
Pending generations greater than the active generation are retried on later polls.
A manifest identical to the active manifest is ignored; an older generation or
changed manifest with the active generation number is rejected. Tools load one
complete generation when invoked; the server's outgoing replica reconnects use its
currently active generation. Invalid replacement leaves the old generation active, with one
redacted failure log per distinct manifest contents until it changes or succeeds.
Success is reported by one redacted reload log after the policy swap, session
closures, and state/delivery barriers above complete. Polling is the only automatic
reload trigger in version 1; this contract adds no network reload command. Operators
observe that success before relying on revocation. A restart validates its complete
on-disk generation; it does not persist security policy or generation in the AOF.

Logs, RESP errors, readiness, INFO, metrics, panics, and CLI errors omit passwords,
verifier hashes, private keys, bearer material, raw AUTH frames, key/channel bytes,
and full certificate contents. Auth metrics are bounded aggregate counters without
user/IP labels. Readiness and INFO may disclose transport mode, auth-required state,
and aggregate configuration status only. INFO never exposes policy, paths, verifier
metadata, trust-root names, or principal names. INFO config/replication still require
their specific diagnostic grants. Backups and AOF may contain application secrets
and require separate protection. Backups never contain users, verifiers, trust roots,
role policy, or private keys; restore cannot reinstate obsolete security policy.

The release guide must cover client/certificate rotation, credential recovery, loss
of internal identity, protected migration, and the compatibility effect of requiring
AUTH or HELLO AUTH. A 1.0.1 receipt does not prove these behaviors. A new candidate
requires platform-specific security and recovery evidence.

## Required integration evidence for #93

Run against loopback and permitted non-loopback protected binds where applicable.
Record SHA, OS, profile, principal names (never secrets), commands, results, and exit
codes. For every denial, assert no unauthorized change to AOF, dataset, subscriptions,
exported bytes, or durable role.

1. Missing, wrong, expired, not-yet-valid, rotated, and revoked credentials; AUTH
   replacement; three-failure and deadline limits; worker/address-budget saturation;
   indistinguishable failure replies.
2. Every matrix command with denied command/key/publish/subscribe grants; multi-key
   denial; WATCH; queued denial; reload between queue and EXEC; reload during queued
   notification; AUTH replacement while subscribed.
3. Plaintext to TLS; missing, invalid, expired, untrusted, wrong-host, wrong-IP-SAN,
   and rotated certificates; missing/expired/incomplete CRLs; rejected non-loopback
   plaintext; incomplete or mixed-generation config before readiness; permitted and
   rejected POSIX ownership/mode and Windows owner/DACL/ancestor/reparse cases.
4. Internal peers without cert, with untrusted/expired cert, wrong instance/principal,
   and wrong role. Probe Export, StatusRequest, Promote separately and verify denial
   precedes data capture/state change.
5. Certificate, trust, user, and role rotation during an existing session and replica
   reconnect; manifest-last rotation and polling success/failure; grant reduction
   versus identity revocation and natural expiry; verify no partial snapshot install,
   forbidden enqueue/write, or promotion. Distinct holder instance IDs must work.
6. Scan logs, readiness, INFO, metrics, panic/CLI errors, backups, and diagnostics
   using canary passwords, verifier strings, keys, channels, and private-key text.
7. Positive RESP2 auth/command, RESP3 HELLO AUTH/push, replica sync, backup export,
   monitor status, promotion after fencing, and restore without credentials.

#93 remains open until these cases run. This target contract alone is not evidence
that implementation or release satisfies it.
