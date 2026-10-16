# Security implementation status

This document records development progress toward [issue #93](https://github.com/djairofilho/sider/issues/93). The normative acceptance criteria are in the [network security boundary](security-boundary.md). The published 1.0.1 artifacts do not contain these development changes. Issue #93 remains open.

## Deployment state

The development executable defaults to the protected profile. It requires an absolute `SIDER_SECURITY_DIR` with a complete validated generation before recovery, bind, readiness publication, or outgoing connections. Missing or invalid material does not select plaintext automatically. `sider-backup export` and `sider-replica` use the same profile selection.

On POSIX, the development executable accepts a complete restricted client/internal generation and starts both protected TLS listeners. A real executable test on Ubuntu 22.04 under WSL2 verifies client `NOAUTH`, authenticated internal `STATUS`, and an allowed backup. An atomic manifest-last rotation removes the backup role while the server stays up; subsequent exports are denied without a destination, while the monitor role still works. The denial persists after restart. The backup tool's separate restricted generation contains only internal files. Broad key permissions and changed bytes fail loading. This is development evidence, not a native Linux release receipt.

Windows protected startup remains explicitly unavailable pending the full filesystem permission matrix. The gate fails before listener or AOF startup. Focused Windows loader tests accept private files, reject broad grants on the security directory or a file, distinguish ancestor rights, and reject a junction in the directory path. The loader now opens components without following reparse points and checks type, owner, and DACL through the opened handles while retaining ancestor handles during material reads. The local host did not permit creation of a file symlink (Windows error 1314), so that negative case remains unexecuted here.

The Windows reader also checks each material file's length, owner, and DACL again after reading, and rechecks every pinned directory handle before returning the generation. Persistent permission or length changes during a read now fail closed. This does not prove that a transient change and restore, or an in-place same-length rewrite, is impossible; the manifest digest checks the returned bytes. The remaining Windows replacement and owner cases still block protected executable startup.

`SIDER_SECURITY_PROFILE=legacy-loopback` explicitly selects plaintext without authentication and restricts client, internal, upstream, and tool addresses to literal loopback. It rejects a security directory and emits a warning. The existing plaintext library APIs still require callers to opt into their secure counterparts.

## Client boundary implemented

- Rustls TLS 1.2/1.3 uses bounded handshakes and writes, with resumption, tickets, and early data disabled. Real TLS tests cover trusted traffic, plaintext, untrusted issuers, and incorrect literal IP identities.
- Named users use fixed Argon2id parameters, bounded passwords, canonical verifier identities, UTC validity windows, and at most two overlapping credentials. Verification admission and failure budgets are bounded.
- ACLs check typed command names, every binary key or channel, INFO sections, transactions, and Pub/Sub. Protected `HELLO AUTH` validates the requested protocol before committing both identity and protocol state. A failed replacement preserves the prior session.
- `ClientRuntime` polls and activates monotonic client generations. Invalid replacements retain the active generation. Activation serializes socket cleanup with accepted worker effects and queued notifications. Accepted operations retain a policy lease after caller cancellation. Expired credentials prevent admission while allowing reauthentication; expired client transport closes the connection.
- Security diagnostics use fixed categories and aggregate counters. Policy and proof objects do not implement `Debug`.

## Internal boundary implemented

- Internal transport requires mutual TLS with a literal IP SAN, exact upstream URI where applicable, distinct local holder identity, explicit roles, and CRLs with validity checks. Initial Hello, Export, StatusRequest, and Promote requests are authorized from the verified peer identity before reading a payload.
- Each receiver selects the permitted next frame kind after its bounded header and before allocating the payload. Replica, backup, snapshot, ACK, status, and promotion phases are covered; an authenticated Reject terminates the operation without granting authority.
- Opaque peer proofs retain the verified chain, URI, roles, local certificate/key identity, and validity intersection. The component API can revalidate a proof against replacement roots, CRLs, roles, and the local pair. Existing streams check certificate and CRL expiry during idle reads and writes with bounded timers.
- A real TLS chain with a root, intermediate CA, and leaf succeeds with both issuer CRLs. Missing root or intermediate coverage, a revoked leaf or intermediate, and an expired intermediate CRL are rejected in focused tests.
- Replica snapshot installation and batch submission recheck peer validity before database admission. Backup rechecks before publishing its final directory. Admin responses are checked before output.

The live internal consumer stages added a shared client/internal generation runtime, a close registry, current-generation reconnects, and operation leases through accepted worker effects to the prepared server path. POSIX executable startup now uses that runtime; broader deployment gates still require end-to-end acceptance. Manifest polling retries every second while logging a rejected manifest identity only when it changes; a successful load resets warning suppression. The warning contains no manifest bytes.

## Verification and remaining gates

The integrated development branch passed `cargo xtask check`, `cargo doc --locked --no-deps`, and `cargo build --locked --release` on Windows after the client, internal, RESP3, memory, and transaction changes were combined. Focused `cargo test --locked --test agent_memory_tcp --test security_internal --test security --lib` passed 177 library, 16 memory TCP, 10 security, and 6 internal security tests. The complete gate also passed the existing non-ignored native suites. The Windows restricted-file loader stage additionally passed `cargo test --locked --lib security_windows_restricted -- --nocapture` and `cargo xtask check` on Windows. These are development checks, not exact-SHA candidate or release receipts.

The intermediate-chain test passed on Windows and Ubuntu 22.04 under WSL2. The full Windows `security_internal` suite passed 36/36. WSL2 is virtualized Linux evidence, not a native Ubuntu deployment receipt.

After enabling POSIX protected internal startup, `cargo xtask check` passed on Windows and Ubuntu 22.04 under WSL2. The focused executable suite passed 36 internal and 13 CLI tests on Windows, and 38 internal and 13 CLI tests under WSL2. Windows `cargo doc --locked --no-deps` and `cargo build --locked --release` also passed. These checks do not replace native Linux or exact candidate-package receipts.

The POSIX restricted loader walks from the root through opened directory
descriptors, validating each owner and mode after `openat` with `O_NOFOLLOW`.
It pins the final directory and reads fixed file names relative to that handle.
A deterministic test renames the directory and places a symlink at its old path;
reads and a child-directory open remain anchored to the original handle, while a
fresh walk rejects the symlink. This is a directory replacement boundary, not
complete filesystem race evidence.

The POSIX material reader now rechecks the opened final directory's owner and
mode before and after each material file read. A focused test changes that
directory to group-writable after its descriptor was opened; the read fails and
succeeds only after the mode is restored. This catches a persistent permission
change at the pinned directory, but does not revalidate ancestor modes after
the initial walk or prove resistance to a transient change and restore.

Each opened material file is also rechecked after reading. The reader rejects
a file whose owner, mode, type, or length remains changed at that check, and
requires the byte count to match both observed lengths. Deterministic tests
change the file mode and extend its contents at the post-read boundary; both
reads fail. This does not prove an in-place same-length rewrite cannot race the
read, although the manifest digest checks the resulting material bytes.

Issue #93 still requires broader executable deployment evidence for generation activation, close barriers, reconnects, and operation leases; full real TLS replica/backup/admin allow and deny matrices across certificate chains; further POSIX replacement and concurrent permission-change tests; and the remaining Windows permission cases including alternate owner, file reparse points, and replacement races. The Windows protected startup gate stays closed. Ignored package, external, soak, and release tests have not been promoted into evidence for this feature.

For the planned Docker-only v1.1.0 target, a development image on Ubuntu 22.04
under WSL2 started with restricted client material, a read-only root filesystem,
and a read-only security bind mount. Loopback TLS, pre-auth denial, named AUTH,
RESP2/RESP3, and basic memory and composed operations succeeded. Both durable
profiles survived restarts on separate volumes; each refused the other's
volume. This was a dirty-checkout rehearsal with an all-zero source-SHA image
label, not a candidate or gate receipt. Issue #208 records the results.
Subsequent local Docker rehearsals also passed manifest-last client credential
rotation, internal mTLS backup and restore for both durable profiles, protected
replica snapshot and reconnect, and composed backup-role revocation while monitor
access remained available. These used the same dirty-checkout development image
and are not candidate receipts. The Linux Docker gate still needs public digest
pull, exact source identity, broader network and crash boundaries, and complete
administrative and denial matrices. Windows protected deployment is outside the
v1.1.0 distribution target.
