# Third-party notices distributed with Sider

This collection preserves upstream texts. It does not choose among alternative
licenses, interpret legal obligations, or change the MIT license of Sider code.

The [inventory](inventory.json) identifies versions, logical sources, paths,
sizes, and SHA-256 for every file. Paths are relative to this directory.
Distribute this complete directory together with Sider's own `LICENSE`.

## Contents

- 40 packages from Cargo's `normal` graph, including proc-macro dependencies.
  The union of Windows MSVC and GNU Linux targets contains 32 packages without
  those edges.
- Full `LICENSE`, `LICENSE-*`, and equivalent texts for every package in `crates/`.
  MIT/Apache alternatives and the Unicode text from `unicode-ident` are preserved.
- Nested notice `tracing-core-0.1.36/src/spin/LICENSE`, retained as a precaution,
  without asserting that this `no_std`-conditional module is in the binary.
- [Rust 1.97.1 standard-library inventory](rust-1.97.1/COPYRIGHT-library.html)
  and the MIT, Apache-2.0, BSD-2-Clause, Unicode-3.0, and LLVM-exception texts.
- [fpconv Boost 1.0 license](fpconv-redis-8.10.1/LICENSE.txt), adapted to safe
  Rust in `src/command/fpconv.rs`. Its source is `deps/fpconv` from Redis tag
  8.10.1; the two source files and their hashes are in the inventory.

Test-only dependencies, the Redis server, and the compiler's general inventory
are not part of the collection. The Rust inventory includes notices from other
platforms; their presence does not assert that Sider uses those components.

The fpconv component has its own Boost 1.0 license. Its notices are:

```text
Copyright (c) 2021, Redis Labs
Copyright (c) 2013-2019, night-shift <as.smljk at gmail dot com>
Copyright (c) 2009, Florian Loitsch <florian.loitsch at inria dot fr>
All rights reserved.
```

## Integrity and updates

The 79 files total 633,554 bytes. In 78 of them, source and copy are identical.
Only `COPYRIGHT-library.html` received a trailing LF: the copy is exactly the
source plus that byte. `source_sha256`/`source_bytes` record the source;
`sha256`/`bytes` record the copy. The six Rust files had identical source hashes
in Windows and Linux 1.97.1 toolchains. `.gitattributes` prevents EOL conversion.

When dependencies or the toolchain change, rerun the inventory commands, review
the notice set, and verify all hashes before distributing packages.
