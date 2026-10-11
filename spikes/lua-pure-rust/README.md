# Pure Rust Lua containment experiment

This isolated experiment evaluates `omnilua` 0.7.1, a Lua 5.4 interpreter
implemented in Rust, as an alternative runtime for
[issue #95](https://github.com/djairofilho/sider/issues/95). It is not linked to
the Sider server and does not enable scripting commands.

The dependency disables optional libraries at compile time and creates each VM
with no host hooks. The tests probe absent host and dynamic-loading globals,
binary strings, an infinite loop, an oversized allocation, and malformed text.
Run them with:

```text
cargo test --locked --manifest-path spikes/lua-pure-rust/Cargo.toml
cargo clippy --locked --manifest-path spikes/lua-pure-rust/Cargo.toml --all-targets -- -D warnings
```

On 2026-10-11, all five tests passed on Windows 11 and Ubuntu 24.04.4 under
WSL2. Windows Clippy completed with warnings denied. The tests show that these
specific requests are rejected or preserved by this pinned runtime and
configuration; they do not establish a general containment guarantee.

The VM's memory budget checks GC-tracked allocations at instruction intervals.
It is not a hard process-memory ceiling, and native interpreter operations may
allocate before the next check. This test cannot establish isolation from a
runtime bug, a bounded compiler, or reliable interruption of every native
function. Production use would still require a reviewed helper boundary,
operating-system resource controls, and the integration and negative tests in
[the proposed contract](../../docs/lua-scripting.md). The Windows Job memory
measurement question in the existing
[containment experiment](../lua-containment/WINDOWS-MEMORY-REPRO.md) remains open.
