#![forbid(unsafe_code)]

use mlua::chunk::ChunkMode;
use mlua::debug::DebugEvent;
use mlua::{HookTriggers, Lua, LuaOptions, StdLib, Table, Value, VmState};
use std::cell::Cell;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::rc::Rc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

type AnyError = Box<dyn std::error::Error>;

fn main() -> Result<(), AnyError> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("--cgroup-root") => {
            let root = args.next().ok_or("missing delegated cgroup root")?;
            if args.next().is_some() || !cfg!(target_os = "linux") {
                return Err("--cgroup-root requires Linux and one path".into());
            }
            parent(std::path::Path::new(&root))?;
        }
        Some(case) => child(case)?,
        None => parent(std::path::Path::new("/sys/fs/cgroup"))?,
    }
    Ok(())
}

fn child(case: &str) -> Result<(), AnyError> {
    println!("READY");
    std::io::stdout().flush()?;
    let mut gate = String::new();
    std::io::stdin().read_line(&mut gate)?;
    if gate != "GO\n" {
        return Err("startup gate denied".into());
    }
    if case == "os-cpu" {
        loop {
            std::hint::black_box(17_u64.wrapping_mul(123));
        }
    }
    if case == "os-kernel-cpu" {
        let path = std::env::current_exe()?;
        loop {
            std::hint::black_box(std::fs::metadata(&path)?);
        }
    }
    if case == "os-memory" {
        let mut blocks = Vec::new();
        for _ in 0..128 {
            let mut block = Vec::new();
            if block.try_reserve_exact(1024 * 1024).is_err() {
                println!("MEMORY_DENIED allocated_mib={}", blocks.len());
                return Ok(());
            }
            // Copy initialized byte slices so the development build does not
            // spend the wall budget in a per-byte Vec::resize clone loop.
            for _ in 0..256 {
                block.extend_from_slice(&[0x42_u8; 4096]);
            }
            blocks.push(block);
            std::hint::black_box(&blocks);
        }
        println!("FAIL memory not stopped");
        return Ok(());
    }
    if case == "os-cancel" {
        loop {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    if case == "os-pids" {
        let attempt = Command::new(std::env::current_exe()?)
            .arg("os-cancel")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        match attempt {
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || (cfg!(windows) && error.raw_os_error() == Some(1816)) =>
            {
                println!("OK os-pids denied")
            }
            Err(error) => println!("FAIL os-pids unexpected {error}"),
            Ok(mut escaped) => {
                escaped.kill()?;
                escaped.wait()?;
                println!("FAIL os-pids child allowed");
            }
        }
        return Ok(());
    }
    let result = runtime_case(case);
    match result {
        Ok(description) => println!("OK {description}"),
        Err(error) => {
            let detail = error.to_string();
            let category = if detail.contains("instruction budget") {
                "instructions"
            } else if detail.contains("depth budget") {
                "depth"
            } else if detail.contains("cancelled") {
                "cancelled"
            } else if detail.contains("memory") {
                "memory"
            } else if matches!(error, mlua::Error::SyntaxError { .. }) {
                "syntax"
            } else {
                "runtime"
            };
            if case == "compiler-stress" {
                println!("ERROR {category} fixture_detail={error:?}");
            } else {
                println!("ERROR {category}");
            }
        }
    }
    Ok(())
}

fn runtime_case(case: &str) -> mlua::Result<String> {
    let lua = Lua::new_with(StdLib::STRING, LuaOptions::default())?;
    lua.set_memory_limit(8 * 1024 * 1024)?;
    let globals = lua.globals();
    let string: Table = globals.get("string")?;
    let restricted_string = lua.create_table()?;
    for name in ["byte", "char", "len", "sub"] {
        restricted_string.set(name, string.get::<Value>(name)?)?;
    }
    // Lua's string metatable shares the original library table: restrict that
    // original in place so method syntax cannot recover omitted functions.
    let names = string
        .clone()
        .pairs::<Value, Value>()
        .map(|pair| pair.map(|(key, _)| key))
        .collect::<mlua::Result<Vec<_>>>()?;
    for key in names {
        let allowed = match &key {
            Value::String(name) => {
                matches!(name.to_str()?.as_ref(), "byte" | "char" | "len" | "sub")
            }
            _ => false,
        };
        if !allowed {
            string.raw_set(key, Value::Nil)?;
        }
    }
    let env = lua.create_table()?;
    for name in [
        "assert", "error", "ipairs", "next", "pairs", "select", "tonumber", "type",
    ] {
        env.set(name, globals.get::<Value>(name)?)?;
    }
    env.set("string", restricted_string)?;
    let args = lua.create_table()?;
    args.set(1, lua.create_string([0_u8, 0xff, b'\r', b'\n'])?)?;
    env.set("ARGV", args)?;
    env.set("KEYS", lua.create_table()?)?;
    if case == "native-call" {
        // Fixture-only callback proves instruction hooks cannot preempt native
        // work. It is absent from every other environment.
        env.set(
            "fixture_native",
            lua.create_function(|_, ()| {
                std::thread::sleep(Duration::from_secs(5));
                Ok(())
            })?,
        )?;
    }

    let instructions = Rc::new(Cell::new(0_u32));
    let depth = Rc::new(Cell::new(0_u32));
    let maximum = Rc::new(Cell::new(0_u32));
    let tail_calls = Rc::new(Cell::new(0_u32));
    let hook_instructions = instructions.clone();
    let hook_depth = depth.clone();
    let hook_maximum = maximum.clone();
    let hook_tails = tail_calls.clone();
    let start = Instant::now();
    let cancellation = case == "hook-cancel";
    lua.set_hook(
        HookTriggers::new()
            .every_nth_instruction(1000)
            .on_calls()
            .on_returns(),
        move |_, debug| {
            match debug.event() {
                DebugEvent::Count => {
                    let count = hook_instructions.get() + 1000;
                    hook_instructions.set(count);
                    if cancellation && start.elapsed() >= Duration::from_millis(10) {
                        return Err(mlua::Error::RuntimeError("cancelled".into()));
                    }
                    if !cancellation && count >= 1_000_000 {
                        return Err(mlua::Error::RuntimeError("instruction budget".into()));
                    }
                }
                DebugEvent::Call => {
                    let current = hook_depth.get() + 1;
                    hook_depth.set(current);
                    hook_maximum.set(hook_maximum.get().max(current));
                    if current > 64 {
                        return Err(mlua::Error::RuntimeError("depth budget".into()));
                    }
                }
                DebugEvent::Ret => hook_depth.set(hook_depth.get().saturating_sub(1)),
                // Lua 5.4 tail calls replace a frame; they do not push one.
                DebugEvent::TailCall => hook_tails.set(hook_tails.get() + 1),
                _ => {}
            }
            Ok(VmState::Continue)
        },
    )?;
    let source: Vec<u8> = match case {
        "binary" => b"return ARGV[1]".to_vec(),
        "sandbox" => br#"assert(_G == nil and os == nil and io == nil and package == nil)
assert(require == nil and load == nil and loadfile == nil and dofile == nil)
assert(debug == nil and coroutine == nil and pcall == nil and xpcall == nil)
assert(getmetatable == nil and setmetatable == nil and print == nil)
assert(string.dump == nil and string.rep == nil and string.format == nil)
assert(('x').dump == nil and ('x').rep == nil and ('x').gsub == nil)
return true"#
            .to_vec(),
        "fresh-env" => b"assert(leaked == nil); leaked = 'local'; return true".to_vec(),
        "syntax" => b"local = )".to_vec(),
        "bytecode" => vec![0x1b, b'L', b'u', b'a', 0x54, 0],
        "loop" | "hook-cancel" => b"while true do end".to_vec(),
        "depth" => {
            b"local function f(n) if n == 0 then return 0 end return 1 + f(n-1) end return f(100)"
                .to_vec()
        }
        "tail" => {
            b"local function f(n) if n == 0 then return 0 end return f(n-1) end return f(1000)"
                .to_vec()
        }
        "allocation" => {
            b"local t = {}; local s = ARGV[1]; for i = 1, 28 do s = s .. s; t[i] = s end return s"
                .to_vec()
        }
        "compiler-stress" => {
            format!("return {}0{}", "(".repeat(30_000), ")".repeat(30_000)).into_bytes()
        }
        "native-call" => b"return fixture_native()".to_vec(),
        _ => return Err(mlua::Error::RuntimeError("unknown fixture".into())),
    };
    let result = lua
        .load(source)
        .set_mode(ChunkMode::Text)
        .set_environment(env)
        .eval::<Value>()?;
    if case == "binary" {
        let Value::String(value) = result else {
            return Err(mlua::Error::RuntimeError("binary result type".into()));
        };
        if value.as_bytes().as_ref() != [0_u8, 0xff, b'\r', b'\n'] {
            return Err(mlua::Error::RuntimeError("binary mismatch".into()));
        }
    }
    Ok(format!(
        "{case} instructions={} max_depth={} tails={} vm_bytes={} elapsed_us={} version={}",
        instructions.get(),
        maximum.get(),
        tail_calls.get(),
        lua.used_memory(),
        start.elapsed().as_micros(),
        globals.get::<String>("_VERSION")?
    ))
}

#[cfg(windows)]
struct Boundary(processkit::ProcessGroup);

#[cfg(windows)]
impl Boundary {
    fn install(child: &Child, _root: &std::path::Path, deny: bool) -> Result<Self, AnyError> {
        let options = processkit::ProcessGroupOptions::default()
            .max_memory(64 * 1024 * 1024)
            .max_processes(if deny { 0 } else { 1 });
        let job = processkit::ProcessGroup::with_options(options)?;
        job.adopt_external(child.id())?;
        let members = job.members()?;
        let stats = job.stats()?;
        if members != [child.id()]
            || stats.total_cpu_time.is_none()
            || stats.peak_memory_bytes.is_none()
        {
            return Err("Job membership or required accounting unavailable".into());
        }
        println!(
            "BOUNDARY windows members={members:?} kill_on_close=true max_memory=67108864 max_processes=1 stats={stats:?}"
        );
        Ok(Self(job))
    }
    fn cpu_us(&self) -> Result<Option<u64>, AnyError> {
        let time = self
            .0
            .stats()?
            .total_cpu_time
            .ok_or("CPU accounting unavailable")?;
        Ok(Some(u64::try_from(time.as_micros())?))
    }
    fn terminate(&self) -> Result<(), AnyError> {
        self.0.kill_all()?;
        Ok(())
    }
    fn memory_events(&self) -> String {
        format!("job_stats={:?}", self.0.stats())
    }
}

#[cfg(target_os = "linux")]
struct Boundary(std::path::PathBuf);

#[cfg(target_os = "linux")]
fn expected_cgroup_mount(root: &std::path::Path, mounts: &str) -> bool {
    let covering: Vec<_> = mounts
        .lines()
        .filter_map(|line| {
            let (attributes, filesystem) = line.split_once(" - ")?;
            let mountpoint = attributes.split_whitespace().nth(4)?;
            let mountpoint = mountpoint
                .replace("\\040", " ")
                .replace("\\011", "\t")
                .replace("\\012", "\n")
                .replace("\\134", "\\");
            let path = std::path::PathBuf::from(mountpoint);
            root.starts_with(&path)
                .then(|| (path, filesystem.split_whitespace().next()))
        })
        .collect();
    // Reject nested or stacked mounts even if an older covering cgroup2 record
    // remains visible in mountinfo. Do not assume record order chooses the top.
    let under_cgroup = covering
        .iter()
        .filter(|(path, _)| path.starts_with("/sys/fs/cgroup"))
        .count();
    let deepest = covering
        .iter()
        .max_by_key(|(path, _)| path.components().count());
    under_cgroup == 1
        && matches!(deepest, Some((path, Some("cgroup2"))) if path == std::path::Path::new("/sys/fs/cgroup"))
}

#[cfg(all(test, target_os = "linux"))]
mod mount_tests {
    use super::expected_cgroup_mount;
    use std::path::Path;
    const CGROUP: &str = "39 24 0:35 / /sys/fs/cgroup rw,nosuid - cgroup2 cgroup rw\n";
    #[test]
    fn genuine_mount_supports_delegated_child() {
        assert!(expected_cgroup_mount(
            Path::new("/sys/fs/cgroup/system.slice/private.service"),
            CGROUP
        ));
    }
    #[test]
    fn fake_or_stacked_root_is_rejected() {
        let fake = "40 39 0:36 / /sys/fs/cgroup rw - tmpfs none rw\n";
        assert!(!expected_cgroup_mount(Path::new("/sys/fs/cgroup"), fake));
        assert!(!expected_cgroup_mount(
            Path::new("/sys/fs/cgroup"),
            &format!("{CGROUP}{fake}")
        ));
        assert!(!expected_cgroup_mount(
            Path::new("/sys/fs/cgroup"),
            &format!("{fake}{CGROUP}")
        ));
    }
    #[test]
    fn escaped_nested_mount_is_rejected() {
        let nested = "40 39 0:36 / /sys/fs/cgroup/team\\040space rw - tmpfs none rw\n";
        assert!(!expected_cgroup_mount(
            Path::new("/sys/fs/cgroup/team space/helper"),
            &format!("{CGROUP}{nested}")
        ));
    }
}

#[cfg(target_os = "linux")]
impl Boundary {
    fn validate_root(root: &std::path::Path) -> Result<std::path::PathBuf, AnyError> {
        let root = root.canonicalize()?;
        if !root.starts_with("/sys/fs/cgroup") {
            return Err("delegated root is outside real cgroup2 mount".into());
        }
        let mounts = std::fs::read_to_string("/proc/self/mountinfo")?;
        if !expected_cgroup_mount(&root, &mounts) {
            return Err(
                "delegated root is not on the expected cgroup2 mount (or has a submount)".into(),
            );
        }
        let enabled = std::fs::read_to_string(root.join("cgroup.subtree_control"))?;
        for required in ["memory", "cpu", "pids"] {
            if !enabled.split_whitespace().any(|name| name == required) {
                return Err(format!("delegated controller unavailable: {required}").into());
            }
        }
        Ok(root)
    }
    fn install(child: &Child, root: &std::path::Path, deny: bool) -> Result<Self, AnyError> {
        if deny {
            return Err(
                std::fs::read_to_string(root.join("missing-controller-fixture/cpu.stat"))?.into(),
            );
        }
        let root = Self::validate_root(root)?;
        let path = root.join(format!("sider-lua-spike-{}", child.id()));
        std::fs::create_dir(&path)?;
        let boundary = Self(path);
        std::fs::write(boundary.0.join("memory.max"), "67108864")?;
        std::fs::write(boundary.0.join("memory.swap.max"), "0")?;
        std::fs::write(boundary.0.join("pids.max"), "1")?;
        std::fs::write(boundary.0.join("cgroup.procs"), child.id().to_string())?;
        println!(
            "BOUNDARY linux root={} memory.max=67108864 memory.swap.max=0 pids.max=1 cpu.stat=available",
            root.display()
        );
        Ok(boundary)
    }
    fn cpu_us(&self) -> Result<Option<u64>, AnyError> {
        let usage = std::fs::read_to_string(self.0.join("cpu.stat"))?
            .lines()
            .find_map(|line| {
                line.strip_prefix("usage_usec ")
                    .and_then(|n| n.parse().ok())
            })
            .ok_or("CPU accounting unavailable or malformed")?;
        Ok(Some(usage))
    }
    fn terminate(&self) -> Result<(), AnyError> {
        std::fs::write(self.0.join("cgroup.kill"), "1")?;
        Ok(())
    }
    fn memory_events(&self) -> String {
        std::fs::read_to_string(self.0.join("memory.events"))
            .unwrap_or_default()
            .replace('\n', ";")
    }
}

#[cfg(target_os = "linux")]
impl Drop for Boundary {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir(&self.0) {
            eprintln!("CGROUP CLEANUP FAILURE {} {error}", self.0.display());
        }
    }
}

fn parent(cgroup_root: &std::path::Path) -> Result<(), AnyError> {
    #[cfg(target_os = "linux")]
    let validated_root = Boundary::validate_root(cgroup_root)
        .map_err(|error| format!("containment unavailable; no helper spawned: {error}"))?;
    #[cfg(target_os = "linux")]
    let cgroup_root = validated_root.as_path();
    println!(
        "SPIKE platform={} architecture={} mlua=0.12.2 Lua=5.4 vm_limit=8388608",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    let cases = [
        ("boundary-denied", "BOUNDARY_DENIED"),
        ("binary", "OK binary"),
        ("sandbox", "OK sandbox"),
        ("fresh-env", "OK fresh-env"),
        ("fresh-env", "OK fresh-env"),
        ("syntax", "ERROR syntax"),
        ("bytecode", "ERROR syntax"),
        ("loop", "ERROR instructions"),
        ("depth", "ERROR depth"),
        ("tail", "OK tail"),
        ("allocation", "ERROR memory"),
        ("compiler-stress", "ERROR runtime"),
        ("hook-cancel", "ERROR cancelled"),
        ("os-cancel", "CANCELLED"),
        ("os-cpu", "CPU_KILLED"),
        ("os-kernel-cpu", "CPU_KILLED"),
        (
            "os-memory",
            if cfg!(windows) {
                "MEMORY_DENIED"
            } else {
                "MEMORY_KILLED"
            },
        ),
        ("os-pids", "OK os-pids denied"),
        ("native-call", "WALL_KILLED"),
    ];
    let mut failures = 0;
    for (case, expected) in cases {
        let started = Instant::now();
        let mut child = Command::new(std::env::current_exe()?)
            .arg(case)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let (send, receive) = mpsc::channel();
        let stdout = child.stdout.take().ok_or("missing stdout")?;
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if send
                    .send(line.unwrap_or_else(|_| "READ_ERROR".into()))
                    .is_err()
                {
                    break;
                }
            }
        });
        if receive.recv_timeout(Duration::from_secs(1)).as_deref() != Ok("READY") {
            child.kill()?;
            child.wait()?;
            reader.join().map_err(|_| "reader panic")?;
            return Err("helper readiness failure".into());
        }
        let boundary = match Boundary::install(&child, cgroup_root, case == "boundary-denied") {
            Ok(boundary) => boundary,
            Err(error) => {
                child.kill()?;
                child.wait()?;
                reader.join().map_err(|_| "reader panic")?;
                if case == "boundary-denied" {
                    println!(
                        "PASS case=boundary-denied source_gate=closed killed_reaped=true error={error}"
                    );
                    continue;
                }
                return Err(format!(
                    "containment unavailable; source gate remained closed: {error}"
                )
                .into());
            }
        };
        let mut stdin = child.stdin.take().ok_or("missing stdin")?;
        stdin.write_all(b"GO\n")?;
        stdin.flush()?;
        drop(stdin);
        let mut reason = String::new();
        loop {
            if let Ok(line) = receive.try_recv() {
                reason = line;
                break;
            }
            if child.try_wait()?.is_some() {
                break;
            }
            if case == "os-cancel" && started.elapsed() >= Duration::from_millis(25) {
                reason = "CANCELLED".into();
                break;
            }
            // Isolate memory-controller evidence from CPU spent in OOM reclaim.
            // This fixture-only exception is not a production limit policy.
            let cpu = match boundary.cpu_us() {
                Ok(cpu) => cpu,
                Err(error) => {
                    child.kill()?;
                    child.wait()?;
                    reader.join().map_err(|_| "reader panic")?;
                    return Err(format!("containment accounting failed: {error}").into());
                }
            };
            if case != "os-memory" && cpu.is_some_and(|cpu| cpu >= 100_000) {
                reason = "CPU_KILLED".into();
                break;
            }
            if started.elapsed() >= Duration::from_millis(500) {
                reason = "WALL_KILLED".into();
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let cpu = boundary.cpu_us()?;
        let memory = boundary.memory_events();
        #[cfg(windows)]
        let windows_peak_within_limit = Some(
            boundary
                .0
                .stats()?
                .peak_memory_bytes
                .is_some_and(|peak| peak <= 64 * 1024 * 1024),
        );
        #[cfg(not(windows))]
        let windows_peak_within_limit: Option<bool> = None;
        let kill = reason.ends_with("KILLED") || reason == "CANCELLED";
        if kill {
            boundary.terminate()?;
            #[cfg(windows)]
            drop(boundary); // Last Job handle closes and terminates the child.
        }
        let status = child.wait()?;
        reader.join().map_err(|_| "reader panic")?;
        if reason.is_empty() {
            if let Ok(line) = receive.try_recv() {
                reason = line;
            } else if case == "os-memory" && !status.success() && memory.contains("oom_kill 1") {
                reason = "MEMORY_KILLED".into();
            } else {
                reason = format!("EXIT {status}");
            }
        }
        let passed = reason.starts_with(expected);
        #[cfg(windows)]
        let passed = passed && windows_peak_within_limit == Some(true);
        if !passed {
            failures += 1;
        }
        println!(
            "{} case={case} result={reason} status={status} elapsed_us={} cpu_us={cpu:?} windows_peak_within_limit={windows_peak_within_limit:?} memory_events={memory}",
            if passed { "PASS" } else { "FAIL" },
            started.elapsed().as_micros()
        );
        #[cfg(target_os = "linux")]
        drop(boundary);
    }
    println!("SUMMARY failures={failures} full_contract_proven=false");
    if failures > 0 {
        return Err("fixture failure".into());
    }
    Ok(())
}
