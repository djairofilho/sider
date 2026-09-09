//! Transport restricted to the private Sider backlog, using the gh CLI session.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

pub const REPOSITORY: &str = "djairofilho/sider";
const PREFIX: &str = "/repos/djairofilho/sider";
const PAGE_SIZE: usize = 100;
const MAX_PAGES: usize = 1_000;
const MAX_OUTPUT: usize = 8 * 1024 * 1024;
const MAX_PAGINATION: usize = 32 * 1024 * 1024;

pub fn repo_path(suffix: &str) -> String {
    if suffix.is_empty() {
        PREFIX.into()
    } else {
        format!("{PREFIX}/{}", suffix.trim_start_matches('/'))
    }
}

/// Does not provide publication, comment, deletion, or authentication endpoints.
pub trait GitHub {
    fn request(&mut self, method: &str, path: &str, body: Option<&Value>) -> Result<Value, String>;

    fn paginate(&mut self, path: &str) -> Result<Vec<Value>, String> {
        let (endpoint, mut query) = endpoint(path)?;
        if !matches!(endpoint, "/issues" | "/milestones" | "/labels") || query.contains_key("page")
        {
            return Err("Invalid initial pagination endpoint".into());
        }
        query.insert("per_page".into(), PAGE_SIZE.to_string());
        let mut rows = Vec::new();
        let mut seen_pages = BTreeSet::new();
        let mut total_bytes = 0_usize;
        for page in 1..=MAX_PAGES {
            query.insert("page".into(), page.to_string());
            let query = query
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("&");
            // Never follow URLs or Link headers supplied by the remote response.
            let result = self.request("GET", &format!("{PREFIX}{endpoint}?{query}"), None)?;
            let values = result
                .as_array()
                .ok_or("Paginated endpoint did not return a list")?;
            if values.len() > PAGE_SIZE {
                return Err("GitHub page exceeded per_page".into());
            }
            let fingerprint = serde_json::to_string(values).map_err(|_| "Invalid GitHub page")?;
            total_bytes = total_bytes
                .checked_add(fingerprint.len())
                .ok_or("Excessive pagination")?;
            if total_bytes > MAX_PAGINATION {
                return Err("GitHub pagination exceeded the byte limit".into());
            }
            if !values.is_empty() && !seen_pages.insert(fingerprint) {
                return Err("Repeated GitHub pagination".into());
            }
            rows.extend(values.iter().cloned());
            if values.len() < PAGE_SIZE {
                return Ok(rows);
            }
        }
        Err("Excessive GitHub pagination".into())
    }
}

fn positive(value: &str) -> bool {
    value.bytes().all(|byte| byte.is_ascii_digit())
        && value.parse::<u64>().is_ok_and(|number| number > 0)
}

fn endpoint(path: &str) -> Result<(&str, BTreeMap<String, String>), String> {
    let (path, query) = path.split_once('?').unwrap_or((path, ""));
    let suffix = path
        .strip_prefix(PREFIX)
        .ok_or("Destination outside the Sider repository")?;
    let components = suffix.split('/').collect::<Vec<_>>();
    let allowed = match components.as_slice() {
        [""] | ["", "issues" | "milestones" | "labels"] => true,
        ["", "issues" | "milestones", number] => positive(number),
        ["", "commits", sha] => {
            (7..=40).contains(&sha.len())
                && sha
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }
        ["", "actions", "runs", number] => positive(number),
        _ => false,
    };
    if !allowed {
        return Err("GitHub endpoint outside the allowed backlog".into());
    }
    let mut parameters = BTreeMap::new();
    if !query.is_empty() {
        if !matches!(suffix, "/issues" | "/milestones" | "/labels") {
            return Err("Query not allowed on this endpoint".into());
        }
        for parameter in query.split('&') {
            let (key, value) = parameter.split_once('=').ok_or("Invalid GitHub query")?;
            let valid = match key {
                "state" => value == "all" && suffix != "/labels",
                "per_page" => value == "100",
                "page" => {
                    positive(value) && value.parse::<usize>().is_ok_and(|page| page <= MAX_PAGES)
                }
                _ => false,
            };
            if !valid || parameters.insert(key.into(), value.into()).is_some() {
                return Err("Invalid or duplicate GitHub query".into());
            }
        }
    }
    Ok((suffix, parameters))
}

trait Transport {
    fn execute(&mut self, arguments: &[String], input: Option<Vec<u8>>) -> Result<Vec<u8>, String>;
}

pub struct GhClient<T = NativeTransport> {
    allow_writes: bool,
    transport: T,
}

impl GhClient {
    pub fn new(allow_writes: bool) -> Self {
        Self {
            allow_writes,
            transport: NativeTransport,
        }
    }
}

impl<T: Transport> GitHub for GhClient<T> {
    fn request(&mut self, method: &str, path: &str, body: Option<&Value>) -> Result<Value, String> {
        let (suffix, query) = endpoint(path)?;
        let allowed = match method {
            "GET" => body.is_none(),
            "POST" => matches!(suffix, "/labels" | "/milestones" | "/issues"),
            "PATCH" => suffix.starts_with("/issues/") || suffix.starts_with("/milestones/"),
            _ => false,
        };
        if !allowed
            || (method != "GET" && (!query.is_empty() || !body.is_some_and(Value::is_object)))
        {
            return Err("GitHub method or body outside the contract".into());
        }
        if method != "GET" && !self.allow_writes {
            return Err("GitHub writes require explicit --apply".into());
        }
        let mut arguments = [
            "api",
            "--hostname",
            "github.com",
            "--method",
            method,
            "--header",
            "Accept: application/vnd.github+json",
            "--header",
            "X-GitHub-Api-Version: 2022-11-28",
            path,
        ]
        .map(str::to_owned)
        .to_vec();
        let input = body
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|_| "GitHub body cannot be serialized")?;
        if input.as_ref().is_some_and(|bytes| bytes.len() > MAX_OUTPUT) {
            return Err("GitHub body exceeded the byte limit".into());
        }
        if input.is_some() {
            arguments.extend(["--input".into(), "-".into()]);
        }
        // One attempt only. The synchronizer rereads state on a new run.
        let output = self.transport.execute(&arguments, input)?;
        if output.len() > MAX_OUTPUT {
            return Err("GitHub response exceeded the byte limit".into());
        }
        serde_json::from_slice(&output)
            .map_err(|_| "GitHub response does not contain valid UTF-8 JSON".into())
    }
}

pub struct NativeTransport;
type Capture = Receiver<Result<Vec<u8>, String>>;

struct OwnedChild(Child);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_some() {
            return;
        }
        let _ = self.0.kill();
        let deadline = Instant::now() + Duration::from_secs(1);
        while matches!(self.0.try_wait(), Ok(None)) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn capture(mut stream: impl Read, exceeded: &AtomicBool) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let count = stream
            .read(&mut buffer)
            .map_err(|_| "Failed to capture GitHub CLI output")?;
        if count == 0 {
            return Ok(output);
        }
        let retain = count.min(MAX_OUTPUT.saturating_sub(output.len()));
        output.extend_from_slice(&buffer[..retain]);
        if retain < count {
            exceeded.store(true, Ordering::Relaxed);
        }
    }
}

fn capture_thread(
    stream: impl Read + Send + 'static,
    exceeded: Arc<AtomicBool>,
) -> Result<Capture, String> {
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("sider-gh-output".into())
        .spawn(move || {
            let _ = sender.send(capture(stream, &exceeded));
        })
        .map_err(|_| "Could not create GitHub CLI reader")?;
    Ok(receiver)
}

fn receive(capture: Capture, deadline: Instant) -> Result<Vec<u8>, String> {
    capture
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .map_err(|_| "GitHub CLI capture deadline exceeded")?
}

impl Transport for NativeTransport {
    fn execute(&mut self, arguments: &[String], input: Option<Vec<u8>>) -> Result<Vec<u8>, String> {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut command = Command::new("gh");
        command
            .args(arguments)
            .env("GH_PROMPT_DISABLED", "1")
            .env_remove("GH_DEBUG")
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut owned = OwnedChild(
            command
                .spawn()
                .map_err(|_| "Could not start gh; check installation and authentication")?,
        );
        let exceeded = Arc::new(AtomicBool::new(false));
        let stdout = capture_thread(
            owned.0.stdout.take().ok_or("missing GitHub stdout")?,
            exceeded.clone(),
        )?;
        let stderr = capture_thread(
            owned.0.stderr.take().ok_or("missing GitHub stderr")?,
            exceeded.clone(),
        )?;
        let writer = if let Some(input) = input {
            let mut stdin = owned.0.stdin.take().ok_or("missing GitHub stdin")?;
            let (sender, receiver) = mpsc::sync_channel(1);
            thread::Builder::new()
                .name("sider-gh-input".into())
                .spawn(move || {
                    let result = stdin
                        .write_all(&input)
                        .map_err(|_| "Failed to send JSON to the GitHub CLI".to_owned());
                    drop(stdin);
                    let _ = sender.send(result);
                })
                .map_err(|_| "Could not create GitHub CLI writer")?;
            Some(receiver)
        } else {
            None
        };
        let status = loop {
            if exceeded.load(Ordering::Relaxed) {
                return Err("GitHub CLI exceeded the output limit".into());
            }
            if Instant::now() >= deadline {
                return Err("GitHub CLI timed out; reread state before retrying".into());
            }
            if let Some(status) = owned
                .0
                .try_wait()
                .map_err(|_| "Failed to wait for GitHub CLI")?
            {
                break status;
            }
            thread::sleep(Duration::from_millis(10));
        };
        let output = receive(stdout, deadline)?;
        // Never include stderr in the error: it may contain credentials, URLs, or a private body.
        let _ = receive(stderr, deadline)?;
        if let Some(writer) = writer {
            writer
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .map_err(|_| "GitHub CLI send deadline exceeded")??;
        }
        if exceeded.load(Ordering::Relaxed) {
            return Err("GitHub CLI exceeded the output limit".into());
        }
        if !status.success() {
            return Err(format!(
                "GitHub CLI failed ({status}); check access and reread state before retrying"
            ));
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Default)]
    struct FakeTransport {
        calls: Vec<(Vec<String>, Option<Vec<u8>>)>,
        responses: Vec<Result<Vec<u8>, String>>,
    }
    impl Transport for FakeTransport {
        fn execute(
            &mut self,
            arguments: &[String],
            input: Option<Vec<u8>>,
        ) -> Result<Vec<u8>, String> {
            self.calls.push((arguments.to_vec(), input));
            if self.responses.is_empty() {
                Ok(b"{}".to_vec())
            } else {
                self.responses.remove(0)
            }
        }
    }
    fn client(apply: bool) -> GhClient<FakeTransport> {
        GhClient {
            allow_writes: apply,
            transport: FakeTransport::default(),
        }
    }

    #[test]
    fn writes_require_opt_in_and_paths_cannot_escape_repository() {
        let mut client = client(false);
        assert!(
            client
                .request(
                    "POST",
                    &repo_path("issues"),
                    Some(&json!({"title":"Title: café"}))
                )
                .is_err()
        );
        for path in [
            "https://evil.example",
            "//evil.example",
            "/repos/other/sider/issues",
            "/repos/djairofilho/sider-other/issues",
            "/repos/djairofilho/sider/../issues",
            "/repos/djairofilho/sider/issues%2f1",
            "/repos/djairofilho/sider/issues/1/comments",
            "/repos/djairofilho/sider/releases",
            "/repos/djairofilho/sider/issues?state=all&state=all",
        ] {
            assert!(client.request("GET", path, None).is_err(), "{path}");
        }
        assert!(
            client
                .request("GET", &repo_path(""), Some(&json!({})))
                .is_err()
        );
        assert!(client.transport.calls.is_empty());
    }

    #[test]
    fn json_utf8_uses_stdin_and_literal_arguments() {
        let mut client = client(true);
        let body = json!({"body":"Publication `sider`: café\nDo not execute $(command)."});
        client
            .request("POST", &repo_path("issues"), Some(&body))
            .unwrap();
        let (arguments, input) = &client.transport.calls[0];
        assert_eq!(&arguments[..3], ["api", "--hostname", "github.com"]);
        assert_eq!(&arguments[arguments.len() - 2..], ["--input", "-"]);
        assert_eq!(
            serde_json::from_slice::<Value>(input.as_ref().unwrap()).unwrap(),
            body
        );
        assert!(
            !arguments
                .iter()
                .any(|value| value.contains("Publication") || value.contains("token"))
        );
    }

    #[test]
    fn write_failure_is_not_retried_and_bad_json_is_rejected() {
        let mut client = client(true);
        client.transport.responses = vec![Err("Response lost".into())];
        assert!(
            client
                .request("POST", &repo_path("issues"), Some(&json!({})))
                .is_err()
        );
        assert_eq!(client.transport.calls.len(), 1);
        for response in [vec![0xff], b"not json".to_vec()] {
            client.transport.responses = vec![Ok(response)];
            assert!(client.request("GET", &repo_path(""), None).is_err());
        }
    }

    #[test]
    fn pagination_is_bounded_and_never_follows_remote_urls() {
        let mut client = client(false);
        let first = (0..100)
            .map(|number| json!({"number":number,"url":"https://evil.example"}))
            .collect::<Vec<_>>();
        client.transport.responses =
            vec![Ok(serde_json::to_vec(&first).unwrap()), Ok(b"[]".to_vec())];
        assert_eq!(
            client
                .paginate(&repo_path("issues?state=all"))
                .unwrap()
                .len(),
            100
        );
        assert!(
            client.transport.calls[1].0.iter().any(|argument| argument
                == "/repos/djairofilho/sider/issues?page=2&per_page=100&state=all")
        );
        let page = serde_json::to_vec(&first).unwrap();
        client.transport.responses = vec![Ok(page.clone()), Ok(page)];
        assert!(
            client
                .paginate(&repo_path("issues"))
                .unwrap_err()
                .contains("Repeated")
        );
        client.transport.responses = vec![Ok(b"{}".to_vec())];
        assert!(client.paginate(&repo_path("issues")).is_err());
        assert!(client.paginate(&repo_path("issues?page=1")).is_err());
    }

    #[test]
    fn capture_limits_bytes_and_capture_deadline_is_explicit() {
        let exceeded = AtomicBool::new(false);
        assert_eq!(
            capture(vec![0; MAX_OUTPUT + 1].as_slice(), &exceeded)
                .unwrap()
                .len(),
            MAX_OUTPUT
        );
        assert!(exceeded.load(Ordering::Relaxed));
        let (_sender, receiver) = mpsc::sync_channel(1);
        assert!(receive(receiver, Instant::now()).is_err());
    }
}
