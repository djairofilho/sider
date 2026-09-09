//! Manifesto de releases e projeção Markdown, sem acesso ao GitHub ou publicação.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::Path;

use serde_json::{Value, json};

const GATES: [&str; 15] = [
    "native",
    "compatibility",
    "tcp_smoke",
    "crash",
    "recovery",
    "migration",
    "sharding",
    "types",
    "sorted_sets",
    "transactions",
    "pubsub",
    "replication",
    "docker",
    "soak",
    "benchmarks",
];
type Version = (u64, u64, u64);
const THRESHOLDS: &[(Version, &[&str])] = &[
    ((0, 3, 0), &["crash", "recovery", "migration"]),
    ((0, 4, 0), &["sharding"]),
    ((0, 5, 0), &["types"]),
    ((0, 6, 0), &["sorted_sets"]),
    ((0, 7, 0), &["transactions"]),
    ((0, 8, 0), &["pubsub"]),
    ((0, 9, 0), &["replication"]),
    ((0, 10, 0), &["docker"]),
    ((1, 0, 0), &["soak", "benchmarks"]),
];

fn text<'a>(record: &'a Value, key: &str, context: &str) -> Result<&'a str, String> {
    record
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| format!("{context}.{key}: texto não vazio obrigatório"))
}

fn array<'a>(value: &'a Value, name: &str, empty: bool) -> Result<&'a [Value], String> {
    value
        .as_array()
        .filter(|values| empty || !values.is_empty())
        .map(Vec::as_slice)
        .ok_or_else(|| {
            format!(
                "{name}: lista {}obrigatória",
                if empty { "" } else { "não vazia " }
            )
        })
}

fn strings<'a>(value: &'a Value, name: &str, empty: bool) -> Result<Vec<&'a str>, String> {
    let mut unique = BTreeSet::new();
    array(value, name, empty)?
        .iter()
        .map(|value| {
            let value = value
                .as_str()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| format!("{name}: valores devem ser textos não vazios"))?;
            if !unique.insert(value) {
                return Err(format!("{name}: valores duplicados"));
            }
            Ok(value)
        })
        .collect()
}

fn object<'a>(value: &'a Value, name: &str) -> Result<&'a serde_json::Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("{name}: objeto obrigatório"))
}

fn number(value: &str) -> Option<u64> {
    if value.is_empty()
        || !value.bytes().all(|b| b.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return None;
    }
    value.parse().ok()
}

fn base_version(value: &str) -> Option<Version> {
    let mut parts = value.split('.');
    let version = (
        number(parts.next()?)?,
        number(parts.next()?)?,
        number(parts.next()?)?,
    );
    parts.next().is_none().then_some(version)
}

fn release_base(value: &str) -> Result<&str, String> {
    let without_tag = value.strip_prefix('v').unwrap_or(value);
    let base = if let Some((base, rc)) = without_tag.split_once("-rc.") {
        if number(rc).is_none_or(|number| number == 0) {
            return Err(format!("Versão de release inválida: {value}"));
        }
        base
    } else {
        without_tag
    };
    if base_version(base).is_none() {
        return Err(format!("Versão de release inválida: {value}"));
    }
    Ok(base)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Bootstrap,
    Release,
    Task,
    Gate,
}

struct Node {
    id: String,
    dependencies: Vec<String>,
    kind: Kind,
}

#[derive(Default)]
struct Graph {
    nodes: Vec<Node>,
    positions: BTreeMap<String, usize>,
}

impl Graph {
    fn register(
        &mut self,
        record: &Value,
        kind: Kind,
        prefix: &str,
        dependencies: Vec<String>,
    ) -> Result<String, String> {
        object(record, "registro")?;
        let id = text(record, "id", "registro")?;
        let bytes = id.as_bytes();
        let two_digits = |value: &[u8]| value.len() == 2 && value.iter().all(u8::is_ascii_digit);
        let valid = match kind {
            Kind::Bootstrap => {
                bytes.len() == 6
                    && bytes[0] == b'B'
                    && bytes[3] == b'-'
                    && two_digits(&bytes[1..3])
                    && two_digits(&bytes[4..])
            }
            Kind::Release => bytes.len() == 3 && bytes[0] == b'R' && two_digits(&bytes[1..]),
            Kind::Task => id
                .strip_prefix(&format!("{prefix}-"))
                .is_some_and(|s| two_digits(s.as_bytes())),
            Kind::Gate => id == format!("{prefix}-GATE"),
        };
        if !valid {
            return Err(format!("ID inválido: {id}"));
        }
        if self.positions.contains_key(id) {
            return Err(format!("ID duplicado: {id}"));
        }
        text(record, "title", id)?;
        self.positions.insert(id.to_owned(), self.nodes.len());
        self.nodes.push(Node {
            id: id.to_owned(),
            dependencies,
            kind,
        });
        Ok(id.to_owned())
    }

    fn validate(&self) -> Result<(), String> {
        let mut remaining = vec![0; self.nodes.len()];
        let mut consumers = vec![Vec::new(); self.nodes.len()];
        for (position, node) in self.nodes.iter().enumerate() {
            for dependency in &node.dependencies {
                let Some(&index) = self.positions.get(dependency) else {
                    return Err(format!("{}: dependência inexistente {dependency}", node.id));
                };
                if node.kind == Kind::Task && self.nodes[index].kind == Kind::Release {
                    return Err(format!(
                        "{}: tarefa deve depender de tarefa, bootstrap ou gate",
                        node.id
                    ));
                }
                remaining[position] += 1;
                consumers[index].push(position);
            }
        }
        // A remoção topológica evita recursão proporcional ao tamanho do manifesto.
        let mut ready: VecDeque<_> = remaining
            .iter()
            .enumerate()
            .filter_map(|(index, &count)| (count == 0).then_some(index))
            .collect();
        let mut visited = 0;
        while let Some(index) = ready.pop_front() {
            visited += 1;
            for &consumer in &consumers[index] {
                remaining[consumer] -= 1;
                if remaining[consumer] == 0 {
                    ready.push_back(consumer);
                }
            }
        }
        if visited != self.nodes.len() {
            return Err("Ciclo de dependências no manifesto".into());
        }
        Ok(())
    }
}

/// Lê UTF-8 e rejeita um manifesto inválido antes de qualquer ação externa.
pub fn load(path: &Path) -> Result<Value, String> {
    let contents =
        fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let plan = serde_json::from_str(&contents)
        .map_err(|error| format!("{}: JSON inválido: {error}", path.display()))?;
    validate(&plan)?;
    Ok(plan)
}

/// Valida o DAG, gates locais dos marcos e cobertura cumulativa da publicação.
pub fn validate(plan: &Value) -> Result<(), String> {
    if plan.get("schema_version").and_then(Value::as_u64) != Some(2) {
        return Err("schema_version deve ser 2".into());
    }
    let repository = text(plan, "repository", "plan")?;
    let parts: Vec<_> = repository.split('/').collect();
    if parts.len() != 2
        || parts.iter().any(|part| {
            part.is_empty()
                || !part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
        })
    {
        return Err("repository deve ter formato owner/repo".into());
    }
    let reference = &plan["reference"];
    object(reference, "reference")?;
    let redis = text(reference, "redis_version", "reference")?;
    let cli = text(reference, "redis_cli_version", "reference")?;
    let image = text(reference, "image", "reference")?;
    let platform = text(reference, "platform", "reference")?;
    if redis != cli {
        return Err("Redis e redis-cli devem usar a mesma versão".into());
    }
    if base_version(redis).is_none() {
        return Err("reference.redis_version inválida".into());
    }
    if platform != "linux/amd64" {
        return Err("reference.platform deve ser linux/amd64".into());
    }
    let digest = image.strip_prefix(&format!("redis:{redis}@sha256:"));
    if !digest.is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }) {
        return Err("reference.image deve fixar tag Redis e digest sha256 consistentes".into());
    }
    let contracts = &plan["contracts"];
    object(contracts, "contracts")?;
    for field in ["decisions", "after_1_0", "sources"] {
        strings(&contracts[field], &format!("contracts.{field}"), false)?;
    }
    let commands = object(&contracts["commands_added"], "contracts.commands_added")?;
    for (version, forms) in commands {
        if base_version(version).is_none() {
            return Err("contracts.commands_added tem versão inválida".into());
        }
        strings(forms, &format!("commands_added.{version}"), false)?;
    }
    let policy = &plan["release_policy"];
    object(policy, "release_policy")?;
    let expected = json!({
        "private": true, "publish_crate": false, "candidate_required": true,
        "ci_enabled": false, "automatic_publication": false,
        "automation_resume_after": "1.0.0", "merge_strategy": "merge",
        "release_branch_prefix": "chore/release-v", "release_label": "type:release",
        "linux_runner": "ubuntu-24.04", "docker_since": "0.10.0",
        "patch_requires_manifest_entry": true, "bundle_change_requires_new_candidate": true,
        "final_promotion": "same_sha_same_assets",
        "targets": ["x86_64-unknown-linux-gnu", "x86_64-pc-windows-msvc"]
    });
    for (field, expected) in object(&expected, "política interna")? {
        if policy.get(field) != Some(expected) {
            return Err(format!(
                "release_policy.{field} diverge do contrato suportado"
            ));
        }
    }
    if policy["stable_soak_seconds"]
        .as_u64()
        .is_none_or(|value| value < 3600)
    {
        return Err("release_policy.stable_soak_seconds deve ser inteiro >= 3600".into());
    }
    let releases = array(&plan["releases"], "releases", false)?;
    let bootstrap = array(&plan["bootstrap"], "bootstrap", true)?;
    let mut graph = Graph::default();
    for item in bootstrap {
        let id = graph.register(item, Kind::Bootstrap, "", vec![])?;
        if item["status"] != "completed" {
            return Err(format!(
                "{id}: bootstrap deve conter somente entregas comprovadas"
            ));
        }
        strings(&item["evidence"], &format!("{id}.evidence"), false)?;
    }
    let mut versions = BTreeSet::new();
    let mut previous_version = None;
    let mut previous_gates = BTreeSet::new();
    for release in releases {
        object(release, "release")?;
        if release.get("depends_on").is_some() {
            return Err(
                "release.depends_on não existe no schema 2; declare dependências nas tarefas"
                    .into(),
            );
        }
        let id = graph.register(release, Kind::Release, "", Vec::new())?;
        let version = text(release, "version", &id)?;
        let numbers =
            base_version(version).ok_or_else(|| format!("Versão-base inválida: {version}"))?;
        if release["publication"].as_bool() != Some(numbers >= (1, 0, 0)) {
            return Err(format!(
                "{id}: publication deve ser false antes da 1.0 e true a partir dela"
            ));
        }
        if !versions.insert(version) {
            return Err(format!("Versão duplicada: {version}"));
        }
        if previous_version.is_some_and(|previous| numbers <= previous) {
            return Err("As releases devem estar em ordem semântica crescente".into());
        }
        previous_version = Some(numbers);
        strings(&release["scope"], &format!("{id}.scope"), false)?;
        let required: BTreeSet<_> = strings(
            &release["required_gates"],
            &format!("{id}.required_gates"),
            false,
        )?
        .into_iter()
        .collect();
        if required.iter().any(|gate| !GATES.contains(gate)) {
            return Err(format!("{id}: gate de evidência desconhecido"));
        }
        let publication = release["publication"] == true;
        let mut expected: BTreeSet<_> = GATES[..3].iter().copied().collect();
        for (threshold, gates) in THRESHOLDS {
            if (publication && numbers >= *threshold) || numbers == *threshold {
                expected.extend(gates.iter().copied());
            }
        }
        if publication {
            expected.extend(previous_gates.iter().copied());
            if !expected.is_subset(&required) {
                return Err(format!(
                    "{id}: publicação exige a união cumulativa dos gates de todas as capacidades"
                ));
            }
        } else if required != expected {
            return Err(format!(
                "{id}: gates internos devem corresponder à capacidade local"
            ));
        }
        previous_gates.extend(required);
        for task in array(&release["tasks"], &format!("{id}.tasks"), false)? {
            object(task, &format!("{id}: tarefa"))?;
            let dependencies = strings(&task["depends_on"], "task.depends_on", true)?;
            let task_id = graph.register(
                task,
                Kind::Task,
                &id,
                dependencies.into_iter().map(str::to_owned).collect(),
            )?;
            for field in ["area", "objective"] {
                text(task, field, &task_id)?;
            }
            for field in ["deliverables", "tests", "acceptance"] {
                strings(&task[field], &format!("{task_id}.{field}"), false)?;
            }
        }
        let gate_id = graph.register(
            &release["gate"],
            Kind::Gate,
            &id,
            gate_dependencies(plan, release)?,
        )?;
        strings(
            &release["gate"]["acceptance"],
            &format!("{gate_id}.acceptance"),
            false,
        )?;
    }
    if commands
        .keys()
        .any(|version| !versions.contains(version.as_str()))
    {
        return Err("commands_added referencia versão sem release".into());
    }
    graph.validate()
}

/// Gates internos dependem das tarefas locais; publicação agrega todos os marcos internos.
pub fn gate_dependencies(plan: &Value, release: &Value) -> Result<Vec<String>, String> {
    let mut dependencies = array(&release["tasks"], "tasks", false)?
        .iter()
        .map(|task| text(task, "id", "task").map(str::to_owned))
        .collect::<Result<Vec<_>, _>>()?;
    if release["publication"] == true {
        for milestone in array(&plan["releases"], "releases", false)? {
            if milestone["publication"] == false {
                dependencies.push(text(&milestone["gate"], "id", "gate")?.to_owned());
            }
        }
    }
    Ok(dependencies)
}

/// Resolve uma final ou RC publicável. Marcos internos não são versões publicáveis.
pub fn release_for_version<'a>(plan: &'a Value, version: &str) -> Result<&'a Value, String> {
    let base = release_base(version)?;
    validate(plan)?;
    let release = array(&plan["releases"], "releases", false)?
        .iter()
        .find(|release| release["version"] == base)
        .ok_or_else(|| format!("Versão {base} sem milestone no manifesto"))?;
    if release["publication"] != true {
        return Err(format!(
            "Marco interno {base} não permite publicação de RC ou final"
        ));
    }
    Ok(release)
}

fn append(lines: &mut Vec<String>, values: &[&str]) {
    lines.extend(values.iter().map(|value| (*value).to_owned()));
}

/// Gera Markdown determinístico. O estado externo das issues não altera o arquivo.
pub fn render(plan: &Value) -> Result<String, String> {
    validate(plan)?;
    let releases = array(&plan["releases"], "releases", false)?;
    let bootstrap = array(&plan["bootstrap"], "bootstrap", true)?;
    let count = releases
        .iter()
        .try_fold(bootstrap.len(), |count, release| {
            Ok::<_, String>(count + array(&release["tasks"], "tasks", false)?.len() + 1)
        })?;
    let mut lines = Vec::new();
    append(
        &mut lines,
        &[
            "# Roadmap de marcos internos e publicação do Sider",
            "",
            "<!-- Gerado por cargo xtask roadmap --write; editar releases/plan.json. -->",
            "",
            "Este roteiro organiza as entregas até a 1.0. R01 a R10 são marcos internos;",
            "R11 reúne a estabilização e a publicação da 1.0, sem retirar funcionalidades do escopo.",
            "O estado operacional das tarefas está nas issues do GitHub, sem duplicar o estado neste arquivo.",
            "",
        ],
    );
    lines.push(format!(
        "São {} milestones e {count} issues: um bootstrap, tarefas funcionais e um gate por marco.",
        releases.len()
    ));
    append(
        &mut lines,
        &[
            "O repositório e os artefatos permanecem privados; a crate usa `publish = false`.",
            "",
            "CI e publicação automática estão desativadas até e incluindo a 1.0.",
            "Uma retomada posterior exige implementação e alteração explícitas da política.",
            "Até lá, execute e registre manualmente as verificações e a publicação; os critérios de qualidade permanecem.",
            "",
            "## Índice",
            "",
            "- [Marcos e publicação](#marcos-e-publicação)",
            "- [Bootstrap comprovado](#bootstrap-comprovado)",
            "- [Contratos transversais](#contratos-transversais)",
            "- [Tarefas por marco](#tarefas-por-marco)",
            "- [Execução e publicação](#execução-e-publicação)",
            "",
            "## Marcos e publicação",
            "",
            "| Milestone | Entrega | Tarefas | Encerramento |",
            "| --- | --- | --- | --- |",
        ],
    );
    for release in releases {
        lines.push(format!(
            "| `{}` | {} | {} + gate | {} |",
            text(release, "version", "release")?,
            text(release, "title", "release")?,
            array(&release["tasks"], "tasks", false)?.len(),
            if release["publication"] == true {
                "Candidata e final com o mesmo SHA e os mesmos assets"
            } else {
                "Validação interna, sem publicação"
            }
        ));
    }
    append(&mut lines, &["", "## Bootstrap comprovado", ""]);
    for item in bootstrap {
        lines.push(format!(
            "- [x] `{}`: {}.",
            text(item, "id", "bootstrap")?,
            text(item, "title", "bootstrap")?
        ));
        for (index, url) in strings(&item["evidence"], "evidence", false)?
            .iter()
            .enumerate()
        {
            lines.push(format!("  [Evidência {}]({url})", index + 1));
        }
    }
    append(&mut lines, &["", "## Contratos transversais", ""]);
    for decision in strings(&plan["contracts"]["decisions"], "decisions", false)? {
        lines.push(format!("- {decision}"));
    }
    lines.push(String::new());
    lines.push(format!(
        "Fora do escopo até a 1.0: {}.",
        strings(&plan["contracts"]["after_1_0"], "after_1_0", false)?.join("; ")
    ));
    lines.push(String::new());
    lines.push(format!(
        "Referência fixada: Redis e `redis-cli` {}, plataforma `{}`.",
        text(&plan["reference"], "redis_version", "reference")?,
        text(&plan["reference"], "platform", "reference")?
    ));
    append(
        &mut lines,
        &[
            "",
            "```text",
            text(&plan["reference"], "image", "reference")?,
            "```",
            "",
            "A imagem fixada é uma entrada da suíte; só uma execução registrada constitui evidência de compatibilidade.",
            "",
            "## Tarefas por marco",
            "",
            "Objetivos, entregáveis, testes e critérios completos de cada issue estão no",
            "[manifesto versionado](releases/plan.json). Somente as dependências técnicas das tarefas",
            "limitam o paralelismo; a ordem dos marcos neste documento não cria dependências.",
            "",
        ],
    );
    for release in releases {
        lines.push(format!(
            "### {}: {}",
            text(release, "version", "release")?,
            text(release, "title", "release")?
        ));
        lines.push(String::new());
        for scope in strings(&release["scope"], "scope", false)? {
            lines.push(format!("- {scope}"));
        }
        append(
            &mut lines,
            &["", "| ID | Entrega | Dependências |", "| --- | --- | --- |"],
        );
        for task in array(&release["tasks"], "tasks", false)? {
            let dependencies = strings(&task["depends_on"], "depends_on", true)?.join(", ");
            lines.push(format!(
                "| `{}` | {} | {} |",
                text(task, "id", "task")?,
                text(task, "title", "task")?,
                if dependencies.is_empty() {
                    "Nenhuma"
                } else {
                    &dependencies
                }
            ));
        }
        let gate = &release["gate"];
        lines.push(format!(
            "| `{}` | {} | {} |",
            text(gate, "id", "gate")?,
            text(gate, "title", "gate")?,
            gate_dependencies(plan, release)?.join(", ")
        ));
        lines.push(String::new());
        lines.push(format!(
            "Evidências obrigatórias: {}.",
            strings(&release["required_gates"], "required_gates", false)?
                .iter()
                .map(|gate| format!("`{gate}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
        append(&mut lines, &["", "Critérios para encerrar o marco:", ""]);
        for acceptance in strings(&gate["acceptance"], "acceptance", false)? {
            lines.push(format!("- {acceptance}"));
        }
        lines.push(String::new());
    }
    append(
        &mut lines,
        &[
            "## Execução e publicação",
            "",
            "1. Selecione tarefas desbloqueadas pelo DAG técnico; trilhas independentes podem avançar em paralelo.",
            "2. Inclua testes e evidências; mantenha código compilável e commits atômicos em cada etapa.",
            "3. Integre o PR vinculado à issue por merge commit após verificação manual registrada.",
            "4. Encerre R01 a R10 após conferir os critérios locais; esses marcos não criam candidata, tag ou release.",
            "5. Depois de validar todos os marcos, prepare a candidata 1.0 em um SHA e bundle imutáveis; o merge não dispara publicação.",
            "6. A final promove exatamente o mesmo SHA e os mesmos assets aprovados na candidata, sem recompilar. Qualquer mudança no bundle exige outra candidata.",
            "7. Encerre R11 somente após conferir a publicação final e seus artefatos.",
            "",
            "O fluxo completo e os comandos de preparação estão no [guia de releases](docs/releases.md).",
            "Não há workflows de CI nem publicador automático neste repositório.",
            "A candidata 1.0 exige uma hora de carga contínua, além de todos os gates das capacidades entregues.",
            "Teste obrigatório ausente, ignorado, cancelado ou sem relatório bloqueia o encerramento do marco e a publicação.",
            "Migração usa fixtures e executáveis das baselines internas congeladas por SHA e hashes; a 1.0 migra a baseline R10.",
            "",
            "Os pacotes são Linux GNU x86_64 (`.tar.gz`, Ubuntu 24.04) e Windows MSVC x86_64 (`.zip`).",
            "R10 valida a imagem Docker Linux amd64 exportada que acompanha a publicação privada da 1.0.",
            "Checksums SHA-256, manifesto de build e notas acompanham os binários testados depois da extração.",
            "",
            "Patches publicáveis, como `1.0.1`, precisam de registro próprio no manifesto e de candidata.",
            "Mudanças de compatibilidade dos marcos internos permanecem documentadas. Não há datas artificiais.",
            "",
            "Na retomada manual, confira drafts e uploads existentes. Tag com SHA divergente ou artefato publicado diferente",
            "interrompe o fluxo. Uma release publicada não é sobrescrita.",
            "",
            "Para validar a fonte e a projeção sem alterar arquivos:",
            "",
            "```sh",
            "cargo xtask validate",
            "```",
            "",
            "Para regenerar a projeção a partir do manifesto:",
            "",
            "```sh",
            "cargo xtask roadmap --write",
            "```",
            "",
        ],
    );
    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn real() -> Value {
        serde_json::from_str(include_str!("../../releases/plan.json")).unwrap()
    }

    fn invalid(change: impl FnOnce(&mut Value), expected: &str) {
        let mut plan = real();
        change(&mut plan);
        let error = validate(&plan).expect_err("manifesto alterado deve ser rejeitado");
        assert!(
            error.contains(expected),
            "esperado {expected:?}, recebido {error:?}"
        );
    }

    #[test]
    fn real_manifest_preserves_all_deliveries_and_quality_policy() {
        let plan = real();
        validate(&plan).unwrap();
        let releases = plan["releases"].as_array().unwrap();
        assert_eq!(releases.len(), 11);
        assert_eq!(
            releases
                .iter()
                .map(|r| r["tasks"].as_array().unwrap().len())
                .sum::<usize>(),
            50
        );
        assert_eq!(plan["bootstrap"].as_array().unwrap().len(), 1);
        for release in releases {
            for task in release["tasks"].as_array().unwrap() {
                assert_ne!(task["status"], "completed");
            }
        }
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../releases/plan.json");
        assert_eq!(load(&root).unwrap(), plan);
    }

    #[test]
    fn rendering_is_deterministic_preserves_history_and_has_no_old_tooling() {
        let plan = real();
        let before = plan.clone();
        let rendered = render(&plan).unwrap();
        assert_eq!(rendered, render(&plan).unwrap());
        assert_eq!(plan, before);
        assert_eq!(rendered.matches("[x]").count(), 1);
        assert_eq!(rendered.matches("### ").count(), 11);
        for text in [
            "cargo xtask validate",
            "cargo xtask roadmap --write",
            "CI multiplataforma",
            "Replicação",
            "desativadas até e incluindo a 1.0",
            "o merge não dispara publicação",
            "verificação manual registrada",
            "uma hora de carga contínua",
            "mesmo SHA e os mesmos assets",
            "sem recompilar",
            "34177280948",
        ] {
            assert!(rendered.contains(text), "trecho ausente: {text}");
        }
        for text in [
            "python",
            "tools.release",
            "workflows-disabled",
            "\u{00c3}",
            "\u{00c2}",
            "\u{fffd}",
            "\\`",
            "\u{7}",
            "\r",
            "—",
        ] {
            assert!(!rendered.contains(text), "trecho indevido: {text}");
        }
        assert!(rendered.ends_with('\n'));
        for release in plan["releases"].as_array().unwrap() {
            for task in release["tasks"].as_array().unwrap() {
                assert!(rendered.contains(task["id"].as_str().unwrap()));
            }
            for acceptance in release["gate"]["acceptance"].as_array().unwrap() {
                assert!(rendered.contains(acceptance.as_str().unwrap()));
            }
        }
    }

    #[test]
    fn rc_and_final_resolve_the_exact_same_manifest_entry() {
        let plan = real();
        for version in ["1.0.0", "v1.0.0", "1.0.0-rc.1", "v1.0.0-rc.12"] {
            assert!(std::ptr::eq(
                release_for_version(&plan, version).unwrap(),
                &plan["releases"][10]
            ));
        }
        for version in ["0.3.1", "0.3.1-rc.1", "2.0.0"] {
            assert!(
                release_for_version(&plan, version)
                    .unwrap_err()
                    .contains("sem milestone")
            );
        }
    }

    #[test]
    fn internal_milestones_cannot_be_published_as_candidates_or_finals() {
        let plan = real();
        for release in plan["releases"].as_array().unwrap().iter().take(10) {
            let version = release["version"].as_str().unwrap();
            for requested in [version.to_owned(), format!("v{version}-rc.1")] {
                assert!(
                    release_for_version(&plan, &requested)
                        .unwrap_err()
                        .contains("Marco interno")
                );
            }
        }
        for (index, value) in [(0, json!(true)), (10, json!(false)), (0, json!("false"))] {
            invalid(
                |p| p["releases"][index]["publication"] = value,
                "publication",
            );
        }
    }

    #[test]
    fn internal_gates_only_require_local_tasks_and_publication_aggregates_them() {
        let plan = real();
        for release in plan["releases"].as_array().unwrap().iter().take(10) {
            let expected: Vec<_> = release["tasks"]
                .as_array()
                .unwrap()
                .iter()
                .map(|task| task["id"].as_str().unwrap().to_owned())
                .collect();
            assert_eq!(gate_dependencies(&plan, release).unwrap(), expected);
        }
        let final_dependencies = gate_dependencies(&plan, &plan["releases"][10]).unwrap();
        assert_eq!(final_dependencies.len(), 15);
        for index in 1..=10 {
            assert!(final_dependencies.contains(&format!("R{index:02}-GATE")));
        }
    }

    #[test]
    fn explicit_patch_entry_uses_new_ids_and_inherits_required_gates() {
        let mut plan = real();
        let mut patch = plan["releases"][10].clone();
        patch["id"] = json!("R12");
        patch["version"] = json!("1.0.1");
        patch["gate"]["id"] = json!("R12-GATE");
        let mut task = patch["tasks"][0].clone();
        task["id"] = json!("R12-01");
        task["depends_on"] = json!(["R11-GATE"]);
        patch["tasks"] = json!([task]);
        plan["releases"].as_array_mut().unwrap().push(patch);
        validate(&plan).unwrap();
        assert!(std::ptr::eq(
            release_for_version(&plan, "1.0.1-rc.2").unwrap(),
            &plan["releases"][11]
        ));
    }

    #[test]
    fn version_parser_rejects_ambiguous_unsupported_and_overflowing_forms() {
        let plan = real();
        for version in [
            "",
            "0.1",
            "01.1.0",
            "0.01.0",
            "+0.1.0",
            "0.1.0-rc.0",
            "0.1.0-rc.01",
            "0.1.0-beta.1",
            "0.1.0+meta",
            " 0.1.0",
            "0.1.0\n",
            "vv0.1.0",
            "0.1.0-rc.1-rc.2",
            "0.1.0-rc.18446744073709551616",
            "18446744073709551616.0.0",
            "0.1.０",
        ] {
            assert!(release_for_version(&plan, version).is_err(), "{version:?}");
        }
    }

    #[test]
    fn duplicate_ids_wrong_prefixes_and_nonobjects_are_rejected() {
        invalid(
            |p| p["releases"][0]["tasks"][1]["id"] = json!("R01-01"),
            "ID duplicado",
        );
        invalid(
            |p| p["releases"][0]["gate"]["id"] = json!("R01-01"),
            "ID inválido",
        );
        for id in ["R1", "r01", "R001", "R０1", "R01\n"] {
            invalid(|p| p["releases"][0]["id"] = json!(id), "ID inválido");
        }
        invalid(
            |p| p["releases"][0]["tasks"][0]["id"] = json!("R02-01"),
            "ID inválido",
        );
        invalid(|p| p["bootstrap"][0]["id"] = json!("B0-01"), "ID inválido");
        for value in [Value::Null, json!(5), json!([]), json!("task")] {
            invalid(|p| p["releases"][0]["tasks"][0] = value, "objeto");
        }
    }

    #[test]
    fn dependency_existence_types_and_cycles_are_checked() {
        invalid(
            |p| p["releases"][0]["tasks"][0]["depends_on"] = json!(["R99-01"]),
            "inexistente",
        );
        invalid(
            |p| p["releases"][1]["tasks"][0]["depends_on"] = json!(["R01"]),
            "tarefa deve depender",
        );
        invalid(
            |p| p["releases"][0]["depends_on"] = json!(["B00-01"]),
            "release.depends_on não existe",
        );
        invalid(
            |p| p["releases"][0]["tasks"][0]["depends_on"] = json!(["R01-02"]),
            "Ciclo",
        );
        invalid(
            |p| p["releases"][0]["tasks"][0]["depends_on"] = json!(["R01-GATE"]),
            "Ciclo",
        );
    }

    #[test]
    fn acyclic_dependencies_can_reference_tasks_later_in_the_json() {
        let mut plan = real();
        plan["releases"][0]["tasks"][0]["depends_on"] = json!(["R01-02"]);
        plan["releases"][0]["tasks"][1]["depends_on"] = json!([]);
        plan["releases"][7]["tasks"][0]["depends_on"] = json!(["R10-01"]);
        validate(&plan).unwrap();
    }

    #[test]
    fn dependencies_must_be_explicit_unique_string_lists() {
        for dependencies in [
            Value::Null,
            json!("R01-GATE"),
            json!(["R01-GATE", "R01-GATE"]),
            json!([5]),
            json!([" "]),
        ] {
            invalid(
                |p| p["releases"][1]["tasks"][0]["depends_on"] = dependencies,
                "task.depends_on",
            );
        }
        invalid(
            |p| {
                p["releases"][0]["tasks"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("depends_on");
            },
            "task.depends_on",
        );
    }

    #[test]
    fn release_versions_are_unique_base_versions_in_numeric_order() {
        invalid(
            |p| p["releases"][1]["version"] = json!("0.1.0"),
            "Versão duplicada",
        );
        for version in ["0.1.0-rc.1", "v0.1.0", "00.1.0"] {
            invalid(
                |p| p["releases"][0]["version"] = json!(version),
                "Versão-base inválida",
            );
        }
        invalid(
            |p| p["releases"].as_array_mut().unwrap().swap(1, 2),
            "ordem semântica",
        );
        assert!(base_version("0.10.0") > base_version("0.9.0"));
    }

    #[test]
    fn missing_unknown_and_unrelated_internal_gates_are_rejected() {
        for (index, gate) in [
            (0, "native"),
            (0, "compatibility"),
            (0, "tcp_smoke"),
            (2, "migration"),
            (3, "sharding"),
            (4, "types"),
            (5, "sorted_sets"),
            (6, "transactions"),
            (7, "pubsub"),
            (8, "replication"),
            (9, "docker"),
            (10, "soak"),
            (10, "benchmarks"),
        ] {
            invalid(
                |p| {
                    p["releases"][index]["required_gates"]
                        .as_array_mut()
                        .unwrap()
                        .retain(|value| value != gate)
                },
                "gate",
            );
        }
        invalid(
            |p| {
                p["releases"][0]["required_gates"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!("pretend_pass"))
            },
            "desconhecido",
        );
        invalid(
            |p| {
                p["releases"][0]["required_gates"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!("types"))
            },
            "capacidade local",
        );
        invalid(
            |p| {
                p["releases"][0]["required_gates"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!("native"))
            },
            "duplicados",
        );
    }

    #[test]
    fn task_specs_require_all_nonempty_sections() {
        for field in [
            "title",
            "area",
            "objective",
            "deliverables",
            "tests",
            "acceptance",
        ] {
            invalid(
                |p| {
                    p["releases"][0]["tasks"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove(field);
                },
                field,
            );
        }
        invalid(|p| p["releases"][0]["tasks"] = json!([]), "tasks");
        invalid(|p| p["releases"][0]["scope"] = json!([]), "scope");
        invalid(
            |p| p["releases"][0]["gate"]["acceptance"] = json!([]),
            "acceptance",
        );
        invalid(
            |p| p["releases"][0]["tasks"][0]["tests"] = json!(["same", "same"]),
            "duplicados",
        );
    }

    #[test]
    fn bootstrap_requires_completion_and_evidence_without_claiming_new_progress() {
        invalid(|p| p["bootstrap"][0]["evidence"] = json!([]), "evidence");
        invalid(
            |p| p["bootstrap"][0]["status"] = json!("planned"),
            "comprovadas",
        );
        invalid(|p| p["bootstrap"] = Value::Null, "bootstrap");
    }

    #[test]
    fn malformed_schema_repository_and_contracts_return_errors_not_panics() {
        for plan in [
            json!([]),
            json!({}),
            json!({"schema_version":true}),
            json!({"schema_version":2}),
            json!({"schema_version":1.0}),
        ] {
            assert!(validate(&plan).is_err());
            assert!(render(&plan).is_err());
        }
        for repo in [
            "",
            "owner",
            "owner/repo/extra",
            "/repo",
            "owner/",
            "owner with space/repo",
            "usuário/repo",
        ] {
            invalid(
                |p| p["repository"] = json!(repo),
                if repo.is_empty() {
                    "texto"
                } else {
                    "owner/repo"
                },
            );
        }
        invalid(|p| p["contracts"] = json!([]), "contracts");
        invalid(
            |p| p["contracts"]["commands_added"]["9.0.0"] = json!(["FUTURE"]),
            "sem release",
        );
        invalid(
            |p| p["contracts"]["commands_added"]["0.1.0-rc.1"] = json!(["PING"]),
            "versão inválida",
        );
        invalid(
            |p| p["contracts"]["decisions"] = json!([" "]),
            "textos não vazios",
        );
    }

    #[test]
    fn redis_reference_requires_matching_versions_digest_and_platform() {
        for (field, value) in [
            ("image", "redis:latest"),
            ("redis_cli_version", "8.10.0"),
            ("redis_version", "8.10.0"),
            ("platform", "linux/arm64"),
            ("image", "redis:8.10.1@sha256:abc"),
        ] {
            invalid(
                |p| p["reference"][field] = json!(value),
                if field.contains("version") {
                    "mesma versão"
                } else {
                    "reference"
                },
            );
        }
        invalid(
            |p| {
                p["reference"]["redis_version"] = json!("latest");
                p["reference"]["redis_cli_version"] = json!("latest");
            },
            "redis_version",
        );
    }

    #[test]
    fn fixed_policies_reject_value_and_type_drift() {
        for field in [
            "private",
            "candidate_required",
            "patch_requires_manifest_entry",
            "bundle_change_requires_new_candidate",
        ] {
            invalid(|p| p["release_policy"][field] = json!(false), field);
            invalid(|p| p["release_policy"][field] = json!(1), field);
        }
        for field in ["publish_crate", "ci_enabled", "automatic_publication"] {
            invalid(|p| p["release_policy"][field] = json!(true), field);
            invalid(|p| p["release_policy"][field] = json!("false"), field);
        }
        for (field, value) in [
            ("automation_resume_after", "0.10.0"),
            ("merge_strategy", "squash"),
            ("release_branch_prefix", "release/"),
            ("release_label", "release"),
            ("linux_runner", "ubuntu-latest"),
            ("docker_since", "1.0.0"),
            ("final_promotion", "rebuild_final"),
        ] {
            invalid(|p| p["release_policy"][field] = json!(value), field);
        }
        invalid(|p| p["release_policy"]["targets"] = json!([]), "targets");
        for value in [json!(3599), json!(true), json!("3600"), json!(3600.0)] {
            invalid(
                |p| p["release_policy"]["stable_soak_seconds"] = value,
                "stable_soak_seconds",
            );
        }
    }

    #[test]
    fn a_smaller_valid_manifest_is_allowed_without_changing_required_contracts() {
        let mut plan = real();
        plan["releases"].as_array_mut().unwrap().truncate(2);
        plan["contracts"]["commands_added"]
            .as_object_mut()
            .unwrap()
            .retain(|version, _| version == "0.1.0" || version == "0.2.0");
        validate(&plan).unwrap();
        assert!(render(&plan).unwrap().contains("São 2 milestones"));
    }

    #[test]
    fn load_rejects_invalid_utf8_json_and_invalid_manifest() {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        struct Temporary {
            directory: std::path::PathBuf,
        }
        impl Drop for Temporary {
            fn drop(&mut self) {
                let _ = fs::remove_file(self.directory.join("plan.json"));
                let _ = fs::remove_dir(&self.directory);
            }
        }
        let directory = std::env::temp_dir().join(format!(
            "sider-xtask-plan-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let fixture = Temporary { directory };
        let path = fixture.directory.join("plan.json");
        assert!(load(&path).is_err());
        for contents in [b"\xff".as_slice(), b"{broken", b"{}"] {
            fs::write(&path, contents).unwrap();
            assert!(load(&path).is_err());
        }
        let plan = real();
        fs::write(&path, serde_json::to_vec(&plan).unwrap()).unwrap();
        assert_eq!(load(&path).unwrap(), plan);
    }
}
