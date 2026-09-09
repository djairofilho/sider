//! Reconciliação do backlog: simulação por padrão, preservando conteúdo humano.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use crate::github::{GhClient, GitHub, REPOSITORY, repo_path};

const START: &str = "<!-- sider:managed:start -->";
const END: &str = "<!-- sider:managed:end -->";
type Index = BTreeMap<String, Value>;

pub fn run(plan: &Value, apply: bool) -> Result<Value, String> {
    sync_plan(plan, &mut GhClient::new(apply), apply)
}

fn text<'a>(record: &'a Value, field: &str) -> Result<&'a str, String> {
    record
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("Campo textual inválido: {field}"))
}

fn array<'a>(record: &'a Value, field: &str) -> Result<&'a [Value], String> {
    record
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("Lista inválida: {field}"))
}

fn strings(record: &Value, field: &str) -> Result<Vec<String>, String> {
    array(record, field)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("Texto inválido em {field}"))
        })
        .collect()
}

fn body_text<'a>(record: &'a Value, field: &str) -> Result<&'a str, String> {
    match record.get(field) {
        None | Some(Value::Null) => Ok(""),
        Some(Value::String(value)) => Ok(value),
        _ => Err(format!("Corpo GitHub inválido: {field}")),
    }
}

fn number(record: &Value) -> Result<u64, String> {
    record
        .get("number")
        .and_then(Value::as_u64)
        .filter(|number| *number > 0)
        .ok_or_else(|| "Número de recurso GitHub inválido".into())
}

fn state(record: &Value) -> Result<&str, String> {
    match text(record, "state")? {
        value @ ("open" | "closed") => Ok(value),
        _ => Err("Estado GitHub inválido".into()),
    }
}

fn block(body: &str) -> Result<Option<(usize, usize)>, String> {
    if !body.contains("sider:managed") {
        return Ok(None);
    }
    if body.matches(START).count() != 1
        || body.matches(END).count() != 1
        || body.matches("sider:managed").count() != 2
    {
        return Err("Bloco gerenciado ausente, duplicado ou ambíguo".into());
    }
    let first = body.find(START).ok_or("Bloco gerenciado ausente")?;
    let last = body.find(END).ok_or("Bloco gerenciado ausente")?;
    if first >= last {
        return Err("Bloco gerenciado fora de ordem".into());
    }
    Ok(Some((first, last + END.len())))
}

fn managed_body(existing: &str, generated: &str) -> Result<String, String> {
    match block(existing)? {
        None if existing.is_empty() => Ok(generated.into()),
        None => Ok(format!("{existing}\n\n{generated}")),
        Some((first, last)) => Ok(format!(
            "{}{generated}{}",
            &existing[..first],
            &existing[last..]
        )),
    }
}

fn valid_id(id: &str, kind: &str) -> bool {
    if kind == "release" {
        return id.len() == 3
            && id.starts_with('R')
            && id.as_bytes()[1..].iter().all(u8::is_ascii_digit);
    }
    id.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        && id
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-')
}

fn marker<'a>(body: &'a str, kind: &str) -> Result<Option<&'a str>, String> {
    let keyword = format!("sider:{kind}");
    if !body.contains(&keyword) {
        return Ok(None);
    }
    let prefix = format!("<!-- {keyword} ");
    if body.matches(&keyword).count() != 1 {
        return Err(format!("Marcador {kind} ambíguo"));
    }
    let start = body
        .find(&prefix)
        .ok_or_else(|| format!("Marcador {kind} inválido"))?;
    let rest = &body[start + prefix.len()..];
    let end = rest
        .find(" -->")
        .ok_or_else(|| format!("Marcador {kind} inválido"))?;
    let id = &rest[..end];
    let bounds = block(body)?.ok_or("Marcador sem bloco gerenciado")?;
    if !valid_id(id, kind)
        || start < bounds.0 + START.len()
        || start + prefix.len() + end + 4 > bounds.1 - END.len()
    {
        return Err(format!(
            "Marcador {kind} inválido ou fora do bloco gerenciado"
        ));
    }
    Ok(Some(id))
}

fn check_generated(body: &str, kind: &str, id: &str) -> Result<(), String> {
    if marker(body, kind)? != Some(id)
        || marker(body, if kind == "task" { "release" } else { "task" })?.is_some()
    {
        return Err("Texto do manifesto invade marcadores reservados".into());
    }
    Ok(())
}

fn labels(issue: &Value) -> Result<Vec<String>, String> {
    match issue.get("labels") {
        None => Ok(Vec::new()),
        Some(Value::Array(values)) => values
            .iter()
            .map(|label| {
                label
                    .as_str()
                    .or_else(|| label.get("name").and_then(Value::as_str))
                    .map(str::to_owned)
                    .ok_or_else(|| "Label GitHub inválido".into())
            })
            .collect(),
        _ => Err("Labels GitHub inválidos".into()),
    }
}

#[derive(Clone)]
struct Item {
    id: String,
    title: String,
    kind: &'static str,
    version: String,
    area: String,
    dependencies: Vec<String>,
    data: Value,
}

fn descriptors(plan: &Value) -> Result<Vec<Item>, String> {
    let releases = array(plan, "releases")?;
    let first_version = text(releases.first().ok_or("Plano sem releases")?, "version")?;
    let release_gates: BTreeMap<_, _> = releases
        .iter()
        .map(|release| {
            Ok((
                text(release, "id")?.to_owned(),
                text(&release["gate"], "id")?.to_owned(),
            ))
        })
        .collect::<Result<_, String>>()?;
    let mut items = Vec::new();
    for data in array(plan, "bootstrap")? {
        items.push(Item {
            id: text(data, "id")?.into(),
            title: text(data, "title")?.into(),
            kind: "bootstrap",
            version: first_version.into(),
            area: "foundation".into(),
            dependencies: Vec::new(),
            data: data.clone(),
        });
    }
    for release in releases {
        let version = text(release, "version")?;
        let previous = strings(release, "depends_on")?
            .iter()
            .map(|id| {
                release_gates
                    .get(id)
                    .cloned()
                    .ok_or_else(|| "Dependência de release ausente".into())
            })
            .collect::<Result<Vec<_>, String>>()?;
        for data in array(release, "tasks")? {
            let mut dependencies = strings(data, "depends_on")?;
            for dependency in &previous {
                if !dependencies.contains(dependency) {
                    dependencies.push(dependency.clone());
                }
            }
            items.push(Item {
                id: text(data, "id")?.into(),
                title: text(data, "title")?.into(),
                kind: "task",
                version: version.into(),
                area: text(data, "area")?.into(),
                dependencies,
                data: data.clone(),
            });
        }
        let mut data = release["gate"].clone();
        data["required_gates"] = release["required_gates"].clone();
        let mut dependencies = array(release, "tasks")?
            .iter()
            .map(|task| text(task, "id").map(str::to_owned))
            .collect::<Result<Vec<_>, _>>()?;
        dependencies.extend(previous);
        items.push(Item {
            id: text(&data, "id")?.into(),
            title: text(&data, "title")?.into(),
            kind: "release",
            version: version.into(),
            area: "release".into(),
            dependencies,
            data,
        });
    }
    Ok(items)
}

fn paragraphs(values: &[String]) -> String {
    if values.is_empty() {
        "- Nenhum.".into()
    } else {
        values
            .iter()
            .map(|value| format!("- {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn issue_url(issue: &Value) -> Option<String> {
    number(issue)
        .ok()
        .map(|number| format!("https://github.com/{REPOSITORY}/issues/{number}"))
}

fn render(item: &Item, issues: &Index) -> Result<String, String> {
    let objective = item
        .data
        .get("objective")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(&item.title);
    let mut lines = vec![
        START.into(),
        format!("<!-- sider:task {} -->", item.id),
        "".into(),
        "## Objetivo".into(),
        "".into(),
        objective.into(),
        "".into(),
        format!("Versão: `v{}`.", item.version),
    ];
    let (deliverables, tests) = match item.kind {
        "bootstrap" => (
            vec!["Fundação existente registrada pelos commits vinculados.".into()],
            vec!["CI concluída com sucesso no SHA de um commit vinculado.".into()],
        ),
        "release" => (
            vec![
                "Publicar uma candidata e a versão final com evidências e artefatos verificados."
                    .into(),
                "Até e incluindo a 1.0, verificar e publicar manualmente; CI e publicação automática ficam para depois da 1.0.".into(),
                "Manter esta issue e o milestone abertos até a publicação final confirmada.".into(),
            ],
            strings(&item.data, "required_gates")?,
        ),
        _ => (
            strings(&item.data, "deliverables")?,
            strings(&item.data, "tests")?,
        ),
    };
    lines.extend([
        "".into(),
        "## Entregáveis".into(),
        "".into(),
        paragraphs(&deliverables),
        "".into(),
        "## Testes exigidos".into(),
        "".into(),
        paragraphs(&tests),
    ]);
    if item.kind == "bootstrap" {
        lines.extend([
            "".into(),
            "## Evidências do bootstrap".into(),
            "".into(),
            paragraphs(&strings(&item.data, "evidence")?),
        ]);
    }
    lines.extend(["".into(), "## Dependências".into(), "".into()]);
    if item.dependencies.is_empty() {
        lines.push("- Nenhuma.".into());
    }
    for dependency in &item.dependencies {
        let issue = issues.get(dependency);
        let url = issue
            .and_then(issue_url)
            .unwrap_or_else(|| format!("https://github.com/{REPOSITORY}/issues?q={dependency}"));
        let checked = if issue
            .and_then(|issue| issue.get("state"))
            .and_then(Value::as_str)
            == Some("closed")
        {
            "x"
        } else {
            " "
        };
        lines.push(format!("- [{checked}] [{dependency}]({url})"));
    }
    let acceptance = if item.kind == "bootstrap" && item.data.get("acceptance").is_none() {
        vec!["Bootstrap comprovado pelos commits e pela CI vinculada.".into()]
    } else {
        strings(&item.data, "acceptance")?
    };
    lines.extend([
        "".into(),
        "## Critério de conclusão".into(),
        "".into(),
        paragraphs(&acceptance),
        "".into(),
        END.into(),
    ]);
    let generated = lines.join("\n");
    check_generated(&generated, "task", &item.id)?;
    Ok(generated)
}

fn index_issues(raw: &[Value], ids: &BTreeSet<String>) -> Result<Index, String> {
    let mut issues = Index::new();
    let mut numbers = BTreeSet::new();
    for issue in raw {
        if issue.get("pull_request").is_some() {
            continue;
        }
        let number = number(issue)?;
        if !numbers.insert(number) {
            return Err("Número de issue duplicado".into());
        }
        let body = body_text(issue, "body")?;
        let marked = marker(body, "task")?;
        if marker(body, "release")?.is_some() || (block(body)?.is_some() && marked.is_none()) {
            return Err(format!(
                "Bloco reservado sem identidade de tarefa na issue #{number}"
            ));
        }
        let title = text(issue, "title")?;
        let title_id = title
            .strip_prefix('[')
            .and_then(|title| title.split_once(']').map(|(id, _)| id));
        if let Some(title_id) = title_id.filter(|id| ids.contains(*id))
            && marked != Some(title_id)
        {
            return Err(format!(
                "Issue #{number} usa ID reservado sem marcador correspondente"
            ));
        }
        if let Some(id) = marked {
            state(issue)?;
            labels(issue)?;
            if let Some(milestone) = issue.get("milestone").filter(|value| !value.is_null()) {
                self::number(milestone)?;
            }
            if issues.insert(id.into(), issue.clone()).is_some() {
                return Err(format!("Identificador duplicado no GitHub: {id}"));
            }
        }
    }
    Ok(issues)
}

fn index_milestones(raw: &[Value], releases: &[Value]) -> Result<Index, String> {
    let expected = releases
        .iter()
        .map(|release| {
            Ok((
                text(release, "id")?.to_owned(),
                format!("v{}", text(release, "version")?),
            ))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;
    let mut milestones = Index::new();
    let mut ids = BTreeSet::new();
    let mut numbers = BTreeSet::new();
    for milestone in raw {
        let title = text(milestone, "title")?;
        if !numbers.insert(number(milestone)?) || milestones.contains_key(title) {
            return Err(format!("Título ou número de milestone duplicado: {title}"));
        }
        state(milestone)?;
        let description = body_text(milestone, "description")?;
        let marked = marker(description, "release")?;
        if marker(description, "task")?.is_some()
            || (block(description)?.is_some() && marked.is_none())
        {
            return Err(format!(
                "Bloco reservado sem identidade de milestone: {title}"
            ));
        }
        if let Some(id) = marked {
            if !ids.insert(id.to_owned()) {
                return Err(format!("ID de milestone duplicado: {id}"));
            }
            if expected.get(id).is_some_and(|version| version != title) {
                return Err(format!("ID de milestone com versão divergente: {id}"));
            }
            if expected.values().any(|version| version == title)
                && expected.get(id).map(String::as_str) != Some(title)
            {
                return Err(format!("Milestone reservado com ID divergente: {title}"));
            }
        }
        milestones.insert(title.into(), milestone.clone());
    }
    Ok(milestones)
}

fn prove_bootstrap(item: &Item, client: &mut impl GitHub, cache: &mut Index) -> Result<(), String> {
    let prefix = format!("https://github.com/{REPOSITORY}/");
    let mut commits = BTreeSet::new();
    let mut runs = Vec::new();
    for evidence in strings(&item.data, "evidence")? {
        let suffix = evidence
            .strip_prefix(&prefix)
            .ok_or("Evidência bootstrap fora do repositório")?;
        let (path, commit) = if let Some(sha) = suffix.strip_prefix("commit/").filter(|sha| {
            (7..=40).contains(&sha.len())
                && sha
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }) {
            (repo_path(&format!("commits/{sha}")), Some(sha))
        } else if let Some(run) = suffix.strip_prefix("actions/runs/").filter(|run| {
            !run.is_empty()
                && run.bytes().all(|byte| byte.is_ascii_digit())
                && run.parse::<u64>().is_ok_and(|id| id > 0)
        }) {
            (repo_path(&format!("actions/runs/{run}")), None)
        } else {
            return Err(format!("Evidência bootstrap não reconhecida: {}", item.id));
        };
        if !cache.contains_key(&path) {
            cache.insert(path.clone(), client.request("GET", &path, None)?);
        }
        let result = &cache[&path];
        if let Some(prefix) = commit {
            let sha = text(result, "sha")?;
            if sha.len() != 40
                || !sha.starts_with(prefix)
                || !sha
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err("SHA de evidência bootstrap divergente".into());
            }
            commits.insert(sha.to_owned());
        } else {
            if result.get("status").and_then(Value::as_str) != Some("completed")
                || result.get("conclusion").and_then(Value::as_str) != Some("success")
            {
                return Err("CI bootstrap não aprovada".into());
            }
            runs.push(text(result, "head_sha")?.to_owned());
        }
    }
    if commits.is_empty() || runs.is_empty() || runs.iter().any(|sha| !commits.contains(sha)) {
        return Err("Bootstrap exige commit e CI aprovada no mesmo SHA".into());
    }
    Ok(())
}

fn desired_labels(
    item: &Item,
    issue: &Value,
    issues: &Index,
    managed: &BTreeSet<String>,
) -> Result<Vec<String>, String> {
    let mut result = labels(issue)?
        .into_iter()
        .filter(|label| !managed.contains(label))
        .collect::<BTreeSet<_>>();
    result.insert(format!("type:{}", item.kind));
    result.insert(format!("area:{}", item.area));
    if item.dependencies.iter().any(|id| {
        issues
            .get(id)
            .and_then(|issue| issue.get("state"))
            .and_then(Value::as_str)
            != Some("closed")
    }) {
        result.insert("status:blocked".into());
    }
    Ok(result.into_iter().collect())
}

fn mutate(
    client: &mut impl GitHub,
    apply: bool,
    changes: &mut Vec<Value>,
    kind: &str,
    path: String,
    body: Value,
    id: &str,
) -> Result<Option<Value>, String> {
    let method = if kind.ends_with("created") {
        "POST"
    } else {
        "PATCH"
    };
    changes.push(json!({"kind":kind,"id":id,"method":method,"path":path,"body":body}));
    if apply {
        client.request(method, &path, Some(&body)).map(Some)
    } else {
        Ok(None)
    }
}

fn patch_path(category: &str, record: &Value, id: &str, apply: bool) -> Result<String, String> {
    match number(record) {
        Ok(number) => Ok(repo_path(&format!("{category}/{number}"))),
        Err(error) if apply => Err(error),
        Err(_) => Ok(repo_path(&format!("{category}/<{id}>"))),
    }
}

fn sync_plan(plan: &Value, client: &mut impl GitHub, apply: bool) -> Result<Value, String> {
    crate::plan::validate(plan)?;
    if text(plan, "repository")? != REPOSITORY {
        return Err("Repositório do manifesto não é o Sider".into());
    }
    let repository = client.request("GET", &repo_path(""), None)?;
    if repository.get("private").and_then(Value::as_bool) != Some(true) {
        return Err("O backlog Sider deve permanecer privado".into());
    }
    if repository
        .get("full_name")
        .is_some_and(|name| name.as_str() != Some(REPOSITORY))
    {
        return Err("Resposta de repositório divergente".into());
    }
    let raw_milestones = client.paginate(&repo_path("milestones?state=all&per_page=100"))?;
    let raw_issues = client.paginate(&repo_path("issues?state=all&per_page=100"))?;
    let raw_labels = client.paginate(&repo_path("labels?per_page=100"))?;
    let items = descriptors(plan)?;
    let ids = items.iter().map(|item| item.id.clone()).collect();
    let mut issues = index_issues(&raw_issues, &ids)?;
    let releases = array(plan, "releases")?;
    let mut milestones = index_milestones(&raw_milestones, releases)?;
    let known_labels = raw_labels
        .iter()
        .map(|label| text(label, "name").map(str::to_owned))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut definitions = BTreeMap::from([
        (
            "type:task".into(),
            ("1d76db", "Entrega funcional do plano de releases".into()),
        ),
        (
            "type:release".into(),
            ("5319e7", "Validação e publicação de uma versão".into()),
        ),
        (
            "type:bootstrap".into(),
            ("0e8a16", "Fundação já comprovada por commits e CI".into()),
        ),
        (
            "status:blocked".into(),
            ("d93f0b", "Aguarda uma dependência ainda aberta".into()),
        ),
        (
            "compatibility:breaking".into(),
            (
                "b60205",
                "Mudança incompatível a documentar nas notas".into(),
            ),
        ),
    ]);
    for item in &items {
        definitions.insert(
            format!("area:{}", item.area),
            ("c5def5", format!("Área de responsabilidade: {}", item.area)),
        );
    }
    let managed = definitions
        .keys()
        .filter(|key| key.as_str() != "compatibility:breaking")
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut descriptions = BTreeMap::new();
    // Toda colisão, corpo gerenciado e evidência é validada antes da primeira escrita.
    for item in &items {
        let generated = render(item, &issues)?;
        let existing = issues
            .get(&item.id)
            .map(|issue| body_text(issue, "body"))
            .transpose()?
            .unwrap_or("");
        managed_body(existing, &generated)?;
        desired_labels(
            item,
            issues.get(&item.id).unwrap_or(&Value::Null),
            &issues,
            &managed,
        )?;
    }
    for release in releases {
        let id = text(release, "id")?;
        let title = format!("v{}", text(release, "version")?);
        let generated = format!(
            "{START}\n<!-- sider:release {id} -->\n\n{}\n\nCI e publicação automática desativadas até e incluindo a 1.0; verificações e publicação manuais.\n\n{}\n\n{END}",
            text(release, "title")?,
            paragraphs(&strings(&release["gate"], "acceptance")?)
        );
        check_generated(&generated, "release", id)?;
        let existing = milestones
            .get(&title)
            .map(|milestone| body_text(milestone, "description"))
            .transpose()?
            .unwrap_or("");
        descriptions.insert(title, managed_body(existing, &generated)?);
    }
    let mut evidence = Index::new();
    for item in &items {
        if item.kind == "bootstrap" {
            prove_bootstrap(item, client, &mut evidence)?;
        }
    }
    let mut changes = Vec::new();
    for (name, (color, description)) in definitions {
        if !known_labels.contains(&name) {
            mutate(
                client,
                apply,
                &mut changes,
                "label_created",
                repo_path("labels"),
                json!({"name":name,"color":color,"description":description}),
                &name,
            )?;
        }
    }
    for release in releases {
        let title = format!("v{}", text(release, "version")?);
        let description = &descriptions[&title];
        if let Some(milestone) = milestones.get_mut(&title) {
            if body_text(milestone, "description")? != description {
                mutate(
                    client,
                    apply,
                    &mut changes,
                    "milestone_updated",
                    patch_path("milestones", milestone, &title, apply)?,
                    json!({"description":description}),
                    &title,
                )?;
                milestone["description"] = json!(description);
            }
        } else {
            let body = json!({"title":title,"description":description,"state":"open"});
            let created = mutate(
                client,
                apply,
                &mut changes,
                "milestone_created",
                repo_path("milestones"),
                body.clone(),
                &title,
            )?;
            let mut milestone = created.unwrap_or(body);
            if apply {
                number(&milestone)?;
                state(&milestone)?;
                if text(&milestone, "title")? != title {
                    return Err("Criação retornou milestone divergente".into());
                }
            } else {
                milestone["number"] = Value::Null;
            }
            milestones.insert(title, milestone);
        }
    }
    // Criar todos os IDs primeiro permite resolver links sem adivinhar números.
    for item in &items {
        if issues.contains_key(&item.id) {
            continue;
        }
        let milestone = milestones[&format!("v{}", item.version)]["number"].clone();
        let body = json!({"title":format!("[{}] {}",item.id,item.title),"body":render(item,&issues)?,"labels":desired_labels(item,&Value::Null,&issues,&managed)?,"milestone":milestone});
        let created = mutate(
            client,
            apply,
            &mut changes,
            "issue_created",
            repo_path("issues"),
            body.clone(),
            &item.id,
        )?;
        let issue = if let Some(issue) = created {
            number(&issue)?;
            state(&issue)?;
            labels(&issue)?;
            if marker(body_text(&issue, "body")?, "task")? != Some(&item.id) {
                return Err("Criação retornou identidade de issue divergente".into());
            }
            issue
        } else {
            let mut issue = body;
            issue["number"] = Value::Null;
            issue["state"] = json!("open");
            issue["milestone"] = json!({"number":milestone});
            issue
        };
        issues.insert(item.id.clone(), issue);
    }
    for item in &items {
        if item.kind != "bootstrap" {
            continue;
        }
        let issue = issues.get_mut(&item.id).ok_or("Bootstrap sem issue")?;
        if state(issue)? != "closed" {
            mutate(
                client,
                apply,
                &mut changes,
                "bootstrap_closed",
                patch_path("issues", issue, &item.id, apply)?,
                json!({"state":"closed","state_reason":"completed"}),
                &item.id,
            )?;
            issue["state"] = json!("closed");
        }
    }
    for item in &items {
        let issue = &issues[&item.id];
        let desired = json!({"title":format!("[{}] {}",item.id,item.title),"body":managed_body(body_text(issue,"body")?,&render(item,&issues)?)?,"labels":desired_labels(item,issue,&issues,&managed)?,"milestone":milestones[&format!("v{}",item.version)]["number"]});
        let mut current_labels = labels(issue)?;
        current_labels.sort();
        let current = json!({"title":issue["title"],"body":issue["body"],"labels":current_labels,"milestone":issue["milestone"]["number"]});
        let update = desired
            .as_object()
            .ok_or("Atualização inválida")?
            .iter()
            .filter(|(key, value)| current.get(*key) != Some(*value))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<serde_json::Map<_, _>>();
        if !update.is_empty() {
            mutate(
                client,
                apply,
                &mut changes,
                "issue_updated",
                patch_path("issues", issue, &item.id, apply)?,
                Value::Object(update),
                &item.id,
            )?;
        }
    }
    let mut counts = BTreeMap::<String, usize>::new();
    for change in &changes {
        *counts.entry(text(change, "kind")?.into()).or_default() += 1;
    }
    let issue_urls = issues
        .iter()
        .map(|(id, issue)| (id.clone(), issue_url(issue)))
        .collect::<BTreeMap<_, _>>();
    let milestone_numbers = milestones
        .iter()
        .map(|(title, milestone)| (title.clone(), milestone["number"].clone()))
        .collect::<BTreeMap<_, _>>();
    Ok(
        json!({"apply":apply,"total_changes":changes.len(),"counts":counts,"changes":changes,"issues":issue_urls,"milestones":milestone_numbers}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn example_plan() -> Value {
        let mut plan: Value =
            serde_json::from_str(include_str!("../../releases/plan.json")).unwrap();
        plan["bootstrap"] = json!([{"id":"B00-01","title":"Fundação","status":"completed","evidence":[format!("https://github.com/{REPOSITORY}/commit/{SHA}"),format!("https://github.com/{REPOSITORY}/actions/runs/7")]}]);
        plan["contracts"]["commands_added"] = json!({});
        let mut releases = Vec::new();
        for index in 1..=2 {
            let id = format!("R{index:02}");
            releases.push(json!({"id":id,"version":format!("0.{index}.0"),"title":format!("Versão {index}"),"depends_on":if index == 1 {vec![]} else {vec!["R01"]},"scope":["Escopo"],"required_gates":["native","compatibility","tcp_smoke"],"tasks":[{"id":format!("{id}-01"),"title":"Implementar operação","area":"storage","objective":"Comportamento binário.","deliverables":["Implementação"],"tests":["Regressão"],"acceptance":["Estado correto"],"depends_on":if index == 1 {vec!["B00-01"]} else {vec![]}}],"gate":{"id":format!("{id}-GATE"),"title":"Validar e publicar","acceptance":["Publicação confirmada"]}}));
        }
        plan["releases"] = json!(releases);
        plan
    }

    struct FakeGitHub {
        private: Value,
        milestones: Vec<Value>,
        issues: Vec<Value>,
        labels: Vec<Value>,
        calls: Vec<(String, String, Option<Value>)>,
        run: Value,
        commit: Value,
        lose_response: Option<&'static str>,
    }
    impl Default for FakeGitHub {
        fn default() -> Self {
            Self {
                private: json!(true),
                milestones: vec![],
                issues: vec![],
                labels: vec![],
                calls: vec![],
                run: json!({"status":"completed","conclusion":"success","head_sha":SHA}),
                commit: json!({"sha":SHA}),
                lose_response: None,
            }
        }
    }
    impl FakeGitHub {
        fn writes(&self) -> usize {
            self.calls
                .iter()
                .filter(|(method, _, _)| method != "GET")
                .count()
        }
        fn issue(&self, id: &str) -> &Value {
            self.issues
                .iter()
                .find(|issue| {
                    marker(body_text(issue, "body").unwrap(), "task").unwrap() == Some(id)
                })
                .unwrap()
        }
        fn issue_mut(&mut self, id: &str) -> &mut Value {
            self.issues
                .iter_mut()
                .find(|issue| {
                    marker(body_text(issue, "body").unwrap(), "task").unwrap() == Some(id)
                })
                .unwrap()
        }
        fn apply(&mut self, plan: &Value) -> Result<Value, String> {
            sync_plan(plan, self, true)
        }
    }
    impl GitHub for FakeGitHub {
        fn request(
            &mut self,
            method: &str,
            path: &str,
            body: Option<&Value>,
        ) -> Result<Value, String> {
            self.calls.push((method.into(), path.into(), body.cloned()));
            let suffix = path
                .strip_prefix(&repo_path(""))
                .unwrap()
                .split('?')
                .next()
                .unwrap();
            if method == "GET" {
                return match suffix {
                    "" => Ok(json!({"private":self.private,"full_name":REPOSITORY})),
                    "/milestones" => Ok(json!(self.milestones)),
                    "/issues" => Ok(json!(self.issues)),
                    "/labels" => Ok(json!(self.labels)),
                    value if value.starts_with("/commits/") => Ok(self.commit.clone()),
                    value if value.starts_with("/actions/runs/") => Ok(self.run.clone()),
                    _ => panic!("GET inesperado: {path}"),
                };
            }
            let body = body.unwrap();
            let mut components = suffix.trim_start_matches('/').split('/');
            let category = components.next().unwrap();
            let rows = match category {
                "issues" => &mut self.issues,
                "milestones" => &mut self.milestones,
                "labels" => &mut self.labels,
                _ => panic!("Escrita fora do contrato"),
            };
            let result = if method == "POST" {
                let mut row = body.clone();
                if category != "labels" {
                    row["number"] = json!(
                        rows.iter()
                            .filter_map(|row| row["number"].as_u64())
                            .max()
                            .unwrap_or(0)
                            + 1
                    );
                    row["state"] = json!("open");
                }
                if category == "issues" {
                    row["comments"] = json!([]);
                    row["milestone"] = json!({"number":body["milestone"]});
                }
                rows.push(row.clone());
                row
            } else {
                assert_eq!(method, "PATCH");
                let id: u64 = components.next().unwrap().parse().unwrap();
                let row = rows.iter_mut().find(|row| row["number"] == id).unwrap();
                for (key, value) in body.as_object().unwrap() {
                    row[key] = if key == "milestone" {
                        json!({"number":value})
                    } else {
                        value.clone()
                    };
                }
                row.clone()
            };
            if self.lose_response == Some(category) {
                self.lose_response = None;
                return Err("Resposta perdida depois da escrita".into());
            }
            Ok(result)
        }
    }

    #[test]
    fn dry_run_does_not_mutate_and_second_apply_has_zero_changes() {
        let plan = example_plan();
        let mut client = FakeGitHub::default();
        let report = sync_plan(&plan, &mut client, false).unwrap();
        assert_eq!(report["counts"]["milestone_created"], 2);
        assert_eq!(report["counts"]["issue_created"], 5);
        assert_eq!(report["counts"]["bootstrap_closed"], 1);
        assert_eq!(client.writes(), 0);
        assert!(client.issues.is_empty());
        client.apply(&plan).unwrap();
        let writes = client.writes();
        assert_eq!(client.apply(&plan).unwrap()["total_changes"], 0);
        assert_eq!(client.writes(), writes);
        assert_eq!(client.issues.len(), 5);
        assert_eq!(client.milestones.len(), 2);
        assert_eq!(client.issue("B00-01")["state"], "closed");
        assert_eq!(client.issue("R01-GATE")["state"], "open");
    }

    #[test]
    fn migration_keeps_existing_policy_text_without_remote_churn() {
        let plan = example_plan();
        let mut client = FakeGitHub::default();
        client.apply(&plan).unwrap();
        assert!(body_text(client.issue("R01-GATE"), "body").unwrap().contains(
            "- Até e incluindo a 1.0, verificar e publicar manualmente; CI e publicação automática ficam para depois da 1.0."
        ));
        assert!(body_text(&client.milestones[0], "description").unwrap().contains(
            "CI e publicação automática desativadas até e incluindo a 1.0; verificações e publicação manuais."
        ));
        assert_eq!(client.apply(&plan).unwrap()["total_changes"], 0);
    }

    #[test]
    fn human_text_comments_labels_and_milestone_state_survive() {
        let mut plan = example_plan();
        let mut client = FakeGitHub::default();
        client.milestones.push(json!({"number":1,"title":"v0.1.0","description":"Data acordada em reunião.","state":"closed","due_on":"2026-12-01T00:00:00Z"}));
        client.apply(&plan).unwrap();
        let issue = client.issue_mut("R01-01");
        issue["body"] = json!(format!(
            "Nota humana: revisão técnica.\n\n{}\n\nNão apagar.",
            issue["body"].as_str().unwrap()
        ));
        issue["comments"] = json!(["Discussão intacta"]);
        issue["labels"].as_array_mut().unwrap().extend([
            json!("priority:high"),
            json!("area:custom"),
            json!("compatibility:breaking"),
        ]);
        issue["state"] = json!("closed");
        plan["releases"][0]["tasks"][0]["objective"] = json!("Novo objetivo com acentuação.");
        client.apply(&plan).unwrap();
        let issue = client.issue("R01-01");
        let body = body_text(issue, "body").unwrap();
        assert!(
            body.starts_with("Nota humana: revisão técnica.\n\n")
                && body.ends_with("\n\nNão apagar.")
        );
        assert!(body.contains("Novo objetivo com acentuação."));
        assert_eq!(issue["comments"], json!(["Discussão intacta"]));
        for label in ["priority:high", "area:custom", "compatibility:breaking"] {
            assert!(labels(issue).unwrap().contains(&label.into()));
        }
        assert_eq!(issue["state"], "closed");
        assert_eq!(client.milestones[0]["state"], "closed");
        assert_eq!(client.milestones[0]["due_on"], "2026-12-01T00:00:00Z");
        assert!(
            client.milestones[0]["description"]
                .as_str()
                .unwrap()
                .starts_with("Data acordada em reunião.\n\n")
        );
        assert!(
            !client
                .calls
                .iter()
                .any(|(_, path, _)| path.contains("/comments"))
        );
    }

    #[test]
    fn dependency_state_unblocks_without_closing_tasks_or_gates() {
        let plan = example_plan();
        let mut client = FakeGitHub::default();
        client.apply(&plan).unwrap();
        assert!(
            !labels(client.issue("R01-01"))
                .unwrap()
                .contains(&"status:blocked".into())
        );
        assert!(
            labels(client.issue("R02-01"))
                .unwrap()
                .contains(&"status:blocked".into())
        );
        let gate_url = issue_url(client.issue("R01-GATE")).unwrap();
        assert!(
            body_text(client.issue("R02-01"), "body")
                .unwrap()
                .contains(&gate_url)
        );
        client.issue_mut("R01-01")["state"] = json!("closed");
        client.apply(&plan).unwrap();
        assert!(
            !labels(client.issue("R01-GATE"))
                .unwrap()
                .contains(&"status:blocked".into())
        );
        assert_eq!(client.issue("R01-GATE")["state"], "open");
        client.issue_mut("R01-GATE")["state"] = json!("closed");
        client.apply(&plan).unwrap();
        assert!(
            !labels(client.issue("R02-01"))
                .unwrap()
                .contains(&"status:blocked".into())
        );
        assert_eq!(client.issue("R02-01")["state"], "open");
    }

    #[test]
    fn lost_create_responses_resume_without_duplicates_or_retrying_writes() {
        for category in ["issues", "milestones", "labels"] {
            let plan = example_plan();
            let mut client = FakeGitHub {
                lose_response: Some(category),
                ..FakeGitHub::default()
            };
            assert!(
                client
                    .apply(&plan)
                    .unwrap_err()
                    .contains("Resposta perdida")
            );
            assert_eq!(client.calls.last().unwrap().0, "POST");
            client.apply(&plan).unwrap();
            assert_eq!(client.issues.len(), 5);
            assert_eq!(client.milestones.len(), 2);
            assert_eq!(client.apply(&plan).unwrap()["total_changes"], 0);
        }
    }

    #[test]
    fn collisions_and_ambiguous_markers_fail_before_any_write() {
        let plan = example_plan();
        for mutation in 0..9 {
            let mut client = FakeGitHub::default();
            client.apply(&plan).unwrap();
            match mutation {
                0 => {
                    let mut duplicate = client.issue("R01-01").clone();
                    duplicate["number"] = json!(77);
                    client.issues.push(duplicate);
                }
                1 => {
                    let issue = client.issue_mut("R02-01");
                    issue["body"] = json!(format!("{}\n{START}", issue["body"].as_str().unwrap()));
                }
                2 => client.issues.push(
                    json!({"number":77,"title":"[R01-01] Issue humana","body":"Texto humano"}),
                ),
                3 => {
                    let mut duplicate = client.milestones[0].clone();
                    duplicate["number"] = json!(77);
                    client.milestones.push(duplicate);
                }
                4 => {
                    client.milestones[0]["description"] =
                        json!(format!("{END}\n{START}\n<!-- sider:release R01 -->"))
                }
                5 => client.milestones[0]["title"] = json!("v8.0.0"),
                6 => {
                    client.issue_mut("R01-01")["body"] =
                        json!(format!("<!-- sider:task R01-01 -->\n{START}\n{END}"))
                }
                7 => client.issue_mut("R01-01")["title"] = json!("[R02-01] Identidade divergente"),
                _ => {
                    client.milestones[1]["description"] =
                        json!(format!("{START}\n<!-- sider:release R99 -->\n{END}"))
                }
            }
            let writes = client.writes();
            assert!(client.apply(&plan).is_err(), "mutação {mutation}");
            assert_eq!(client.writes(), writes, "mutação {mutation}");
        }
    }

    #[test]
    fn generated_marker_injection_and_public_repository_fail_before_writes() {
        let mut plan = example_plan();
        let mut client = FakeGitHub::default();
        plan["releases"][1]["tasks"][0]["objective"] = json!(format!("Acrescentar {END}"));
        assert!(client.apply(&plan).is_err());
        assert_eq!(client.writes(), 0);
        for private in [json!(false), json!("true"), Value::Null] {
            let mut client = FakeGitHub {
                private,
                ..FakeGitHub::default()
            };
            assert!(client.apply(&example_plan()).is_err());
            assert_eq!(client.writes(), 0);
        }
    }

    #[test]
    fn bootstrap_needs_original_commits_and_successful_ci_on_same_sha() {
        let plan = example_plan();
        for run in [
            json!({"status":"in_progress","conclusion":null,"head_sha":SHA}),
            json!({"status":"completed","conclusion":"failure","head_sha":SHA}),
            json!({"status":"completed","conclusion":"success","head_sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}),
        ] {
            let mut client = FakeGitHub {
                run,
                ..FakeGitHub::default()
            };
            assert!(client.apply(&plan).is_err());
            assert_eq!(client.writes(), 0);
        }
        let mut client = FakeGitHub {
            commit: json!({"sha":"aaaaaaa"}),
            ..FakeGitHub::default()
        };
        assert!(client.apply(&plan).is_err());
        assert_eq!(client.writes(), 0);
        let mut plan = plan;
        plan["bootstrap"][0]["evidence"] = json!(["https://evil.example/claim"]);
        let mut client = FakeGitHub::default();
        assert!(client.apply(&plan).is_err());
        assert_eq!(client.writes(), 0);
    }

    #[test]
    fn pull_requests_do_not_claim_issue_identity() {
        let mut client = FakeGitHub::default();
        client
            .issues
            .push(json!({"number":99,"title":"[R01-01] PR","body":"","pull_request":{"url":"pr"}}));
        client.apply(&example_plan()).unwrap();
        assert_eq!(client.issues.len(), 6);
    }
}
