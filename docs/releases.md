# Desenvolvimento e releases locais

Banco, testes e ferramentas próprias usam Rust. CI e publicação automática ficam
adiadas até depois da 1.0. Os workflows e helpers Python foram removidos, não
arquivados para manutenção. A CLI `gh` continua responsável pela autenticação e
pelas operações manuais no GitHub. Não há um segundo publicador para manter.

## Ciclo curto

| Momento | Comando | O que verificar |
| --- | --- | --- |
| Durante a implementação | `cargo test --locked <filtro>` | Comportamento que está sendo alterado |
| Antes de integrar código do banco | `cargo xtask check` | fmt, Clippy, build do binário real e testes nativos |
| Ao alterar ferramentas ou plano | `cargo xtask check --tools` | fmt, Clippy, testes do xtask, manifesto, gates e roadmap |
| Antes de publicar | Gates da versão no SHA exato | Linux, Windows, diferenciais e pacotes extraídos |

Clippy já verifica os targets; não repita `cargo check` na mesma sequência.
Os checks param na primeira falha e não iniciam Docker ou testes externos.
Documentação isolada pede revisão de texto, links e comandos. Interfaces e build
também exigem `cargo doc --locked --no-deps` e build de distribuição.

Preserve os caches Cargo. Rode testes em paralelo quando não disputarem o mesmo
estado externo. Não limpe `target/` nem reconstrua ambientes em cada edição.
Resultados de release continuam vinculados ao SHA, à plataforma e à configuração
em que foram produzidos; cache de compilação não
é reaproveitamento de recibos de aprovação.

Na publicação, use um `CARGO_TARGET_DIR` isolado por versão, SHA e plataforma;
não use o diretório incremental do desenvolvimento para os binários distribuídos.
Preserve downloads de crates e imagens já verificadas, mas nunca deixe o binário
de uma RC anterior ser tratado como build da final.

## Ferramenta Rust

`cargo xtask` é um [alias Cargo](https://doc.rust-lang.org/cargo/reference/config.html#alias)
para o pacote em [xtask/](../xtask/). Seu manifesto e lockfile são independentes:
as dependências de tooling não entram no servidor ou nos pacotes do banco.
Use os comandos a partir da raiz. A toolchain é a mesma do projeto.

```sh
cargo xtask --help
cargo xtask validate
cargo xtask roadmap
cargo xtask roadmap --write
cargo xtask sync
cargo xtask sync --json
cargo xtask sync --apply
```

`validate` verifica o plano, os comandos declarados de gates e a projeção do roadmap.
Gates futuros com `command: null` são pendências válidas no plano, nunca testes
aprovados. `roadmap` mostra o documento; somente `--write` o atualiza.

`sync` simula por padrão e resume as mudanças. `--json` mostra os corpos completos
para revisão. Só `--apply` autoriza escritas no backlog privado.
O cliente usa [`gh api`](https://cli.github.com/manual/gh_api) com argumentos
separados e JSON UTF-8 via stdin, sem shell, PAT adicional ou extração de token.
Os testes do sincronizador usam um cliente falso, sem mutações reais no GitHub.

O utilitário não cria commits, PRs, tags ou releases automaticamente. Os antigos
comandos de publicação e preparação acoplados à CI não foram portados. Preparação,
build, empacotamento e publicação seguem os comandos Cargo/Git/`gh` e o
[guia de pacotes](packages.md), sem exigir Python ou um novo framework de scripts.
Os scripts de ensaios antigos, quando anexados como evidências, são registros
históricos imutáveis; não são ferramentas atuais nem dependências do desenvolvimento.

## Backlog e execução

[releases/plan.json](../releases/plan.json) define os 11 milestones, as 50 tarefas
funcionais e os gates de publicação. [ROADMAP.md](../ROADMAP.md) é gerado.
O estado operacional fica nas issues do GitHub; não o duplique no roadmap.

1. Escolha a próxima issue desbloqueada do milestone atual.
2. Implemente em branch própria, com testes focados e commits por responsabilidade.
3. Valide o diff final localmente e abra um PR com `Closes #N`.
4. Integre por merge commit. Registre testes e limitações no PR, sem aguardar CI.
5. Atualize compatibilidade e notas quando o comportamento mudar.
6. Sincronize o backlog quando o manifesto ou o estado das dependências mudar,
   não em cada edição. Confira a simulação antes de aplicar.
7. Prepare a candidata quando as tarefas funcionais terminarem. O gate de
   publicação não usa `Closes`: só fecha depois da final publicada e conferida.

Marcadores de tarefa, como `<!-- sider:task R01-01 -->`, são estáveis. O bloco
`sider:managed` pode ser regenerado; texto fora dele, comentários humanos e labels
não gerenciados são preservados. IDs duplicados ou ambíguos interrompem o sync
antes das escritas. Uma nova execução encontra recursos pelos mesmos IDs.

O sincronizador não fecha tarefas funcionais, gates ou milestones. O bootstrap
só pode ser fechado após comprovar os commits e a execução histórica de CI
registrados no manifesto. Esse histórico não significa que existe CI ativa.
Após aplicar, repetir `sync` deve mostrar zero mudanças.

Patches exigem entrada própria no manifesto, ID novo, dependência da versão-base,
teste de regressão e gate próprio. Toda patch tem RC. Novas capacidades entram em
minor; antes da 1.0, incompatibilidades ficam nas minors e aparecem nas notas.
Não crie datas artificiais nem renumere tarefas.

## Preparar uma release

A [v0.1.0-rc.1](https://github.com/djairofilho/sider/releases/tag/v0.1.0-rc.1)
já foi publicada como candidata privada. A final ainda não foi publicada.
A migração de tooling e a remoção do fuzz exigem outra candidata para publicar
essas alterações na final. Tags e evidências anteriores são preservadas e
continuam vinculadas aos seus próprios SHAs; não aprovam a nova revisão.
O bootstrap não deve ser publicado como banco funcional.

Atualize `main`, busque tags e use a branch `chore/release-v<VERSAO>`.
Atualize juntos a versão em `Cargo.toml`, a entrada local `sider` em `Cargo.lock`,
o changelog e `releases/notes/v<VERSAO>.md`. O título das notas é `# Sider v<VERSAO>`.
Preserve `publish = false` e as dependências.
O pacote `sider-xtask` tem versão e lockfile independentes; não acompanha a versão
do servidor.

Confira o manifesto do servidor com `cargo metadata --locked --format-version 1`.
Abra PR com `type:release`, do próprio repositório para `main`, e referência ao
gate, sem fechá-lo.
O merge commit não publica nada. Registre seu SHA completo, nunca o substitua
por um `main` mais recente.

A final exige uma candidata aprovada da mesma versão-base e ancestral do commit
final. Só podem mudar a identidade do pacote em `Cargo.toml` e `Cargo.lock`, changelog
e notas. Mudanças em código, dependências, configuração, ferramentas ou gates no
SHA da final exigem outra candidata. A final recompila e testa novamente.
Evidências de um SHA congelado anterior continuam válidas apenas para esse SHA.

Confira bundles históricos com o procedimento e a política do SHA da publicação.
O verificador atual exige a matriz de gates atual e pode rejeitar bundles antigos
que incluam gates removidos. Preserve os recibos e artefatos originais nesses casos.

## Gates do produto

[releases/gates.json](../releases/gates.json) registra os comandos externos.
Um comando é um array de argumentos executado sem shell. Gates ausentes, com
erro, cancelados, ignorados ou sem casos positivos bloqueiam publicação.

| Gate | Plataforma e início |
| --- | --- |
| `native`, `tcp_smoke` | Linux GNU e Windows MSVC desde 0.1 |
| `compatibility` | Linux desde 0.1 |
| `crash`, `recovery`, `migration` | Linux e Windows desde 0.3 |
| `sharding`, `types`, `sorted_sets`, `transactions`, `pubsub`, `replication` | Linux, cumulativos desde cada capacidade |
| `docker` | Linux desde 0.10 |
| `soak`, `benchmarks` | Linux na 1.0 |

O runner recebe `SIDER_REFERENCE_IMAGE`, `SIDER_RELEASE_VERSION`,
`SIDER_RELEASE_SHA`, `SIDER_RELEASE_TARGET` e `SIDER_RELEASE_DIR`.
O diretório de evidências é novo. O recibo `receipt-ID.json` registra SHA,
versão, target, `status: "success"`, `cases` positivo e detalhes reais da execução.
Não reutilize recibos antigos ou sintetize resultados.

O soak da 1.0 dura 3600 segundos. Benchmarks registram throughput, p50/p95/p99,
memória, pipelines, hot keys e quantidades de shards.

Redis e `redis-cli` usam a versão e o digest fixados no plano. Respostas sem ordem
garantida são normalizadas por conteúdo; as ordenadas também comparam ordem.
Veja os guias de [diferenciais](differential.md) e
[testes](testing.md). A migração inicial da 0.3 usa fixtures AOF; a partir da versão
seguinte, também abre dados reais da versão anterior suportada.

### Contrato de prontidão do binário extraído

O smoke usa o binário realmente extraído, `SIDER_ADDR=127.0.0.1:0` e
`SIDER_READY_FILE`. Confere PID, loopback, porta efetiva, `--version`, PING e
operações TCP. O próprio servidor publica o JSON de prontidão atomicamente.
A imagem Docker da 0.10 precisa executar, ser exportada e restaurada; não há
registry público nem arquivo fictício aceito como prova.

## Conferência e publicação manual

1. Confirme repositório privado, origem/branch/label do PR integrado, SHA completo,
   tarefas concluídas, milestones anteriores e aprovação da candidata, se final.
2. Execute os gates desse SHA nas plataformas exigidas, com logs e recibos.
3. Gere Linux GNU x86_64 em `.tar.gz` no Ubuntu 24.04 e Windows MSVC x86_64 em
   `.zip`. Inclua binário, `releases/README.md` como README, MIT e todos os avisos
   de `releases/licenses/`. Confira os hashes e execute os smokes extraídos.
4. Reúna notas, requisitos de runtime, manifesto, evidências e `SHA256SUMS`.
   Registre toolchain, SHA, targets, comandos e resultados reais como evidência
   local, sem inventar execuções de CI.
5. Confira os arquivos locais com o verificador Rust:

   ```sh
   cargo xtask verify-release <VERSAO> <SHA_COMPLETO> <DIRETORIO_DOS_ASSETS>
   ```

   Ele confere nomes, tamanhos, SHA-256, identidade dos recibos, matriz de gates
   declarada e conteúdo do ZIP de evidências. Não executa binários, não publica,
   não confirma o estado remoto e não transforma um relatório em prova independente.
6. Crie ou confira a tag no SHA aprovado, com mensagem explícita de tag para
   respeitar a configuração de assinatura do usuário. Crie ou retome um draft
   com `gh release`. Não mova tags nem sobrescreva assets divergentes.
7. Envie os assets sem `--clobber`, baixe em diretório novo e execute o verificador
   novamente. Compare também os hashes com os arquivos originais e os digests
   retornados pelo GitHub; consistência interna não prova a origem dos bytes.
8. Publique RC como prerelease e nunca latest. Para final, confirme novamente a
   candidata e a validação. Leia a release publicada e confira SHA, nomes,
   tamanhos e digests. Só então registre o resultado e feche gate/milestone.

Mantenha uma publicação por vez. Após erro ou resposta perdida, releia o estado
remoto antes de repetir. Drafts podem ser retomados; uma release publicada não é
sobrescrita. Falha ao comentar ou fechar o milestone exige apenas reconciliar o
backlog, sem recompilar ou republicar. A [API de releases](https://docs.github.com/en/rest/releases/releases)
é a referência para as operações manuais.

A licença do código próprio é [MIT](../LICENSE); dependências mantêm seus avisos.
Licença não muda visibilidade: repositório e artefatos continuam privados e a crate
não é publicada no crates.io. A imagem da 0.10 também carrega os avisos aplicáveis.

## Retomada futura de CI

Somente uma solicitação explícita depois da 1.0 deve introduzir CI. Ela deverá
reutilizar os comandos Rust existentes. Não há workflow arquivado para restaurar,
publicador Python ou reativação automática ao lançar a 1.0.
