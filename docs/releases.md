# Desenvolvimento e releases locais

Banco, testes e ferramentas próprias usam Rust. CI e publicação automática ficam
adiadas até depois da 1.0. A CLI `gh` executa operações manuais no GitHub; não há
outro publicador ou framework de scripts. Repositório e artefatos permanecem
privados, com `publish = false`.

## Ciclo curto

| Momento | Comando ou evidência |
| --- | --- |
| Durante implementação | `cargo test --locked <filtro>` |
| Antes de integrar cada PR do banco | Uma execução de `cargo xtask check` sobre o diff final |
| Ferramentas ou plano alterados | `cargo xtask check --tools` |
| Semântica de comandos alterada | Diferenciais da família afetada contra a referência fixada |
| Filesystem, persistência e atomicidade alterados | Testes de falha/replay/migração relevantes em Linux e Windows |
| Núcleo durável com shards e transações integrado | Bateria integrada dos subsistemas disponíveis |
| Build candidato 1.0 congelado | Matriz completa de gates, pacotes extraídos, imagem Docker e evidências |

Clippy já verifica os targets; não duplique com `cargo check`. Os checks param
na primeira falha e não iniciam Docker ou testes externos. Documentação isolada
pede revisão de texto, links e comandos. Interfaces e build também exigem
`cargo doc --locked --no-deps` e `cargo build --locked --release`.

Preserve caches Cargo e não repita verificações aprovadas sem mudanças relevantes.
Ensaios internos chamam as suítes diretamente, com ID da tarefa e SHA registrado,
sem exigir empacotamento ou recibos de release. Teste ignorado não conta como
aprovação. Benchmarks não disputam a máquina com builds ou outras cargas.

## Ferramenta Rust e backlog

`cargo xtask` usa o pacote em [xtask/](../xtask/), com manifesto e lockfile
independentes. As dependências de tooling não entram no servidor. Execute da raiz:

```sh
cargo xtask --help
cargo xtask validate
cargo xtask roadmap
cargo xtask roadmap --write
cargo xtask sync
cargo xtask sync --json
cargo xtask sync --apply
```

[releases/plan.json](../releases/plan.json), schema 2, preserva os 11 milestones,
as 50 tarefas e seus IDs. `publication: false` em R01–R10 define checkpoints
internos; somente R11 é publicável. [ROADMAP.md](../ROADMAP.md) é gerado.
O estado operacional fica nas issues. Não altere a versão do pacote por checkpoint.

O DAG usa dependências técnicas explícitas, independentemente da posição no JSON.
Cada checkpoint depende das tarefas do próprio marco; o gate publicável depende
também de todos os checkpoints internos. Referências inválidas e ciclos falham.
Nenhuma tarefa recebe automaticamente o gate da versão anterior.

Escolha tarefas desbloqueadas, implemente em worktrees separados e integre PRs
coesos por merge commit após validação local. Até três frentes podem implementar
em paralelo com um integrador responsável pelos contratos compartilhados.
PRs funcionais podem fechar várias issues relacionadas. Checkpoints internos
fecham após conferir critérios e evidências de origem, sem candidata ou publicação.
`R11-GATE` fecha somente após a final publicada e conferida.

`validate` confere plano, gates e roadmap. Runners futuros com `command: null`
são pendências válidas, nunca resultados aprovados. Implemente-os junto das
funcionalidades. `roadmap` só escreve com `--write`.

`sync` simula por padrão; `--json` mostra os corpos completos e `--apply` permite
escritas. Confira a simulação antes de aplicar. Marcadores `sider:task` e
`sider:managed` são estáveis; texto fora do bloco gerenciado, comentários humanos,
labels não gerenciados e estados existentes são preservados. IDs duplicados ou
ambíguos interrompem o sync antes das escritas. O sincronizador não fecha issues
ou milestones. Após aplicar, nova simulação deve mostrar zero mudanças.
O cliente usa `gh api`, argumentos separados e JSON UTF-8, sem extração de token.

## Histórico e baseline interna

A [v0.1.0-rc.1](https://github.com/djairofilho/sider/releases/tag/v0.1.0-rc.1)
permanece como publicação histórica. A preparação da final 0.1 foi encerrada sem
publicação; não haverá outra candidata 0.1 neste fluxo. Tags, notas e evidências
anteriores continuam vinculadas aos seus SHAs e à política daquele momento.
Não transferir resultados antigos para o SHA atual nem reescrever assets históricos.

`R01-GATE` pode usar as entregas e validações já registradas, identificando os
SHAs de origem. A conclusão desse checkpoint técnico não é aprovação de um novo
build para publicação. Confira bundles históricos com a política do seu SHA.

Em R10, congele uma baseline interna com executável, dados de todos os tipos,
TTL e transações, configuração, formato AOF, toolchain, SHA e hashes. Preserve os
arquivos e o procedimento de backup/restauração. O gate de migração da 1.0 parte
dessa baseline, sem exigir uma release 0.10 final publicada. Também valide fixtures
de formato inicial, corrupção e versões desconhecidas conforme os contratos AOF.

O [runbook da baseline R10](internal-baseline.md) fixa o sidecar observacional de
build, as entradas do congelamento e a migração por pacotes extraídos. Preserve
o hash externo de `baseline.json` e nunca inicie um servidor sobre os diretórios
de dados congelados. O ensaio da mesma versão não substitui o gate da candidata.

## Preparar o build candidato 1.0

Conclua escopo, documentação e notas antes de construir a candidata. Atualize
`main` e tags e use a branch `chore/release-v1.0.0`. Fixe juntos a versão do
servidor em `Cargo.toml` e sua entrada em `Cargo.lock` como `1.0.0`; atualize
changelog e `releases/notes/v1.0.0.md`. O pacote `sider-xtask` é independente.
Confira `cargo metadata --locked --format-version 1` e preserve `publish = false`.

Abra o PR de preparação no próprio repositório para `main`, com `type:release`
e referência ao gate, sem fechá-lo. Integre por merge commit e registre seu SHA
completo. O merge não publica. Construa e valide esse SHA exato, sem substituir
por um `main` posterior. Use `CARGO_TARGET_DIR` isolado por SHA e plataforma para
os binários distribuídos; caches de downloads e imagens podem ser preservados.

Pacotes, binários e recibos já usam `1.0.0` desde a candidata. O número RC é
identidade de publicação, não versão do executável. Qualquer mudança no SHA ou
no conjunto de arquivos aprovado exige uma nova candidata.

## Gates do produto

[releases/gates.json](../releases/gates.json) define comandos como arrays de
argumentos sem shell. Cada checkpoint interno valida as capacidades afetadas;
a candidata 1.0 exige a matriz completa abaixo. Gate ausente, cancelado, ignorado,
com falha ou sem casos positivos bloqueia publicação.

| Gate | Plataforma obrigatória na candidata 1.0 |
| --- | --- |
| `native`, `tcp_smoke` | Linux GNU x86_64 e Windows MSVC x86_64 |
| `crash`, `recovery`, `migration` | Linux GNU x86_64 e Windows MSVC x86_64 |
| `compatibility`, `sharding`, `types`, `sorted_sets`, `transactions`, `pubsub`, `replication` | Linux GNU x86_64 |
| `docker`, `soak`, `benchmarks` | Linux GNU x86_64 |

O runner recebe `SIDER_REFERENCE_IMAGE`, `SIDER_RELEASE_VERSION=1.0.0`,
`SIDER_RELEASE_SHA`, `SIDER_RELEASE_TARGET` e `SIDER_RELEASE_DIR`.
O diretório de evidências deve ser novo. Recibos schema 1 registram
`version: "1.0.0"`, SHA, target, `status: "success"`, número positivo de casos
e detalhes reais. Não sintetize resultados ou reutilize recibos de outro SHA.

O soak dura pelo menos 3600 segundos. Benchmarks registram throughput,
p50/p95/p99, memória, pipelines, hot keys e quantidades de shards com configuração
reproduzível. Redis e `redis-cli` usam versão/digest fixados no plano. Normalize
somente respostas sem ordem garantida. Consulte [diferenciais](differential.md)
e [testes](testing.md).

O smoke executa o binário extraído com `SIDER_ADDR=127.0.0.1:0` e
`SIDER_READY_FILE`; confere PID, loopback, porta, `--version`, PING e operações TCP.
O servidor publica prontidão atomicamente. A [imagem Docker](docker.md) precisa
executar, ser exportada e restaurada; nenhum arquivo fictício conta como imagem
testada. O runner copia os quatro executáveis do pacote Linux validado e preserva
hashes e Image ID antes e depois de `docker load`.

## Conjunto imutável de arquivos

Siga o [guia de pacotes](packages.md): Linux GNU x86_64 em `.tar.gz`, produzido no
Ubuntu 24.04, e Windows MSVC x86_64 em `.zip`, com binário, README, MIT e avisos de
[releases/licenses/](../releases/licenses/). Inclua a imagem Docker exportada,
notas, requisitos, manifesto, evidências e `SHA256SUMS`.

O manifesto de artefatos usa schema 2 e `artifact_version: "1.0.0"`. Os nomes
usam `sider-v1.0.0-*` tanto na RC quanto na final. Recibos e o relatório local de
preflight pertencem a esse conjunto e não são reescritos durante a promoção.
Tag, status prerelease e aprovação da candidata ficam no GitHub ou em registro
externo ao conjunto. Não inserir `tag`, `prerelease` ou `approved_candidate`
no manifesto imutável.

```sh
cargo xtask verify-release 1.0.0-rc.1 <SHA_COMPLETO> <DIRETORIO_DOS_ASSETS>
cargo xtask verify-release 1.0.0 <SHA_COMPLETO> <DIRETORIO_DOS_ASSETS>
```

Os dois identificadores aceitam o mesmo build. O verificador confere nomes,
tamanhos, SHA-256, versão/SHA dos recibos, matriz de gates e ZIP de evidências.
Rejeita publicações de marcos internos e identidades divergentes. Não executa
binários nem confirma origem dos bytes, aprovação remota ou autorização de
publicação; seu resultado mantém `publication_authorized: false`.

## Publicar a candidata e promover a final

1. Confirme repositório privado, PR de preparação integrado, label, SHA, tarefas
   concluídas e matriz completa aprovada no build congelado.
2. Confira o bundle local com o identificador RC. Crie a tag `v1.0.0-rc.N` no SHA
   aprovado, respeitando a configuração de assinatura, e crie um draft privado.
3. Envie os arquivos sem `--clobber`. Baixe tudo em diretório novo, verifique o
   bundle e compare hashes com os originais e digests retornados pelo GitHub.
4. Publique a candidata como prerelease, nunca latest. Confira novamente a release
   publicada e registre aprovação com tag, SHA e hashes fora dos assets imutáveis.
5. Para promover, confirme a aprovação e crie `v1.0.0` no mesmo SHA. Publique os
   mesmos arquivos baixados da candidata. Não faça novo merge, bump, compilação,
   empacotamento ou soak. O identificador final deve passar no mesmo verificador.
6. Confira os downloads finais contra os da candidata, incluindo manifesto,
   evidências e checksums. Registre a promoção fora do diretório de assets.
   Somente então encerre `R11-GATE` e o milestone 1.0.

Qualquer mudança nos arquivos exige nova candidata e validação do build afetado.
Não mova tags nem sobrescreva publicações existentes. Mantenha uma publicação
por vez; após falha ou resposta perdida, releia o estado remoto antes de repetir.
Draft pode ser retomado. Falha ao comentar ou fechar milestone pede reconciliação
do backlog, não recompilação ou republicação.

A licença [MIT](../LICENSE) não torna o repositório ou os artefatos públicos.
A crate não é publicada no crates.io; a imagem é um asset privado, sem registry
público. CI futura depende de solicitação explícita e deve reutilizar os comandos
Rust existentes, sem reativação automática ao lançar a 1.0.
