# Sider

Sider é um projeto de servidor de banco de dados em memória, escrito em Rust.
O objetivo é oferecer um subconjunto explícito de compatibilidade
com Redis pelo protocolo RESP2. O nome é Redis ao contrário.

O binário atende strings, operações multichave, opções de `SET` e TTL por RESP2/TCP.
Hashes, listas, sets e sorted sets compartilham TTL, quota e persistência tipada.
Pub/Sub oferece canais binários, assinaturas por conexão e filas limitadas.
Transações de um shard oferecem `MULTI`, `EXEC`, `DISCARD`, `WATCH` e `UNWATCH`,
com um append AOF por lote e sem rollback de erros individuais de execução.
Workers proprietários serializam cada shard, com filas, conexões e buffers
limitados e quota lógica total de 64 MiB por padrão. AOF opcional oferece replay,
compactação global e migração offline de shards.

A [replicação assíncrona Sider → Sider](docs/replication.md) transfere snapshots
e lotes duráveis, retoma o histórico disponível e permite promoção manual.
Réplicas exigem a mesma versão e configuração de shards; Pub/Sub permanece local.
O [backup consistente](docs/backup.md) exporta dados enquanto o tráfego continua,
com verificação de integridade e restauração em um diretório novo.
Ainda não há autenticação, TLS ou failover automático; use um ambiente controlado.

O [guia de strings](docs/strings.md) descreve comandos e limites.
Os guias de [coleções](docs/collections.md) e [sorted sets](docs/sorted-sets.md)
descrevem os comandos dessas famílias e sua evidência diferencial.
A [matriz completa](docs/compatibility-matrix.md) reúne formas suportadas,
restrições e evidências, incluindo transações e sequências cruzadas entre famílias.
A referência diferencial usa Redis e `redis-cli` 8.10.1 fixados no plano. A
[candidata 0.1.0-rc.1](https://github.com/djairofilho/sider/releases/tag/v0.1.0-rc.1)
foi publicada no repositório privado e permanece como registro histórico.

As entregas até a 1.0 estão organizadas no [ROADMAP](ROADMAP.md), com 11 milestones,
50 tarefas de implementação. Os marcos 0.1–0.10 têm checkpoints técnicos;
somente a 1.0 terá candidata e final. O
[guia de releases](docs/releases.md) descreve como sincronizar o backlog e preparar
candidata e final com o mesmo SHA e os mesmos arquivos aprovados.
O [plano de execução até a v1](docs/execution-to-v1.md) resume o ponto atual,
a próxima entrega e os critérios de cada versão.

CI e publicação automática estão adiadas para depois da 1.0. Até a 1.0 inclusive,
o desenvolvimento usa validação local e as releases são publicadas manualmente.

`INFO` consulta métricas da instância e `sider --diagnose` valida a configuração
sem iniciar o servidor. O [guia operacional](docs/metrics.md) define os campos,
limites e procedimentos para filas cheias, clientes lentos e falhas de AOF.
Os [pacotes](docs/packages.md) incluem `sider`, `sider-aof-migrate`, `sider-backup`
e `sider-replica`. A [imagem Docker privada](docs/docker.md) copia esses mesmos
executáveis Linux, sem recompilar o servidor.

## Executar o servidor

Instale Rust com `rustup`. A toolchain e os componentes de desenvolvimento estão
definidos em [rust-toolchain.toml](rust-toolchain.toml); o `rustup` usa esse arquivo
ao executar os comandos no diretório do projeto.

```sh
git clone https://github.com/djairofilho/sider.git
cd sider
cargo run --locked -- --help
cargo run --locked -- --version
cargo run --locked
```

O repositório é privado. O clone exige acesso à conta ou uma autenticação Git
autorizada para o projeto.

Sem argumentos, o programa valida a configuração e abre o listener TCP.
Use Ctrl+C para encerrar. Em outro terminal, um cliente RESP2 pode enviar os
comandos suportados. Sem `SIDER_AOF_DIR`, dados são perdidos ao terminar o processo.
Com AOF, a recuperação termina antes do bind e segue a política de sincronização
descrita no [guia de persistência](docs/persistence.md).

### Configuração

| Variável | Padrão | Formato |
| --- | --- | --- |
| `SIDER_ADDR` | `127.0.0.1:6379` | Endereço IP e porta; IPv6 entre colchetes |
| `SIDER_READY_FILE` | Ausente | Arquivo novo de prontidão com PID, IP e porta efetiva |
| `SIDER_MAX_DATASET_BYTES` | `67108864` | Quota lógica positiva, distinta do RSS; sem eviction |
| `SIDER_SHARDS` | `1` | Entre 1 e 256 workers, com quota dividida e configuração fixa |
| `SIDER_AOF_DIR` | Ausente | Diretório exclusivo de dados; habilita AOF |
| `SIDER_AOF_SYNC` | `always` | `always` aguarda sync por lote; `everysec` sincroniza periodicamente |
| `SIDER_REPLICATION_ADDR` | Ausente | Listener interno de replicação, backup e administração; exige AOF |
| `SIDER_REPLICA_OF` | Ausente | IP e porta do upstream; configura a instância como réplica |
| `SIDER_REPLICATION_READY_FILE` | Ausente | Arquivo separado de prontidão do listener interno |

O endereço é validado de forma estrita. Use um IP, como `127.0.0.1:6380` ou
`[::1]:6380`, em vez de um hostname. Configuração inválida encerra o programa com
erro. A porta `0` solicita uma porta efêmera ao sistema. O
[guia de rede](docs/network.md) lista todos os limites e prazos `SIDER_*`,
seus padrões e o contrato do arquivo de prontidão.

Para experimentar outra configuração no PowerShell:

```powershell
$env:SIDER_ADDR = '127.0.0.1:6380'
cargo run --locked
Remove-Item Env:SIDER_ADDR
```

Em um shell POSIX:

```sh
SIDER_ADDR=127.0.0.1:6380 cargo run --locked
```

## Verificar as alterações

Durante a implementação, rode o teste afetado. Antes de integrar, use o comando
local que reúne formatação, Clippy, build do binário e testes nativos:

```sh
cargo test --locked <filtro>
cargo xtask check
```

Amplie a validação conforme a mudança: documentação e build de distribuição quando
suas interfaces ou configuração forem afetadas. Não repita `cargo check` depois
do Clippy, que já verifica os targets. Preserve o cache Cargo entre execuções.
O [guia de contribuição](CONTRIBUTING.md#verificações-locais) lista os comandos.
Não é necessário aguardar CI para integrar um PR. Registre os testes locais no PR.

Os testes cobrem a fundação, o codec, comandos, worker, TCP e o binário real.
As fixtures Redis são reproduzidas por TCP e pela suíte diferencial de servidores.
O [guia de testes](docs/testing.md) explica os comandos locais; o
[guia de diferenciais](docs/differential.md) cobre Redis/CLI e os recibos de gates.
Todos os testes novos usam Rust e os testes externos são opt-in.
Antes de cada release, os gates cumulativos continuam obrigatórios,
com execução manual e evidências nas plataformas previstas.

## Estrutura

| Caminho | Responsabilidade |
| --- | --- |
| `src/lib.rs` | Interface da biblioteca |
| `src/main.rs` | Entrada do binário, runtime, logs e sinais de parada |
| `src/config.rs` | Endereço, limites e prazos validados |
| `src/resp/` | Frames, limites, encoder atômico e decoder incremental |
| `src/command/` e `src/storage/` | Parser, respostas tipadas e mapa proprietário síncrono |
| `src/storage/worker.rs` | Fila limitada, aceitação e execução proprietária |
| `src/persistence/` e `src/storage/snapshot.rs` | Formato AOF, escritor, replay e snapshots globais |
| `src/bin/sider-aof-migrate.rs` | Migração offline explícita para novo diretório |
| `src/replication/` e `src/bin/sider-replica.rs` | Snapshot, histórico, retomada e administração de réplicas |
| `src/persistence/backup/` e `src/bin/sider-backup.rs` | Exportação, manifesto, verificação e restauração |
| `src/server.rs` e `src/connection.rs` | TCP, ordenação, timeouts e supervisão |
| `src/readiness.rs` | Publicação atômica do arquivo de prontidão |
| `src/error.rs` | Erros tipados da configuração |
| `tests/cli.rs` | Testes de integração do binário |
| `tests/reference.rs` e `tests/common/` | Fixtures binárias e referência Redis descartável |
| `tests/resp_codec.rs` | Testes literais, fragmentação e propriedades do codec |
| `tests/commands.rs` | Semântica dos cinco comandos e rejeições sem mutação |
| `tests/tcp.rs` | Fixtures por TCP, pipelines, fragmentação e ciclo das conexões |
| `tests/compatibility.rs` | Comparação independente Sider/Redis e integração com redis-cli |
| `tests/gate_contract.rs` e `tests/harness.rs` | Evidências, isolamento e falhas da infraestrutura de testes |
| `dev/test.Dockerfile` | Ambiente local Ubuntu para testes, distinto da imagem de distribuição |
| `Cargo.toml` e `Cargo.lock` | Pacote Rust e dependências fixadas |
| `rust-toolchain.toml` | Toolchain e componentes de desenvolvimento |
| `xtask/` e `.cargo/config.toml` | Ferramentas locais Rust, isoladas das dependências do banco |
| `AGENTS.md` | Instruções locais para agentes de programação |
| [PLANO.md](PLANO.md) | Registro histórico do desenho e das etapas iniciais da 0.1 |
| [ROADMAP.md](ROADMAP.md) | Sequência de releases e dependências até a 1.0 |
| [releases/plan.json](releases/plan.json) | Fonte versionada dos milestones, tarefas e critérios |
| [docs/releases.md](docs/releases.md) | Execução do backlog, candidatas, publicação e recuperação |
| [docs/architecture.md](docs/architecture.md) | Fronteiras atuais e arquitetura planejada |
| [docs/compatibility.md](docs/compatibility.md) | Escopo e estado da compatibilidade |
| [docs/compatibility-matrix.md](docs/compatibility-matrix.md) | Formas suportadas, restrições e evidência por capacidade |
| [docs/testing.md](docs/testing.md) | Testes locais e reprodução da referência Redis |
| [docs/resp.md](docs/resp.md) | Contratos, limites e uso do codec RESP2 |
| [docs/network.md](docs/network.md) | Configuração TCP, aceitação, timeouts e encerramento |
| [docs/pubsub.md](docs/pubsub.md) | Canais binários, modo assinante, filas limitadas e diferencial Pub/Sub |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Fluxo de trabalho e critérios de revisão |

## Próximo passo

As funcionalidades de strings, TTL, coleções, [transações](docs/transactions.md),
Pub/Sub, AOF, shards, replicação, backup e administração estão integradas.
A estabilização reúne a baseline interna R10, a migração entre executáveis,
a auditoria de compatibilidade, o [soak](docs/soak.md) e os
[benchmarks](docs/benchmarks.md). Resultados de desenvolvimento permanecem
vinculados aos seus SHAs; os runners presentes não significam gates aprovados.

Depois de concluir esses critérios, o PR de preparação fixará a versão `1.0.0`.
O SHA exato do merge será compilado, empacotado e validado nas duas plataformas
para publicar a candidata. A final promoverá os mesmos arquivos aprovados.
O [plano da 0.1](PLANO.md) preserva o desenho histórico; o
[ROADMAP](ROADMAP.md) e as issues registram os critérios e o andamento atual.

Banco, testes e ferramentas próprias usam Rust. Não há scripts Python nem workflows
de CI no projeto. Ao alterar as ferramentas ou o manifesto, use:

```sh
cargo xtask check --tools
cargo xtask roadmap --write
cargo xtask sync
```

`sync` simula; só `sync --apply` escreve no GitHub pela CLI `gh` autenticada.
O [guia de releases](docs/releases.md) separa o ciclo rápido dos testes de publicação.
A publicação 1.0 exige candidata e evidências de todas as capacidades.
Gates pendentes bloqueiam a publicação, mesmo quando os testes do bootstrap passam.

## Licença

O código próprio do Sider está sob a [licença MIT](LICENSE), com o texto padrão
da [Open Source Initiative](https://opensource.org/license/mit).
As dependências preservam suas próprias licenças.

Os pacotes distribuídos incluem o arquivo `LICENSE`. A adoção da licença não altera
a visibilidade privada do repositório nem habilita publicação no crates.io.
