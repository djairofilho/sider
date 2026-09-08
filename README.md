# Sider

Sider é um projeto de servidor de banco de dados em memória, escrito em Rust.
O objetivo da versão 0.1 é oferecer um subconjunto explícito de compatibilidade
com Redis pelo protocolo RESP2. O nome é Redis ao contrário.

O binário atende `PING`, `ECHO`, `GET`, `SET` básico e `DEL` por RESP2/TCP.
Um worker proprietário serializa o armazenamento, com filas, conexões e buffers
limitados. É um protótipo local, sem persistência, autenticação ou quota do dataset.
A comparação diferencial com Redis e o fuzz da primeira release ainda estão pendentes.

As entregas até a 1.0 estão organizadas no [ROADMAP](ROADMAP.md), com 11 milestones,
50 tarefas de implementação e um gate de publicação por versão. O
[guia de releases](docs/releases.md) descreve como sincronizar o backlog e preparar
candidatas e versões finais. O bootstrap não será publicado como banco funcional.
O [plano de execução até a v1](docs/execution-to-v1.md) resume o ponto atual,
a próxima entrega e os critérios de cada versão.

CI e publicação automática estão adiadas para depois da 1.0. Até a 1.0 inclusive,
o desenvolvimento usa validação local e as releases são publicadas manualmente.

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
cinco comandos suportados. Dados em memória são perdidos ao terminar o processo.

### Configuração

| Variável | Padrão | Formato |
| --- | --- | --- |
| `SIDER_ADDR` | `127.0.0.1:6379` | Endereço IP e porta; IPv6 entre colchetes |
| `SIDER_READY_FILE` | Ausente | Arquivo novo de prontidão com PID, IP e porta efetiva |

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

Para o ciclo rápido, execute na raiz do projeto:

```sh
cargo fmt --all --check
cargo check --locked --all-targets
cargo test --locked
```

Amplie a validação conforme a mudança: Clippy para código Rust; documentação e
build de distribuição quando suas interfaces ou configuração forem afetadas.
O [guia de contribuição](CONTRIBUTING.md#verificações-locais) lista os comandos.
Não é necessário aguardar CI para integrar um PR. Registre os testes locais no PR.

Os testes cobrem a fundação, o codec, comandos, worker, TCP e o binário real.
As fixtures Redis são reproduzidas por TCP, mas a suíte diferencial de servidores
e o fuzz ainda pertencem a R01-05. O [guia de testes](docs/testing.md) explica
os comandos locais e a verificação externa opt-in, todos escritos em Rust.
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
| `src/server.rs` e `src/connection.rs` | TCP, ordenação, timeouts e supervisão |
| `src/readiness.rs` | Publicação atômica do arquivo de prontidão |
| `src/error.rs` | Erros tipados da configuração |
| `tests/cli.rs` | Testes de integração do binário |
| `tests/reference.rs` e `tests/common/` | Fixtures binárias e referência Redis descartável |
| `tests/resp_codec.rs` | Testes literais, fragmentação e propriedades do codec |
| `tests/commands.rs` | Semântica dos cinco comandos e rejeições sem mutação |
| `tests/tcp.rs` | Fixtures por TCP, pipelines, fragmentação e ciclo das conexões |
| `Cargo.toml` e `Cargo.lock` | Pacote Rust e dependências fixadas |
| `rust-toolchain.toml` | Toolchain e componentes de desenvolvimento |
| `.github/workflows-disabled/` | Workflows inativos, preservados para revisão depois da 1.0 |
| `AGENTS.md` | Instruções locais para agentes de programação |
| [PLANO.md](PLANO.md) | Etapas, contratos e critérios de conclusão da versão 0.1 |
| [ROADMAP.md](ROADMAP.md) | Sequência de releases e dependências até a 1.0 |
| [releases/plan.json](releases/plan.json) | Fonte versionada dos milestones, tarefas e critérios |
| [docs/releases.md](docs/releases.md) | Execução do backlog, candidatas, publicação e recuperação |
| [docs/architecture.md](docs/architecture.md) | Fronteiras atuais e arquitetura planejada |
| [docs/compatibility.md](docs/compatibility.md) | Escopo e estado da compatibilidade |
| [docs/testing.md](docs/testing.md) | Testes locais e reprodução da referência Redis |
| [docs/resp.md](docs/resp.md) | Contratos, limites e uso do codec RESP2 |
| [docs/network.md](docs/network.md) | Configuração TCP, aceitação, timeouts e encerramento |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Fluxo de trabalho e critérios de revisão |

## Próximo passo

`R01-01` entrega fixtures literais verificadas com Redis e `redis-cli` 8.10.1,
na imagem fixada no manifesto: oito casos, 48 trocas sequenciais e oito pipelines.
`R01-02` entrega o [codec RESP2 isolado](docs/resp.md), com tipos, limites,
encoder atômico, decoder incremental e testes de propriedades.
`R01-03` entrega parsing e armazenamento síncrono dos cinco comandos, com
validação das fixtures e das divergências de `SET`/comando desconhecido.
`R01-04` conecta o núcleo ao worker e ao TCP, com configuração, timeouts,
prontidão e encerramento supervisionado. A próxima tarefa é `R01-05`:
comparação diferencial Sider/Redis, integração com `redis-cli` e fuzz.

O alvo da versão 0.1 inclui `PING`, `ECHO`, `GET`, `SET` básico e `DEL`, com um
único worker de armazenamento. TTL, persistência e múltiplos shards pertencem às
versões seguintes. O [plano da 0.1](PLANO.md) detalha os contratos técnicos e o
[ROADMAP](ROADMAP.md) organiza as versões posteriores.

O desenvolvimento e os testes do banco usam Cargo, sem exigir Python. Apenas quem
alterar ou executar os helpers opcionais de backlog e releases precisa de Python
3.11 ou posterior, sem dependências adicionais:

```sh
python -m tools.release.cli validate
python -m unittest discover -s tools/release -t . -p "test_*.py"
```

Releases exigem candidata e evidências dos testes específicos da capacidade.
Gates pendentes bloqueiam a publicação, mesmo quando os testes do bootstrap passam.

## Licença

O código próprio do Sider está sob a [licença MIT](LICENSE), com o texto padrão
da [Open Source Initiative](https://opensource.org/license/mit).
As dependências preservam suas próprias licenças.

Os pacotes distribuídos incluem o arquivo `LICENSE`. A adoção da licença não altera
a visibilidade privada do repositório nem habilita publicação no crates.io.
