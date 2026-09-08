# Sider

Sider é um projeto de servidor de banco de dados em memória, escrito em Rust.
O objetivo da versão 0.1 é oferecer um subconjunto explícito de compatibilidade
com Redis pelo protocolo RESP2. O nome é Redis ao contrário.

O projeto tem pacote Rust com biblioteca, binário, configuração validada,
codec RESP2 isolado e testes locais. O binário ainda não abre uma conexão de
escuta TCP e nenhum comando Redis está implementado.

As entregas até a 1.0 estão organizadas no [ROADMAP](ROADMAP.md), com 11 milestones,
50 tarefas de implementação e um gate de publicação por versão. O
[guia de releases](docs/releases.md) descreve como sincronizar o backlog e preparar
candidatas e versões finais. O bootstrap não será publicado como banco funcional.

CI e publicação automática estão adiadas para depois da 1.0. Até a 1.0 inclusive,
o desenvolvimento usa validação local e as releases são publicadas manualmente.

## Executar o bootstrap

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

Sem argumentos, o programa valida a configuração e informa que o servidor TCP
ainda não foi implementado. Essa execução não aceita clientes nem armazena dados.

### Configuração inicial

| Variável | Padrão | Formato |
| --- | --- | --- |
| `SIDER_ADDR` | `127.0.0.1:6379` | Endereço IP e porta; IPv6 entre colchetes |

O endereço é validado de forma estrita. Use um IP, como `127.0.0.1:6380` ou
`[::1]:6380`, em vez de um hostname. Configuração inválida encerra o programa com
erro. A porta `0` é permitida para futuros testes com portas efêmeras. Neste
estágio, o endereço é validado, mas nenhuma porta é aberta.

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

Os testes atuais cobrem a fundação, o codec RESP2 e as fixtures Redis; ainda não
demonstram compatibilidade do Sider. O [guia de testes](docs/testing.md) explica a
verificação externa opt-in, escrita em Rust. Testes de TCP, comparação Sider
versus Redis e fuzzing serão adicionados nas suas etapas.
Antes de cada release, os gates cumulativos continuam obrigatórios,
com execução manual e evidências nas plataformas previstas.

## Estrutura

| Caminho | Responsabilidade |
| --- | --- |
| `src/lib.rs` | Interface da biblioteca |
| `src/main.rs` | Entrada do binário e tratamento dos argumentos iniciais |
| `src/config.rs` | Configuração inicial e validação do endereço |
| `src/resp/` | Frames, limites, encoder atômico e decoder incremental |
| `src/error.rs` | Erros tipados da configuração |
| `tests/cli.rs` | Testes de integração do binário |
| `tests/reference.rs` e `tests/common/` | Fixtures binárias e referência Redis descartável |
| `tests/resp_codec.rs` | Testes literais, fragmentação e propriedades do codec |
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
| [CONTRIBUTING.md](CONTRIBUTING.md) | Fluxo de trabalho e critérios de revisão |

## Próximo passo

`R01-01` entrega fixtures literais verificadas com Redis e `redis-cli` 8.10.1,
na imagem fixada no manifesto: oito casos, 48 trocas sequenciais e oito pipelines.
`R01-02` entrega o [codec RESP2 isolado](docs/resp.md), com tipos, limites,
encoder atômico, decoder incremental e testes de propriedades.
A próxima tarefa é `R01-03`: parser e armazenamento síncrono dos cinco comandos,
sem integrar rede ou tarefas assíncronas ainda.

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
