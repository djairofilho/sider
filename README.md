# Sider

Sider é um projeto de servidor de banco de dados em memória, escrito em Rust.
O objetivo da versão 0.1 é oferecer um subconjunto explícito de compatibilidade
com Redis pelo protocolo RESP2. O nome é Redis ao contrário.

O projeto está na fundação: pacote Rust com biblioteca, binário, configuração
validada e verificações automatizadas. O binário ainda não abre uma conexão de
escuta TCP e nenhum comando Redis está implementado.

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

Execute na raiz do projeto:

```sh
cargo fmt --all --check
cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo doc --locked --no-deps
cargo build --locked --release
```

A CI executa as verificações em Linux e Windows. Os testes atuais cobrem a
fundação do projeto; ainda não demonstram compatibilidade com Redis. Os testes de
RESP2, TCP, comparação com Redis e fuzzing serão adicionados nas suas etapas.

## Estrutura

| Caminho | Responsabilidade |
| --- | --- |
| `src/lib.rs` | Interface da biblioteca |
| `src/main.rs` | Entrada do binário e tratamento dos argumentos iniciais |
| `src/config.rs` | Configuração inicial e validação do endereço |
| `src/error.rs` | Erros tipados da configuração |
| `tests/cli.rs` | Testes de integração do binário |
| `Cargo.toml` e `Cargo.lock` | Pacote Rust e dependências fixadas |
| `rust-toolchain.toml` | Toolchain e componentes de desenvolvimento |
| `.github/workflows/` | Verificações automatizadas |
| `AGENTS.md` | Instruções locais para agentes de programação |
| [PLANO.md](PLANO.md) | Etapas, contratos e critérios de conclusão da versão 0.1 |
| [docs/architecture.md](docs/architecture.md) | Fronteiras atuais e arquitetura planejada |
| [docs/compatibility.md](docs/compatibility.md) | Escopo e estado da compatibilidade |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Fluxo de trabalho e critérios de revisão |

## Próximo passo

Concluir os itens pendentes da fundação e implementar o codec RESP2 isolado:
tipos de frame, limites, encoder e decoder incremental. Os testes precisam cobrir
fragmentação, frames concatenados, conteúdo binário e entradas inválidas antes da
integração com a rede.

O alvo da versão 0.1 inclui `PING`, `ECHO`, `GET`, `SET` básico e `DEL`, com um
único worker de armazenamento. TTL, persistência e múltiplos shards pertencem às
versões seguintes. O [plano completo](PLANO.md) detalha essa sequência.
