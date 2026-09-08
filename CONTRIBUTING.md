# Como contribuir com o Sider

O desenvolvimento segue as etapas de [PLANO.md](PLANO.md). Cada entrega deve
compilar, passar nas verificações disponíveis e documentar seu comportamento
real. Uma etapa só termina quando seus critérios de saída forem verificados.

## Preparar o ambiente

Use `rustup` com a toolchain declarada em
[rust-toolchain.toml](rust-toolchain.toml). Execute os comandos Cargo na raiz e
preserve `Cargo.lock`, pois o projeto distribui um binário. Atualize dependências
de forma deliberada e revise as mudanças no lockfile.

O bootstrap não exige Redis, Docker ou um serviço em execução. As etapas futuras
de compatibilidade terão instruções próprias para dependências externas.

## Fluxo de trabalho

1. Atualize a branch `main` e abra uma branch com uma responsabilidade clara, como
   `feat/resp2-decoder` ou `docs/compatibility-matrix`.
2. Implemente uma parte revisável da etapa, com os testes necessários para provar
   o comportamento. Crie módulos conforme surgirem consumidores reais.
3. Execute as verificações locais e confira o diff, incluindo arquivos novos.
4. Organize commits atômicos. Cada commit deve deixar o projeto compilando e com
   as verificações disponíveis passando.
5. Abra um pull request para `main`, descrevendo o problema, o comportamento
   resultante, os testes executados e eventuais limites conhecidos.

Use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/):

```text
feat(config): validate the initial server address
test(resp): cover fragmented bulk strings
docs(compatibility): record supported command forms
```

Separe responsabilidades independentes quando cada parte puder ser revisada e
revertida sozinha. Mantenha o código junto de seus testes e artefatos gerados
necessários. A integração de pull requests usa merge commit por padrão.

## Verificações locais

```sh
cargo fmt --all --check
cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo doc --locked --no-deps
cargo build --locked --release
```

As verificações de CI cobrem Linux e Windows. A aprovação em uma plataforma não
substitui a execução na outra. Registre falhas ou testes que dependam de ferramentas
ausentes; ausência de execução não conta como teste aprovado.

## Testes e fronteiras

Teste primeiro a menor unidade que expõe o comportamento: configuração sem rede,
codec sem armazenamento e armazenamento sem runtime. Use fixtures literais quando
for necessário conferir os bytes do protocolo.

Nos testes de configuração, passe valores explicitamente para o parser ou execute
o binário em um processo filho com ambiente próprio. Não altere o ambiente global
do processo de testes com `std::env::set_var` ou `std::env::remove_var`: isso
introduz interferência entre testes paralelos e exige `unsafe` na Edition 2024.

Quando os testes de TCP existirem, use portas efêmeras e sincronização explícita.
Não dependa de uma porta fixa disponível ou de pausas arbitrárias para coordenar
tarefas. Comparações com Redis devem usar instâncias descartáveis e uma versão de
referência registrada na matriz de compatibilidade.

## Código e documentação

O código próprio proíbe `unsafe`. Trate entradas inválidas com erros tipados e
evite panics no caminho de dados do cliente. Introduza dependências quando houver
uso concreto e mantenha suas funcionalidades habilitadas no mínimo necessário.

Escreva a documentação em português brasileiro com acentos corretos. Preserve
identificadores, caminhos e comandos. Antes de enviar uma alteração, confira se
o texto não contém caracteres corrompidos por codificação.

Atualize [docs/architecture.md](docs/architecture.md) quando uma fronteira mudar e
[docs/compatibility.md](docs/compatibility.md) quando houver evidência nova de
comportamento compatível. Registre funcionalidades planejadas como planejadas;
não marque uma etapa inteira como concluída por ter implementado apenas parte dela.
