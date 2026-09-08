# Como contribuir com o Sider

O desenvolvimento segue o [ROADMAP](ROADMAP.md), cujo manifesto é
[releases/plan.json](releases/plan.json). Os contratos da 0.1 estão em
[PLANO.md](PLANO.md). Cada entrega deve compilar, passar nas verificações
disponíveis e documentar seu comportamento real. Uma etapa só termina quando
seus critérios de saída forem verificados.

## Preparar o ambiente

Use `rustup` com a toolchain declarada em
[rust-toolchain.toml](rust-toolchain.toml). Execute os comandos Cargo na raiz e
preserve `Cargo.lock`, pois o projeto distribui um binário. Atualize dependências
de forma deliberada e revise as mudanças no lockfile.

O bootstrap não exige Redis, Docker ou um serviço em execução. As etapas futuras
de compatibilidade terão instruções próprias para dependências externas.

O desenvolvimento e os testes do banco exigem apenas a toolchain Rust e as
dependências específicas da tarefa. Os helpers opcionais de backlog e releases
usam Python 3.11 ou posterior e sua biblioteca padrão. Operações no GitHub usam
`gh` autenticado na conta com acesso ao repositório privado.

CI e publicação automática ficam adiadas para depois da 1.0. Os workflows estão
inativos em [.github/workflows-disabled/](.github/workflows-disabled/).

## Fluxo de trabalho

1. Selecione a próxima issue desbloqueada do milestone atual, atualize a branch
   `main` e abra uma branch com uma responsabilidade clara, como
   `feat/resp2-decoder` ou `docs/compatibility-matrix`.
2. Implemente uma parte revisável da etapa, com os testes necessários para provar
   o comportamento. Crie módulos conforme surgirem consumidores reais.
3. Execute as verificações locais e confira o diff, incluindo arquivos novos.
4. Organize commits atômicos. Cada commit deve deixar o projeto compilando e com
   as verificações disponíveis passando.
5. Abra um pull request para `main` vinculado à issue, descrevendo o problema, o
   comportamento resultante, os testes executados e eventuais limites conhecidos.
6. Integre por merge commit após validação local, sem aguardar CI. Atualize a matriz
   de compatibilidade e as notas conforme cada capacidade for comprovada.

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

Use este ciclo rápido durante a implementação:

```sh
cargo fmt --all --check
cargo check --locked --all-targets
cargo test --locked
```

Amplie a validação conforme a responsabilidade alterada:

```sh
# Código Rust e seus testes
cargo clippy --locked --all-targets -- -D warnings

# Interfaces, documentação de API ou configuração de build
cargo doc --locked --no-deps
cargo build --locked --release

# Somente ao alterar o manifesto ou as ferramentas de backlog/releases
python -m tools.release.cli validate
python -m unittest discover -s tools/release -t . -p "test_*.py"
```

Não há execução automática em Linux ou Windows nesta fase. Antes de publicar uma
release, execute manualmente todos os gates exigidos nas plataformas do manifesto.
Uma aprovação em uma plataforma não substitui a outra. Registre comandos, SHA,
ambiente, resultados e testes pendentes no PR ou na issue de publicação.
Ferramenta ausente e teste não executado não contam como aprovação.

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

## Backlog e releases

Edite o manifesto e regenere sua projeção com `python -m tools.release.plan --write`.
IDs como `R01-01` são estáveis e não devem ser reutilizados. Datas de milestones
não são obrigatórias. Patches precisam de uma entrada própria no manifesto e de
candidata, assim como minors e a 1.0.

O modo padrão apresenta a sincronização planejada; `--apply` grava no GitHub:

```sh
python -m tools.release.cli sync
python -m tools.release.cli sync --apply
```

Quando todas as tarefas funcionais estiverem concluídas, prepare as notas UTF-8
e a candidata conforme o [guia de releases](docs/releases.md). Até a 1.0 inclusive,
o merge do PR `chore/release-v<versão>`, com label `type:release`, não publica nada.
Um responsável valida e publica manualmente o SHA exato integrado. Toda versão
final exige uma candidata aprovada; alterações funcionais posteriores exigem outra RC.

Teste obrigatório ausente, ignorado, cancelado ou sem evidência bloqueia a release.
Nenhuma release funcional é criada para o bootstrap. O milestone só encerra após
conferir a publicação final e os pacotes privados Linux/Windows; a 0.10 acrescenta
imagem Docker exportada. Publicação da crate permanece desabilitada.
