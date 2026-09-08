# Executar e publicar releases do Sider sem CI

CI e publicação automática estão adiadas para depois da 1.0. Até a 1.0 inclusive,
o desenvolvimento é validado localmente e a publicação é manual. Integrar um PR
não inicia build, testes ou publicação. Os workflows estão preservados, inativos,
em [.github/workflows-disabled/](../.github/workflows-disabled/).

## Índice

- [Estado e fontes](#estado-e-fontes)
- [Backlog e ciclo de execução](#backlog-e-ciclo-de-execução)
- [Preparar uma candidata ou final](#preparar-uma-candidata-ou-final)
- [Gates do produto](#gates-do-produto)
- [Publicação manual](#publicação-manual)
- [Automação reservada para depois da 1.0](#automação-reservada-para-depois-da-10)
- [Validação da automação](#validação-da-automação)

## Estado e fontes

O bootstrap não é uma versão funcional. Nenhuma release é criada para testar esta
automação. A primeira publicação será `v0.1.0-rc.1`, depois de concluir o núcleo
RESP2, os testes diferenciais e o fuzz. O pacote mantém `publish = false`.

[releases/plan.json](../releases/plan.json) é a fonte dos 11 milestones, das 50
tarefas funcionais, dos 11 gates de publicação e do bootstrap comprovado.
[ROADMAP.md](../ROADMAP.md) é gerado; o estado corrente das tarefas está no GitHub.
Não edite o roadmap manualmente nem marque funcionalidades pendentes como prontas.

O banco é desenvolvido e testado com Cargo, sem exigir Python. Os helpers opcionais
de backlog e releases usam Python 3.11+ com biblioteca padrão, Git, Cargo e a
autenticação da CLI `gh`. Eles permanecem disponíveis, mas não precisam rodar a
cada alteração no banco. Não há migração de linguagem nesta mudança.

O repositório precisa continuar privado; não há publicação no crates.io ou em
registry público. Os únicos uploads permitidos são no repositório privado.

## Backlog e ciclo de execução

Ao alterar o manifesto ou sincronizar o GitHub, use os helpers existentes:

```sh
python -m tools.release.cli validate
python -m tools.release.cli render --write
python -m tools.release.sync
python -m tools.release.sync --apply
python -m tools.release.sync
```

Sem `--apply`, a sincronização só mostra alterações previstas. Uma repetição após
aplicar deve informar zero mudanças. São criados labels por tipo e área,
`status:blocked` e `compatibility:breaking`, sem inventar datas de entrega.

Os identificadores `R01-01`, `R01-GATE` e `B00-01` ficam em marcadores HTML nas
issues. A sincronização preserva texto fora do bloco `sider:managed` e nunca edita
comentários humanos. Não remova os marcadores nem copie um ID para outra issue.
Duplicatas interrompem a execução. Se uma requisição falhar, execute novamente:
recursos já criados serão encontrados pelos mesmos IDs.

1. Escolha a primeira tarefa aberta sem `status:blocked` no milestone atual.
2. Implemente em branch própria, com testes e commits atômicos.
3. Execute `cargo fmt --check`, `cargo check --locked` e `cargo test --locked`.
   Amplie a validação conforme a mudança e registre comandos e resultados no PR.
   Vincule a tarefa funcional com `Closes #N`; não há CI a aguardar.
4. Integre por merge commit. Atualize compatibilidade, arquitetura e changelog.
5. Execute `sync --apply` para atualizar as dependências e os labels.
6. Com todas as tarefas funcionais concluídas, prepare a candidata.
7. Não feche a issue de publicação nem o milestone ao integrar o PR de release.
   O responsável fecha ambos somente depois de conferir a versão final publicada.

O bootstrap só é fechado após a API confirmar os commits e a execução histórica
de CI registrados no manifesto. Essa evidência continua válida como histórico;
não significa que a CI esteja ativa. Fechar uma tarefa não substitui seus testes.

Patches exigem uma entrada explícita nova no manifesto, com ID estável novo,
dependência da versão-base, tarefa da correção, testes de regressão e gate próprio.
Execute `render --write` e `sync --apply`. Patches também passam por RC. Não reutilize
nem renumere IDs; novas capacidades entram em minor. Incompatibilidades antes da
1.0 só entram em minors e devem constar nas notas e no label correspondente.

## Preparar uma candidata ou final

Atualize `main`, busque as tags e escreva notas UTF-8. O primeiro título deve ser exatamente
`# Sider v0.1.0-rc.1`. Inclua comandos entregues, limitações, incompatibilidades,
garantias de durabilidade e links para testes. Não deixe conteúdo pendente.

```sh
git switch main
git pull --ff-only
git fetch origin --tags
```

Prepare uma branch `chore/release-v0.1.0-rc.1`. Atualize a versão no `Cargo.toml`,
a entrada do próprio pacote nos arquivos `Cargo.lock` e `fuzz/Cargo.lock`, o `CHANGELOG.md` e
`releases/notes/v0.1.0-rc.1.md`. Preserve `publish = false` e as versões das
dependências. Abra um PR com label `type:release`, referenciando o gate do milestone
sem fechá-lo pelo merge. Esses cinco arquivos representam uma responsabilidade:
identificar a mesma versão em todas as superfícies.

Não mude a versão `0.0.0` do pacote `sider-fuzz`. Nos dois lockfiles, somente a
entrada local `sider` acompanha a versão do produto. Confira o diff inteiro e
execute `cargo metadata --locked --format-version 1` para cada manifesto,
incluindo `--manifest-path fuzz/Cargo.toml`. Qualquer outra mudança de dependência
é funcional e exige outra candidata.

Use a preparação manual nesta fase. O helper `prepare` arquivado antecede o
workspace de fuzz e ainda só atualiza o lockfile raiz; sua política de promoção
também não aceita o segundo lockfile. Não use `prepare --apply` ou o publicador
automatizado como substitutos deste procedimento. A revisão desses caminhos,
assim como proveniência local e `CARGO_TARGET_DIR`, faz parte da retomada posterior
da automação. O sincronizador de backlog e sua validação continuam disponíveis.

Para consultar a simulação histórica do helper `prepare`, salve as notas em
`target/notes-0.1.0-rc.1.md` e execute primeiro a simulação:

```sh
python -m tools.release.cli prepare 0.1.0-rc.1 --notes-file target/notes-0.1.0-rc.1.md
```

No fluxo histórico, `--apply` autoriza o helper a criar branch, atualizar quatro
arquivos, criar commit, enviar push e abrir o PR. Não use essa opção com o workspace
atual de fuzz. O helper exige worktree limpa,
`main` igual a `origin/main` e tarefas funcionais concluídas. Não publica a release
e não dispensa os testes. O preparo manual atual mantém esses contratos e inclui
o segundo lockfile conforme descrito acima.

Se a preparação parar depois de criar a branch ou enviar o push, preserve o trabalho
e retome a branch/PR existente. Ela não apaga, reseta nem substitui branches.
Se o POST do PR perder a resposta, procure a branch no GitHub antes de abrir outro.

Para a final, repita manualmente com `0.1.0` e notas próprias. É obrigatória uma RC publicada da
mesma versão-base, ancestral do commit final. O diff só aceita versão do pacote,
versão do próprio pacote nos dois lockfiles, changelog e notas da final. Mudança de código,
dependência, workflow ou gate exige uma nova RC. A final compila e testa novamente.

Valide e revise o PR localmente. A conta do usuário pode integrá-lo em `main` com:

```sh
gh pr merge NUMERO --repo djairofilho/sider --merge
```

O merge não dispara publicação. Registre seu SHA exato e siga a
[publicação manual](#publicação-manual), sem substituí-lo pelo `main` mais recente.

## Gates do produto

[releases/gates.json](../releases/gates.json) registra comandos ainda pendentes
como `null`. Isso é uma falha explícita, nunca um teste ignorado aprovado.
A tarefa responsável pela capacidade deve implementar o runner do gate e seus testes.

Adiar CI não adia os testes do produto. Antes de cada publicação, o responsável
executa manualmente os gates cumulativos no SHA da release e registra as evidências.
Eles não precisam ser executados a cada edição. Ausência, cancelamento, teste
ignorado ou falha mantém a publicação bloqueada.

| Gates | Execução |
| --- | --- |
| `native`, `tcp_smoke` | Linux GNU e Windows MSVC; executável realmente extraído |
| `compatibility`, `fuzz` | Linux desde 0.1; Redis/CLI e imagem fixados no manifesto |
| `crash`, `recovery`, `migration` | Linux e Windows desde 0.3 |
| `sharding`, `types`, `sorted_sets`, `transactions`, `pubsub`, `replication` | Linux, cumulativos a partir da respectiva capacidade |
| `docker` | Linux desde 0.10, com execução e exportação da imagem amd64 |
| `soak`, `benchmarks` | Linux na 1.0, além de todos os anteriores |

Um comando de gate é um array de argumentos, executado sem shell. Recebe:

- `SIDER_REFERENCE_IMAGE`: Redis com tag e digest fixos.
- `SIDER_RELEASE_VERSION`, `SIDER_RELEASE_SHA`, `SIDER_RELEASE_TARGET`.
- `SIDER_RELEASE_DIR`: diretório novo para resultados desta execução.

Ao concluir com sucesso, o comando deve escrever `receipt-ID.json` nesse diretório,
com `sha`, `status: "success"` e `cases` inteiro positivo. Registre também seeds,
cenários, versão da ferramenta e resultados relevantes no recibo e nos logs. O
runner recusa recibos antigos, zero casos, SHA divergente e processo com erro.

Fuzz precisa executar por pelo menos 900 segundos em cada candidata e também na
revalidação final. O soak da 1.0 exige 3600 segundos; benchmarks devem registrar
throughput, p50/p95/p99, memória, pipelines, hot keys e quantidades de shards.
O teste diferencial normaliza conteúdo de respostas sem ordem garantida, mas
confere a ordem em respostas ordenadas.

A migração da 0.3 começa por fixtures versionadas de AOF e recuperação na mesma
versão, pois a 0.2 não tem persistência. Das versões seguintes em diante, também
abre dados reais produzidos pela versão anterior. Não confunda versão do produto
com versão do formato de persistência.

O gate `docker` deve produzir
`sider-vVERSAO-linux-amd64-image.tar.gz`, exportado após executar e testar a imagem,
sem registry público. Build, smoke, `docker save` e restauração da imagem pertencem
à tarefa da 0.10; nenhum arquivo fictício é aceito como evidência.

### Contrato de prontidão do binário extraído

A tarefa `R01-04` implementa `SIDER_READY_FILE`. O smoke inicia o binário extraído
com `SIDER_ADDR=127.0.0.1:0`. Depois de abrir o listener, o próprio processo grava
atomicamente no caminho indicado um JSON com `pid`, `host: "127.0.0.1"` e `port`
efetiva. O smoke confere o PID, conecta nessa porta e exige `+PONG\r\n` para um PING
RESP2. Isso evita reservar e liberar uma porta antes de iniciar o servidor.
O bootstrap não oferece prontidão nem TCP e deve falhar nesse gate.

Os gates Rust de compatibilidade e fuzz, seus recibos e o ambiente Linux estão
descritos no [guia de diferenciais](differential.md) e no [guia de fuzz](../fuzz/README.md).

## Publicação manual

Não há workflow ativo para publicar. O responsável pela release executa e registra
estas verificações antes de usar a interface ou a CLI do GitHub:

1. Confirme que o PR veio do próprio repositório, foi integrado em `main` e tem
   branch, label e versão válidos. Fixe o SHA exato do merge para todos os builds.
2. Confirme as tarefas funcionais, os milestones anteriores e todos os gates da
   versão. Para a final, confira a candidata aprovada e a ausência de mudanças
   funcionais posteriores. Não use testes do bootstrap como evidência do banco.
3. Compile e teste esse mesmo SHA em Linux GNU x86_64 no Ubuntu 24.04 e em Windows
   MSVC x86_64. Execute os gates nas plataformas da tabela, com logs e resultados.
4. Empacote binário, README e LICENSE em `.tar.gz` no Linux e `.zip` no Windows.
   Use o README de distribuição em `releases/README.md` e inclua `releases/licenses/`
   como `licenses/`, com todos os avisos e hashes conferidos. Extraia cada pacote e
   execute o [smoke Rust](packages.md) de `--version` e TCP. Desde a 0.10, teste e exporte
   também a imagem Docker Linux amd64 em arquivo compactado.
5. Prepare `release-manifest.json`, `release-notes.md` e `SHA256SUMS`. Registre
   versão, SHA, toolchain, targets, comandos, ambientes e resultados reais. O
   manifesto deve distinguir evidências locais de execuções de CI; não invente
   IDs de workflows ou relatórios aprovados.
6. Confirme que o repositório continua privado. Crie a tag no SHA validado e um
   rascunho de release. Se tag ou rascunho já existir, confira o SHA antes de
   continuar. Não mova tags nem sobrescreva artefatos divergentes.
7. Envie os arquivos, baixe-os novamente e confira nomes, tamanhos e checksums.
   Publique a candidata como prerelease e nunca latest. Publique a final somente
   depois da candidata aprovada e da nova validação dos pacotes finais.
8. Leia a release publicada e registre seu link e as evidências na issue do gate.
   A candidata mantém issue e milestone abertos. Feche ambos apenas depois de
   conferir a publicação final e todos os seus artefatos.

Mantenha uma publicação por vez. Se houver falha ou resposta perdida, confira o
estado remoto antes de repetir. Uma release publicada não é sobrescrita. Falha ao
registrar evidência ou fechar o milestone exige apenas reconciliar o backlog,
sem republicar. Os helpers de empacotamento podem ser usados individualmente;
o comando de publicação da automação não é um atalho para dispensar seus gates.

### Licença nos artefatos

O Sider adota a [MIT](../LICENSE). Cada pacote Linux e Windows inclui uma cópia
integral do `LICENSE`, junto do binário e do README. O empacotamento falha se o
arquivo estiver ausente ou vazio. A coleção `releases/licenses/` preserva os avisos
das dependências e da biblioteca padrão da toolchain fixada; ela também acompanha
os dois pacotes e deve ser revisada quando essas entradas mudarem.
A imagem Docker da 0.10 também deverá carregar
essa licença e os avisos exigidos pelas dependências que distribuir.

Licenciamento e visibilidade são decisões separadas: o repositório e os assets
continuam privados, e `publish = false` permanece no `Cargo.toml`.

## Automação reservada para depois da 1.0

Esta seção descreve o fluxo arquivado, não a execução atual. Depois da 1.0, uma
tarefa própria deverá revisar e testar os workflows de
`.github/workflows-disabled/`, seus gatilhos, permissões, runners e contratos antes
de restaurá-los em `.github/workflows/`. Não há reativação automática ao lançar a
1.0. Até lá, nenhum dos comportamentos abaixo é acionado por um PR.

O workflow `ci.yml` também está desabilitado no GitHub. Depois de restaurar e
validar seus arquivos, a retomada exigirá habilitá-lo explicitamente com
`gh workflow enable ci.yml --repo djairofilho/sider`. Não execute isso nesta fase.

O workflow arquivado aceita apenas PR da própria origem para `main`, com branch, label e
versão válidos. O checkout usa o SHA do merge do evento, nunca o `main` mais recente.
PR fechado sem merge, milestone incompleto, falta de RC ou teste com falha,
cancelado, ausente ou ignorado não publica.

Esse fluxo usa o `GITHUB_TOKEN` nativo, sem PAT adicional. A conta do usuário faz
o merge: eventos criados por outro workflow com esse token normalmente não iniciam
novos workflows. Tag, build e publicação ficam na mesma execução, sem depender de
uma execução acionada pela tag.
[Regra oficial do GitHub](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow).

Os jobs de build e testes só leem o repositório. Apenas `publish` recebe permissões
de escrita. A fila de concorrência serializa as publicações com `queue: max` e
`cancel-in-progress: false`. Essa sintaxe é documentada pelo GitHub; versões de
linters anteriores ao suporte à fila podem sinalizar `queue` como desconhecido.
[Concorrência oficial](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency).

Cada release privada contém pacotes Linux `.tar.gz` e Windows `.zip`,
`release-manifest.json`, `release-notes.md` e `SHA256SUMS`; desde a 0.10 inclui a
imagem exportada. O manifesto informa versão, SHA, compiladores, toolchain, targets,
resultados e execução de origem. O SHA-256 abrange os artefatos, as notas e o
manifesto; o arquivo de checksums não inclui seu próprio hash.

O publicador cria ou encontra a tag, confere seu SHA real, encontra drafts,
retoma uploads ausentes e baixa os assets para verificar todos os hashes antes de
publicar. RC é prerelease, nunca latest, e mantém o milestone aberto. A final usa
a seleção semântica do GitHub para latest, registra a publicação e só então fecha
a issue de publicação e o milestone.
[API oficial de releases](https://docs.github.com/en/rest/releases/releases).

| Falha | Retomada segura |
| --- | --- |
| Resposta perdida ao criar tag, draft ou upload | Reler pelo identificador e conferir SHA/bytes antes de repetir |
| Upload parcial em draft | Reexecutar jobs com falha; uploads completos devem ser idênticos; apenas asset `starter` vazio pode ser removido e reenviado |
| Publicação falhou, build já aprovado | Reexecutar somente jobs com falha da mesma execução, preservando artefatos e evidências originais |
| Release publicada, comentário/fechamento falhou | Reexecutar; o fluxo baixa e verifica os artefatos publicados e reconcilia o backlog, sem recompilar nem republicar |
| Tag aponta para outro SHA ou asset completo diverge | Interromper e investigar; não mover tag, excluir release ou sobrescrever asset automaticamente |
| Artefatos originais de draft expiraram | Recuperar os bytes/evidências originais; se impossível, investigar o draft com intervenção humana e preparar outra RC |

Reexecutar todos os jobs pode produzir binários diferentes, apesar dos mesmos
fontes. Os metadados dos arquivos compactados são normalizados, mas não há promessa
de builds bit a bit reproduzíveis entre ambientes. Divergência nunca autoriza
sobrescrever um upload. Os artefatos intermediários da execução duram 30 dias;
assets de releases publicadas são a fonte da reconciliação posterior.

## Validação da automação

Execute apenas ao alterar os helpers ou preparar a retomada da automação:

```sh
python -m tools.release.cli validate
python -m unittest discover -s tools/release -t . -p "test_*.py"
python -m tools.release.cli package --bootstrap --target x86_64-pc-windows-msvc --out target/package-simulation
```

No Linux, use `x86_64-unknown-linux-gnu`. O modo `--bootstrap` testa checks nativos,
empacotamento e `--version`, mas registra TCP como `not_run`. Não possui caminho
para criar release. O workflow arquivado de publicação nunca passa esse argumento.

Os testes com cliente GitHub falso cobrem publicação, respostas perdidas, drafts,
uploads parciais, imutabilidade, reconciliação, dependências, estados de testes e
duplicatas. Eles não validam o futuro protocolo do banco. Não há CI executando
essa suíte ou a simulação de empacotamento nesta fase. Os comandos continuam
disponíveis localmente, sem fazer parte do ciclo rápido de desenvolvimento Rust.
