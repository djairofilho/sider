# Trabalho no Sider

## Escopo e arquitetura

- Leia `PLANO.md`, `README.md`, `ROADMAP.md` e `docs/compatibility.md` antes de implementar.
- Avance pela próxima tarefa desbloqueada do milestone atual, em etapas compiláveis
  e testáveis. Após validar e integrar diferenciais/fuzz de `R01-05`, o próximo
  item é `R01-GATE`, com candidata e final verificadas antes de avançar à 0.2.
- Crie módulos quando houver implementação real; evite stubs e diretórios vazios.
- Preserve as fronteiras entre RESP, comandos, armazenamento e rede.
- Use dados binários para chaves e valores. Não limite o protocolo a UTF-8.
- O armazenamento será propriedade do worker, com canais limitados.
- Não prometa compatibilidade ou desempenho sem evidência reproduzível.

## Backlog e releases

- `releases/plan.json` é a fonte dos IDs, dependências e critérios; `ROADMAP.md` é
  gerado por `cargo xtask roadmap --write`.
- Leia `docs/releases.md` antes de alterar automação ou preparar uma release.
- CI e publicação automática estão adiadas para depois da 1.0. Os workflows e
  helpers Python foram removidos; não os restaure nem reative CI sem solicitação.
- Banco, testes e ferramentas próprias usam Rust. `xtask/` tem manifesto e lockfile
  independentes; não acrescente suas dependências ao servidor ou ao fuzz.
- Ao alterar o plano ou `xtask/`, execute `cargo xtask check --tools`.
  `cargo xtask sync` só simula; `--apply` permite alterar o backlog no GitHub.
  Não crie outro framework de publicação enquanto a publicação for manual.
- Preserve IDs de tarefas, marcadores gerenciados e comentários humanos ao sincronizar.
- Não trate testes do bootstrap como evidência das funcionalidades futuras.
- Gates ausentes, ignorados ou cancelados bloqueiam publicação; nunca sintetize sucesso.
- Toda versão exige candidata; mudança funcional após RC exige outra candidata.
- Até a 1.0 inclusive, valide os gates e publique manualmente o SHA exato do merge
  de release. O merge não publica nada. Mantenha repositório e artefatos privados
  e `publish = false`; só encerre milestone após conferir a release final.

## Rust e testes

- Respeite `rust-toolchain.toml` e mantenha `Cargo.lock` versionado.
- Ao mudar dependências de produção ou toolchain, revise os avisos de distribuição
  em `releases/licenses/` e seu inventário antes da próxima publicação.
- Mantenha `#![forbid(unsafe_code)]` no código próprio.
- Adicione dependências quando houver um consumidor real.
- Teste comportamento e limites relevantes junto da implementação.
- Injete a leitura de configuração nos testes. Não altere o ambiente global.
- Use portas efêmeras e sincronização explícita nos testes de rede.
- Durante a implementação, execute testes focados (`cargo test --locked <filtro>`).
  Antes de integrar mudanças do banco, execute `cargo xtask check`: fmt, Clippy,
  build do binário e testes nativos. Clippy já verifica os targets; não duplique
  com `cargo check` nessa sequência. Não espere CI para integrar PRs.
- Valide documentação isolada sem repetir toda a suíte do banco. Execute os testes
  das ferramentas somente quando elas, seus contratos ou seu manifesto mudarem.
  Preserve os caches Cargo e execute gates externos apenas quando relevantes à
  tarefa ou obrigatórios na release. Nunca use o check rápido como prova de fuzz.
- Verifique também `cargo doc --locked --no-deps` e `cargo build --locked --release`
  quando mudar interfaces, configuração de build ou documentação de API.

## Texto e Git

- Documentação e mensagens ao usuário em português brasileiro, com acentos corretos.
- Preserve identificadores, caminhos, comandos e URLs exatamente como definidos.
- Confira UTF-8 e corrija mojibake antes de publicar texto.
- Para texto Markdown no GitHub via CLI, prefira arquivo UTF-8 com `--body-file`.
- Use as skills `organize-atomic-commits` e `conventional-commit` ao preparar commits.
- Separe responsabilidades independentes; todos os commits integrados devem passar
  na validação da respectiva etapa.
- Não faça commits ou push sem solicitação ou fluxo explicitamente autorizado.
- Preserve mudanças preexistentes e adicione caminhos explícitos ao staging.
- Em merges sem estratégia definida, prefira merge commit.
- Registre no PR os comandos locais, seus resultados e as limitações da validação.
- Ao deixar mudanças sem commit, informe um plano de commits atômicos.
