# Trabalho no Sider

## Escopo e arquitetura

- Leia `PLANO.md`, `README.md`, `ROADMAP.md` e `docs/compatibility.md` antes de implementar.
- Avance pela próxima tarefa desbloqueada do milestone atual, em etapas compiláveis
  e testáveis. O próximo item funcional do bootstrap é `R01-01`.
- Crie módulos quando houver implementação real; evite stubs e diretórios vazios.
- Preserve as fronteiras entre RESP, comandos, armazenamento e rede.
- Use dados binários para chaves e valores. Não limite o protocolo a UTF-8.
- O armazenamento será propriedade do worker, com canais limitados.
- Não prometa compatibilidade ou desempenho sem evidência reproduzível.

## Backlog e releases

- `releases/plan.json` é a fonte dos IDs, dependências e critérios; `ROADMAP.md` é
  gerado por `python -m tools.release.plan --write`.
- Leia `docs/releases.md` antes de alterar automação ou preparar uma release.
- CI e publicação automática estão adiadas para depois da 1.0; mantenha os workflows
  inativos em `.github/workflows-disabled/`. Não reative sem solicitação.
- O desenvolvimento e os testes do banco usam Cargo, sem exigir Python. Apenas ao
  alterar ou usar os helpers opcionais de release, use Python 3.11 ou posterior e valide com
  `python -m tools.release.cli validate` e execute
  `python -m unittest discover -s tools/release -t . -p "test_*.py"`.
- Preserve IDs de tarefas, marcadores gerenciados e comentários humanos ao sincronizar.
- Não trate testes do bootstrap como evidência das funcionalidades futuras.
- Gates ausentes, ignorados ou cancelados bloqueiam publicação; nunca sintetize sucesso.
- Toda versão exige candidata; mudança funcional após RC exige outra candidata.
- Até a 1.0 inclusive, valide os gates e publique manualmente o SHA exato do merge
  de release. O merge não publica nada. Mantenha repositório e artefatos privados
  e `publish = false`; só encerre milestone após conferir a release final.

## Rust e testes

- Respeite `rust-toolchain.toml` e mantenha `Cargo.lock` versionado.
- Mantenha `#![forbid(unsafe_code)]` no código próprio.
- Adicione dependências quando houver um consumidor real.
- Teste comportamento e limites relevantes junto da implementação.
- Injete a leitura de configuração nos testes. Não altere o ambiente global.
- Use portas efêmeras e sincronização explícita nos futuros testes de rede.
- Use `cargo fmt --check`, `cargo check --locked` e `cargo test --locked`
  no ciclo rápido local. Não espere CI para integrar PRs.
- Execute `cargo clippy --locked --all-targets -- -D warnings` quando alterar
  código Rust ou seus testes; amplie as verificações conforme o risco da mudança.
- Verifique também `cargo doc --locked --no-deps` e `cargo build --locked`
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
