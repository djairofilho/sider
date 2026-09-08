# Trabalho no Sider

## Escopo e arquitetura

- Leia `PLANO.md`, `README.md` e `docs/compatibility.md` antes de implementar.
- Avance pela versão 0.1, em etapas compiláveis e testáveis.
- Crie módulos quando houver implementação real; evite stubs e diretórios vazios.
- Preserve as fronteiras entre RESP, comandos, armazenamento e rede.
- Use dados binários para chaves e valores. Não limite o protocolo a UTF-8.
- O armazenamento será propriedade do worker, com canais limitados.
- Não prometa compatibilidade ou desempenho sem evidência reproduzível.

## Rust e testes

- Respeite `rust-toolchain.toml` e mantenha `Cargo.lock` versionado.
- Mantenha `#![forbid(unsafe_code)]` no código próprio.
- Adicione dependências quando houver um consumidor real.
- Teste comportamento e limites relevantes junto da implementação.
- Injete a leitura de configuração nos testes. Não altere o ambiente global.
- Use portas efêmeras e sincronização explícita nos futuros testes de rede.
- Execute `cargo fmt --check`, `cargo check --locked`,
  `cargo clippy --locked --all-targets -- -D warnings` e `cargo test --locked`.
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
- Ao deixar mudanças sem commit, informe um plano de commits atômicos.
