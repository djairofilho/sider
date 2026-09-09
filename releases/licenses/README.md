# Avisos de terceiros distribuídos com Sider

Esta coleção preserva textos upstream. Não escolhe entre licenças alternativas,
não interpreta obrigações jurídicas e não muda a licença MIT do código Sider.

O [inventário](inventory.json) identifica versões, origens lógicas, caminhos,
tamanhos e SHA-256 de cada arquivo. Os caminhos são relativos a este diretório.
Distribua este diretório completo junto do `LICENSE` próprio do Sider.

## Conteúdo

- 27 pacotes do grafo `normal` de Cargo, incluindo dependências de proc-macros.
  A união dos alvos Windows MSVC e Linux GNU contém 19 pacotes sem essas arestas.
- Textos integrais `LICENSE`, `LICENSE-*` e equivalentes de cada pacote em `crates/`.
  Alternativas MIT/Apache e o texto Unicode de `unicode-ident` foram preservados.
- Aviso aninhado `tracing-core-0.1.36/src/spin/LICENSE`, preservado por precaução,
  sem afirmar que esse módulo condicionado a `no_std` esteja no binário.
- [Inventário da biblioteca padrão Rust 1.97.1](rust-1.97.1/COPYRIGHT-library.html)
  e os textos MIT, Apache-2.0, BSD-2-Clause, Unicode-3.0 e LLVM-exception.

Dependências exclusivas de testes, Redis e o inventário
geral do compilador não fazem parte da coleção. O inventário Rust inclui avisos
de outras plataformas; sua presença não afirma uso desses componentes pelo Sider.

## Integridade e atualização

Os 53 arquivos somam 513.058 bytes. Em 52 deles, origem e cópia são idênticas.
Somente `COPYRIGHT-library.html` recebeu um LF final: a cópia equivale exatamente
à origem mais esse byte. `source_sha256`/`source_bytes` registram a origem;
`sha256`/`bytes` registram a cópia. Os seis arquivos Rust tiveram hashes de origem
iguais nas toolchains Windows e Linux 1.97.1. `.gitattributes` impede conversão de EOL.

Ao mudar dependências ou toolchain, repita os comandos do inventário, revise o
conjunto de avisos e confira todos os hashes antes de distribuir os pacotes.
