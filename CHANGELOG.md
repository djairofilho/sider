# Changelog

## [Unreleased]

- Worker proprietário e servidor RESP2/TCP, com filas e conexões limitadas,
  timeouts, prontidão atômica e encerramento supervisionado.
- Bootstrap Rust com configuração validada e testes.
- Parsing e armazenamento síncrono de `PING`, `ECHO`, `GET`, `SET` básico e
  `DEL`, com rejeição sem mutação, dados binários e respostas verificadas nas fixtures.
- Codec RESP2 incremental e encoder atômico, com limites, dados binários,
  fixtures literais, fragmentação e testes de propriedades.
- Fixtures binárias e infraestrutura descartável de Redis/CLI 8.10.1, testadas
  em Rust sem dependência do futuro codec Sider.
- Licença MIT, mantendo o repositório e os artefatos privados.
- Planejamento versionado, backlog sincronizável e ferramentas opcionais de release.
- Validação e publicação manuais até a 1.0 inclusive; CI e publicação automática
  adiadas para depois da 1.0.

Nenhuma versão funcional foi publicada. As notas por versão serão preparadas
quando o respectivo milestone cumprir seus critérios.
