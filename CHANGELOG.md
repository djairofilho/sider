# Changelog

## [Unreleased]

## [1.0.0] - Preparação da candidata

O pacote identifica `1.0.0`; a candidata e a final usam o mesmo build e arquivos.
As [notas da versão](releases/notes/v1.0.0.md) descrevem capacidades e limites.
A aprovação depende dos gates do SHA exato e do conjunto de evidências produzido
para ele; esta seção não declara publicação ou gates aprovados.

- Transações de um shard com `MULTI`, `EXEC`, `DISCARD`, `WATCH` e `UNWATCH`;
  validação antes de aplicar, um lote AOF por EXEC e erros individuais sem rollback.
  Publicações em transações preservam a ordem após aplicar o lote confirmado
  segundo a política AOF configurada.
- Replicação assíncrona Sider → Sider com snapshot, histórico limitado,
  reconexão por FULL/CONTINUE, réplica somente leitura e promoção manual durável.
  O cabeçalho AOF v3 preserva papel e época; leitores atuais mantêm suporte a v1/v2.
- `sider-backup` exporta snapshots consistentes durante tráfego, confere manifesto
  e checksums e restaura em diretório novo, preservando tipos, lotes e TTL absoluto.
- `INFO`, seções operacionais e `sider --diagnose` para métricas, filas, persistência,
  replicação e validação da configuração sem iniciar listeners.
- Pacotes Windows/Linux com quatro executáveis: servidor, migrador AOF,
  backup e administração de réplica. Imagem Docker privada usa os binários do
  pacote, usuário sem privilégio, persistência e ensaio real de exportação/load.
- Matriz completa de compatibilidade e auditoria diferencial de sequências
  entre tipos, TTL, transações e Pub/Sub, com seeds fixas e contagens próprias.
- Runners de soak de 3600 segundos e benchmarks do pacote extraído, com amostras,
  hashes e limites documentados. Aprovação depende da execução no build candidato.
- Hashes, listas, sets e sorted sets binários, com WRONGTYPE, TTL e quota atômica;
  AOF preserva postimages tipadas, scores e deadlines em replay/compactação.
- Diferenciais de coleções e ordenação contra Redis; conversão de scores em Rust
  seguro baseada no fpconv sob Boost 1.0, com avisos de licença preservados.

- AOF binário com checksum, lotes resolvidos, sync configurável, recuperação antes
  do bind, compactação global e rejeição recuperável de registros excessivos.
- Metadados de shards no formato AOF v2, leitor legado v1 e migração offline
  explícita com `sider-aof-migrate`. Snapshots aguardam apply de pedidos aceitos.
- Testes de crash/migração nas duas plataformas e medição exploratória TCP de R04.

- SUBSCRIBE, UNSUBSCRIBE, PUBLISH e PING no modo assinante, com canais binários,
  filas limitadas e cleanup em fila cheia, timeout, EOF, cancelamento e shutdown.
  Mensagens efêmeras permanecem separadas do dataset e do AOF.
- Shards fixos com workers e filas independentes, hash binário estável e hash tags.
  Comandos multichave entre shards são rejeitados antes do enqueue; quota total
  é dividida entre workers e conferida novamente durante a recuperação do AOF.
- Strings adicionais (`EXISTS`, `INCR`, `DECR`, `MGET`, `MSET`) e SET com NX, XX,
  EX, PX, GET e KEEPTTL, com overflow e rejeições sem alterações parciais.
- EXPIRE, PEXPIRE, TTL, PTTL e PERSIST; relógio injetável e limpeza ativa limitada.
- Quota lógica configurável do dataset, padrão 64 MiB, com contabilidade de lotes
  pelo estado final e rejeição de crescimento sem eviction.
- Diferenciais R02 contra Redis 8.10.1 e regressões de tempo, quota e limites TCP.
- Ferramentas locais de planejamento, backlog, verificação e integridade de
  artefatos em Rust, acessíveis por `cargo xtask` e isoladas do servidor.
- Remoção dos helpers Python e workflows arquivados. Publicação continua manual;
  CI permanece adiada para depois da 1.0.
- Ciclo curto com testes focados e checks separados para banco e ferramentas,
  sem executar gates externos a cada edição ou duplicar check e Clippy.
- Remoção completa da estrutura, dependências e gate de fuzz. Permanecem os
  testes nativos, de propriedades, diferenciais e de pacotes extraídos.
- Marcos R01–R10 internos, checkpoints técnicos e dependências explícitas,
  preservando IDs e escopo. Somente R11 publica candidata e final.
- Manifesto de artefatos v2: o build já usa `1.0.0` na candidata, e a final promove
  o mesmo SHA e arquivos. Divergências de identidade ou hashes são rejeitadas.
- Contrato de migração para 1.0 a partir de baseline interna R10, sem publicação intermediária.
- Congelamento e migração por pacotes extraídos, com proveniência observacional,
  inventário imutável, cinco tipos, EXEC, TTL durante a parada, backup/restauração
  e recriação de réplica da mesma versão. A baseline R10 mantém o pacote `0.1.0`.

## [0.1.0] - Preparação anterior, não publicada

- A preparação anterior foi encerrada sem publicação. O marco 0.1 passa a ser
  um checkpoint interno; a candidata anterior permanece como registro histórico.
- [Rascunho das notas da final](releases/notes/v0.1.0.md), incluindo o requisito do runtime
  Visual C++ v14 Redistributable x64 para o executável Windows.

## [0.1.0-rc.1] - 2026-09-08

- Primeira candidata funcional, com pacotes Linux GNU e Windows MSVC validados
  após extração, README de distribuição e avisos das dependências incluídos.
- Suíte diferencial independente Sider/Redis, integração com `redis-cli`,
  testes de reutilização de conexões e gates locais em Rust com recibos verificados.
- Alvo isolado de fuzz com AddressSanitizer, corpus versionado e execução mínima
  de 900 segundos; ambiente local Ubuntu reproduzível para testes.
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

As [notas da candidata](releases/notes/v0.1.0-rc.1.md) descrevem o subconjunto e suas
limitações e preservam a política de validação vigente naquela publicação.
A RC foi publicada e aprovada sob a política daquela revisão. Sua evidência não
aprova os SHAs posteriores; o fluxo atual publica somente a candidata e a final 1.0.
