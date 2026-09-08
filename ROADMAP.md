# Roadmap de releases do Sider

<!-- Gerado por cargo xtask roadmap --write; editar releases/plan.json. -->

Este roteiro organiza as entregas até a 1.0. O bootstrap é a única capacidade
concluída nesta linha de base; as funcionalidades do banco permanecem planejadas.
O estado operacional das tarefas está nas issues do GitHub, sem duplicar o estado neste arquivo.

São 11 milestones e 62 issues: um bootstrap, tarefas funcionais e um gate de publicação por versão.
O repositório e os artefatos permanecem privados; a crate usa `publish = false`.

CI e publicação automática estão desativadas até e incluindo a 1.0.
Uma retomada posterior exige implementação e alteração explícitas da política.
Até lá, execute e registre manualmente as verificações e a publicação; os critérios de qualidade permanecem.

## Índice

- [Sequência de versões](#sequência-de-versões)
- [Bootstrap comprovado](#bootstrap-comprovado)
- [Contratos transversais](#contratos-transversais)
- [Tarefas por versão](#tarefas-por-versão)
- [Execução e publicação](#execução-e-publicação)

## Sequência de versões

| Milestone | Entrega | Tarefas | Depende de |
| --- | --- | --- | --- |
| `0.1.0` | Núcleo RESP2 | 5 + publicação | Bootstrap |
| `0.2.0` | Strings, TTL e memória | 4 + publicação | R01 |
| `0.3.0` | Persistência AOF | 5 + publicação | R02 |
| `0.4.0` | Shards | 5 + publicação | R03 |
| `0.5.0` | Coleções básicas | 5 + publicação | R04 |
| `0.6.0` | Sorted sets | 3 + publicação | R05 |
| `0.7.0` | Transações | 4 + publicação | R06 |
| `0.8.0` | Pub/Sub | 4 + publicação | R07 |
| `0.9.0` | Replicação | 5 + publicação | R08 |
| `0.10.0` | Operação e distribuição | 5 + publicação | R09 |
| `1.0.0` | Estabilização | 5 + publicação | R10 |

## Bootstrap comprovado

- [x] `B00-01`: Fundação Rust, configuração validada e CI multiplataforma.
  [Evidência 1](https://github.com/djairofilho/sider/commit/fddbfaa)
  [Evidência 2](https://github.com/djairofilho/sider/commit/bd85ac1)
  [Evidência 3](https://github.com/djairofilho/sider/commit/eacc3ae)
  [Evidência 4](https://github.com/djairofilho/sider/actions/runs/34177280948)

## Contratos transversais

- RESP2 com chaves e valores binários; limites de entrada e canais limitados desde a 0.1.
- Armazenamento com proprietário único por worker; preserve as fronteiras entre protocolo, comandos, armazenamento e rede.
- Quota do dataset rejeita crescimento acima do limite, sem eviction automática.
- AOF inicialmente com escritor global, registros de mutações resolvidas e formato versionado independente da versão do produto.
- Shards fixos durante a execução; operações multichave e transações exigem o mesmo shard e rejeitam cruzamento antes de qualquer mutação.
- Transações distinguem erros de enfileiramento e de execução conforme Redis; não há rollback de erros individuais durante EXEC.
- Replicação assíncrona Sider→Sider, com a mesma versão e configuração de shards, réplica somente leitura e promoção manual.
- Compatibilidade é declarada somente com testes reproduzíveis; respostas sem ordem garantida são comparadas por conteúdo normalizado.
- Ambiente suportado até a 1.0 é controlado; o endereço padrão permanece em loopback.

Fora do escopo até a 1.0: Redis Cluster; Sentinel; failover automático; resharding online; RESP3; Lua; operações bloqueantes; transações entre shards; TLS; ACL.

Referência fixada: Redis e `redis-cli` 8.10.1, plataforma `linux/amd64`.

```text
redis:8.10.1@sha256:76961cd2a0f40ef6fdd334b6b1b3a76a2bad1848d89f3030ca30a7521d4a9493
```

A imagem fixada é uma entrada da suíte; só uma execução registrada constitui evidência de compatibilidade.

## Tarefas por versão

Objetivos, entregáveis, testes e critérios completos de cada issue estão no
[manifesto versionado](releases/plan.json). As dependências indicam a ordem de execução.

### 0.1.0: Núcleo RESP2

- Implementar PING, ECHO, GET, SET básico e DEL via RESP2/TCP, com worker único.
- Preservar dados binários, fragmentação, ordenação por conexão, limites e encerramento definidos em PLANO.md.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R01-01` | Fixtures e referência Redis | B00-01 |
| `R01-02` | Codec RESP2 incremental | R01-01 |
| `R01-03` | Cinco comandos e armazenamento | R01-02 |
| `R01-04` | Worker e TCP | R01-03 |
| `R01-05` | Testes diferenciais e fuzz | R01-04 |
| `R01-GATE` | Validar e publicar a 0.1.0 | Todas as tarefas da versão e os gates anteriores |

Evidências obrigatórias: `native`, `compatibility`, `fuzz`, `tcp_smoke`.

Critérios para publicação:

- Todas as tarefas R01 concluídas com PR e verificação manual registrada; PING, ECHO, GET, SET básico e DEL funcionam via redis-cli.
- Gates native, compatibility, fuzz e tcp_smoke aprovados no SHA exato; fragmentação, binários, limites e ordenação cobertos.
- Publicar v0.1.0-rc.1 antes da final; conferir pacotes Linux/Windows extraídos, checksums e manifesto; fechar o milestone apenas após a final confirmada.

### 0.2.0: Strings, TTL e memória

- Adicionar EXISTS, INCR, DECR, MGET, MSET, EXPIRE, PEXPIRE, TTL, PTTL e PERSIST.
- SET aceita NX, XX, EX, PX, GET e KEEPTTL; expiração passiva/ativa e quota com rejeição de crescimento.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R02-01` | Comandos adicionais de strings | R01-GATE |
| `R02-02` | Opções de SET | R02-01 |
| `R02-03` | Expiração passiva e ativa | R02-02 |
| `R02-04` | Quota do dataset | R02-03 |
| `R02-GATE` | Validar e publicar a 0.2.0 | Todas as tarefas da versão e os gates anteriores |

Evidências obrigatórias: `native`, `compatibility`, `fuzz`, `tcp_smoke`.

Critérios para publicação:

- Milestone 0.1 final publicado e tarefas R02 concluídas; overflow, expiração antiga e rejeições preservam estado.
- Gates cumulativos aprovados; quota recusa crescimento e opções de SET/TTL têm evidência diferencial.
- Publicar candidata com 900 segundos de fuzz e depois final com pacotes verificados; encerrar milestone após conferência da final.

### 0.3.0: Persistência AOF

- AOF com formato versionado, checksum e escritor global de mutações resolvidas.
- Políticas de durabilidade explícitas, replay seguro, compactação e testes de crash em Linux e Windows.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R03-01` | Formato AOF versionado e checksum | R02-GATE |
| `R03-02` | Append e fsync | R03-01 |
| `R03-03` | Recuperação AOF | R03-02 |
| `R03-04` | Compactação AOF | R03-03 |
| `R03-05` | Testes de crash e durabilidade | R03-04 |
| `R03-GATE` | Validar e publicar a 0.3.0 | Todas as tarefas da versão e os gates anteriores |

Evidências obrigatórias: `native`, `compatibility`, `fuzz`, `tcp_smoke`, `crash`, `recovery`, `migration`.

Critérios para publicação:

- Nenhum registro parcial aplicado; garantias de fsync e recuperação comprovadas em Linux e Windows.
- Gates cumulativos incluem crash, recovery e migration; na primeira AOF, migration usa fixtures do formato inicial.
- Compactação e substituição de arquivos testadas; candidata e final publicadas com evidências e pacotes conferidos antes de fechar milestone.

### 0.4.0: Shards

- Roteamento determinístico com hash tags e número fixo de workers proprietários.
- Operações multichave exigem mesmo shard; AOF inicialmente mantém escritor global e ordem recuperável.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R04-01` | Roteamento estável e hash tags | R03-GATE |
| `R04-02` | Workers independentes | R04-01 |
| `R04-03` | Operações multichave restritas | R04-02 |
| `R04-04` | Integração de shards com AOF | R04-03 |
| `R04-05` | Medições iniciais de shards | R04-04 |
| `R04-GATE` | Validar e publicar a 0.4.0 | Todas as tarefas da versão e os gates anteriores |

Evidências obrigatórias: `native`, `compatibility`, `fuzz`, `tcp_smoke`, `crash`, `recovery`, `migration`, `sharding`.

Critérios para publicação:

- Mesma distribuição em Linux e Windows; rejeição entre shards comprovada antes de qualquer mutação.
- AOF e migração preservam recuperação; gate sharding passa e medições iniciais estão publicadas como evidência.
- Notas registram restrição multichave; candidata aprovada precede final e encerramento do milestone.

### 0.5.0: Coleções básicas

- Adicionar hashes, listas e sets tipados com o subconjunto de comandos registrado em contracts.commands_added.
- LPOP e RPOP inicialmente sem count; preservar TTL, quota, AOF e roteamento.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R05-01` | Valores tipados | R04-GATE |
| `R05-02` | Hashes | R05-01 |
| `R05-03` | Listas | R05-02 |
| `R05-04` | Sets | R05-03 |
| `R05-05` | Integração de coleções com TTL, quota e AOF | R05-04 |
| `R05-GATE` | Validar e publicar a 0.5.0 | Todas as tarefas da versão e os gates anteriores |

Evidências obrigatórias: `native`, `compatibility`, `fuzz`, `tcp_smoke`, `crash`, `recovery`, `migration`, `sharding`, `types`.

Critérios para publicação:

- Hashes, listas e sets têm semântica, persistência e WRONGTYPE verificados por família.
- Gate types e todos os anteriores aprovados, incluindo migração AOF e contabilidade de memória.
- Candidata e pacotes finais conferidos; milestone encerra somente após publicação final.

### 0.6.0: Sorted sets

- Adicionar ZADD básico, ZREM, ZCARD, ZSCORE e ZRANGE start stop [WITHSCORES].
- Ordenar por score e desempatar por bytes do membro; manter persistência e limites.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R06-01` | Armazenamento ordenado | R05-GATE |
| `R06-02` | Comandos básicos de sorted sets | R06-01 |
| `R06-03` | Integração de sorted sets com persistência e limites | R06-02 |
| `R06-GATE` | Validar e publicar a 0.6.0 | Todas as tarefas da versão e os gates anteriores |

Evidências obrigatórias: `native`, `compatibility`, `fuzz`, `tcp_smoke`, `crash`, `recovery`, `migration`, `sharding`, `types`, `sorted_sets`.

Critérios para publicação:

- Scores inválidos tratados, desempate binário e índices negativos verificados.
- Gate sorted_sets e cumulativos aprovados; recovery preserva scores e ordenação.
- Candidata aprovada antecede a final com pacotes e evidências conferidos.

### 0.7.0: Transações

- Adicionar MULTI, EXEC, DISCARD, WATCH e UNWATCH limitados a um shard.
- Separar erros de enfileiramento de erros individuais de execução sem rollback; persistir o lote atomicamente.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R07-01` | Estado transacional por conexão | R06-GATE |
| `R07-02` | Execução transacional em lote | R07-01 |
| `R07-03` | WATCH e UNWATCH | R07-02 |
| `R07-04` | Persistência atômica do lote | R07-03 |
| `R07-GATE` | Validar e publicar a 0.7.0 | Todas as tarefas da versão e os gates anteriores |

Evidências obrigatórias: `native`, `compatibility`, `fuzz`, `tcp_smoke`, `crash`, `recovery`, `migration`, `sharding`, `types`, `sorted_sets`, `transactions`.

Critérios para publicação:

- Transações de um shard, conflitos e expiração de WATCH verificados; erros seguem o subconjunto Redis declarado.
- Gate transactions e cumulativos aprovados; replay não aplica meia transação.
- Candidata e final passam por todos os gates e conferência de pacotes antes do encerramento do milestone.

### 0.8.0: Pub/Sub

- Adicionar SUBSCRIBE, UNSUBSCRIBE, PUBLISH e PING no modo assinante RESP2.
- Inscrições e mensagens são efêmeras; filas limitadas e clientes lentos não bloqueiam o banco.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R08-01` | Registro de assinaturas | R07-GATE |
| `R08-02` | Publicação de mensagens | R08-01 |
| `R08-03` | Modo assinante | R08-02 |
| `R08-04` | Controle de clientes lentos | R08-03 |
| `R08-GATE` | Validar e publicar a 0.8.0 | Todas as tarefas da versão e os gates anteriores |

Evidências obrigatórias: `native`, `compatibility`, `fuzz`, `tcp_smoke`, `crash`, `recovery`, `migration`, `sharding`, `types`, `sorted_sets`, `transactions`, `pubsub`.

Critérios para publicação:

- Assinantes lentos não bloqueiam banco ou assinantes saudáveis; filas limitadas e cleanup comprovados.
- Gate pubsub e cumulativos aprovados, incluindo comportamento de PING no modo assinante.
- Candidata e final publicadas com evidências; milestone permanece aberto até conferir a final.

### 0.9.0: Replicação

- Replicação assíncrona Sider→Sider da mesma versão e configuração de shards.
- Sincronização completa e incremental, reconexão, réplicas somente leitura e promoção manual; sem failover automático.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R09-01` | Protocolo interno versionado | R08-GATE |
| `R09-02` | Sincronização completa | R09-01 |
| `R09-03` | Acompanhamento incremental | R09-02 |
| `R09-04` | Reconexão de réplicas | R09-03 |
| `R09-05` | Promoção manual | R09-04 |
| `R09-GATE` | Validar e publicar a 0.9.0 | Todas as tarefas da versão e os gates anteriores |

Evidências obrigatórias: `native`, `compatibility`, `fuzz`, `tcp_smoke`, `crash`, `recovery`, `migration`, `sharding`, `types`, `sorted_sets`, `transactions`, `pubsub`, `replication`.

Critérios para publicação:

- Sincronização consistente entre shards, réplicas somente leitura, TTL e transações preservados.
- Gate replication e cumulativos aprovados; backlog insuficiente força sincronização completa e promoção é manual.
- Candidata e final verificadas; procedimentos não prometem failover automático nem ausência de perda assíncrona.

### 0.10.0: Operação e distribuição

- Métricas, diagnóstico e procedimentos reproduzíveis de backup e restauração.
- Distribuir imagem Docker Linux amd64 como arquivo anexado à release privada; comprovar operação e encerramento.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R10-01` | Métricas | R09-GATE |
| `R10-02` | Diagnóstico operacional | R10-01 |
| `R10-03` | Procedimentos de backup e restauração | R10-02 |
| `R10-04` | Imagem Docker | R10-03 |
| `R10-05` | Ensaios operacionais | R10-04 |
| `R10-GATE` | Validar e publicar a 0.10.0 | Todas as tarefas da versão e os gates anteriores |

Evidências obrigatórias: `native`, `compatibility`, `fuzz`, `tcp_smoke`, `crash`, `recovery`, `migration`, `sharding`, `types`, `sorted_sets`, `transactions`, `pubsub`, `replication`, `docker`.

Critérios para publicação:

- Métricas verificadas, restauração reproduzível, encerramento e limites documentados.
- Gate docker e cumulativos aprovados; imagem executável exportada acompanha os pacotes e checksums privados.
- Candidata e final conferidas antes do encerramento; não há publicação em crates.io ou registry público.

### 1.0.0: Estabilização

- Congelar o subconjunto compatível, comprovar recuperação e migração, executar carga prolongada e benchmarks reproduzíveis.
- Incluir sorted sets, transações e replicação Sider→Sider; documentar garantias sem prometer capacidades fora do escopo.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R11-01` | Congelamento do subconjunto | R10-GATE |
| `R11-02` | Auditoria diferencial | R11-01 |
| `R11-03` | Testes prolongados | R11-02 |
| `R11-04` | Migração da versão anterior | R11-03 |
| `R11-05` | Benchmarks e documentação final | R11-04 |
| `R11-GATE` | Validar e publicar a 1.0.0 | Todas as tarefas da versão e os gates anteriores |

Evidências obrigatórias: `native`, `compatibility`, `fuzz`, `tcp_smoke`, `crash`, `recovery`, `migration`, `sharding`, `types`, `sorted_sets`, `transactions`, `pubsub`, `replication`, `docker`, `soak`, `benchmarks`.

Critérios para publicação:

- Sorted sets, transações e replicação incluídos; matriz completa sem falha conhecida de corrupção ou perda além das garantias declaradas.
- Todos os gates aprovados, incluindo soak de 3600 segundos, migração da 0.10, benchmarks e fuzz de candidata de 900 segundos.
- Candidata aprovada sem mudança funcional posterior; final recompilada/testada, pacotes/imagem/checksums conferidos e milestone encerrado após publicação confirmada.

## Execução e publicação

1. Selecione a próxima issue desbloqueada do milestone atual e implemente em branch própria.
2. Inclua testes e evidências; mantenha código compilável e commits atômicos em cada etapa.
3. Integre o PR vinculado à issue por merge commit após verificação manual registrada.
4. Atualize compatibilidade e notas; prepare `v<versão>-rc.1` quando as tarefas funcionais terminarem.
5. O merge do PR `chore/release-v<versão>`, com label `type:release`, não dispara publicação: valide e publique manualmente o SHA exato do merge.
6. Mudança funcional após a RC exige outra RC; a final recompila e testa o mesmo conteúdo funcional aprovado.
7. Encerre o milestone somente após conferir a publicação final e seus artefatos.

O fluxo completo e os comandos de preparação estão no [guia de releases](docs/releases.md).
Não há workflows de CI nem publicador automático neste repositório.
Cada candidata exige pelo menos 15 minutos de fuzz; a 1.0 acrescenta uma hora de carga contínua.
As evidências são cumulativas. Teste ausente, ignorado, cancelado ou sem relatório bloqueia a publicação.
Na primeira versão AOF, migração valida fixtures do formato inicial; nas seguintes, testa a versão anterior suportada.

Os pacotes são Linux GNU x86_64 (`.tar.gz`, Ubuntu 24.04) e Windows MSVC x86_64 (`.zip`).
Desde a 0.10, uma imagem Docker Linux amd64 exportada acompanha a release privada.
Checksums SHA-256, manifesto de build e notas acompanham os binários testados depois da extração.

Patches, como `0.3.1`, precisam de registro próprio no manifesto e de milestone criado quando necessário.
Patches também têm candidata. Novas capacidades entram em minor; incompatibilidades antes da 1.0
são restritas às minors e descritas nas notas. Não há datas artificiais.

Na retomada manual, confira drafts e uploads existentes. Tag com SHA divergente ou artefato publicado diferente
interrompe o fluxo. Uma release publicada não é sobrescrita.

Para validar a fonte e a projeção sem alterar arquivos:

```sh
cargo xtask validate
```

Para regenerar a projeção a partir do manifesto:

```sh
cargo xtask roadmap --write
```
