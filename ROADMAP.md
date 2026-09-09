# Roadmap de marcos internos e publicação do Sider

<!-- Gerado por cargo xtask roadmap --write; editar releases/plan.json. -->

Este roteiro organiza as entregas até a 1.0. R01 a R10 são marcos internos;
R11 reúne a estabilização e a publicação da 1.0, sem retirar funcionalidades do escopo.
O estado operacional das tarefas está nas issues do GitHub, sem duplicar o estado neste arquivo.

São 11 milestones e 62 issues: um bootstrap, tarefas funcionais e um gate por marco.
O repositório e os artefatos permanecem privados; a crate usa `publish = false`.

CI e publicação automática estão desativadas até e incluindo a 1.0.
Uma retomada posterior exige implementação e alteração explícitas da política.
Até lá, execute e registre manualmente as verificações e a publicação; os critérios de qualidade permanecem.

## Índice

- [Marcos e publicação](#marcos-e-publicação)
- [Bootstrap comprovado](#bootstrap-comprovado)
- [Contratos transversais](#contratos-transversais)
- [Tarefas por marco](#tarefas-por-marco)
- [Execução e publicação](#execução-e-publicação)

## Marcos e publicação

| Milestone | Entrega | Tarefas | Encerramento |
| --- | --- | --- | --- |
| `0.1.0` | Núcleo RESP2 | 5 + gate | Validação interna, sem publicação |
| `0.2.0` | Strings, TTL e memória | 4 + gate | Validação interna, sem publicação |
| `0.3.0` | Persistência AOF | 5 + gate | Validação interna, sem publicação |
| `0.4.0` | Shards | 5 + gate | Validação interna, sem publicação |
| `0.5.0` | Coleções básicas | 5 + gate | Validação interna, sem publicação |
| `0.6.0` | Sorted sets | 3 + gate | Validação interna, sem publicação |
| `0.7.0` | Transações | 4 + gate | Validação interna, sem publicação |
| `0.8.0` | Pub/Sub | 4 + gate | Validação interna, sem publicação |
| `0.9.0` | Replicação | 5 + gate | Validação interna, sem publicação |
| `0.10.0` | Operação e distribuição | 5 + gate | Validação interna, sem publicação |
| `1.0.0` | Estabilização | 5 + gate | Candidata e final com o mesmo SHA e os mesmos assets |

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

## Tarefas por marco

Objetivos, entregáveis, testes e critérios completos de cada issue estão no
[manifesto versionado](releases/plan.json). Somente as dependências técnicas das tarefas
limitam o paralelismo; a ordem dos marcos neste documento não cria dependências.

### 0.1.0: Núcleo RESP2

- Implementar PING, ECHO, GET, SET básico e DEL via RESP2/TCP, com worker único.
- Preservar dados binários, fragmentação, ordenação por conexão, limites e encerramento definidos em PLANO.md.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R01-01` | Fixtures e referência Redis | B00-01 |
| `R01-02` | Codec RESP2 incremental | R01-01 |
| `R01-03` | Cinco comandos e armazenamento | R01-02 |
| `R01-04` | Worker e TCP | R01-03 |
| `R01-05` | Testes diferenciais e robustez | R01-04 |
| `R01-GATE` | Validar marco interno 0.1.0 | R01-01, R01-02, R01-03, R01-04, R01-05 |

Evidências obrigatórias: `native`, `compatibility`, `tcp_smoke`.

Critérios para encerrar o marco:

- Todas as tarefas R01 concluídas com PR e verificação manual registrada; PING, ECHO, GET, SET básico e DEL funcionam via redis-cli.
- Gates native, compatibility e tcp_smoke aprovados no SHA exato; fragmentação, binários, limites e ordenação cobertos.
- Conferir os critérios técnicos e registrar evidências locais; encerrar este marco interno sem candidata, tag ou release publicada. Artefatos usados em migração ficam congelados por SHA e hashes.

### 0.2.0: Strings, TTL e memória

- Adicionar EXISTS, INCR, DECR, MGET, MSET, EXPIRE, PEXPIRE, TTL, PTTL e PERSIST.
- SET aceita NX, XX, EX, PX, GET e KEEPTTL; expiração passiva/ativa e quota com rejeição de crescimento.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R02-01` | Comandos adicionais de strings | R01-GATE |
| `R02-02` | Opções de SET | R02-01 |
| `R02-03` | Expiração passiva e ativa | R02-02 |
| `R02-04` | Quota do dataset | R02-03 |
| `R02-GATE` | Validar marco interno 0.2.0 | R02-01, R02-02, R02-03, R02-04 |

Evidências obrigatórias: `native`, `compatibility`, `tcp_smoke`.

Critérios para encerrar o marco:

- Marco R01 validado e tarefas R02 concluídas; overflow, expiração antiga e rejeições preservam estado.
- Gates locais aprovados; quota recusa crescimento e opções de SET/TTL têm evidência diferencial.
- Conferir os critérios técnicos e registrar evidências locais; encerrar este marco interno sem candidata, tag ou release publicada. Artefatos usados em migração ficam congelados por SHA e hashes.

### 0.3.0: Persistência AOF

- AOF com formato versionado, checksum e escritor global de mutações resolvidas.
- Políticas de durabilidade explícitas, replay seguro, compactação e testes de crash em Linux e Windows.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R03-01` | Formato AOF versionado e checksum | R02-04 |
| `R03-02` | Append e fsync | R03-01 |
| `R03-03` | Recuperação AOF | R03-02 |
| `R03-04` | Compactação AOF | R03-03 |
| `R03-05` | Testes de crash e durabilidade | R03-04 |
| `R03-GATE` | Validar marco interno 0.3.0 | R03-01, R03-02, R03-03, R03-04, R03-05 |

Evidências obrigatórias: `native`, `compatibility`, `tcp_smoke`, `crash`, `recovery`, `migration`.

Critérios para encerrar o marco:

- Nenhum registro parcial aplicado; garantias de fsync e recuperação comprovadas em Linux e Windows.
- Gates locais incluem crash, recovery e migration; na primeira AOF, migration usa fixtures do formato inicial.
- Conferir os critérios técnicos e registrar evidências locais; encerrar este marco interno sem candidata, tag ou release publicada. Artefatos usados em migração ficam congelados por SHA e hashes.

### 0.4.0: Shards

- Roteamento determinístico com hash tags e número fixo de workers proprietários.
- Operações multichave exigem mesmo shard; AOF inicialmente mantém escritor global e ordem recuperável.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R04-01` | Roteamento estável e hash tags | R01-04 |
| `R04-02` | Workers independentes | R04-01, R02-04 |
| `R04-03` | Operações multichave restritas | R04-02, R02-01 |
| `R04-04` | Integração de shards com AOF | R04-03, R03-04 |
| `R04-05` | Medições iniciais de shards | R04-04 |
| `R04-GATE` | Validar marco interno 0.4.0 | R04-01, R04-02, R04-03, R04-04, R04-05 |

Evidências obrigatórias: `native`, `compatibility`, `tcp_smoke`, `sharding`.

Critérios para encerrar o marco:

- Mesma distribuição em Linux e Windows; rejeição entre shards comprovada antes de qualquer mutação.
- AOF e migração preservam recuperação; gate sharding passa e medições iniciais estão registradas como evidência local.
- Conferir os critérios técnicos e registrar evidências locais; encerrar este marco interno sem candidata, tag ou release publicada. Artefatos usados em migração ficam congelados por SHA e hashes.

### 0.5.0: Coleções básicas

- Adicionar hashes, listas e sets tipados com o subconjunto de comandos registrado em contracts.commands_added.
- LPOP e RPOP inicialmente sem count; preservar TTL, quota, AOF e roteamento.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R05-01` | Valores tipados | R02-04 |
| `R05-02` | Hashes | R05-01 |
| `R05-03` | Listas | R05-01 |
| `R05-04` | Sets | R05-01 |
| `R05-05` | Integração de coleções com TTL, quota e AOF | R05-02, R05-03, R05-04, R03-04, R04-04 |
| `R05-GATE` | Validar marco interno 0.5.0 | R05-01, R05-02, R05-03, R05-04, R05-05 |

Evidências obrigatórias: `native`, `compatibility`, `tcp_smoke`, `types`.

Critérios para encerrar o marco:

- Hashes, listas e sets têm semântica, persistência e WRONGTYPE verificados por família.
- Gate types e verificações locais aprovados, incluindo migração AOF e contabilidade de memória.
- Conferir os critérios técnicos e registrar evidências locais; encerrar este marco interno sem candidata, tag ou release publicada. Artefatos usados em migração ficam congelados por SHA e hashes.

### 0.6.0: Sorted sets

- Adicionar ZADD básico, ZREM, ZCARD, ZSCORE e ZRANGE start stop [WITHSCORES].
- Ordenar por score e desempatar por bytes do membro; manter persistência e limites.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R06-01` | Armazenamento ordenado | R05-01 |
| `R06-02` | Comandos básicos de sorted sets | R06-01 |
| `R06-03` | Integração de sorted sets com persistência e limites | R06-02, R05-05 |
| `R06-GATE` | Validar marco interno 0.6.0 | R06-01, R06-02, R06-03 |

Evidências obrigatórias: `native`, `compatibility`, `tcp_smoke`, `sorted_sets`.

Critérios para encerrar o marco:

- Scores inválidos tratados, desempate binário e índices negativos verificados.
- Gate sorted_sets e verificações locais aprovados; recovery preserva scores e ordenação.
- Conferir os critérios técnicos e registrar evidências locais; encerrar este marco interno sem candidata, tag ou release publicada. Artefatos usados em migração ficam congelados por SHA e hashes.

### 0.7.0: Transações

- Adicionar MULTI, EXEC, DISCARD, WATCH e UNWATCH limitados a um shard.
- Separar erros de enfileiramento de erros individuais de execução sem rollback; persistir o lote atomicamente.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R07-01` | Estado transacional por conexão | R04-03, R03-02 |
| `R07-02` | Execução transacional em lote | R07-01 |
| `R07-03` | WATCH e UNWATCH | R07-02, R02-03 |
| `R07-04` | Persistência atômica do lote | R07-03, R04-04 |
| `R07-GATE` | Validar marco interno 0.7.0 | R07-01, R07-02, R07-03, R07-04 |

Evidências obrigatórias: `native`, `compatibility`, `tcp_smoke`, `transactions`.

Critérios para encerrar o marco:

- Transações de um shard, conflitos e expiração de WATCH verificados; erros seguem o subconjunto Redis declarado.
- Gate transactions e verificações locais aprovados; replay não aplica meia transação.
- Conferir os critérios técnicos e registrar evidências locais; encerrar este marco interno sem candidata, tag ou release publicada. Artefatos usados em migração ficam congelados por SHA e hashes.

### 0.8.0: Pub/Sub

- Adicionar SUBSCRIBE, UNSUBSCRIBE, PUBLISH e PING no modo assinante RESP2.
- Inscrições e mensagens são efêmeras; filas limitadas e clientes lentos não bloqueiam o banco.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R08-01` | Registro de assinaturas | R01-04 |
| `R08-02` | Publicação de mensagens | R08-01 |
| `R08-03` | Modo assinante | R08-02 |
| `R08-04` | Controle de clientes lentos | R08-03 |
| `R08-GATE` | Validar marco interno 0.8.0 | R08-01, R08-02, R08-03, R08-04 |

Evidências obrigatórias: `native`, `compatibility`, `tcp_smoke`, `pubsub`.

Critérios para encerrar o marco:

- Assinantes lentos não bloqueiam banco ou assinantes saudáveis; filas limitadas e cleanup comprovados.
- Gate pubsub e verificações locais aprovados, incluindo comportamento de PING no modo assinante.
- Conferir os critérios técnicos e registrar evidências locais; encerrar este marco interno sem candidata, tag ou release publicada. Artefatos usados em migração ficam congelados por SHA e hashes.

### 0.9.0: Replicação

- Replicação assíncrona Sider→Sider da mesma versão e configuração de shards.
- Sincronização completa e incremental, reconexão, réplicas somente leitura e promoção manual; sem failover automático.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R09-01` | Protocolo interno versionado | R03-04, R04-04, R07-04 |
| `R09-02` | Sincronização completa | R09-01, R05-05, R06-03 |
| `R09-03` | Acompanhamento incremental | R09-02 |
| `R09-04` | Reconexão de réplicas | R09-03 |
| `R09-05` | Promoção manual | R09-04 |
| `R09-GATE` | Validar marco interno 0.9.0 | R09-01, R09-02, R09-03, R09-04, R09-05 |

Evidências obrigatórias: `native`, `compatibility`, `tcp_smoke`, `replication`.

Critérios para encerrar o marco:

- Sincronização consistente entre shards, réplicas somente leitura, TTL e transações preservados.
- Gate replication e verificações locais aprovados; backlog insuficiente força sincronização completa e promoção é manual.
- Conferir os critérios técnicos e registrar evidências locais; encerrar este marco interno sem candidata, tag ou release publicada. Artefatos usados em migração ficam congelados por SHA e hashes.

### 0.10.0: Operação e distribuição

- Métricas, diagnóstico e procedimentos reproduzíveis de backup e restauração.
- Distribuir imagem Docker Linux amd64 como arquivo anexado à release privada; comprovar operação e encerramento.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R10-01` | Métricas | R01-04 |
| `R10-02` | Diagnóstico operacional | R10-01 |
| `R10-03` | Procedimentos de backup e restauração | R03-04, R04-04, R05-05, R06-03, R07-04 |
| `R10-04` | Imagem Docker | R03-03 |
| `R10-05` | Ensaios operacionais | R10-01, R10-02, R10-03, R10-04, R01-GATE, R02-GATE, R03-GATE, R04-GATE, R05-GATE, R06-GATE, R07-GATE, R08-GATE, R09-GATE |
| `R10-GATE` | Validar marco interno 0.10.0 | R10-01, R10-02, R10-03, R10-04, R10-05 |

Evidências obrigatórias: `native`, `compatibility`, `tcp_smoke`, `docker`.

Critérios para encerrar o marco:

- Métricas verificadas, restauração reproduzível, encerramento e limites documentados.
- Gate docker e verificações locais aprovados; imagem executável exportada acompanha os pacotes e checksums privados.
- Conferir os critérios técnicos e registrar evidências locais; encerrar este marco interno sem candidata, tag ou release publicada. Artefatos usados em migração ficam congelados por SHA e hashes.

### 1.0.0: Estabilização

- Congelar o subconjunto compatível, comprovar recuperação e migração, executar carga prolongada e benchmarks reproduzíveis.
- Incluir sorted sets, transações e replicação Sider→Sider; documentar garantias sem prometer capacidades fora do escopo.

| ID | Entrega | Dependências |
| --- | --- | --- |
| `R11-01` | Congelamento do subconjunto | R10-GATE |
| `R11-02` | Auditoria diferencial | R11-01 |
| `R11-03` | Testes prolongados | R11-01 |
| `R11-04` | Migração da baseline interna R10 | R10-GATE |
| `R11-05` | Benchmarks e documentação final | R11-01 |
| `R11-GATE` | Validar e publicar a 1.0.0 | R11-01, R11-02, R11-03, R11-04, R11-05, R01-GATE, R02-GATE, R03-GATE, R04-GATE, R05-GATE, R06-GATE, R07-GATE, R08-GATE, R09-GATE, R10-GATE |

Evidências obrigatórias: `native`, `compatibility`, `tcp_smoke`, `crash`, `recovery`, `migration`, `sharding`, `types`, `sorted_sets`, `transactions`, `pubsub`, `replication`, `docker`, `soak`, `benchmarks`.

Critérios para encerrar o marco:

- Sorted sets, transações e replicação incluídos; matriz completa sem falha conhecida de corrupção ou perda além das garantias declaradas.
- Todos os gates aprovados, incluindo soak de 3600 segundos, migração da baseline interna R10 identificada por SHA e hashes e benchmarks.
- Candidata aprovada no SHA e bundle imutáveis; final promove os mesmos arquivos e o mesmo SHA, sem recompilação. Qualquer mudança no bundle exige outra candidata; encerrar o milestone após conferir a publicação final.

## Execução e publicação

1. Selecione tarefas desbloqueadas pelo DAG técnico; trilhas independentes podem avançar em paralelo.
2. Inclua testes e evidências; mantenha código compilável e commits atômicos em cada etapa.
3. Integre o PR vinculado à issue por merge commit após verificação manual registrada.
4. Encerre R01 a R10 após conferir os critérios locais; esses marcos não criam candidata, tag ou release.
5. Depois de validar todos os marcos, prepare a candidata 1.0 em um SHA e bundle imutáveis; o merge não dispara publicação.
6. A final promove exatamente o mesmo SHA e os mesmos assets aprovados na candidata, sem recompilar. Qualquer mudança no bundle exige outra candidata.
7. Encerre R11 somente após conferir a publicação final e seus artefatos.

O fluxo completo e os comandos de preparação estão no [guia de releases](docs/releases.md).
Não há workflows de CI nem publicador automático neste repositório.
A candidata 1.0 exige uma hora de carga contínua, além de todos os gates das capacidades entregues.
Teste obrigatório ausente, ignorado, cancelado ou sem relatório bloqueia o encerramento do marco e a publicação.
Migração usa fixtures e executáveis das baselines internas congeladas por SHA e hashes; a 1.0 migra a baseline R10.

Os pacotes são Linux GNU x86_64 (`.tar.gz`, Ubuntu 24.04) e Windows MSVC x86_64 (`.zip`).
R10 valida a imagem Docker Linux amd64 exportada que acompanha a publicação privada da 1.0.
Checksums SHA-256, manifesto de build e notas acompanham os binários testados depois da extração.

Patches publicáveis, como `1.0.1`, precisam de registro próprio no manifesto e de candidata.
Mudanças de compatibilidade dos marcos internos permanecem documentadas. Não há datas artificiais.

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
