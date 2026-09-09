# Matriz do subconjunto 1.0

Esta é a consolidação técnica para R11-01 e R11-02. O checkout prepara o pacote
`1.0.0`; os ensaios do build candidato e a publicação dependem dos gates e dos
arquivos exatos. Os marcos internos e a baseline R10 conservaram `0.1.0`, sem
ampliar a release histórica 0.1 ou aprovar a candidata atual.

O contrato Redis usa servidor e `redis-cli` **8.10.1**, imagem Linux amd64
fixada por tag e digest em [releases/plan.json](../releases/plan.json).
Todas as formas abaixo usam arrays RESP2 não vazios de bulk strings não nulas.
Nomes e opções não distinguem maiúsculas/minúsculas ASCII; chaves, valores, membros,
campos e canais preservam bytes arbitrários, inclusive vazio e bytes não UTF-8.
`[argumento]` significa opcional; `...` repete o grupo anterior.

## Formas atendidas

As siglas de evidência remetem aos arquivos e resultados na seção seguinte.
Limites próprios de memória, filas e rede também se aplicam a cada forma.

| Forma | Contrato e restrições | Evidência |
| --- | --- | --- |
| `PING [mensagem]` | Modo normal: `PONG` ou bulk; modo assinante: array `pong`/mensagem | R, P, X |
| `ECHO mensagem` | Devolve exatamente os bytes | R |
| `GET chave` | String ou nulo; coleção produz `WRONGTYPE` | R, S, C, X |
| `SET chave valor [NX\|XX] [EX segundos\|PX ms\|KEEPTTL] [GET]` | Última escrita substitui o tipo; GET exige string antes de avaliar NX/XX; sem EXAT/PXAT | S, C, X |
| `DEL chave [chave ...]` | Conta entradas removidas uma vez; elimina TTL; multichave exige mesmo shard | R, S, C, X |
| `EXISTS chave [chave ...]` | Conta duplicatas; não conta entradas expiradas; mesmo shard | S |
| `INCR chave` | i64 decimal canônico; ausência parte de zero; erro preserva valor e TTL | S, X |
| `DECR chave` | Mesmas regras numéricas de INCR; detecta underflow | S |
| `MGET chave [chave ...]` | Array em ordem, preserva duplicatas; coleção aparece como nulo; mesmo shard | S, C, X |
| `MSET chave valor [chave valor ...]` | Um lote, último par repetido vence; substitui tipos e limpa TTL; mesmo shard | S, C |
| `EXPIRE chave segundos` | Prazo relativo; valor não positivo remove; sem NX/XX/GT/LT | S |
| `PEXPIRE chave ms` | Prazo relativo em milissegundos; remoção imediata com zero/negativo | S, C, X |
| `TTL chave` | Segundos restantes, -1 persistente, -2 ausente/expirada | S, X |
| `PTTL chave` | Milissegundos restantes, -1 persistente, -2 ausente/expirada | S |
| `PERSIST chave` | Remove prazo existente e responde 1; demais casos respondem 0 | S, C, X |
| `HSET chave campo valor [campo valor ...]` | Conta campos novos; último campo repetido vence | C, X |
| `HGET chave campo` | Bulk ou nulo | C, X |
| `HDEL chave campo [campo ...]` | Conta campos removidos; último campo elimina a chave/TTL | C |
| `HEXISTS chave campo` | 0 ou 1 | C |
| `HLEN chave` | Cardinalidade; zero se ausente | C |
| `HGETALL chave` | Pares campo/valor, sem ordem pública garantida | C |
| `LPUSH chave valor [valor ...]` | Insere cada argumento à esquerda | C |
| `RPUSH chave valor [valor ...]` | Insere cada argumento à direita | C, X |
| `LPOP chave` | Remove um elemento à esquerda; sem opção count | C |
| `RPOP chave` | Remove um elemento à direita; sem opção count | C |
| `LLEN chave` | Comprimento; zero se ausente | C |
| `LRANGE chave início fim` | Índices i64, negativos a partir do fim; extremos inclusivos | C, X |
| `SADD chave membro [membro ...]` | Conta membros novos, sem duplicatas | C, X |
| `SREM chave membro [membro ...]` | Conta remoções, sem duplicatas; último membro elimina a chave | C |
| `SISMEMBER chave membro` | 0 ou 1 | C, X |
| `SCARD chave` | Cardinalidade; zero se ausente | C |
| `SMEMBERS chave` | Membros únicos, sem ordem pública garantida | C |
| `ZADD chave score membro [score membro ...]` | Somente pares básicos; valida todos os scores antes de alterar | Z, X |
| `ZREM chave membro [membro ...]` | Conta remoções, sem duplicatas; último membro elimina a chave | Z |
| `ZCARD chave` | Cardinalidade; zero se ausente | Z |
| `ZSCORE chave membro` | Score como bulk ou nulo | Z |
| `ZRANGE chave início fim [WITHSCORES]` | Rank inclusivo, índices negativos; sem BYSCORE/BYLEX/REV/LIMIT | Z, X |
| `MULTI` | Inicia fila da conexão; não executa os comandos enfileirados | T, X |
| `EXEC` | Executa no mesmo shard; array em ordem, sem rollback de erros individuais | T, X |
| `DISCARD` | Descarta fila e observações; erro fora de MULTI | T, X |
| `WATCH chave [chave ...]` | Observa até EXEC/DISCARD/UNWATCH/EOF; conflito inclui expiração e ABA | T, X |
| `UNWATCH` | Remove observações; dentro de MULTI é enfileirado | T, X |
| `SUBSCRIBE canal [canal ...]` | Confirma cada argumento e entra no modo assinante RESP2 | P, T, X |
| `UNSUBSCRIBE [canal ...]` | Confirma cada argumento; sem argumentos remove todas; volta ao normal na última | P, T, X |
| `PUBLISH canal mensagem` | Conta filas que aceitaram; somente modo normal ou comando previamente enfileirado | P, T, X |
| `INFO [seção ...]` | Diagnóstico próprio do Sider; não replica todos os campos do Redis | I |

Operações específicas de coleção exigem o tipo correspondente. Mutações válidas
preservam TTL; remoção do último item elimina chave, prazo e quota. `SET` sem GET
e `MSET` podem substituir coleções. Erro de tipo, score, inteiro ou quota não
deixa uma alteração parcial do comando. Consulte [coleções](collections.md),
[sorted sets](sorted-sets.md) e [strings](strings.md) para detalhes e precedência
dos erros verificados.

Scores são IEEE-754 f64. Aceitam os decimais, expoentes, hexadecimais e infinitos
verificados; rejeitam NaN, espaços, overflow finito e underflow não zero até zero.
Zero negativo é normalizado. Ordem e representação textual de scores são
comparadas exatamente, sem normalização pelo teste.

## Evidência por forma

| Sigla | Testes e referência | Escopo observado |
| --- | --- | --- |
| R | [commands.rs](../tests/commands.rs), [tcp.rs](../tests/tcp.rs), [compatibility.rs](../tests/compatibility.rs), Redis/CLI 8.10.1 | 3.588 comparações binárias históricas R01; nove casos CLI; fixtures, pipelines, binários e payloads até 1 MiB |
| S | [compatibility.rs](../tests/compatibility.rs), testes de strings/TTL em armazenamento e TCP, Redis 8.10.1 | 461 comparações binárias R02; prazos observados separadamente; 48 combinações de SET |
| C | [collections.rs](../tests/collections.rs), [collections_differential.rs](../tests/collections_differential.rs), Redis 8.10.1 | 2.383 comparações: 2.188 exatas e 195 normalizadas; apenas pares de HGETALL e membros de SMEMBERS têm ordem normalizada |
| Z | [sorted_sets.rs](../tests/sorted_sets.rs), [collections_differential.rs](../tests/collections_differential.rs), Redis 8.10.1 | 8.561 respostas exatas; scores, empate binário, rank, erros e extremos numéricos |
| T | [transactions.rs](../tests/transactions.rs), [transactions_persistence.rs](../tests/transactions_persistence.rs), Redis 8.10.1 | 91 respostas diferenciais históricas; WATCH, erros, framing de EXEC, cancelamento, append único e replay |
| P | [pubsub.rs](../tests/pubsub.rs), testes do hub e conexão, Redis 8.10.1 | 245 comparações históricas, 64 mensagens, 16 reconexões; filas e clientes lentos em testes próprios |
| I | [metrics.rs](../tests/metrics.rs), testes INFO/diagnóstico | Valores do Sider observados no sistema real; sem equivalência de campos Redis |
| X | [cross_family.rs](../tests/common/cross_family.rs), consumido pelo gate compatibility | Corpus R11 entre tipos, TTL, EXEC/WATCH e Pub/Sub; relatório próprio sem reatribuir contagens R01/R02 |

Os números históricos identificam seus ensaios de origem. Não comprovam outro
SHA ou uma candidata futura. A matriz completa da candidata exige execução dos
gates no build congelado, conforme [releases](releases.md). A contagem do corpus
X e seus comandos de reprodução ficam em [differential.md](differential.md).

## Protocolos, persistência e operação

| Capacidade | Contrato e limitação | Evidência responsável |
| --- | --- | --- |
| RESP2/TCP | Framing limitado, fragmentação/pipeline, um pedido por conexão em voo; requisição inválida fecha conexão | [resp_codec.rs](../tests/resp_codec.rs), [tcp.rs](../tests/tcp.rs), fixtures e diferenciais |
| Shards | FNV-1a 64 com hash tags; multichave e EXEC no mesmo shard; CROSSSLOT antes de efeito | [sharding.rs](../tests/sharding.rs), [transações](transactions.md) |
| AOF | Formato próprio, lote resolvido indivisível, checksum/limites/selo; sem compatibilidade de arquivo Redis | [persistence.rs](../tests/persistence.rs), [types-persistence.md](types-persistence.md) |
| Sync | `always` confirma após sync; `everysec` admite janela anterior ao sync | [persistence.md](persistence.md), testes de falha e recovery |
| Compactação | Snapshot global e delta; publicação mantém corte e lotes completos | [sharding.rs](../tests/sharding.rs), testes de persistência/transações |
| TTL durável | Prazo Unix absoluto no arquivo; tempo durante parada consome TTL | [aof_migration.rs](../tests/aof_migration.rs), [backup.rs](../tests/backup.rs) |
| Replicação | Sider→Sider assíncrona, mesma versão/layout, FULL/CONTINUE e ACK após apply durável | [replication_network.rs](../tests/replication_network.rs), [replication_storage.rs](../tests/replication_storage.rs) |
| Papel/promover | Réplica rejeita escrita de cliente; promoção explícita local; sem eleição, fencing distribuído ou failover automático | [replication_persistence.rs](../tests/replication_persistence.rs), CLI sider-replica |
| Backup | Export por listener interno, corte consistente, manifesto/checksums, restauração em diretório novo | [backup.rs](../tests/backup.rs), [backup_process.rs](../tests/backup_process.rs), [guia](backup.md) |
| Mudança de shards | Migração offline para novo diretório com quotas e replay verificados | [aof_migration.rs](../tests/aof_migration.rs), [guia](aof-migration.md) |
| Distribuição | Pacotes Linux/Windows com quatro binários; Docker privado contém os mesmos bytes Linux | [package.rs](../tests/package.rs), [docker_distribution.rs](../tests/docker_distribution.rs) |
| Observabilidade | INFO e `--diagnose` descrevem estado/configuração; sem conteúdo do dataset | [metrics.rs](../tests/metrics.rs), [metrics.md](metrics.md) |

Sem AOF, o processo não promete recuperação após término. Pub/Sub é efêmero e
não entra no AOF, backup ou replicação. ACK de replicação não torna a escrita
do primário síncrona. Uma queda do primário pode perder dados ainda não aplicados
na réplica. Timeout ou desconexão de um cliente depois do aceite não desfaz
comando nem prova ausência de efeito.

## Divergências intencionais

Há somente o banco lógico padrão e RESP2 em arrays. AUTH, ACL, TLS, SELECT,
RESP3, protocolo inline, Cluster/replicação Redis, scripts, módulos, streams,
eviction e comandos não listados estão fora do subconjunto. Não se promete
compatibilidade com bibliotecas que exigem HELLO, CLIENT ou COMMAND no handshake.

As opções não listadas de SET, EXPIRE, ZADD e ZRANGE não são capacidades
suportadas. O texto de rejeição de comando desconhecido é simplificado e não
repete argumentos. Aridades e erros das formas declaradas têm evidência própria;
isso não estende equivalência às formas excluídas.

Quota lógica por shard, filas, conexões, prazos e limite de resposta são políticas
do Sider. Não representam maxmemory, RSS ou defaults do Redis. Saída acima do
limite pode fechar a conexão; o comando aceito pode já ter alterado dados.
Pub/Sub expulsa assinante com fila cheia e sua contagem confirma aceitação na
fila, não leitura pelo cliente. HGETALL/SMEMBERS não têm ordem pública. A ordem
das confirmações de UNSUBSCRIBE sem argumentos e vários canais pode diferir.

## Política após a 1.0 e pendências de congelamento

Cada nova forma precisa de parser, semântica, limites e evidência antes de entrar
na matriz. Mudança incompatível no contrato declarado exige nova versão major
e guia de migração. Correções que restabeleçam o contrato podem ser patches,
com regressão reproduzível e descrição do comportamento alterado. Adições
compatíveis podem entrar em minor; nenhuma delas amplia retroativamente os
artefatos já publicados.

Evolução do AOF exige versão explícita, fixtures e migração verificadas. Não se
promete ler versões futuras nem manter replicação entre versões diferentes.
A matriz registra as divergências intencionais; um resultado divergente fora
dela é investigado como defeito ou lacuna antes da publicação.

O responsável pela integração confere a baseline R10 completa, migração para
1.0, soak de pelo menos 3.600 segundos, benchmarks e todos os gates da candidata.
Esses itens continuam pendentes até evidência do SHA e assets exatos. Este
documento e o corpus X não encerram R10, R11 ou a aprovação de release.
