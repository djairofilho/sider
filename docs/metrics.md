# Métricas e diagnóstico operacional

O Sider expõe indicadores da instância pelo comando `INFO` e imprime a
configuração efetiva com `sider --diagnose`. Não há listener HTTP, exporter,
arquivo periódico de métricas nem rótulos derivados de chaves ou canais.

- [Consultar a instância](#consultar-a-instância)
- [Validar a configuração sem iniciar o banco](#validar-a-configuração-sem-iniciar-o-banco)
- [Contrato dos indicadores](#contrato-dos-indicadores)
- [Procedimentos de diagnóstico](#procedimentos-de-diagnóstico)
- [Verificação reproduzível](#verificação-reproduzível)

## Consultar a instância

```sh
redis-cli -h 127.0.0.1 -p 6379 INFO
redis-cli -h 127.0.0.1 -p 6379 INFO clients stats
redis-cli -h 127.0.0.1 -p 6379 INFO memory persistence
redis-cli -h 127.0.0.1 -p 6379 INFO replication persistence
redis-cli -h 127.0.0.1 -p 6379 INFO config
```

A resposta é uma bulk string RESP2, com títulos `# Section` e linhas
`nome:valor` terminadas em CRLF. As seções disponíveis são `server`, `clients`,
`stats`, `memory`, `persistence`, `replication` e `config`. Os nomes não distinguem caixa ASCII;
repetições não duplicam a saída. Sem argumentos, ou com `all`, `default` ou
`everything`, todas as seções são selecionadas. Nomes desconhecidos são
ignorados; uma seleção inteiramente desconhecida produz uma string vazia.

A sintaxe de seleção segue a [interface INFO do Redis](https://redis.io/docs/latest/commands/info/).
O conteúdo é um contrato próprio do Sider, identificado por
`metrics_schema_version:1`; não reproduz todos os campos ou seções do Redis.
Ferramentas que exigem campos Redis específicos precisam de adaptação.

`INFO` não entra na fila do worker em uma conexão normal. Uma conexão já
admitida pode consultá-lo durante saturação da fila. O comando continua sujeito
ao limite de conexões, às restrições do modo assinante e aos limites de resposta
e escrita do [contrato de rede](network.md). Se a resposta inteira não couber,
a conexão fecha sem escrever um prefixo parcial. Se apenas uma seção interessar,
selecione-a para reduzir o tamanho da resposta.

Dentro de `MULTI`, `INFO` é enfileirado e executado com `EXEC`. O diagnóstico
descreve o estado confirmado quando o worker monta as respostas do lote, após
aplicar suas mutações. Não é uma fotografia do ponto intermediário de cada
comando da fila. A consulta não acrescenta um registro AOF. As regras de
[transações](transactions.md), inclusive limites do agregado RESP2, permanecem.

O servidor ainda não oferece autorização administrativa para esse comando.
Use a exposição existente da instância apenas no ambiente controlado previsto
para o projeto. A saída não contém chaves, valores, nomes de canais, caminhos de
arquivos ou valores inválidos de configuração.

## Validar a configuração sem iniciar o banco

```sh
cargo run --locked -- --diagnose
```

O binário carrega e valida as mesmas variáveis `SIDER_*` da inicialização,
imprime versão, endereço configurado, limites e política AOF, e termina com
código zero. Uma configuração inválida termina com código diferente de zero e
identifica a opção ou restrição, sem repetir o valor recebido.

O campo `diagnostic_scope:configuration_only` delimita essa verificação:
nenhum runtime, listener, arquivo de prontidão ou diretório AOF é criado.
Não há recuperação, truncamento, leitura de dados persistidos ou teste de
conectividade. Por isso, uma porta ocupada ou diretório sem permissão pode
coexistir com um diagnóstico de configuração válido. O comando não atesta
prontidão, integridade da AOF, espaço em disco ou saúde de uma réplica.

`ready_file_enabled`, `aof_configured`, `replication_configured`,
`replication_upstream_configured` e `replication_ready_file_enabled` indicam
presença das opções, sem imprimir caminhos nem endpoints de replicação. Os
limites numéricos da replicação também são impressos quando configurados. Isso
não determina o papel persistido da instância: uma promoção durável prevalece
sobre a configuração de upstream. Em uma instância em execução, `tcp_port` informa a porta efetiva;
no diagnóstico offline, `bind_addr` informa o endereço solicitado, inclusive
porta zero quando configurada.

## Contrato dos indicadores

Os nomes são fixos. Não existem labels por cliente, shard, chave ou canal.
Os vetores internos têm o número configurado de shards, limitado a 256; não
crescem com o dataset. Os contadores usam incrementos atômicos saturados em
`u64::MAX` e reiniciam quando a instância é recriada. As sequências e a geração
AOF vêm do escritor real e podem ser restauradas de uma execução anterior.

Cada subsistema fornece uma observação curta, sem I/O de arquivos ou sockets.
A leitura inteira não é uma barreira global: comandos concorrentes podem mudar
o estado entre campos ou shards. Gauges de dataset são publicados após `apply`;
preparar uma operação ou rejeitá-la por quota/AOF não publica um estado futuro.
As filas são medidas em pedidos, sem incluir o pedido já retirado para execução.
Uma transação aceita ocupa um pedido, mesmo contendo vários comandos.

### Servidor, conexões e comandos

| Campo | Unidade e definição |
| --- | --- |
| `sider_version` | Versão Cargo do binário; não identifica aprovação de release |
| `metrics_schema_version` | Versão do contrato de métricas, atualmente `1` |
| `uptime_seconds` | Segundos monotônicos desde a criação do coletor da instância |
| `tcp_port` | Porta efetiva do listener |
| `connected_clients` | Tarefas de conexão ativas; drop, cancelamento e EOF liberam o gauge |
| `total_connections_received` | Conexões admitidas que iniciaram sua tarefa de atendimento |
| `rejected_connections` | Conexões recusadas pelo limite simultâneo |
| `commands_received_total` | Frames completos entregues ao parser, inclusive comandos inválidos e controles transacionais |
| `command_error_replies_total` | Frames de erro de comando produzidos para resposta; inclui erros individuais de EXEC, mas não confirma entrega ao cliente |
| `protocol_errors_total` | Conexões encerradas por codec, formato inválido, EOF truncado, buffer de entrada ou prazo de formação do frame |
| `connection_failures_total` | Falhas que encerram atendimento, mais falhas ao configurar o socket; parada normal e cancelamento externo não contam |
| `client_write_timeouts_total` | Conexões encerradas pelo prazo de escrita |
| `response_encoding_failures_total` | Conexões encerradas porque a resposta não pôde ser codificada nos limites |

Um comando recebido dentro de `MULTI` conta uma vez ao chegar, mesmo que seja
descartado depois. `EXEC` conta como outra requisição; os comandos da fila não
são contados novamente ao executar. Um frame recusado pelo codec antes de
ficar completo não incrementa `commands_received_total`. O erro genérico de
protocolo é classificado em `protocol_errors_total`, sem ser confundido com
erro de execução de comando.

### Filas, dataset, expiração e Pub/Sub

| Campo | Unidade e definição |
| --- | --- |
| `worker_requests_accepted_total` | Envios concluídos às filas de workers, inclusive lotes e WATCH |
| `worker_timeouts_total` | Pedidos cujo chamador observou o prazo total excedido, antes ou depois da aceitação |
| `worker_failures_total` | Pedidos cujo chamador observou `Unavailable` |
| `worker_queue_used` | Soma observada dos pedidos aguardando nos canais |
| `worker_queue_capacity` | Soma das capacidades fixas desses canais |
| `dataset_keys` | Entradas físicas, inclusive expiradas ainda não retiradas |
| `dataset_expiring_keys` | Eventos presentes nos índices de expiração |
| `dataset_logical_bytes` | Soma do consumo lógico contabilizado pelo Store |
| `dataset_quota_bytes` | Soma das quotas lógicas atribuídas aos Stores |
| `expiration_batches_total` | Lotes não vazios de origem `Expiration` aplicados |
| `expiration_batch_keys_removed_total` | Tombstones aplicados nesses lotes de expiração |
| `pubsub_channels` | Canais com pelo menos uma inscrição |
| `pubsub_subscribers` | Conexões com pelo menos uma inscrição |
| `pubsub_subscriptions` | Pares únicos conexão/canal ativos |
| `pubsub_deliveries_total` | Filas de assinantes que aceitaram notificações |
| `pubsub_evictions_total` | Assinantes removidos quando `try_send` recusou uma notificação |

`dataset_logical_bytes` e `dataset_quota_bytes` não são RSS: não incluem todo o
custo de sockets, buffers, filas, runtime, alocador ou estruturas temporárias.
Um erro de quota preserva o consumo anterior. As contagens de expiração cobrem
lotes cuja origem resolvida é `Expiration`; não classificam como expiração
toda remoção feita por comandos ou por um lote misto de `EXEC`.

`worker_timeouts_total` não permite inferir que o comando deixou de executar.
Após aceitação, o worker mantém a operação mesmo quando o chamador desiste.
`pubsub_deliveries_total` confirma entrada na fila, não escrita no socket nem
leitura pelo cliente. As regras de desligamento do assinante estão no
[guia de Pub/Sub](pubsub.md).

### Persistência

`aof_enabled` informa se há um escritor anexado aos workers. Os demais campos
dessa seção só existem quando esse consumidor real está presente.

| Campo | Unidade e definição |
| --- | --- |
| `aof_running`, `aof_failed` | Estado do escritor, representado por `0` ou `1` |
| `aof_written_sequence` | Última sequência cujo registro concluiu a escrita no arquivo ativo |
| `aof_synced_sequence` | Última sequência coberta por sincronização confirmada do escritor |
| `aof_generation` | Geração ativa do arquivo |
| `aof_bytes_since_compaction` | Bytes contabilizados pelo escritor desde a última compactação |
| `aof_dirty`, `aof_compacting` | Escrita pendente de sincronização e compactação em andamento, em `0`/`1` |
| `aof_queue_used`, `aof_queue_capacity` | Pedidos na fila e capacidade fixa do escritor |
| `aof_records_written_total` | Registros de append escritos nesta execução |
| `aof_active_file_syncs_total` | Sincronizações confirmadas do arquivo ativo; não soma fsyncs auxiliares do produtor de snapshots |
| `aof_fatal_failures_total` | Encerramentos do escritor com erro |
| `aof_record_rejections_total` | Appends recusados pelo limite de formato antes de escrever |
| `aof_compactions_total` | Compactações concluídas |
| `aof_compaction_failures_total` | Compactações iniciadas e abortadas ou concluídas com erro |
| `aof_last_error` | Última categoria estática de erro, ou `none`; não é apagada por um sucesso posterior |

As categorias incluem `io`, `record_limit`, `format`, `configuration`, `replay`,
`directory_locked`, `unavailable`, `sequence`, `compaction_busy`,
`compaction_delta_limit`, `layout_mismatch`, `cross_shard`, `shard_quota` e
`migration`. Elas não retêm o texto original do erro nem caminhos.
O gauge de fila fica em zero quando o observador já não encontra um canal ativo.

Escrita na AOF e aplicação no Store são fronteiras diferentes. Uma falha antes
da sincronização pode deixar `written_sequence > synced_sequence` e impedir a
aplicação da mutação ao dataset. Em política periódica, essa diferença também
pode representar a janela normal entre sincronizações. Leia ambos os campos
junto de `aof_failed`, `aof_dirty`, política e logs; uma sequência isolada não
prova confirmação ao cliente.

### Replicação

`INFO replication` observa o runtime usado pelas sessões e pelo escritor AOF.
Sem replicação configurada, a seção contém apenas `replication_enabled:0`.
As demais linhas existem somente com o observador real anexado. A leitura não
envia pedidos aos workers, não consulta o upstream e não mantém canais do banco
ou do escritor abertos após shutdown.

| Campo | Unidade e definição |
| --- | --- |
| `replication_enabled` | Presença do runtime de replicação, em `0`/`1` |
| `replication_role` | Papel efetivo `primary` ou `replica`, incluindo promoção persistida |
| `replication_connected` | Sessão de upstream estabelecida, em `0`/`1`; no primário é `0` e não conta conexões de réplicas |
| `replication_epoch_known` | Cursor pertence a uma época inicializada, em `0`/`1` |
| `replication_epoch` | Identificador fixo de 32 dígitos hexadecimais, presente quando a época é conhecida |
| `replication_head_sequence` | Somente primário: último lote publicado no journal após append; pode preceder apply no Store |
| `replication_applied_sequence` | Somente réplica: última posição confirmada após instalação de snapshot ou flush e apply do lote |
| `replication_upstream_sequence` | Somente réplica: última sequência informada pelo upstream; pode estar desatualizada após desconexão |
| `replication_lag_known` | Há sessão conectada, época conhecida e upstream observado maior ou igual ao aplicado, em `0`/`1` |
| `replication_lag_batches` | Diferença upstream menos aplicado, em lotes; só existe quando `replication_lag_known:1` |
| `replication_full_syncs_total` | Sincronizações completas que estabeleceram sessão nesta execução |
| `replication_partial_syncs_total` | Continuações aceitas que estabeleceram sessão nesta execução |
| `replication_reconnects_total` | Tentativas de sessão nesta execução, incluindo a primeira |
| `replication_backlog_bytes`, `replication_backlog_batches` | Bytes e lotes retidos no journal ativo do primário |
| `replication_oldest_sequence` | Primeira sequência retida; ausente quando o journal está vazio |

Campos de sequência aplicada/head são omitidos até a época ser conhecida.
Durante sincronização inicial, desconexão ou observação de upstream anterior
ao cursor local, o atraso fica desconhecido. Sua ausência não representa zero.
Mesmo um atraso conhecido igual a zero compara com a última observação do
upstream; não promete igualdade instantânea com escritas concorrentes. A unidade
é lote, não comando, byte ou segundo. Uma transação pode conter vários comandos
em uma sequência. Compare sequências entre processos apenas na mesma época.

Gauges de dataset são atualizados após os controles de instalação e aplicação
da replicação. Os contadores de sessão reiniciam com o processo; papel, época
e cursor podem vir do estado durável. Uma sessão antiga não pode sobrescrever
o estado da sessão nova ou desfazer uma promoção. As observações do journal e
do runtime são curtas e podem refletir instantes próximos, sem barreira global.

### Configuração

A seção `config` imprime limites numéricos do codec, buffers, conexões, filas,
shards, dataset, Pub/Sub, transações, WATCH e prazos em milissegundos. Quando
AOF está configurado, também imprime capacidade de fila, limites de registros,
mutações e delta, limiar de compactação e política de sincronização. Opções
de arquivo são expostas somente por flags de presença.

Com replicação configurada, aparecem `replication_backlog_limit_bytes`,
`replication_backlog_limit_batches`, `replication_max_connections` e os prazos
`replication_frame_timeout_ms`, `replication_sync_timeout_ms`,
`replication_reconnect_min_ms` e `replication_reconnect_max_ms`. Esses campos
descrevem limites configurados; os gauges da seção `replication` medem o uso.

`worker_queue_capacity_per_shard` é a capacidade configurada de cada canal;
`worker_queue_capacity`, na seção `stats`, é a soma observada de todos eles.
Os nomes permanecem únicos mesmo quando todas as seções são selecionadas.

## Procedimentos de diagnóstico

| Situação | Evidência e ação |
| --- | --- |
| Fila cheia | Leia `INFO stats config` por uma conexão já admitida. Compare uso/capacidade e variação de timeouts; reduza concorrência de produtores e confira os prazos. Não repita automaticamente uma escrita com resultado desconhecido. Aumentar a fila exige considerar memória e tempo de espera. |
| Limite de conexões | Compare `connected_clients`, `rejected_connections` e `max_connections`; feche conexões ociosas no cliente. Não há vaga administrativa reservada. |
| Disco indisponível | Consulte `INFO persistence` enquanto a instância ainda atende e preserve os logs de falha do AOF. Um erro fatal encerra o escritor e a supervisão, então INFO pode deixar de estar acessível. Corrija espaço/permissão/dispositivo e faça a recuperação normal em um procedimento controlado; `--diagnose` não valida nem repara o disco. |
| Registro grande | `aof_record_rejections_total` cresce e `aof_failed` permanece zero. Reduza o lote ou revise o limite de registro dentro dos limites suportados. A rejeição ocorre antes de aplicar o lote. |
| Cliente lento | Veja `client_write_timeouts_total`, `pubsub_evictions_total` e logs de prazo de escrita/assinante encerrado. Faça o consumidor drenar respostas e notificações; reconecte e refaça inscrições após encerramento. Uma fila maior só amplia a tolerância temporária. |
| Resposta acima do limite | `response_encoding_failures_total` aumenta. Reduza o tamanho solicitado, selecione menos seções de INFO ou ajuste limites coerentes. O comando pode já ter produzido efeitos antes da falha da resposta. |
| Quota lógica | Compare consumo e quota em `INFO memory`; confira erros de quota. Remova dados ou aumente o orçamento de forma controlada, sem tratar quota lógica como memória do processo. |
| Réplica conectada com atraso | Consulte `INFO replication persistence` nos dois lados. Compare épocas, head do primário, aplicado e último upstream da réplica em observações sucessivas. Verifique fila/disco da réplica e crescimento do backlog. Zero observado não comprova recebimento de uma escrita posterior. |
| Réplica desconectada | `replication_connected:0` e `replication_lag_known:0` tornam o atraso desconhecido. Confira processos, prontidão interna, rede e logs estáticos; corrija a causa. A sessão tenta reconectar dentro dos prazos configurados. O aumento de `replication_reconnects_total` mostra tentativas, não sucesso. |
| Repetição de sincronização completa | Observe crescimento de `replication_full_syncs_total`, retenção por bytes/lotes e capacidade de aplicar no destino. Se o cursor sair da retenção, a retomada exige FULL. Corrija a lentidão ou dimensione o backlog dentro do orçamento de memória; apenas aumentar timeout não preserva histórico descartado. |
| Falha de AOF na réplica | Compare os indicadores AOF locais e preserve os logs. Não interprete append sem flush/apply como posição aplicada. Corrija o armazenamento antes da recuperação; `--diagnose` não inspeciona o arquivo e não o repara. |
| Promoção deliberada | Confira a posição aplicada e a época antes da decisão operacional. Isole o antigo primário e redirecione os clientes em procedimento controlado. `sider-replica --addr IP:PORT --promote`, no listener interno de loopback, persiste novo papel/época e cancela a sessão antiga. Confirme `replication_role:primary` e a nova época. Não há failover automático nem reconciliação de escritas divergentes. |

Para consultar o protocolo administrativo existente, use
`sider-replica --addr IP:PORT --status` no endpoint interno. Esse endpoint é
diferente da porta RESP usada por `redis-cli`. A prontidão interna publicada
por `SIDER_REPLICATION_READY_FILE` identifica o listener da instância; sua
presença não comprova que a réplica terminou de sincronizar. Não remova o papel
persistido nem a AOF para forçar uma reconexão depois de promover: a recuperação
de topologia exige decidir qual história será preservada.

Os eventos existentes registram início/parada, recusa de conexões, falhas de
atendimento, falhas de append/sync e conclusão/aborto de compactação automática.
Os indicadores identificam categorias e volumes sem acrescentar payloads aos
logs. Não existe comando de redefinição dos contadores nesta entrega.

## Verificação reproduzível

```sh
cargo test --locked --lib metrics_ -- --nocapture
cargo test --locked --test metrics -- --nocapture
cargo test --locked --test cli metrics_ -- --nocapture
cargo test --locked --test replication_network metrics_replication -- --nocapture
cargo clippy --locked --lib --test metrics --test cli --test replication_network -- -D warnings
```

Os testes nativos cobrem concorrência sem perda de incrementos, nomes
fixos, seções desconhecidas/binárias, Pub/Sub lento/rápido, cancelamento,
contagens de EXEC, fila saturada, timeout, quota, expiração e limites de saída.
Um deles usa quatro shards: mantém um pedido aceito sem executar, consulta INFO
enquanto o snapshot global aguarda esse pedido e confere os gauges após apply.
Quatro testes de integração conferem dois cenários TCP com quatro shards e o diagnóstico AOF em
append, compactação, falha fatal de disco e rejeição recuperável por tamanho.
Dois testes CLI verificam código de saída, ocultação de valores inválidos e
ausência de efeitos em listener, diretório AOF e arquivo de prontidão.
O teste de replicação por TCP inicia processos reais com quatro shards, confere
FULL e delta, consulta INFO dentro de EXEC, encerra o primário e verifica que
o atraso vira desconhecido; depois promove a réplica e confere papel, época e
head. O teste de estado cobre uma sessão antiga tentando atualizar a nova,
retomada parcial, cursor ainda desconhecido e upstream temporariamente anterior
ao aplicado. O teste offline usa também uma porta interna ocupada e confirma
ausência de criação de AOF e prontidão.

Esses resultados conferem o contrato funcional no Windows. Não constituem
benchmark do custo da instrumentação nem gate Docker/Linux. A validação integrada do milestone deve repetir apenas os
caminhos exigidos pelas mudanças compostas e registrar o SHA efetivo.
