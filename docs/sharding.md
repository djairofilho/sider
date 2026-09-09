# Shards e roteamento

`SIDER_SHARDS` define de 1 a 256 workers proprietários, com padrão 1. A configuração
é fixa durante a execução. Cada worker possui seu mapa, índice de TTL e fila
limitada por `SIDER_WORKER_QUEUE_CAPACITY`; não há acesso concorrente direto ao mapa.

## Distribuição estável

O roteador extrai o conteúdo entre a primeira abertura `{` e o próximo fechamento
`}` quando esse conteúdo não é vazio. Primeiro par vazio, fechamento ausente ou
ausência de abertura fazem usar a chave inteira. Aberturas aninhadas pertencem aos
bytes da tag; `a{{tag}}` usa `{tag`. Não há conversão de UTF-8.

Sobre esses bytes, FNV-1a de 64 bits começa em `0xcbf29ce484222325`; para cada
byte aplica XOR e multiplica por `0x100000001b3`, módulo 2^64. O shard é o hash
módulo a quantidade de workers. Os vetores em `storage::routing` fixam o resultado
independentemente de plataforma ou do hasher aleatório usado dentro de cada mapa.

Hash tags permitem colocalizar `cliente:{42}:nome` e `cliente:{42}:email`. Isso
não implementa o protocolo Redis Cluster nem seus slots. Não há resharding online.

## Comandos multichave e progresso

`DEL`, `EXISTS`, `MGET` e `MSET` precisam ter todas as chaves no mesmo shard.
O roteador confere o comando inteiro antes do envio e retorna
`CROSSSLOT Keys in request don't hash to the same slot` se encontrar cruzamento.
Nenhum worker recebe parte de uma operação rejeitada. Duplicatas, ordem das chaves
e último valor de MSET continuam sob a semântica original. A conexão permanece
utilizável após a rejeição.

Comandos sem chave usam o worker zero. Cada conexão continua com um pedido em voo.
Filas de shards distintos admitem progresso independente; clientes que disputam
um shard quente compartilham a fila desse worker. Falha de um worker interrompe a
admissão do servidor e inicia drenagem, evitando servir um dataset incompleto.

## Quota e expiração

A quota total `SIDER_MAX_DATASET_BYTES` é dividida de forma fixa: cada shard recebe
`total / shards`, e os primeiros `total % shards` recebem mais um byte. A soma
nunca excede o total e cada partição precisa de ao menos um byte. Não há empréstimo
de quota entre workers: um shard cheio pode rejeitar escrita mesmo com espaço em
outro. O orçamento continua lógico, distinto do RSS e dos buffers de rede.

Cada worker mantém sua expiração passiva e limpa até 64 eventos por rodada de
100 ms. Shutdown fecha a admissão de todos os workers e drena comandos aceitos
dentro do prazo global. Cancelar o supervisor aborta as tarefas assíncronas.
O escritor usa I/O bloqueante do sistema operacional; abortar ou esgotar a espera
não interrompe um `write`/`sync` preso no kernel. O prazo limita a espera da API;
encerrar um processo com I/O bloqueado depende do sistema operacional.

## Evidência e integração durável

Testes verificam vetores binários e hash tags, rejeição antes de enqueue, fila
saturada com progresso de outro worker, quota particionada e lotes via TCP:

```sh
cargo test --locked --lib storage::routing::
cargo test --locked --lib storage::worker::tests::cross_shard
cargo test --locked --lib storage::worker::tests::saturated_shard
cargo test --locked --test tcp shard
```

R04-04 integra escritor AOF global, sequência, snapshot e recuperação entre shards.
Cada pedido mantém uma admissão compartilhada desde o enqueue até apply/resposta.
O snapshot adquire exclusão, aguarda pedidos aceitos e coleta os workers por canais
próprios. A expiração apenas tenta admissão e pula a rodada quando há snapshot,
evitando bloquear o worker que precisa responder à coleta.

A compactação enfileira o snapshot no escritor antes de liberar novas mutações;
o AOF captura o delta até publicar a geração nova. A recuperação valida layout,
sequência, integridade e quota de cada shard antes do bind. Trocar a quantidade de
workers com dados existentes exige [migração offline](aof-migration.md).
A API `Worker::with_aof` desabilita limiares de compactação local quando o worker
pertence a um conjunto de vários shards; somente a coordenação global pode compactá-los.

```sh
cargo test --locked --lib storage::snapshot
cargo test --locked --test sharding
cargo test --locked --test persistence
cargo test --locked --test aof_migration
```

## Medição exploratória R04-05

O build release do SHA `626e1aef8c093b390527719eae49b451ff113edb` executou
16 cenários e verificou o estado final de 32.768 operações INCR por TCP. O ensaio
usou Windows x86_64, Intel i5-9300H (4 núcleos/8 threads), quatro clientes,
seed `0x52404005`, 512 operações por cliente, 1/4 shards, pipeline 1/16 e chaves
concentradas/distribuídas. Não havia builds ou outra carga de testes concorrente.
Os [resultados brutos](evidence/R04-05-626e1ae-windows.json) incluem compiler,
throughput, RTT de cada lote, percentis, RSS antes/depois e configuração completa.

Sem AOF, o ensaio mediu de 33.687 a 85.788 operações/s. Com AOF `always`, de 665 a
945 operações/s. Quatro shards não mostraram ganho consistente nesta amostra;
o sync do escritor global dominou o custo durável. O pipeline aumenta o lote
observado pelo cliente, mas mantém sync por mutação, sem agrupar confirmações.

São resultados exploratórios de loopback, sem aquecimento ou repetição estatística;
clientes compartilham a máquina com o servidor. RSS é amostrado, não um pico.
RTT é por lote, não a latência individual dos comandos de um pipeline. Não há
comparação de velocidade com Redis ou promessa de desempenho em outro ambiente.

Para reproduzir no PowerShell, com checkout limpo e destino novo:

```powershell
cargo test --locked --release --test shard_benchmark --no-run
$env:SIDER_SHARD_BENCH_OUTPUT = Join-Path (Get-Location) 'target/R04-05-novo.json'
cargo test --locked --release --test shard_benchmark -- --ignored --exact exploratory_shard_benchmark --nocapture
```

Compile primeiro e execute a medição com a máquina livre de builds e outras cargas.
Os marcos seguintes mantêm esta evidência vinculada ao SHA original.
