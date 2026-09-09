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
dentro do prazo global. Cancelar o supervisor recolhe todas as tarefas possuídas.

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
R04-GATE continua pendente até essa integração e suas verificações Linux/Windows;
o roteamento em memória sozinho não comprova persistência ou migração.
