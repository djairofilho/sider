# Hashes, listas e sets

As três famílias usam chaves e payloads binários. Strings e coleções compartilham
TTL, quota e mutações resolvidas do armazenamento. O worker continua sendo o
proprietário dos dados.

## Formas suportadas

| Forma | Resposta e efeito |
| --- | --- |
| `HSET key field value [field value ...]` | Inteiro com campos novos; a última ocorrência de um campo prevalece. |
| `HGET key field` | Bulk com valor ou nulo se campo/chave ausente. |
| `HDEL key field [field ...]` | Inteiro com campos removidos, sem contar duplicatas. |
| `HEXISTS key field` | `1` se existe, `0` se ausente. |
| `HLEN key` | Quantidade de campos, `0` se ausente. |
| `HGETALL key` | Array alternando campo e valor; vazio se ausente, sem ordem pública garantida. |
| `LPUSH key value [value ...]` | Insere cada argumento à esquerda e retorna o tamanho final. |
| `RPUSH key value [value ...]` | Insere cada argumento à direita e retorna o tamanho final. |
| `LPOP key` / `RPOP key` | Remove um elemento da ponta; bulk nulo se ausente. A opção `count` fica fora do subconjunto. |
| `LLEN key` | Tamanho da lista, `0` se ausente. |
| `LRANGE key start stop` | Array na ordem da lista, com extremos inclusivos e índices negativos a partir do fim. |
| `SADD key member [member ...]` | Inteiro com membros novos, sem contar duplicatas. |
| `SREM key member [member ...]` | Inteiro com membros removidos, sem contar duplicatas. |
| `SISMEMBER key member` | `1` se pertence ao conjunto, `0` se ausente. |
| `SCARD key` | Quantidade de membros, `0` se ausente. |
| `SMEMBERS key` | Array com membros únicos; vazio se ausente, sem ordem pública garantida. |

`LPUSH l a b` produz a lista `b, a`. Em ranges, índices menores que o início
são limitados a zero; o fim é limitado ao último elemento. Um intervalo invertido
ou inteiramente fora da lista retorna array vazio. Os argumentos de índice usam
inteiros decimais `i64`, com as mesmas rejeições de formato de [strings](strings.md).

## Tipos, TTL e quota

Um comando aplicado a outro tipo retorna
`WRONGTYPE Operation against a key holding the wrong kind of value` sem mudar o
valor. `GET`, `INCR`, `DECR` e `SET ... GET` também rejeitam coleções. `MGET`
retorna nulo para cada chave de outro tipo. `SET` sem `GET` e `MSET` substituem
qualquer tipo; `SET ... GET` verifica o tipo antes das condições `NX`/`XX`.

Leituras de chaves ausentes não criam coleções. A remoção do último campo,
elemento ou membro elimina a entrada, seu índice de expiração e seu consumo
lógico. Mutações preservam TTL, enquanto a substituição por strings segue as
opções de `SET`. A expiração passiva acontece antes da consulta ao tipo.

A quota soma `128 + key.len()` por entrada e o payload abaixo:

| Tipo | Custo lógico do payload |
| --- | --- |
| String | `value.len()` |
| Hash | Soma de `field.len() + value.len() + 64` por campo |
| Lista | Soma de `value.len() + 32` por elemento |
| Set | Soma de `member.len() + 64` por membro |

Esse orçamento não mede RSS. Escritas validam o resultado completo antes de
substituir a entrada. Um lote recusado por quota não deixa efeito parcial;
campos e membros repetidos são contabilizados pelo estado final.

Os limites RESP e de resposta da [rede](network.md) também se aplicam às
coleções. Ranges e arrays acima deles fecham a conexão sem resposta parcial.
`LRANGE`, `HGETALL` e `SMEMBERS` não alteram dados ao exceder esse limite.

## Persistência e evidência

Snapshots compartilham payloads imutáveis por `Arc`; uma escrita cria uma nova
pós-imagem. `Mutation::Put` inclui o tipo completo e o prazo absoluto já resolvido.
O codec AOF v1 mantém a representação anterior de strings (tag `1`) e usa tags
`3`, `4` e `5` para hash, lista e set. O replay valida tipos, unicidade e quota;
coleções vazias ou registros com campos/membros duplicados são inválidos.

```powershell
cargo test --locked --test collections
cargo test --locked --test tcp r05_
cargo test --locked --test collections_differential -- --ignored --exact collections_match_redis --nocapture
```

O diferencial usa o binário Sider Windows e a imagem Redis/CLI 8.10.1 fixada em
`releases/plan.json`, com instâncias descartáveis. São 2.188 respostas exatas e
195 arrays normalizados, totalizando 2.383 comparações. A normalização de
`HGETALL` preserva pares campo/valor; ambas as normalizações rejeitam duplicatas.
As quatro seeds estão no teste e cada uma executa 256 operações com leituras
intermediárias e conferência do estado final.

Os testes nativos cobrem quota, TTL, tipos, binários, aridade, índices extremos,
codec e replay. Crash, compactação e migração da baseline anterior precisam da
integração com o writer AOF e suas evidências específicas. Esse diferencial não
comprova essas operações nem execução nativa em Linux.
