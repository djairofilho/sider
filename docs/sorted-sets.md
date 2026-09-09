# Sorted sets

Sorted sets associam cada membro binário a um score IEEE-754 de 64 bits. Os
resultados seguem score crescente e, em empate, os bytes do membro. A ordem
independe de UTF-8, locale ou plataforma.

## Comandos

| Forma | Contrato |
| --- | --- |
| `ZADD key score member [score member ...]` | Retorna o número de membros novos. Atualizar score não aumenta a contagem; o último par de um membro prevalece. |
| `ZREM key member [member ...]` | Retorna membros removidos, sem contar duplicatas; a última remoção elimina a entrada. |
| `ZCARD key` | Retorna a cardinalidade ou zero se ausente. |
| `ZSCORE key member` | Retorna score como bulk string ou nulo se membro/chave ausente. |
| `ZRANGE key start stop [WITHSCORES]` | Retorna membros no intervalo inclusivo. `WITHSCORES` intercala membro e score no array RESP2. |

Índices negativos contam a partir do fim. Intervalos invertidos ou fora do
conjunto retornam array vazio, com as mesmas regras de [listas](collections.md).
Um comando sobre outro tipo retorna `WRONGTYPE` sem alterar os dados.

`ZADD` aceita somente pares básicos. `NX`, `XX`, `GT`, `LT`, `CH` e `INCR` ficam
fora do subconjunto. `ZRANGE` não aceita `BYSCORE`, `BYLEX`, `REV` ou `LIMIT`.
Essas formas são rejeitadas antes da execução; o texto da rejeição de opções
não suportadas não representa uma promessa de equivalência com Redis.

## Scores

O parser aceita decimais, notação exponencial, valores hexadecimais e infinitos
conforme os casos verificados em Redis 8.10.1. `inf`, `+inf` e `-inf` são válidos;
as grafias `Infinity` e variantes de caixa também são aceitas. Zero negativo
é normalizado para zero. Espaços, `NaN`, overflow finito e underflow de valor
não zero até zero são inválidos. Subnormais representáveis são válidos.

Todos os scores de `ZADD` são validados antes de consultar ou alterar a entrada.
Um score inválido no último par impede também os pares anteriores. O erro
`ERR value is not a valid float` permite continuar usando a conexão.

`ZSCORE` e `WITHSCORES` usam o mesmo conversor. Por exemplo, `1e-7` produz
`1e-7`, `1e-6` produz `0.000001` e `1e20` produz `1e+20`. A representação de
`1e23` é `99999999999999990000000`, conforme o arredondamento da referência.
O conversor em Rust seguro adapta o gerador
[fpconv de Redis 8.10.1](https://github.com/redis/redis/tree/8.10.1/deps/fpconv),
com os avisos e a licença Boost 1.0 preservados no módulo. Não há dependência
de código C nem alteração nas dependências Cargo para essa conversão.

## Índices, TTL, quota e AOF

Um índice por membro e outro por `(score, member)` são atualizados juntos.
Os payloads de snapshot são compartilhados por `Arc`, e alterações produzem
uma nova pós-imagem antes do apply. A leitura por membro usa `HashMap`; a ordem
usa `BTreeSet`. A obtenção de um range por rank percorre o prefixo até `start`.
Não há promessa de equivalência de desempenho com Redis.

A quota contabiliza `128 + key.len()` por entrada e `member.len() + 96` por
membro, cobrindo logicamente ambos os índices e o score. Esse valor não mede
RSS. Um lote que excede a quota é recusado inteiro, incluindo atualizações de
scores de membros que já existiam. Escritas aceitas preservam TTL; última
remoção e expiração liberam entrada, índice de expiração e quota.

AOF v1 usa tag `6` para a pós-imagem de sorted set. O score é armazenado pelos
bits IEEE-754, sem reconversão decimal durante replay. O decoder rejeita NaN,
conjuntos vazios e membros duplicados mesmo quando o checksum é válido. Os
tipos anteriores mantêm suas tags e representações.

Respostas obedecem aos limites RESP e de saída descritos no [guia de rede](network.md).
Se `ZRANGE` exceder o limite, a conexão fecha sem enviar um array parcial e sem
alterar o conjunto. `WITHSCORES` consome dois nós de resposta por membro.

## Verificação reproduzível

```powershell
cargo test --locked --test sorted_sets
cargo test --locked --test tcp r06_
cargo test --locked --test collections_differential -- --ignored --exact sorted_sets_match_redis --nocapture
```

Os sete testes nativos incluem 4.096 alterações geradas dos índices e 28.658
combinações de expoente, mantissa e sinal para roundtrip da conversão. Também
verificam quota, TTL, aridade, rejeições, ordem binária, limites numéricos e AOF.

O diferencial passou com Sider Windows e a imagem Redis/CLI 8.10.1 fixada em
`releases/plan.json`: 8.561 respostas comparadas byte a byte. Além das fixtures,
são quatro seeds com 512 operações cada e 32 lotes com até 400 scores derivados
de padrões IEEE-754. Nos lotes, padrões NaN/inf são filtrados para manter o lote
válido; fixtures separadas verificam infinitos e rejeição de NaN. Scores, pares
e ordenação nunca são normalizados pelo comparador.

Os [testes do writer AOF completo](types-persistence.md) também verificam crash,
compactação, recuperação dos scores e evolução da fixture de strings. A evidência
separa a migração do formato da migração entre executáveis de baselines internas;
não comprova execução nativa Linux ou aprovação de pacote de release.
