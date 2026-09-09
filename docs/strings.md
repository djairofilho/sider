# Strings, expiração e quota

R02 amplia os comandos do núcleo sem alterar as fronteiras entre protocolo,
execução e rede. Todos os comandos válidos continuam passando por um worker
proprietário, que aplica cada operação ou lote sem suspender a execução.

## Operações de strings

| Forma | Resultado |
| --- | --- |
| `EXISTS chave [chave ...]` | Quantidade de ocorrências de chaves existentes; duplicatas contam |
| `INCR chave` / `DECR chave` | Novo inteiro i64; chave ausente começa em zero |
| `MGET chave [chave ...]` | Array ordenado de valores ou nulos, preservando duplicatas |
| `MSET chave valor [chave valor ...]` | `OK` após aplicar o lote inteiro; último valor de uma chave vence |

Incrementos aceitam somente decimais canônicos: `0` ou sinal negativo opcional
seguido de dígitos, com primeiro dígito entre `1` e `9`. `+1`, `-0`, zeros à
esquerda, espaços, vazio, bytes não numéricos e valores fora de i64 são rejeitados.
Overflow da operação tem erro distinto de um valor que já era inválido. Ambos
preservam o valor e seu prazo de expiração.

Aridade e formato completos são validados antes do envio ao worker. Com limites
padrão, `EXISTS` e `MGET` admitem até 1.022 chaves e `MSET` até 511 pares, também
sujeitos aos limites de bytes. São valores derivados do limite de nós do RESP.
Um lote rejeitado por aridade ou quota não aplica pares parcialmente.

`MGET` compartilha os payloads imutáveis de `Bytes`. Repetir chaves pode gerar uma
resposta maior que a requisição. Se a resposta exceder `SIDER_MAX_RESPONSE_BYTES`,
o encoder a rejeita antes de emitir bytes e a conexão fecha; o estado permanece.
Cancelar o receptor depois da aceitação de `MSET` não desfaz o lote.

## Opções de SET

```text
SET chave valor [NX | XX] [EX segundos | PX milissegundos | KEEPTTL] [GET]
```

- `NX` escreve somente quando ausente; `XX`, somente quando presente.
- `GET` devolve o valor anterior, inclusive quando uma condição impede a escrita.
  Sem `GET`, uma condição não satisfeita retorna bulk nula.
- `EX` e `PX` definem prazo relativo positivo, validado antes de testar a condição.
  Zero, duração negativa, overflow ou deadline não representável são rejeitados.
- `KEEPTTL` preserva o prazo anterior. SET básico, inclusive com `GET`, remove o
  prazo quando não há opção temporal. `MSET` também remove prazos anteriores.
- Ordem das opções é livre. Repetir `NX`, `XX`, `GET` ou `KEEPTTL` é aceito;
  repetir `EX` ou `PX` usa seu último argumento. `NX` com `XX`, `EX` com `PX` e
  expiração explícita com `KEEPTTL` retornam `ERR syntax error`.

`EXAT`, `PXAT`, `IFEQ`, `IFNE`, `IFDEQ` e `IFDNE` não integram este subconjunto.
As formas implementadas foram comparadas com o
[código de strings do Redis 8.10.1](https://github.com/redis/redis/blob/8.10.1/src/t_string.c)
e com uma instância dessa versão.

## Expiração

| Forma | Resultado |
| --- | --- |
| `EXPIRE chave segundos` / `PEXPIRE chave milissegundos` | `1` se o prazo foi aplicado ou a chave removida; `0` quando ausente |
| `TTL chave` / `PTTL chave` | Tempo restante; `-1` sem prazo; `-2` ausente ou expirada |
| `PERSIST chave` | `1` ao remover prazo existente; `0` se não havia chave ou prazo |

Prazo não positivo em `EXPIRE`/`PEXPIRE` remove a chave imediatamente. Os comandos
aceitam somente essas formas básicas, sem opções `NX`, `XX`, `GT` ou `LT`.
`TTL` arredonda milissegundos com `(restante + 500) / 1000`, como a
[implementação Redis](https://github.com/redis/redis/blob/8.10.1/src/expire.c).

`Clock` fornece relógios monotônico e Unix injetáveis. Na escrita, `Entry`
guarda deadline monotônico, deadline absoluto em milissegundos e geração. O
monotônico governa o processo em execução, portanto saltos posteriores do relógio
civil não mudam a expiração. O valor absoluto permite futura persistência; replay
precisará convertê-lo novamente para o relógio monotônico no reinício.

Todo acesso às chaves trata `deadline <= agora` como expirado. Além da expiração
passiva, o worker processa até 64 eventos a cada 100 ms, sem percorrer todo o mapa.
Um índice ordenado mantém no máximo um evento por chave: substituição, remoção e
`PERSIST` retiram o anterior. Conferir geração e deadline impede que um evento
antigo apague um valor novo. Grandes lotes expirados podem exigir várias rodadas.

## Quota lógica

`SIDER_MAX_DATASET_BYTES` tem padrão de 64 MiB e aceita valores positivos até
`isize::MAX`. A configuração é verificada antes da abertura do servidor.
O consumo de uma entrada é `chave.len() + valor.len() + 128` bytes. A taxa fixa
inclui metadados e índice temporal, mesmo quando a entrada não tem expiração.

Isso é uma contabilidade lógica, não uma medição de RSS. Não inclui capacidade
reservada das tabelas, alocador, buffers de rede, pedidos na fila nem respostas
que ainda compartilhem valores removidos. O limite não garante que o processo
consuma apenas essa quantidade de memória física.

`SET`, `MSET` e incrementos calculam o estado final antes de aplicar crescimento.
Em `MSET`, duplicatas são reduzidas ao último valor antes dessa conta. Exceder a
quota retorna `OOM dataset memory quota exceeded`, preservando valores, prazos e
contadores das chaves vivas. Não há eviction automática. `DEL`, expiração e
substituições menores liberam consumo; expirações passivas já devidas podem ser
recolhidas durante a inspeção das chaves de um comando posteriormente rejeitado.
Chaves expiradas ainda não visitadas contam até sua limpeza ativa ou passiva.

## Validação reproduzível

```sh
cargo test --locked --test strings --test expiration --test memory
cargo test --locked --test tcp r02
cargo test --locked --lib storage::worker::tests::r02
cargo test --locked --test compatibility -- --ignored --exact sider_matches_redis --nocapture
```

A execução Windows MSVC com Redis 8.10.1 em Docker comparou 3.588 respostas
binárias de R01 e 461 de R02. R02 inclui 48 combinações de SET, inteiros nos
limites, rejeições, valores binários, duplicatas e MGET com três payloads de 1 MiB.
Os relatórios separam essas comparações das observações temporais: tolerância de
100 ms para `PTTL` e um segundo para `TTL`, mais polling com prazo de cinco
segundos até a expiração observada nos dois processos. A quantidade de iterações
temporais varia com o escalonamento e não é anunciada como comparação binária.

Testes com relógio injetado verificam a fronteira exata, geração antiga, limpeza
ativa limitada, quota e preservação de estado. Os testes TCP verificam resposta
MGET de 128 bytes e rejeição de 129 bytes sem saída parcial, além de MSET
indivisível entre clientes. Isso é evidência local de implementação, sem aprovar
pacotes distribuídos ou afirmar execução nativa em Linux.
