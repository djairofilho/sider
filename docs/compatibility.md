# Compatibilidade com Redis

Os cinco comandos da 0.1 estão implementados e verificados por comparação
diferencial do binário Sider com Redis 8.10.1. O teste separado com `redis-cli`
também passou contra o Sider. Isso comprova o subconjunto abaixo nos casos
registrados, não compatibilidade com todos os comandos ou clientes Redis.

O desenvolvimento de R02 acrescenta strings, opções de SET, TTL e quota. Essa
extensão está descrita abaixo e no [guia de strings](strings.md); a versão Cargo
continua `0.1.0` durante os marcos internos, sem representar uma nova publicação.

## Matriz da versão 0.1

| Forma do comando | Comportamento alvo | Implementação | Verificação contra Redis |
| --- | --- | --- | --- |
| `PING` | Responder com simple string `PONG` | TCP | Diferencial literal e CLI |
| `PING mensagem` | Devolver a mensagem como bulk string | TCP | Diferencial literal/gerado e CLI |
| `ECHO mensagem` | Devolver exatamente os bytes da mensagem | TCP | Diferencial, fronteiras até 1 MiB e CLI |
| `GET chave` | Devolver bulk string ou bulk string nula se ausente | TCP | Diferencial, estado final e CLI |
| `SET chave valor` | Criar ou substituir; responder com simple string `OK` | TCP | Diferencial, sobrescrita/binários e CLI |
| `DEL chave [chave ...]` | Contar apenas as chaves efetivamente removidas | TCP | Diferencial, duplicatas/ausentes e CLI |

A referência inicial é Redis e `redis-cli` **8.10.1**, na plataforma Linux amd64.
A tag e o digest da imagem estão fixados em [releases/plan.json](../releases/plan.json):

```text
redis:8.10.1@sha256:76961cd2a0f40ef6fdd334b6b1b3a76a2bad1848d89f3030ca30a7521d4a9493
```

Em `R01-01`, a referência passou pelas fixtures literais: oito casos, 48 trocas
sequenciais, oito pipelines e os cinco comandos via `redis-cli`. O teste confere
digest, plataforma e versões do servidor e da CLI antes da execução. Consulte
os [comandos de reprodução](testing.md).

Em `R01-03`, `tests/commands.rs` executa essas mesmas fixtures no núcleo Sider e
confere os bytes esperados, sem sockets. R01-04 repete as fixtures pelo TCP do
Sider, incluindo pipelines. Em R01-05, `tests/compatibility.rs` passou com 3.588
comparações binárias no Windows e no Linux Ubuntu 24.04. O caminho Linux também
passou em nove cenários de CLI contra ambos os servidores, com processos e
containers descartáveis. Os [comandos reproduzíveis](differential.md) registram
seeds, cobertura e limites. A candidata 1.0 valida a matriz completa no seu SHA;
a final promove os mesmos arquivos e evidências desse build aprovado.

## Matriz implementada em R02

| Forma | Contrato verificado | Evidência |
| --- | --- | --- |
| `EXISTS chave [chave ...]` | Duplicatas contam; ausentes e expiradas não contam | Nativo, TCP e diferencial Redis |
| `INCR chave` / `DECR chave` | Decimal canônico i64; ausência começa em zero; rejeição preserva valor/TTL | Nativo, TCP e diferencial Redis |
| `MGET chave [chave ...]` | Array ordenado com nulos e duplicatas | Nativo, TCP e diferencial até três payloads de 1 MiB |
| `MSET chave valor [chave valor ...]` | Lote indivisível; último par vence; limpa TTL | Nativo, concorrência TCP e diferencial Redis |
| `SET ... [NX|XX] [EX segundos|PX ms|KEEPTTL] [GET]` | Condições, retorno anterior, combinações e prazos validados | 48 combinações e casos inválidos comparados com Redis |
| `EXPIRE chave segundos` / `PEXPIRE chave ms` | Prazo relativo; não positivo remove; ausência retorna zero | Relógio injetado e diferencial Redis |
| `TTL chave` / `PTTL chave` | Prazo restante, -1 persistente, -2 ausente/expirada | Fronteiras exatas nativas; diferencial com tolerância declarada |
| `PERSIST chave` | Remove somente um prazo existente | Nativo e diferencial Redis |

O relatório separa 3.588 comparações binárias históricas de R01 e 461 novas de
R02. As observações temporais usam tolerância de 100 ms para `PTTL` e um segundo
para `TTL`; a expiração real é observada com deadline de cinco segundos. Esse
caminho passou com Sider Windows e Redis Linux em Docker; não comprova build
nativo Linux nem aprovação de pacotes. Veja a [reprodução](strings.md#validação-reproduzível).

## Subconjunto alvo

O Sider aceitará requisições RESP2 formadas por arrays não vazios de bulk strings
não nulas. Os nomes dos comandos serão comparados sem distinguir maiúsculas de
minúsculas ASCII. Chaves e valores serão binários, sem exigir UTF-8.

O codec representa os cinco tipos RESP2, mas isso não significa que todos os
tipos serão aceitos como argumentos de comandos. Valor nulo, valor vazio,
array nulo e array vazio têm representações distintas.

Frames fragmentados e comandos concatenados são tratados desde R01-04.
Cada conexão processa os comandos em sequência, com um único pedido em voo.

## Limitações e divergências atuais

| Área | Contrato do desenvolvimento atual |
| --- | --- |
| Opções de `SET` | NX, XX, EX, PX, GET e KEEPTTL; EXAT, PXAT e condições de valor não integram o subconjunto |
| Comando desconhecido | Responder `ERR unknown command`, com texto simplificado que não reproduz os argumentos |
| Aridade dos comandos suportados | Produzir resposta compatível com a versão de Redis selecionada, após verificação |
| Formato de requisição inválido | Rejeitar e fechar a conexão; sem promessa de equivalência com Redis fora do subconjunto declarado |
| Protocolo | RESP2; sem RESP3 e sem comandos inline |
| Banco lógico | Somente o banco padrão; sem `SELECT` |
| Handshake e autenticação | Sem `AUTH`, `HELLO`, `COMMAND` ou `CLIENT`; clientes que exigem esses comandos não estarão cobertos |
| Tipos de dados | Somente chaves e valores binários; sem listas, hashes, sets ou sorted sets |
| Expiração | EXPIRE e PEXPIRE básicos; sem NX, XX, GT ou LT; monotônico durante execução |
| Persistência e replicação | Sem AOF, snapshots, replicação ou Redis Cluster |
| Memória do dataset | Quota lógica própria com rejeição atômica, sem eviction; não reproduz o maxmemory/RSS do Redis |
| Uso operacional | Protótipo para desenvolvimento local e testes, com endereço padrão em loopback |

Os limites de entrada e os prazos estão no [guia de rede](network.md). São limites
próprios do Sider e não uma reprodução dos valores padrão do Redis.

## Evidência necessária

Para repetir a validação do núcleo sem rede:

```sh
cargo test --locked --lib command::
cargo test --locked --lib storage::
cargo test --locked --test commands
cargo test --locked --test tcp
cargo test --locked --test cli
```

`tests/commands.rs` também confere que opções inválidas de `SET`, comandos desconhecidos e
formatos inválidos não chegam ao armazenamento. O parser move os `Bytes` para o
comando; `GET` compartilha conteúdo imutável, sem uma cópia proporcional ao valor.

Para declarar uma forma de comando verificada, registre o teste reproduzível e
a versão de referência. A suíte diferencial compara respostas brutas e
estado observado usando instâncias descartáveis. Sua leitura das respostas usa
um leitor independente do codec sob teste.

Inclua casos com bytes não UTF-8, chaves e valores vazios, chaves ausentes,
sobrescrita e chaves repetidas em `DEL`. Verifique também caixa dos comandos,
aridade, rejeição sem mutação e continuidade após erros recuperáveis.

O teste com `redis-cli` é uma evidência de integração separada: sua saída
textual não substitui a comparação dos bytes no protocolo. Uma ferramenta ausente
ou um teste ignorado deve permanecer registrado como pendente.

## Evolução planejada

O [ROADMAP](../ROADMAP.md) é a sequência oficial. Strings, opções de `SET`, TTL e
quota de R02 estão implementadas; AOF fica na 0.3; shards fixos na 0.4; hashes, listas e sets na 0.5;
sorted sets na 0.6; transações de um shard na 0.7; Pub/Sub na 0.8; replicação
Sider→Sider na 0.9; operação e imagem Docker na 0.10. A 1.0 estabiliza esse subconjunto.

A partir da 0.4, operações multichave serão restritas ao mesmo shard, com rejeição
antes de qualquer efeito. Isso altera uma forma aceita no worker único e deverá
aparecer nas notas de incompatibilidade. Replicação será assíncrona entre instâncias
Sider da mesma versão/configuração, sem compatibilidade com replicação Redis.

Para coleções sem ordem garantida, como `SMEMBERS`, a suíte compara conteúdo
normalizado. Respostas ordenadas, como `LRANGE` e `ZRANGE`, preservam a ordem na
comparação. Cada nova forma de comando só muda de planejada para verificada quando
seu teste e sua referência estiverem registrados.

Os gates cumulativos e o fluxo de candidatas estão no
[guia de releases](releases.md). O bootstrap não atende esses gates e não será
publicado como versão funcional.
