# Compatibilidade com Redis

Os cinco comandos da 0.1 estão implementados no núcleo síncrono do Sider, com
parser e armazenamento testados pelas fixtures da referência. O binário valida
configuração, mas ainda não oferece serviço TCP. Esta matriz distingue testes
do núcleo de compatibilidade demonstrada com clientes de rede.

## Matriz da versão 0.1

| Forma do comando | Comportamento alvo | Implementação | Verificação contra Redis |
| --- | --- | --- | --- |
| `PING` | Responder com simple string `PONG` | Núcleo síncrono | Fixtures; TCP pendente |
| `PING mensagem` | Devolver a mensagem como bulk string | Núcleo síncrono | Fixtures; TCP pendente |
| `ECHO mensagem` | Devolver exatamente os bytes da mensagem | Núcleo síncrono | Fixtures; TCP pendente |
| `GET chave` | Devolver bulk string ou bulk string nula se ausente | Núcleo síncrono | Fixtures; TCP pendente |
| `SET chave valor` | Criar ou substituir; responder com simple string `OK` | Núcleo síncrono | Fixtures; TCP pendente |
| `DEL chave [chave ...]` | Contar apenas as chaves efetivamente removidas | Núcleo síncrono | Fixtures; TCP pendente |

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
confere os bytes esperados, sem sockets. As duas etapas comprovam equivalência
nos casos literais, mas não substituem a comparação diferencial de servidores
ou o teste do Sider via CLI. `R01-05` comprovará o subconjunto por esses caminhos.

## Subconjunto alvo

O Sider aceitará requisições RESP2 formadas por arrays não vazios de bulk strings
não nulas. Os nomes dos comandos serão comparados sem distinguir maiúsculas de
minúsculas ASCII. Chaves e valores serão binários, sem exigir UTF-8.

O codec representa os cinco tipos RESP2, mas isso não significa que todos os
tipos serão aceitos como argumentos de comandos. Valor nulo, valor vazio,
array nulo e array vazio têm representações distintas.

Frames fragmentados e comandos concatenados serão tratados desde a versão 0.1.
Cada conexão processará os comandos em sequência, com um único pedido em voo.

## Limitações e divergências planejadas

| Área | Contrato alvo da versão 0.1 |
| --- | --- |
| Opções de `SET` | Aceitar somente `SET chave valor`; argumentos adicionais produzirão `ERR unsupported SET options`, sem alterar estado |
| Comando desconhecido | Responder `ERR unknown command`, com texto simplificado que não reproduz os argumentos |
| Aridade dos comandos suportados | Produzir resposta compatível com a versão de Redis selecionada, após verificação |
| Formato de requisição inválido | Rejeitar e fechar a conexão; sem promessa de equivalência com Redis fora do subconjunto declarado |
| Protocolo | RESP2; sem RESP3 e sem comandos inline |
| Banco lógico | Somente o banco padrão; sem `SELECT` |
| Handshake e autenticação | Sem `AUTH`, `HELLO`, `COMMAND` ou `CLIENT`; clientes que exigem esses comandos não estarão cobertos |
| Tipos de dados | Somente chaves e valores binários; sem listas, hashes, sets ou sorted sets |
| Expiração e contagem | Sem TTL e sem `EXISTS` na versão 0.1 |
| Persistência e replicação | Sem AOF, snapshots, replicação ou Redis Cluster |
| Memória do dataset | Sem quota ou eviction; limites de rede não limitam o tamanho do banco |
| Uso operacional | Protótipo para desenvolvimento local e testes, com endereço padrão em loopback |

Os limites de entrada e os prazos propostos estão no
[plano de implementação](../PLANO.md#limites-e-ciclo-de-vida). Eles serão limites
próprios do Sider e não uma reprodução dos valores padrão do Redis.

## Evidência necessária

Para repetir a validação do núcleo sem rede:

```sh
cargo test --locked --lib command::
cargo test --locked --lib storage::
cargo test --locked --test commands
```

`tests/commands.rs` também confere que opções de `SET`, comandos desconhecidos e
formatos inválidos não chegam ao armazenamento. O parser move os `Bytes` para o
comando; `GET` compartilha conteúdo imutável, sem uma cópia proporcional ao valor.

Para declarar uma forma de comando verificada, registre o teste reproduzível e
a versão de referência. A suíte diferencial deverá comparar respostas brutas e
estado observado usando instâncias descartáveis. Sua leitura das respostas não
poderá depender apenas do codec sob teste.

Inclua casos com bytes não UTF-8, chaves e valores vazios, chaves ausentes,
sobrescrita e chaves repetidas em `DEL`. Verifique também caixa dos comandos,
aridade, rejeição sem mutação e continuidade após erros recuperáveis.

O teste com `redis-cli` será uma evidência de integração separada: sua saída
textual não substitui a comparação dos bytes no protocolo. Uma ferramenta ausente
ou um teste ignorado deve permanecer registrado como pendente.

## Evolução planejada

O [ROADMAP](../ROADMAP.md) é a sequência oficial. Strings, opções de `SET`, TTL e
quota entram na 0.2; AOF na 0.3; shards fixos na 0.4; hashes, listas e sets na 0.5;
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
