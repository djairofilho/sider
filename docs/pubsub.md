# Pub/Sub

O marco R08 implementa `SUBSCRIBE`, `UNSUBSCRIBE` e `PUBLISH` no TCP, com canais
e mensagens binários. O registro é compartilhado pelas conexões de uma instância
do servidor e separado do `Store` e das filas do worker. Publicações não criam
chaves, não possuem replay e desaparecem ao encerrar o processo.

## Comandos e transições

| Comando | Modo normal | Modo assinante RESP2 |
| --- | --- | --- |
| `SUBSCRIBE canal [canal ...]` | Inscreve e entra no modo assinante | Acrescenta inscrições |
| `UNSUBSCRIBE [canal ...]` | Confirma contagem zero | Remove as inscrições indicadas; sem argumentos, remove todas |
| `PUBLISH canal mensagem` | Retorna o número de filas que aceitaram a mensagem | Retorna erro de comando proibido |
| `PING [mensagem]` | `PONG` ou bulk com a mensagem | Array de dois bulks: `pong` e mensagem, vazia quando ausente |
| Outros comandos implementados | Seguem seus contratos normais | Retornam erro sem executar no banco |

A última remoção devolve a conexão ao modo normal. Cada argumento de inscrição
ou remoção recebe uma confirmação, inclusive canais repetidos ou ausentes.
Inscrições repetidas não duplicam destinatários. A confirmação contém o nome da
operação, canal e quantidade de canais ainda inscritos. `UNSUBSCRIBE` sem canais
ativos devolve canal nulo e contagem zero. Esses formatos seguem o
[contrato RESP2 do Redis](https://redis.io/docs/latest/develop/pubsub/).

Uma mensagem usa o array `[message, canal, payload]`, com três bulk strings. Canal
vazio, bytes nulos, CRLF e bytes fora de UTF-8 são preservados. No modo assinante,
`PSUBSCRIBE`, `PUNSUBSCRIBE`, comandos sharded, `QUIT` e `RESET` ainda não estão
implementados. O texto de erro dos comandos reconhecidos reproduz o Redis e cita
essa família mais ampla; a matriz acima delimita o subconjunto efetivo.

`UNSUBSCRIBE` sem argumentos confirma os canais em ordem binária crescente. Essa
ordem é uma escolha do Sider; não se exige a mesma ordem do Redis quando houver
vários canais. Os diferenciais comparam o caso vazio e o caso de um canal restante.

## Entrega, ordem e clientes lentos

`PUBLISH` faz envios imediatos para filas limitadas. Sua contagem confirma aceitação
pela fila, sem confirmar leitura no socket. Não há reenvio, confirmação do cliente
ou persistência. Desconexões podem perder notificações já aceitas. A entrega
efêmera é compatível com a semântica
[at-most-once do Redis](https://redis.io/docs/latest/develop/pubsub/).

O mutex do hub ordena publicações concorrentes. Ele protege somente metadados e
`try_send`, sem esperar por sockets. Os assinantes recebem a mesma ordem de
publicação e uma única tarefa escreve cada socket. Uma notificação completa nunca
se mistura aos bytes de uma resposta de comando. Mensagens já aceitas são
drenadas antes das confirmações de alteração de inscrição; a remoção impede novas
mensagens daquele canal após a confirmação. Entre comandos consecutivos, o loop
permite progresso das notificações.

| Configuração | Padrão | Efeito |
| --- | --- | --- |
| `SIDER_PUBSUB_MAX_CHANNELS` | 32 | Canais distintos por conexão; excesso rejeita o comando inteiro, sem alterar inscrições |
| `SIDER_PUBSUB_QUEUE_CAPACITY` | 32 | Notificações pendentes por assinante |
| `SIDER_WRITE_TIMEOUT_MS` | 5000 | Prazo para escrever cada resposta ou notificação |
| `SIDER_MAX_RESPONSE_BYTES` | 4194304 | Limite da notificação completa, incluindo framing |

Fila cheia remove imediatamente todas as inscrições daquele cliente e sinaliza o
encerramento da conexão, inclusive se ela estiver bloqueada na escrita. O cliente
removido não entra na contagem dessa publicação. Essa política e os limites por
quantidade são próprios do Sider; não reproduzem os limites de buffer do Redis.
Os demais clientes e o banco continuam progredindo. Timeout, EOF, shutdown,
cancelamento e erros também liberam as inscrições pelo guard da conexão.

A fila guarda até o limite configurado, além de uma notificação em escrita.
Alterações de inscrição podem manter temporariamente o lote drenado de tamanho
limitado enquanto novas notificações entram na fila. O total permanece limitado
pelas capacidades, pelo número de conexões e pelos limites RESP. `Bytes` compartilha
payloads imutáveis entre destinatários. Esses limites não são uma quota do RSS.
Uma notificação que exceda o limite de resposta é rejeitada antes de qualquer
envio, com `ERR pubsub message exceeds response limit`.

## Integração com armazenamento e transações

`connection::run_with_pubsub` recebe o hub pertencente ao servidor. O parser produz
comandos tipados; a conexão intercepta os três comandos Pub/Sub antes de chamar
`DbHandle::execute`. Uma chamada direta ao `Store` retorna
`ERR command requires connection context`, protegendo essa fronteira.

`MULTI` pode enfileirar comandos Pub/Sub junto dos comandos de banco. O worker
aprova o único append durável antes de aplicar os efeitos; erro de AOF impede
publicações e inscrições daquele lote. A execução efêmera mantém a ordem no hub
sem esperar sockets e não produz registros Pub/Sub na AOF. Em `EXEC`, mensagens
para a própria conexão aparecem depois das respostas do lote. A saída completa
continua sujeita ao limite de resposta. Consulte [transações](transactions.md)
para WATCH, framing RESP2, limites e evidências da integração.

## Reprodução e evidências

```sh
cargo test --locked --lib pubsub
cargo test --locked --test pubsub pubsub_tcp_contract -- --exact
cargo test --locked --test pubsub pubsub_matches_redis -- --ignored --exact --nocapture
```

O último comando exige Docker Linux e verifica a imagem Redis 8.10.1 fixada em
`releases/plan.json`. Em 8 de setembro de 2026, passou no Windows com 245
comparações binárias, 64 mensagens para dois assinantes e 16 reconexões, usando
processo Sider e container Redis descartáveis. O teste nativo TCP passou, assim
como os cenários determinísticos de publicação concorrente, fila cheia, socket
lento paralelo a socket rápido e worker, limite, timeout e limpeza. Os testes de
conexão usam I/O controlada e relógio pausado; não dependem de sleeps.

O gate `pubsub` executa os testes nativos com filtro `pubsub_`, exige casos reais,
executa o diferencial e só então publica `receipt-pubsub.json`. O recibo exige o
contexto de release Linux e checkout limpo; execução local avulsa não gera recibo
de publicação. O gate no contexto final da 1.0 ainda deve ser executado no SHA
do bundle. Consulte o [fluxo de releases](releases.md).
