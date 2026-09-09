# Transações

`MULTI`, `EXEC`, `DISCARD`, `WATCH` e `UNWATCH` pertencem à conexão TCP. Uma
transação pode acessar um único shard. Chaves e argumentos preservam bytes
arbitrários; hash tags permitem colocar as chaves relacionadas no mesmo shard.

## Fila e execução

`MULTI` inicia a fila e responde `OK`. Cada comando aceito responde `QUEUED` e
fica sem efeito até `EXEC`. `EXEC` retorna um array de respostas na ordem dos
comandos; uma fila vazia retorna array vazio. `DISCARD` remove a fila e as
observações. EOF, cancelamento e encerramento também descartam a fila que ainda
não foi enviada ao worker.

Um erro de aridade ou parsing, uma tentativa de cruzar shards ou um limite da
fila deixa a transação inválida. `EXEC` então retorna `EXECABORT` e nenhum comando
da fila executa. `MULTI` aninhado e `WATCH` dentro de `MULTI` retornam erro sem
invalidar a fila anterior. `EXEC` e `DISCARD` fora de `MULTI` retornam erro.

Erros de execução, como inteiro inválido, overflow, `WRONGTYPE` ou quota, ocupam
sua posição no array; os outros comandos continuam. Não há rollback das operações
válidas. Essa distinção entre falha de enfileiramento e falha individual segue o
[contrato transacional do Redis](https://redis.io/docs/latest/develop/using-commands/transactions/).

O worker prepara todos os comandos com um relógio congelado, resolve condições,
TTL e quota e reúne as pós-imagens finais em um único `ResolvedBatch`. Só as chaves
tocadas entram no estado temporário. Nenhum outro pedido do mesmo shard intercala
com o lote. A barreira global de snapshot mantém a admissão do pedido até a
aplicação e resposta, inclusive quando o cliente cancela a espera após o aceite.

## WATCH e liberação de recursos

`WATCH chave [chave ...]` observa as chaves até `EXEC`, `DISCARD`, `UNWATCH` ou o
fim da conexão. Uma escrita da própria conexão também invalida a observação.
Escrever o mesmo valor, criar e remover uma chave ou alcançar seu prazo de
expiração são conflitos. Remover uma chave já ausente não gera conflito.
`EXEC` com conflito retorna array nulo (`*-1\r\n`) e não grava nem aplica o lote.
`UNWATCH` dentro de `MULTI` fica enfileirado; portanto não elimina um conflito já
detectado antes da execução. A semântica de expiração segue
[WATCH no Redis](https://redis.io/docs/latest/commands/watch/).

Cada conexão mantém tokens com propriedade exclusiva. O registro compartilha um
indicador de mudança entre observadores da mesma chave e o remove ao invalidar
essa geração ou liberar seu último token. Não existe um mapa permanente de
versões de todas as chaves já usadas. A observação nova recebe seu próprio estado
quando uma geração anterior já foi invalidada.

Antes de registrar WATCH, o worker resolve expirações pendentes dessas chaves.
Com AOF habilitada, os tombstones precisam ser aceitos pelo escritor primeiro.
Uma rejeição de limite não cria tokens. O prazo de uma chave já observada também
é conferido em `EXEC`, mesmo antes da limpeza ativa ou passiva do registro.

## Persistência e Pub/Sub

Com AOF habilitada, um lote com alterações duráveis produz um único append antes
de alterar o Store. O replay aplica esse registro inteiro. Um erro individual
não impede que as operações válidas do lote sejam persistidas. Um WATCH abortado
e uma transação sem alterações duráveis não produzem registro de mutação.
Os limites e garantias de fsync seguem a configuração da AOF.

`PUBLISH`, `SUBSCRIBE` e `UNSUBSCRIBE` podem ser enfileirados em `MULTI`.
Depois que o append é aceito, uma seção curta do hub aplica as mudanças do banco
e executa os efeitos efêmeros na ordem dos comandos. Ela não aguarda sockets.
Pub/Sub não entra na AOF. Falha de append/fsync ou rejeição do tamanho do registro
impede tanto a aplicação do lote quanto publicações e inscrições pendentes.

Uma inscrição dentro de `EXEC` altera o formato dos `PING` seguintes. Os comandos
de banco que já estavam enfileirados continuam executando, e a conexão termina no
modo correspondente às inscrições restantes. Novos comandos recebidos após
`EXEC` respeitam as restrições do modo assinante.

Em RESP2, `SUBSCRIBE a b` e `UNSUBSCRIBE a b` geram uma confirmação por argumento
dentro de `EXEC`, enquanto o cabeçalho externo mantém a quantidade de comandos.
Uma mensagem publicada para a própria conexão aparece depois de todas as
respostas do lote. O teste preserva o transcript literal observado no Redis
8.10.1; o adiamento também consta no
[caminho de escrita do Redis](https://github.com/redis/redis/blob/8.10.1/src/networking.c).

Toda a saída de `EXEC`, incluindo confirmações e mensagens próprias, é codificada
em um buffer limitado antes da primeira escrita. Se a codificação exceder o
limite, a conexão encerra sem transmitir um prefixo incompleto dessa resposta.
O lote já aceito pode ter sido aplicado; timeout ou desconexão após o aceite não
promete ausência de efeitos e não provoca repetição automática.

## Limites

| Configuração | Padrão | Efeito |
| --- | --- | --- |
| `SIDER_TRANSACTION_MAX_COMMANDS` | 128 | Quantidade de comandos retidos na fila |
| `SIDER_TRANSACTION_MAX_BYTES` | 1048576 | Soma dos tamanhos RESP completos dos comandos enfileirados |
| `SIDER_WATCH_MAX_KEYS` | 128 | Chaves distintas observadas por conexão |

O limite de bytes inclui argumentos, comprimentos e framing. Uma fila inválida
libera os comandos já retidos e conserva apenas o estado necessário para devolver
`EXECABORT`. Excesso de WATCH rejeita o novo comando sem remover as observações
anteriores. Esses limites são próprios do Sider e não simulam a política de memória
do Redis.

Também se aplicam os limites de entrada e resposta RESP, canais e fila Pub/Sub,
quota do dataset, capacidade da fila do worker, prazo total de pedido e tamanho do
registro AOF. A quantidade de conexões limita a soma das filas e observações
pertencentes aos clientes; o orçamento lógico não representa o RSS do processo.

## Reprodução

```sh
cargo test --locked --lib transactions_
cargo test --locked --test transactions transactions_tcp_contract -- --exact
cargo test --locked --test transactions_persistence transactions_ -- --nocapture
cargo test --locked --test transactions transactions_matches_redis -- --ignored --exact --nocapture
```

O diferencial usa a imagem Redis 8.10.1 fixada em `releases/plan.json`, um servidor
Sider descartável e comparação de bytes. Os testes nativos cobrem fila, ausência
de efeitos antecipados, WATCH, relógio pausado, shards, snapshot, limite de resposta
e falhas AOF sem efeitos Pub/Sub. Os testes de persistência usam o lote real do
worker, truncamento de cada prefixo do registro e nove pontos de crash de processo,
incluindo publicação de gerações compactadas.

Na execução local em Windows, passaram 16 testes de implementação, 91 comparações
binárias com Redis, o transcript Pub/Sub de 274 bytes, 71 prefixos do registro e
nove crashes de processo. O replay após compactação preservou hash, lista, set e
sorted set escritos no mesmo lote, inclusive com `WRONGTYPE` em outra posição.
Essas evidências não substituem a execução Linux exigida para publicação.

O gate `transactions` executa essas verificações e só publica recibo depois de
sucesso e limpeza dos processos. O contexto de release exige Linux e checkout
limpo. A validação local do marco interno não produz recibo de publicação; o gate
da 1.0 ainda deve executar no SHA exato do bundle.
