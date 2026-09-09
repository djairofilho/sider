# Rede e ciclo de vida do Sider

Este guia descreve a implementação de `R01-04`: servidor RESP2/TCP, worker
proprietário, configuração, prazos e prontidão. R02 acrescenta comandos de strings,
expiração e quota lógica ao mesmo caminho de rede. Consulte a
[matriz de compatibilidade](compatibility.md) para distinguir suporte implementado
de equivalência já demonstrada contra Redis.

## Índice

- [Executar localmente](#executar-localmente)
- [Configuração](#configuração)
- [Requisições e limites](#requisições-e-limites)
- [Aceitação e resultado incerto](#aceitação-e-resultado-incerto)
- [Prazos de leitura e escrita](#prazos-de-leitura-e-escrita)
- [EOF e erros](#eof-e-erros)
- [Encerramento e supervisão](#encerramento-e-supervisão)
- [Arquivo de prontidão](#arquivo-de-prontidão)
- [Código e validação](#código-e-validação)

## Executar localmente

Na raiz do repositório, com a toolchain de `rust-toolchain.toml`:

```sh
cargo run --locked -- --help
cargo run --locked -- --version
cargo run --locked
```

Sem argumentos, o binário inicia o servidor em `127.0.0.1:6379`. A execução fica
em primeiro plano; use `Ctrl+C` para solicitar a parada. `--help` e `--version`
não abrem o listener. Argumentos desconhecidos, configuração inválida e falha de
bind encerram o programa com código de erro.

O endereço é local por padrão. Não há autenticação, ACL ou TLS nesta versão.
Configurar um IP fora de loopback emite um aviso, mas não bloqueia o bind nem
adiciona proteção. Use apenas um ambiente controlado, sem exposição pública.
Logs de inicialização, falhas e encerramento vão para `stderr`, sem registrar
chaves e valores dos pedidos.

Para escolher outra porta no PowerShell:

```powershell
$env:SIDER_ADDR = '127.0.0.1:6380'
try {
    cargo run --locked
} finally {
    Remove-Item Env:SIDER_ADDR
}
```

Em um shell POSIX:

```sh
SIDER_ADDR=127.0.0.1:6380 cargo run --locked
```

## Configuração

Variável ausente usa o padrão. Tamanhos são expressos em bytes, contagens em
inteiros e prazos em milissegundos. `1 MiB` corresponde a `1048576` bytes.

| Variável | Padrão | Significado |
| --- | --- | --- |
| `SIDER_ADDR` | `127.0.0.1:6379` | IP literal e porta do listener |
| `SIDER_MAX_CONNECTIONS` | `32` | Conexões admitidas simultaneamente |
| `SIDER_WORKER_QUEUE_CAPACITY` | `32` | Pedidos que podem aguardar na fila de cada worker; cada EXEC ocupa um pedido |
| `SIDER_SHARDS` | `1` | Workers proprietários, entre 1 e 256; configuração fixa |
| `SIDER_PUBSUB_MAX_CHANNELS` | `32` | Canais distintos por conexão |
| `SIDER_PUBSUB_QUEUE_CAPACITY` | `32` | Notificações pendentes por conexão; fila cheia desconecta |
| `SIDER_TRANSACTION_MAX_COMMANDS` | `128` | Comandos retidos entre MULTI e EXEC |
| `SIDER_TRANSACTION_MAX_BYTES` | `1048576` | Soma dos bytes RESP dos comandos enfileirados |
| `SIDER_WATCH_MAX_KEYS` | `128` | Chaves distintas observadas por conexão |
| `SIDER_MAX_FRAME_BYTES` | `4194304` | Frame de entrada completo, incluindo framing |
| `SIDER_MAX_BULK_BYTES` | `1048576` | Payload de cada bulk string de entrada |
| `SIDER_MAX_LINE_BYTES` | `1024` | Linha ou cabeçalho de entrada, incluindo prefixo e CRLF |
| `SIDER_MAX_NODES` | `1024` | Nós do frame, contando raiz, arrays e elementos |
| `SIDER_MAX_DEPTH` | `16` | Níveis de arrays; array raiz conta como nível 1 |
| `SIDER_MAX_INPUT_BUFFER_BYTES` | `4194304` | Bytes ainda não consumidos no buffer da conexão |
| `SIDER_MAX_RESPONSE_BYTES` | `4194304` | Resposta completa, incluindo framing |
| `SIDER_MAX_DATASET_BYTES` | `67108864` | Consumo lógico do dataset, incluindo 128 bytes por entrada |
| `SIDER_FRAME_TIMEOUT_MS` | `10000` | Formação de um frame desde o primeiro byte lido |
| `SIDER_REQUEST_TIMEOUT_MS` | `5000` | Prazo total de envio à fila e espera da resposta |
| `SIDER_WRITE_TIMEOUT_MS` | `5000` | Escrita de uma resposta completa |
| `SIDER_SHUTDOWN_TIMEOUT_MS` | `5000` | Drenagem antes de solicitar aborto das tarefas restantes |
| `SIDER_READY_FILE` | Ausente | Caminho opcional para o JSON de prontidão |

### Validação

O binário valida antes de abrir o listener. A API `serve` também valida a
configuração recebida, embora seu chamador já tenha aberto o listener.

- O endereço exige IP literal e porta. `localhost:6379` não é aceito; use
  `127.0.0.1:6379`. IPv6 usa colchetes, como `[::1]:6379`. A porta `0` é válida e
  solicita uma porta efêmera ao sistema operacional.
- Números aceitam somente dígitos ASCII. Sinal, espaços, sufixos como `MiB`, texto
  vazio e overflow são rejeitados. Zeros à esquerda são permitidos. Endereço e
  números precisam de texto UTF-8 válido.
- Todos os limites, contagens e prazos precisam ser positivos. A profundidade
  máxima permitida é `128`; conexões e capacidade da fila não podem ultrapassar
  `tokio::sync::Semaphore::MAX_PERMITS`.
- `max_bulk_bytes` e `max_line_bytes` não podem superar `max_frame_bytes`.
  `max_frame_bytes` não pode superar `max_input_buffer_bytes`.
- Os limites de bytes não podem superar `isize::MAX` no alvo. Contagens usam
  `usize`; prazos recebidos pelo ambiente usam `u64` em milissegundos. Além do
  parse, o prazo deve caber em uma soma com o relógio monotônico `Instant`.
- `max_response_bytes` precisa comportar pelo menos `128` bytes e também o maior
  bulk configurado com seu framing: `B + dígitos_decimais(B) + 5`, onde `B` é
  `max_bulk_bytes`. A soma é verificada contra overflow.
- `SIDER_READY_FILE` preserva o caminho nativo, inclusive fora de UTF-8, mas não
  aceita um caminho vazio. Diretório, permissões e suporte a hard links são
  verificados pela operação de arquivo depois do bind.

Uma configuração aceita não garante memória disponível para todas as alocações
possíveis. Também não garante que um comando caiba se os limites forem reduzidos
demais. Por exemplo, o número de nós inclui o array raiz e cada argumento.

## Requisições e limites

As requisições executáveis são arrays RESP2 não vazios de bulk strings não nulas.
Nomes de comandos ignoram diferenças de caixa ASCII; chaves e valores preservam
todos os bytes, incluindo vazio, `NUL` e conteúdo não UTF-8.

Cada conexão tem decoder, buffer de entrada e buffer de saída próprios. A leitura
é limitada ao espaço disponível antes de receber mais bytes. O codec valida o
frame antes de materializar seus payloads; os detalhes estão no
[guia RESP2](resp.md).

Comandos de dados passam pela fila limitada do shard escolhido; `PING` e `ECHO`
usam o shard zero. Cada worker possui seu mapa e aplica o comando sem `await`
durante a mutação. Entre conexões, vale a ordem recebida pelo worker, sem garantia de ordem
por chegada ao socket ou de justiça estrita.

O [guia de shards](sharding.md) define hash tags, divisão da quota e rejeição de
comandos multichave entre shards antes do envio. Filas independentes permitem
progresso de um shard mesmo quando outro está saturado.

Pub/Sub usa um registro separado do dataset. A conexão serializa confirmações e
notificações pelo mesmo escritor, com fila limitada e prazo de escrita. No modo
assinante, PING responde sem passar pela fila de dados. SUBSCRIBE, UNSUBSCRIBE e
PUBLISH não chegam ao Store. Consulte [Pub/Sub](pubsub.md).

O cliente pode enviar vários comandos em uma escrita TCP. A conexão decodifica,
envia ao worker, recebe e escreve uma resposta antes de despachar o comando
seguinte. Há um pedido em voo por conexão; pipeline não significa execução
paralela ou batching. Conexões excedentes são fechadas sem criar uma tarefa
persistente. Encerrar uma conexão libera sua vaga.

Os limites não são uma quota de memória do processo. A fila conta mensagens,
os buffers podem reter capacidade de alocação e o sistema mantém seus próprios
buffers de socket. O número de conexões e os tamanhos máximos precisam ser
considerados juntos, como no orçamento do
[plano da 0.1](../PLANO.md#limites-e-ciclo-de-vida).

O dataset de R02 tem quota lógica própria e expiração. `SET`, `MSET` e incrementos
recusam crescimento acima do orçamento sem eviction. Cada entrada conta os bytes
da chave, do valor e uma taxa fixa de 128 bytes; isso não limita o RSS. Consulte
o [guia de strings](strings.md#quota-lógica) para a contabilidade e seus limites.

## Aceitação e resultado incerto

A conclusão do envio ao canal `mpsc` é a fronteira de aceitação:

1. Antes dela, cancelar o envio não modifica o mapa.
2. Depois dela, o pedido pertence ao worker e será executado mesmo se a conexão
   cair ou o receptor da resposta for descartado, enquanto o worker continuar
   funcionando e não for abortado.
3. A resposta confirma o resultado ao cliente. Perder a resposta não desfaz uma
   mutação já aplicada.

`SIDER_REQUEST_TIMEOUT_MS` cobre a espera por capacidade da fila e pela resposta
com um único prazo. Ele não recomeça quando surge espaço na fila. Timeout fecha a
conexão e impede o próximo despacho daquele cliente. O Sider não repete comandos
automaticamente nem envia uma resposta que prometa ausência de efeitos.

Se houver timeout ou desconexão após a aceitação, o resultado é incerto para o
cliente. Essa regra também vale para falha do worker depois do envio. Reconectar
não transforma o pedido anterior em cancelado.

## Prazos de leitura e escrita

Uma conexão ociosa, ainda sem bytes de um próximo frame, não tem timeout de
ociosidade. O prazo de formação começa quando a conexão lê o primeiro byte do
frame incompleto. Novos fragmentos não renovam esse prazo.

No pipeline, o primeiro byte de um sufixo pode ter sido lido junto com o comando
anterior. Se esse sufixo estiver incompleto, seu prazo continua contando enquanto
a resposta anterior é processada. Ao voltar à leitura, um prazo expirado fecha a
conexão antes de aceitar mais bytes daquele frame.

Um frame que já está completo no buffer não expira apenas porque a execução ou
a escrita da resposta anterior demorou. O timeout de formação não é um prazo de
execução do pipeline. Um sufixo que começou numa leitura posterior usa o instante
dessa leitura, não o instante do frame anterior.

Cada resposta tem seu próprio prazo de escrita. O encoder valida a resposta
inteira antes de alterar o buffer de saída. O limite de linha de entrada não
limita os erros internos: a saída usa `SIDER_MAX_RESPONSE_BYTES` para seu tamanho
total e os tamanhos de bulk/linha, preservando os limites de nós e profundidade.

Se a escrita falhar ou expirar, a conexão fecha. Mesmo que parte dos bytes tenha
chegado ao cliente, não há tentativa de continuar com uma nova resposta nessa
mesma conexão.

## EOF e erros

EOF sem bytes pendentes encerra a conexão normalmente. Se o cliente fechar
apenas sua metade de escrita, os frames completos já recebidos são processados
na ordem e suas respostas ainda podem ser lidas. Um frame parcial no EOF é
descartado como truncado, sem execução.

Erros de aridade, comando desconhecido e opções não suportadas de `SET` geram
respostas de erro e permitem o próximo comando. Eles não chegam ao armazenamento.
O texto de comando desconhecido é simplificado e não inclui argumentos privados.

Formato inválido de requisição é fatal, mesmo quando o frame RESP2 é bem formado.
Framing inválido e limites excedidos também fecham a conexão. Quando possível,
o servidor tenta escrever um erro curto antes de fechar. Não procura um próximo
comando para recuperar sincronização. Timeout de frame, EOF truncado ou falha de
escrita não garantem uma resposta de erro ao cliente.

## Encerramento e supervisão

No Unix, o binário trata `SIGINT` e `SIGTERM`. No Windows, registra os eventos
de `Ctrl+C`, `Ctrl+Break` e fechamento do console. Uma parada forçada pelo sistema
pode impedir a drenagem; não equivale ao fluxo normal de encerramento.

Ao receber a parada, o servidor:

1. Fecha o listener e sinaliza as conexões e o worker.
2. Cancela leituras ociosas e envios que ainda aguardam aceitação.
3. Fecha a admissão da fila e drena os comandos já aceitos.
4. Permite a tentativa de resposta dos pedidos aceitos, respeitando seus prazos.
5. Aguarda as tarefas. Se a drenagem exceder o prazo configurado, solicita seu
   aborto, registra o encerramento forçado e aguarda os términos.

O padrão de `5000` ms limita a drenagem, não promete matar uma thread ou um
processo em exatamente cinco segundos. O aborto de tarefas Tokio é cooperativo e
só se completa quando elas cedem execução. Código síncrono travado não é
interrompido à força por esse mecanismo.
[Contrato de cancelamento do Tokio](https://docs.rs/tokio/latest/tokio/task/index.html#cancellation).

Término inesperado ou panic do worker fecha o listener e encerra o servidor com
erro. Falhas de uma conexão são registradas e não derrubam as demais. Cancelar a
future `serve` solicita aborto de suas tarefas, sem deixá-las destacadas do
supervisor. Essa saída não oferece a garantia de drenagem da parada normal.

Nenhum desses caminhos oferece durabilidade na 0.1. Ao encerrar o processo, o
dataset em memória é perdido.

## Arquivo de prontidão

`SIDER_READY_FILE` permite que testes e empacotadores descubram a porta efetiva
sem reservar uma porta, liberá-la e disputar um novo bind. O diretório pai deve
existir e ser gravável. Para um processo de teste, use um caminho exclusivo e
`SIDER_ADDR=127.0.0.1:0`.

Depois de registrar os sinais e abrir o listener, o binário publica um JSON
completo. Exemplo ilustrativo:

```json
{"pid":12345,"host":"127.0.0.1","port":49152}
```

`pid` é o PID do próprio Sider. `host` e `port` vêm do endereço retornado pelo
listener, incluindo a porta escolhida pelo sistema. Um bind genérico, como
`0.0.0.0`, também aparece assim no JSON; o smoke usa loopback para ter um destino
de conexão explícito.

O conteúdo é escrito e sincronizado em um arquivo temporário no mesmo diretório.
Um hard link publica o destino apenas quando o JSON está completo. Isso exige
suporte a hard links no filesystem, como NTFS ou ext4. Não existe fallback para
uma escrita parcial no destino. A operação falha se o destino já existir, sem
substituí-lo; o binário não inicia o atendimento nessa situação.
[Contrato de `std::fs::hard_link`](https://doc.rust-lang.org/std/fs/fn.hard_link.html).

No encerramento normal, o Sider compara o conteúdo do destino com o JSON que
publicou e tenta removê-lo somente se ainda forem iguais. Conteúdo alterado é
preservado. Use um diretório controlado: comparação e remoção não são uma operação
atômica contra substituições concorrentes. Falhas de remoção e término forçado
podem deixar arquivos antigos; a prontidão não é um registro durável de saúde.

O consumidor deve manter o handle do processo filho iniciado, conferir se ele
continua vivo, comparar seu PID com o JSON e fazer um `PING` RESP2 na porta
informada. Verificar apenas se algum processo possui o PID lido não basta, pois
PIDs podem ser reutilizados. Um arquivo antigo não autoriza encerrar um processo
encontrado por esse número. A publicação do JSON prova o bind, não substitui o
smoke TCP nem garante que o processo continuará disponível.

O [guia de releases](releases.md#contrato-de-prontidão-do-binário-extraído) define
o uso desse contrato para os binários realmente extraídos dos pacotes.

## Código e validação

| Arquivo | Responsabilidade |
| --- | --- |
| [src/config.rs](../src/config.rs) | Padrões, parse injetável e validação |
| [src/connection.rs](../src/connection.rs) | Buffers, pipeline, framing, respostas e prazos |
| [src/storage/worker.rs](../src/storage/worker.rs) | Fila, aceitação e mapa proprietário |
| [src/server.rs](../src/server.rs) | Admissão, supervisão e drenagem |
| [src/readiness.rs](../src/readiness.rs) | Publicação e limpeza do JSON |
| [src/main.rs](../src/main.rs) | Argumentos, logs, bind, sinais e prontidão |

Antes de integrar alterações dessa etapa, execute os testes focados e a bateria
local. Os comandos abaixo não constituem, por si só, um registro de aprovação:

```sh
cargo test --locked --lib config::
cargo test --locked --lib storage::worker::
cargo test --locked --lib connection::
cargo test --locked --lib server::
cargo test --locked --lib readiness::
cargo test --locked --test tcp
cargo test --locked --test cli
cargo xtask check
cargo doc --locked --no-deps
cargo build --locked --release
```

A validação de `R01-04` deve cobrir as duas plataformas, limites exatos, bytes
binários, fragmentação, pipeline, backpressure, cancelamento, clientes lentos,
half-close, falhas do worker, encerramento e prontidão. Testes de ordenação e
prazos usam sincronização explícita e I/O controlada; pausas arbitrárias não
comprovam a ordem dos eventos.

Registre resultados efetivamente obtidos, comandos, alvo e limitações no PR e no
[guia de testes](testing.md). Comparação diferencial contra a referência, uso de
`redis-cli` e pacotes extraídos têm seus próprios gates. Testes unitários de
rede não substituem essas evidências nem autorizam publicar a 0.1 antecipadamente.
CI permanece desativada até a 1.0 inclusive, conforme o
[fluxo de releases](releases.md).
