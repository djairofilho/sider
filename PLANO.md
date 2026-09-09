# Plano de implementação do Sider

Sider será um servidor de banco de dados em memória, escrito em Rust, com um
subconjunto explícito de compatibilidade com Redis. O nome é Redis ao contrário.

Este plano detalha a versão 0.1. O [ROADMAP](ROADMAP.md) e seu
[manifesto versionado](releases/plan.json) definem a sequência oficial até a 1.0,
com tarefas, dependências e critérios. O [guia de releases](docs/releases.md)
descreve candidatas e publicação. O bootstrap já contém pacote Rust, configuração de
endereço e binário com testes. A referência Redis e o codec RESP2 isolado estão
implementados, assim como parser e armazenamento síncrono dos cinco comandos.
Worker e servidor TCP também estão implementados em R01-04. A suíte diferencial
e a integração com CLI de R01-05 estão implementadas. As execuções
e seus limites estão no [guia de testes](docs/testing.md).

CI e publicação automática foram adiadas para depois da 1.0. Até a 1.0 inclusive,
as etapas avançam com testes locais e publicação manual, mantendo os critérios
funcionais e as evidências exigidas por release.

## Índice

- [Ponto de partida](#ponto-de-partida)
- [Escopo da versão 0.1](#escopo-da-versão-01)
- [Arquitetura e decisões de Rust](#arquitetura-e-decisões-de-rust)
- [Estrutura inicial](#estrutura-inicial)
- [Contratos principais](#contratos-principais)
- [Limites e ciclo de vida](#limites-e-ciclo-de-vida)
- [Checklist incremental](#checklist-incremental)
- [Critérios de conclusão](#critérios-de-conclusão)
- [Riscos e evolução](#riscos-e-evolução)

## Ponto de partida

Inspeção inicial em 7 de setembro de 2026, antes do bootstrap:

- A pasta `NovoRedis` está vazia, sem código e sem repositório Git inicializado.
- Rust e Cargo 1.97.1 estão disponíveis, com toolchain estável Windows MSVC.
- O executável Docker está instalado. O funcionamento do daemon não foi verificado.
- `redis-cli` não foi encontrado no `PATH`.

A implementação usa pacote e binário chamados `sider`, versão `0.1.0` e Edition
2024. A pasta atual mantém seu nome sem afetar o nome do programa. O bootstrap
inclui o repositório privado `djairofilho/sider`, solicitado após o planejamento.
A publicação da crate está desabilitada com `publish = false`.

## Escopo da versão 0.1

O resultado esperado é executar `sider`, conectar com `redis-cli` em RESP2 e testar
operações sobre chaves e valores binários, com um único worker de armazenamento.

| Comando | Contrato inicial | Evidência esperada |
| --- | --- | --- |
| `PING` | Responder `+PONG\r\n` | Comparação dos bytes com Redis |
| `PING mensagem` | Devolver a mensagem como bulk string | Testar vazio e bytes não UTF-8 |
| `ECHO mensagem` | Devolver exatamente os bytes recebidos | Testar CRLF dentro do conteúdo |
| `GET chave` | Bulk string ou `$-1\r\n` quando ausente | Distinguir ausência de valor vazio |
| `SET chave valor` | Criar ou substituir; responder `+OK\r\n` | Verificar o estado com `GET` |
| `DEL chave [chave ...]` | Contar as chaves efetivamente removidas | Testar ausentes e repetidas |

Referências de comportamento: [PING](https://redis.io/docs/latest/commands/ping/),
[ECHO](https://redis.io/docs/latest/commands/echo/),
[GET](https://redis.io/docs/latest/commands/get/),
[SET](https://redis.io/docs/latest/commands/set/) e
[DEL](https://redis.io/docs/latest/commands/del/).
O tipo exato de `PONG` também foi conferido no
[código oficial do Redis](https://github.com/redis/redis/blob/unstable/src/server.c),
nas definições de `shared.pong` e `pingCommand`.

As seguintes regras fecham as ambiguidades do escopo:

- Nomes de comandos serão comparados sem distinguir maiúsculas de minúsculas ASCII.
  Chaves e valores manterão todos os bytes, inclusive `NUL` e sequências não UTF-8.
- `SET` aceitará apenas sua forma básica. Argumentos adicionais produzirão
  `ERR unsupported SET options`, sem alteração de estado. Essa é uma divergência
  intencional para opções válidas no Redis e estará na matriz de compatibilidade.
- Aridade incorreta dos comandos suportados terá resposta compatível com Redis.
  Comando desconhecido produzirá `ERR unknown command`. O texto simplificado desse
  erro será uma divergência documentada; não reproduzirá argumentos do cliente.
- Será oferecido apenas o banco lógico padrão. Não haverá `SELECT`, `AUTH`, `HELLO`,
  `COMMAND`, `CLIENT`, RESP3 ou formato de comandos inline.
- O codec representará os cinco tipos RESP2. As requisições executáveis precisarão
  ser arrays não vazios de bulk strings não nulas.
- Arrays raiz vazios/nulos e argumentos de tipos diferentes serão rejeitados como
  formato de requisição inválido. O tratamento não será anunciado como idêntico ao
  Redis para entradas fora do subconjunto declarado.
- `EXISTS` fica para a 0.2: embora apareça na lista geral de comandos iniciais, o
  recorte específico da 0.1 enumera somente os cinco comandos acima.

A forma das requisições e os tipos são definidos na
[especificação RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/).
A distinção entre erros de framing e comandos é apoiada pelo
[processamento de requisições do Redis](https://github.com/redis/redis/blob/unstable/src/networking.c).

Haverá múltiplas conexões e tratamento sequencial de comandos concatenados desde a
0.1. TCP entrega um fluxo de bytes: uma leitura pode conter parte de um comando ou
vários comandos. Isso antecipa a correção básica de pipeline, mas não inclui batching,
vários comandos em execução por conexão ou otimizações de throughput.

Limites de entrada, backpressure e encerramento básico também entram nesta versão.
São necessários para controlar recursos e executar testes confiáveis. TTL, limite do
dataset, AOF, shards, replicação, novos tipos e benchmarks comparativos ficam depois.

## Arquitetura e decisões de Rust

Uma única crate terá uma biblioteca testável e um binário pequeno. A separação em
várias crates será considerada quando houver consumidores ou ciclos de evolução
independentes. No início, módulos oferecem fronteiras suficientes.

```text
listener TCP
    |
    +-- conexão A: buffer -> decoder -> parser de comando --+
    |                                                      |
    +-- conexão B: buffer -> decoder -> parser de comando --+--> mpsc limitado
                                                                  |
                                                           worker único
                                                           dono do HashMap
                                                                  |
                                                oneshot para a conexão de origem
                                                                  |
                                                         encoder -> socket
```

### Ownership do armazenamento

O worker possuirá `HashMap<Bytes, Bytes>`. Apenas essa tarefa acessará o mapa.
Cada comando será aplicado de forma síncrona, sem `await` durante a alteração do
estado. `DEL` com várias chaves terminará antes do próximo comando começar.

O worker será uma tarefa Tokio, não uma thread dedicada ou fixada em um núcleo.
O runtime poderá executar tarefas de rede em paralelo, mas as operações do mapa
serão serializadas. Criar mais tarefas não torna o armazenamento paralelo.

Manteremos o hasher padrão do `HashMap` inicialmente. Trocar o algoritmo exige
medição e análise da resistência a entradas adversariais.

### Canais e ordenação

Cada conexão terá um handle clonável contendo um `mpsc::Sender<Request>`.
O envelope carregará um comando e um canal `oneshot` de resposta. O enum de comandos
permanecerá independente dos canais, para poder ser testado e reutilizado no replay
futuro. O padrão de tarefa proprietária e troca de mensagens é documentado no
[tutorial de canais do Tokio](https://tokio.rs/tokio/tutorial/channels).

Todos os comandos válidos, inclusive `PING` e `ECHO`, passarão pelo worker na 0.1.
Isso simplifica o fluxo. O custo dos canais será avaliado posteriormente.

Cada conexão aguardará a execução e a escrita de uma resposta antes de despachar
seu próximo comando. Assim, preservaremos a ordem por conexão e limitaremos a um
pedido em voo por cliente. Entre conexões, valerá a ordem recebida pelo worker;
não haverá promessa de ordem por instante de chegada ao socket ou justiça estrita.

### Buffers e dados binários

A conexão possuirá seu `BytesMut`. O decoder guardará índices e estado de parsing,
sem referências emprestadas que atravessem leituras ou realocações do buffer.

Na primeira implementação, os payloads de um frame completo serão copiados para
`Bytes` independentes antes de liberar o prefixo do buffer. O parser de comandos
moverá esses valores para o comando, sem uma segunda cópia do conteúdo.

Essa cópia deliberada evita que uma chave pequena mantenha vivo um grande buffer
de rede. A retenção é uma consequência possível do compartilhamento de armazenamento
descrito na [documentação de Bytes](https://docs.rs/bytes/latest/bytes/struct.Bytes.html).
`GET` poderá clonar o `Bytes` já armazenado para a resposta, compartilhando conteúdo
imutável. Zero-copy na entrada será uma otimização posterior, acompanhada de medidas.

### Fronteiras e dependências

O codec não conhecerá sockets nem armazenamento. O parser de comandos não alterará
estado. O mapa não conhecerá RESP. A conexão coordenará essas partes.

Dependências de produção: `tokio`, `bytes`, `thiserror`, `tracing` e
`tracing-subscriber`. Ativar apenas as funcionalidades Tokio necessárias para rede,
I/O, runtime multithread, canais, timers e sinais. Nos testes, usar `proptest` e as
utilidades de controle de tempo do Tokio.

O código próprio começará com `#![forbid(unsafe_code)]`. Isso não afirma que todas
as dependências transitivas sejam livres de `unsafe`. O projeto não usará panics
como forma de tratar dados enviados pelo cliente.

## Estrutura inicial

```text
sider/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── README.md
├── PLANO.md
├── src/
│   ├── lib.rs
│   ├── main.rs
│   ├── config.rs
│   ├── error.rs
│   ├── server.rs
│   ├── connection.rs
│   ├── resp/
│   │   ├── mod.rs
│   │   ├── frame.rs
│   │   ├── decoder.rs
│   │   └── encoder.rs
│   ├── command/
│   │   ├── mod.rs
│   │   ├── parser.rs
│   │   └── reply.rs
│   └── storage/
│       ├── mod.rs
│       ├── store.rs
│       └── worker.rs
├── tests/
│   ├── common/mod.rs
│   ├── tcp.rs
│   ├── robustness.rs
│   ├── differential.rs
│   └── redis_cli.rs
└── docs/
    ├── architecture.md
    └── compatibility.md
```

Os arquivos serão criados quando sua etapa começar. Testes unitários ficarão junto
dos módulos. `store.rs` conterá a execução síncrona dos cinco comandos; não haverá
um arquivo por comando enquanto essas implementações forem pequenas.

Não criaremos módulos vazios de expiração, persistência, replicação ou roteamento.
O handle do worker será a fronteira a preservar quando o roteador surgir na 0.4.

## Contratos principais

Os trechos abaixo são esboços de interface, não arquivos de implementação completos.
Todos os tipos de erro citados serão definidos com `thiserror` em sua etapa.

### Codec RESP2

```rust
use bytes::{Bytes, BytesMut};

pub enum Frame {
    Simple(Bytes),
    Error(Bytes),
    Integer(i64),
    Bulk(Option<Bytes>),
    Array(Option<Vec<Frame>>),
}

impl Decoder {
    pub fn new(limits: RespLimits) -> Result<Self, ConfigError>;
    pub fn decode(&mut self, src: &mut BytesMut)
        -> Result<Option<Frame>, ProtocolError>;
}

pub fn encode(frame: &Frame, dst: &mut BytesMut, limits: RespLimits)
    -> Result<(), EncodeError>;
```

Usaremos `Bytes` também em simple strings e erros para não exigir UTF-8 no codec.
Esses tipos continuarão proibindo CR e LF no conteúdo, conforme RESP. Respostas
internas usarão mensagens ASCII controladas.

Invariantes do decoder:

1. `Ok(None)` significa frame incompleto. O conteúdo de `src` fica intacto; somente
   o estado interno de varredura avança. O chamador pode acrescentar bytes ao final.
2. `Ok(Some(frame))` consome exatamente um frame, mantém o restante de `src` e
   reinicializa o estado para o próximo. Cada conexão terá seu próprio decoder.
3. `Err` significa formato inválido ou limite excedido. A conexão será encerrada;
   não tentaremos recuperar sincronização depois de framing inválido.
4. O scanner preservará cursor, pilha de arrays e metadados dos elementos já lidos.
   Bytes e elementos anteriores não serão reprocessados a cada fragmento recebido.
5. Comprimentos e offsets usarão operações verificadas. Não haverá reserva de
   memória proporcional ao tamanho declarado antes de validar os limites.
6. O limite agregado incluirá os bytes de framing. O contador de nós incluirá arrays
   e seus elementos, inclusive o nó raiz. Profundidade e quantidade serão independentes.
7. Somente após validar o frame completo, o decoder materializará a árvore e copiará
   os payloads. Metadados parciais também terão crescimento limitado.

O encoder validará o tamanho total e o conteúdo dos tipos simples antes de alterar
`dst`. Em erro, não deixará resposta parcial no buffer. Round trips só serão exigidos
para frames válidos dentro dos limites; codificações equivalentes podem ser normalizadas.

### Comando e resposta

```rust
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Get { key: Bytes },
    Set { key: Bytes, value: Bytes },
    Del { keys: Vec<Bytes> },
}

pub enum Reply {
    Pong,
    Ok,
    Bulk(Option<Bytes>),
    Integer(i64),
}

pub fn parse(frame: Frame) -> Result<Command, RequestError>;
```

`RequestError` distinguirá estrutura inválida de requisição, que fecha a conexão,
de erro de comando, que permite continuar. Aridade será validada antes do envio ao
worker. `Reply` será convertido para `Frame` na camada de protocolo.

O parser preservará a lista de `DEL`, inclusive duplicatas. O store contará apenas
remoções bem-sucedidas. Não é necessário deduplicar a entrada para obter esse resultado.

### Worker e servidor

```rust
pub struct Request {
    pub command: Command,
    pub reply: tokio::sync::oneshot::Sender<Result<Reply, DbError>>,
}

impl DbHandle {
    pub async fn execute(&self, command: Command) -> Result<Reply, DbError>;
}

impl Store {
    pub fn execute(&mut self, command: Command) -> Reply;
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), ServerError>;
```

`serve` criará o worker e supervisionará as tarefas. Receber um listener já aberto
permitirá testes com `127.0.0.1:0`, sem disputar portas fixas. O binário carregará
configuração, iniciará logs, abrirá o listener e fornecerá o sinal de encerramento.

`Store::execute` será testável sem runtime. O `Result` do envelope e do handle
separará indisponibilidade do worker de uma resposta normal com chave ausente.
Nenhum contrato promete recuperar falta de memória do processo.

## Limites e ciclo de vida

Defaults propostos para desenvolvimento local, ajustáveis em configuração:

| Parâmetro | Valor inicial | Regra |
| --- | --- | --- |
| Endereço | `127.0.0.1:6379` | Alterável por `SIDER_ADDR` |
| Conexões ativas | 32 | Excedentes são fechadas sem criar tarefa persistente |
| Fila do worker | 32 mensagens | Envio aguarda capacidade disponível |
| Pedidos em voo por conexão | 1 | Abrange espera na fila, execução e resposta |
| Frame de entrada | 4 MiB | Inclui cabeçalhos, payloads e CRLF |
| Bulk string | 1 MiB | Vale individualmente para chaves e valores |
| Buffer de entrada | 4 MiB | Limitar antes de cada leitura |
| Linha simples/cabeçalho | 1 KiB | Evitar busca ilimitada por CRLF |
| Nós por frame | 1.024 | Inclui raiz; até 1.023 argumentos em requisição plana |
| Profundidade de arrays | 16 | Array raiz conta como nível 1 |
| Buffer de resposta | 4 MiB | No máximo uma resposta sendo escrita |
| Formação de frame incompleto | 10 s | Desde o primeiro byte, sem renovar a cada fragmento |
| Espera por envio e resposta do worker | 5 s | Prazo total para a chamada `execute` |
| Escrita de resposta | 5 s | Timeout fecha a conexão |
| Encerramento | 5 s | Drenagem limitada, seguida de término forçado |

`ServerConfig` agrupará esses limites; `RespLimits` conterá a parte do codec.
Parâmetros serão expostos por variáveis `SIDER_*` documentadas, com parse estrito.
Limites, capacidades e timeouts iguais a zero, relações incoerentes e overflow
serão rejeitados antes de abrir o servidor. A porta `0` é uma exceção intencional:
permitirá que o sistema escolha uma porta efêmera nos testes de rede. Os testes
usarão limites menores para exercitar as fronteiras.

Uma fila limitada conta mensagens, não bytes. O limite por frame, o número de
conexões e um único pedido em voo devem ser analisados juntos. Como orçamento
conservador, considerar `conexões × (buffer + pedido + resposta)`, mais
`capacidade_fila × pedido` e um pedido em execução no worker, além de metadados e
capacidade das alocações. Essa conta pode duplicar pedidos de clientes ainda ativos,
mas cobre comandos que permanecem aceitos depois de uma desconexão, quando a vaga
do cliente já pode ter sido reutilizada. O
[contrato de mpsc do Tokio](https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html)
documenta capacidade e backpressure, mas não estabelece quota de memória do processo.

O tamanho do dataset continuará sem quota na 0.1. Vários `SET` válidos ainda poderão
esgotar a memória disponível. Limites de rede não resolvem esse problema; contabilidade
do armazenamento e política de rejeição serão implementadas na 0.2, sem eviction
automática.

O bind padrão em loopback acompanha o escopo sem autenticação. A 0.1 será apresentada
como protótipo para uso local e testes, sem promessa de serviço pronto para produção.

### Aceitação, cancelamento e desconexão

O envio concluído ao `mpsc` será a fronteira de aceitação. Antes desse ponto, cancelar
a tentativa de envio não modifica o mapa. Depois dele, o worker executará o comando
aceito mesmo se o receptor `oneshot` tiver sido descartado.

Qualquer timeout de `execute` fechará a conexão, sem despachar outro pedido desse
cliente. Esse fechamento não cancela comandos já aceitos. Se houver timeout ou queda
da conexão após a aceitação, o resultado será desconhecido para o cliente. Não
enviaremos uma mensagem que prometa ausência de efeitos e não repetiremos comandos
automaticamente. Falhar ao entregar a resposta não derrubará o worker nem desfará
uma mutação.

Em EOF, frames completos já recebidos serão processados na ordem. Se restar um
frame parcial, ele será descartado como truncado, sem execução. Um cliente que fecha
apenas sua escrita ainda poderá receber respostas de comandos completos.

Erro de comando produzirá um frame de erro e permitirá ler o comando seguinte.
Erro de protocolo produzirá uma resposta curta, quando possível, seguida de fechamento.
Uma escrita interrompida não será retomada na mesma conexão com uma nova resposta.

### Encerramento e supervisão

Ao receber o sinal de parada, o servidor deixará de aceitar conexões e novos pedidos.
As conexões finalizarão o pedido já aceito e tentarão entregar sua resposta.
Leituras ociosas e envios ainda não aceitos serão cancelados. Todos os handles de envio
serão liberados, o worker drenará o que já recebeu e as tarefas serão aguardadas.

O prazo de encerramento limita essa drenagem. Se expirar, tarefas restantes serão
abortadas e registradas; comandos pendentes não terão garantia de conclusão nessa
saída forçada. O servidor também monitorará falha inesperada do worker e encerrará
o listener, evitando continuar aceitando clientes sem armazenamento funcional.

Essa organização segue as fases de detectar, comunicar e aguardar a parada descritas
em [Graceful Shutdown no Tokio](https://tokio.rs/tokio/topics/shutdown).
É um encerramento básico testável, sem durabilidade: o dataset continua apenas em memória.

## Checklist incremental

Cada etapa termina com código compilável e os testes disponíveis passando.
O avanço depende desses critérios, não de uma estimativa fixa de dias.

### 1. Fundação e especificação executável

- [x] Criar pacote `sider`, `lib.rs`, binário mínimo e configuração inicial.
- [x] Fixar a toolchain estável de desenvolvimento, inicialmente 1.97.1, e gerar
      `Cargo.lock`. Definir MSRV apenas se ele também for testado.
- [x] Adicionar `forbid(unsafe_code)`, formatação e lint.
- [x] Escrever os primeiros testes de configuração e integração do binário.
- [x] Adicionar fixtures literais das futuras respostas RESP2.
- [x] Criar matriz de compatibilidade com todos os itens como pendentes.
- [x] Fixar Redis e `redis-cli` 8.10.1 e o digest Linux amd64 no manifesto de releases.
- [x] Preparar a infraestrutura de referência e conferir sua execução em `R01-01`.
      A imagem fixada não constitui evidência de compatibilidade sem executar os testes.

Saída: `cargo check --locked` e `cargo test --locked` passam. O binário é executável,
mas ainda não oferece serviço TCP. A escolha da versão de referência precede a
consolidação dos fixtures de erros e dos testes diferenciais.

### 2. Codec RESP2 isolado

- [x] Implementar tipos, validação de limites e encoder.
- [x] Implementar decoder incremental com cursor e pilha limitados.
- [x] Testar tipos RESP2, nulos, vazios, inteiros nos extremos e conteúdo binário.
- [x] Para cada fixture curta, testar todos os pontos de fragmentação e entrega
      byte a byte. Para payloads grandes, testar divisões representativas e aleatórias.
- [x] Testar frames concatenados, preservação do sufixo e CRLF dividido entre leituras.
- [x] Testar comprimento inválido, overflow, prefixo desconhecido, CRLF inválido,
      limites exatos e limite excedido por um byte/nó/nível.
- [x] Adicionar `proptest`: round trip de frames válidos, fragmentação equivalente,
      consumo correto e entrada arbitrária sem panic sob limites pequenos.

Saída: codec testado sem rede e sem banco. Um contador de trabalho disponível apenas
nos testes verifica crescimento aproximadamente linear ao fragmentar cabeçalhos e
arrays, para detectar reprocessamento quadrático sem depender do relógio.
Os contratos implementados estão no [guia do codec](docs/resp.md). Linhas incluem
prefixo e CRLF no seu limite; a profundidade configurável tem teto de 128 para
proteger também a destruição da árvore de frames.

### 3. Comandos e semântica do mapa

- [x] Implementar parsing dos cinco comandos e classificação de erros.
- [x] Implementar `Store`, com operação síncrona e resposta tipada.
- [x] Testar caixa do nome do comando, aridade e rejeição das opções de `SET`.
- [x] Testar sobrescrita, ausentes, chave vazia, valor vazio e bytes não UTF-8.
- [x] Testar `DEL a a inexistente`: contar apenas uma remoção quando `a` existir.
- [x] Testar que comandos rejeitados não alteram o estado.

Saída: semântica completa da 0.1 testada sem sockets ou tarefas assíncronas.

### 4. Worker proprietário e canais

- [x] Implementar `Request`, `DbHandle` e worker com fila limitada.
- [x] Testar ordem, compartilhamento do estado entre handles e encerramento do canal.
- [x] Testar backpressure com fila pequena e sincronização explícita.
- [x] Descartar o receptor de resposta após aceitar `SET` e verificar o efeito por `GET`.
- [x] Testar indisponibilidade do worker sem panic e sem espera infinita.

Saída: nenhum acesso concorrente direto ao mapa; concorrência testada com canais e
barreiras, sem usar sleeps arbitrários para determinar a ordem das operações.

### 5. TCP e ciclo completo

- [x] Implementar `serve`, tarefa por conexão, buffer limitado e encoder de respostas.
- [x] Integrar configuração, logs, supervisão e sinal de parada no binário.
- [x] Testar o ciclo `SET -> GET -> DEL -> GET` por TCP em porta efêmera.
- [x] Testar dois clientes compartilhando estado e isolamento dos buffers.
- [x] Testar vários comandos enviados em uma escrita, com respostas na ordem.
- [x] Testar erro recuperável seguido de comando válido na mesma conexão.
- [x] Testar EOF limpo, half-close, frame truncado, conexão excedente e cliente lento.
- [x] Testar timeout e shutdown em leitura, fila cheia e escrita de resposta.

Saída: `cargo run --locked --bin sider` inicia o servidor em loopback. Logs indicam
inicialização, erros e encerramento, sem registrar chaves ou valores por padrão.

### 6. Compatibilidade com Redis e redis-cli

- [x] Criar suíte diferencial que envia os mesmos bytes a instâncias isoladas de
      Redis e Sider. O oráculo de teste não dependerá apenas do codec sob teste.
- [x] Comparar respostas brutas, tipos, códigos e mensagens de erro declaradas
      compatíveis, além do estado observado pelos comandos suportados.
- [x] Executar sequências determinísticas e geradas de `SET`, `GET` e `DEL`, com
      prefixos de chaves exclusivos por caso e instâncias descartáveis.
- [x] Executar os cinco comandos usando `redis-cli` em modo RESP2 e não interativo.
- [x] Separar testes nativos obrigatórios da suíte externa, marcada explicitamente
      como dependente de Redis/CLI. Ausência da ferramenta não conta como aprovação.
- [x] Atualizar a matriz por comando, forma suportada, limitação, teste e versão usada.

Saída: os testes externos passam antes de declarar a 0.1 concluída. Não usaremos a
saída textual de `redis-cli` como comparação binária: ele apresenta valores ao usuário,
conforme o [guia oficial da CLI](https://redis.io/docs/latest/develop/tools/cli/).

No Windows, priorizar um ambiente Linux de testes com Redis e CLI da mesma versão,
via Docker ou WSL. O daemon e a conectividade serão verificados nessa etapa. O Docker
servirá como infraestrutura de teste; uma imagem de distribuição do Sider fica depois.
Clientes que exigem handshake automático terão sua limitação documentada.

### 7. Robustez e entrega da 0.1

- [x] Validar liberação de recursos após clientes lentos e múltiplas desconexões.
- [x] Documentar arquitetura, comandos suportados, limites e como reproduzir os testes.
- [x] Executar a bateria final e registrar resultados reais, incluindo testes ignorados.

Verificação rápida local das etapas, conforme os alvos forem surgindo:

```powershell
cargo test --locked <filtro>
cargo xtask check
```

O check reúne formatação, Clippy, build do binário e testes nativos.
Mudanças de interfaces, documentação de API ou build exigem
`cargo doc --locked --no-deps` e
`cargo build --locked --release`. Registre os resultados locais no PR e integre
por merge commit, sem aguardar CI.

Os testes externos têm comandos próprios documentados e não são executados
implicitamente em `cargo test`.
Antes de publicar, o build e os testes comuns serão verificados manualmente em
Windows e Linux. A CI
multiplataforma foi verificada no bootstrap, mas está desativada nesta fase.
Os workflows foram removidos; uma futura CI exige solicitação explícita depois da 1.0.

## Critérios de conclusão

A versão 0.1 estará pronta quando:

1. Os cinco comandos funcionarem via TCP e `redis-cli` no subconjunto declarado.
2. Chaves e valores binários, vazios e ausentes tiverem comportamento verificado.
3. Fragmentação, concatenação, limites e entradas malformadas tiverem testes passando.
4. A suíte diferencial passar contra a versão registrada do Redis.
5. Cancelamento, backpressure, clientes lentos e shutdown tiverem comportamento testado.
6. Formatação, lint e testes nativos estiverem registrados.
7. A matriz não confundir comportamento planejado com compatibilidade demonstrada.

Não haverá meta de superar Redis nesta versão. O custo de cópias, canais e alocações
será registrado como hipótese para medição futura, sem alegação de desempenho.

## Riscos e evolução

| Risco ou decisão | Tratamento planejado |
| --- | --- |
| Parser consumir memória ou CPU com entrada hostil | Limites agregados, aritmética verificada, estado incremental, fixtures e propriedades |
| Chave pequena reter buffer grande | Copiar payloads para alocações independentes na entrada |
| Limites de rede serem confundidos com limite do banco | Documentar dataset sem quota; resolver contabilidade na 0.2 |
| Cliente interpretar timeout como operação desfeita | Definir aceitação no enqueue e resultado desconhecido depois dele |
| Worker único virar gargalo | Medir antes de particionar; limitar trabalho por comando e por conexão |
| Comando grande atrasar os demais | Limitar bytes e argumentos; evitar prometer justiça estrita |
| Teste diferencial repetir o mesmo bug do codec | Fixtures literais e leitura do wire independentes para as respostas esperadas |
| Diferenças de versão serem confundidas com bugs | Fixar Redis, CLI, toolchain e dependências; versionar a matriz |
| Interface crescer cedo demais | Manter módulos concretos; introduzir abstrações quando surgir o segundo caso |

As decisões de evolução estão fechadas no manifesto; detalhes de implementação e
evidências serão produzidos na tarefa correspondente, sem antecipar suporte:

| Versão | Contrato e entrega |
| --- | --- |
| 0.2 | Strings adicionais, opções de `SET`, TTL passivo/ativo e quota com rejeição de crescimento, sem eviction automática |
| 0.3 | AOF versionado e checksum, escritor global, mutações resolvidas, políticas de fsync, recuperação e compactação |
| 0.4 | Roteamento estável, hash tags, shards fixos e rejeição de operações multichave entre shards antes de efeitos |
| 0.5 | Hashes, listas e sets com TTL, quota, `WRONGTYPE` e persistência |
| 0.6 | Sorted sets com `ZADD` básico, `ZREM`, `ZCARD`, `ZSCORE` e `ZRANGE start stop [WITHSCORES]` |
| 0.7 | `MULTI`, `EXEC`, `DISCARD`, `WATCH` e `UNWATCH` no mesmo shard; replay atômico do lote |
| 0.8 | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` e `PING` no modo assinante; filas limitadas |
| 0.9 | Replicação assíncrona Sider→Sider da mesma versão/configuração, réplica somente leitura e promoção manual |
| 0.10 | Métricas, diagnóstico, backup/restauração e imagem Docker Linux amd64 distribuída privadamente |
| 1.0 | Subconjunto congelado, auditoria diferencial, carga contínua de uma hora, migração e benchmarks reproduzíveis |

O AOF exigirá validar também condições dependentes do estado antes de registrar uma
mutação e preservar a ordem entre log, aplicação e resposta. Com fsync periódico,
uma resposta de sucesso não terá a mesma garantia que fsync por escrita. Após uma
falha, também pode existir operação durável cuja resposta não chegou ao cliente.
Replay de TTL exigirá prazo absoluto persistido; `Instant` não serve como formato
durável. A substituição atômica de arquivos precisará de testes específicos por sistema.

Para shards, a proposta é paralelismo entre partições, não entre quaisquer chaves
distintas: duas chaves independentes ainda podem cair no mesmo shard. Usar hash tags
não implica compatibilidade com Redis Cluster. Isso exigiria contratos adicionais
de slots, descoberta e redirecionamentos.

Na 0.4, `DEL`, `MGET`, `MSET` e demais operações multichave ficarão restritos ao
mesmo shard, com erro antes de qualquer efeito. Essa restrição altera o conjunto
de operações aceitas no worker único e deverá aparecer nas notas e na matriz de
compatibilidade. O AOF manterá inicialmente um escritor global; as medições da
0.4 avaliarão seu custo sem alterar a garantia de recuperação.

Na 0.7, erros de enfileiramento abortam a transação conforme o subconjunto Redis;
erros individuais durante `EXEC` não desfazem as outras operações. AOF registra
o lote de mutações resolvidas de modo que replay não aplique meia transação.
Replicação na 0.9 preserva esse lote e TTL, mas permanece assíncrona, sem failover
automático e sem suporte entre versões/configurações distintas.

Redis Cluster, Sentinel, resharding online, RESP3, Lua, operações bloqueantes,
transações entre shards, TLS e ACL ficam após a 1.0. O ambiente suportado até lá
é controlado. Toda publicação requer candidata e gates cumulativos aprovados;
o bootstrap não será publicado como versão funcional.

A fundação executável, os testes de configuração e as fixtures de referência
Redis/CLI estão implementados. A execução de `R01-01` está documentada no
[guia de testes](docs/testing.md). O codec isolado de `R01-02` também está
implementado, assim como parser e armazenamento síncrono de `R01-03`, worker e
TCP de `R01-04`. `R01-05` integra diferenciais e CLI. `R01-GATE` publicou a
primeira candidata; a final aguarda nova candidata com as alterações de
ferramentas e gates. O roadmap preserva os checkpoints internos deste plano.
