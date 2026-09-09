# Arquitetura do Sider

O Sider começa como uma única crate Rust, com biblioteca testável e um binário
pequeno. O [plano de implementação](../PLANO.md) define os contratos completos da
versão 0.1; este documento resume as fronteiras e identifica o que já existe.

## Implementado

| Parte | Responsabilidade atual |
| --- | --- |
| Biblioteca | Expor a configuração reutilizável pelo binário e pelos testes |
| Codec RESP2 | Representar, validar, codificar e decodificar frames com limites |
| Parser de comandos | Validar formato/aridade e mover argumentos para comandos tipados |
| Armazenamento síncrono | Executar strings e TTL sobre `HashMap<Bytes, Entry>` privado, com quota lógica |
| Worker | Possuir o mapa, receber comandos na fila limitada e responder por oneshot |
| Conexão e servidor | Coordenar RESP2/TCP, limites, timeouts, ordenação e supervisão |
| Configuração | Validar endereço, limites, prazos e arquivo opcional de prontidão |
| Erros de configuração | Representar falhas de validação com tipos explícitos |
| Binário | Processar ajuda/versão; abrir listener, registrar sinais e publicar prontidão |
| Ferramentas de desenvolvimento | Fixar toolchain e dependências; verificar formatação, lint, testes e build |

A execução normal atende TCP com Tokio. Parser e mapa continuam testáveis sem
runtime. As dependências de produção são `thiserror`, `bytes`, `tokio`, `tracing`
e `tracing-subscriber`, com funcionalidades selecionadas. O código próprio usa
`#![forbid(unsafe_code)]`.

`ServerConfig` agrupa endereço, limites e prazos. `from_env` lê o ambiente do
processo; `from_lookup` permite fornecer valores explícitos nos testes, sem alterar
o ambiente global. O binário trata argumentos desconhecidos e configuração
inválida como falhas, com código de saída diferente de zero.

## Fronteiras implementadas na versão 0.1

| Componente | Conhece | Não precisa conhecer |
| --- | --- | --- |
| Codec RESP2 | Frames, bytes e limites do protocolo | Sockets, comandos e armazenamento |
| Parser de comandos | Frames e contratos dos comandos | I/O e estado do banco |
| Armazenamento | Comandos tipados e mapa de chaves e valores | RESP e tarefas de rede |
| Worker | Armazenamento, fila de pedidos e respostas | Framing e buffers de conexão |
| Conexão | Buffer, codec, parser, handle do worker e socket | Acesso direto ao mapa |
| Servidor | Listener, configuração, tarefas e encerramento | Detalhes de cada comando |

Worker, conexão e servidor foram adicionados em R01-04. A estrutura não
antecipa módulos vazios nem várias crates sem consumidores independentes.

### Propriedade do armazenamento

Cada `Store` possui seu `HashMap` e executa comandos de forma síncrona, com um único
worker proprietário. `DbHandle` roteia o comando pela chave e confere todas as
chaves antes do enqueue. Cada shard tem um canal `mpsc` limitado; respostas usam
`oneshot`. `PING` e `ECHO` passam pelo worker zero. O padrão continua com um shard.

Pub/Sub usa um hub separado, que protege apenas metadados e envios sem espera
para filas limitadas. Cada conexão possui uma inscrição RAII, liberada também em
cancelamento. O socket tem um único escritor para confirmações/notificações;
PING no modo assinante não usa o worker. Nenhuma inscrição pertence ao dataset.

A fila define a ordem de execução entre conexões. Cada conexão aguarda sua
resposta antes de despachar o próximo comando. Assim, a versão 0.1 mantém um
pedido em voo por cliente e preserva a ordem dos comandos concatenados daquele
cliente, sem prometer justiça estrita entre clientes.

### Protocolo e dados binários

O decoder é incremental: sua entrada pode conter um fragmento, um frame completo
ou vários frames. Ele consome exatamente um frame completo, preserva o sufixo do
buffer e mantém estado limitado enquanto aguarda mais bytes.

O codec representa os cinco tipos RESP2. O parser aceita como requisição apenas
arrays não vazios de bulk strings não nulas. Chaves e valores preservam os bytes,
inclusive conteúdo vazio e sequências que não sejam UTF-8.

Payloads de entrada são copiados para alocações independentes após validação completa.
Isso evita que uma chave pequena retenha um buffer grande de rede. A decisão poderá
ser revista com medições. O [guia do codec](resp.md) detalha contratos e limites.

O parser move os payloads para `Command`, sem copiar novamente seu conteúdo.
`Reply` não depende de canais. A conversão de resposta para `Frame` fica na camada
de comandos; o mapa não importa RESP. `GET` clona o handle imutável `Bytes`, de
modo que uma resposta já obtida continua válida após sobrescrita ou remoção.

R02 acrescenta arrays de respostas e erros recuperáveis de execução. `MGET`
preserva a ordem e compartilha os mesmos payloads imutáveis. `MSET` pré-valida
o saldo do lote inteiro antes de alterar o mapa, com último valor por chave.

### Expiração e quota

`Entry` guarda valor, geração, deadline monotônico e deadline Unix em milissegundos.
O relógio é injetável. A execução usa o monotônico; o absoluto fica disponível
para futura persistência. Um índice ordenado mantém no máximo um evento por chave,
removido em reescritas ou `PERSIST`; a geração impede aplicar expiração antiga.
O worker processa até 64 eventos a cada 100 ms, além da expiração em acesso.

`StoreConfig` define a quota lógica. Cada entrada conta bytes da chave, do valor
e uma taxa fixa de 128 bytes. Crescimento é rejeitado antes da mutação, sem eviction.
Buffers, filas, respostas retidas e o RSS não pertencem a essa conta. O
[guia de strings](strings.md) descreve comandos, invariantes e limites temporais.

Erro de formato é fatal para a conexão. Aridade, comando desconhecido e
opções não suportadas são recuperáveis e nunca chegam ao mapa. O texto do erro
desconhecido não reproduz os argumentos enviados pelo cliente.

### Limites e ciclo de vida

Os limites abrangem conexões, fila, tamanho dos frames, buffers, profundidade,
número de elementos e prazos. O binário valida tudo antes de abrir o listener;
`serve` também valida configurações recebidas diretamente pela API.
O [guia de rede](network.md) documenta padrões e variáveis.

O envio concluído à fila é a fronteira de aceitação. Depois dele, o worker
executa o comando mesmo se o cliente desconectar. Perder a resposta pode
deixar o resultado desconhecido para o cliente; não há repetição automática.

O encerramento para novas conexões e pedidos e drena o trabalho aceito até o
prazo configurado. Depois solicita aborto cooperativo às tarefas restantes.
Término inesperado do worker encerra o listener. `JoinSet` também aborta as
tarefas pertencentes ao servidor se a future de supervisão for cancelada.

## Evolução

A base mantém dados em memória com workers independentes por shard. TTL, quota e
strings adicionais estão implementados em R02; roteamento, filas e restrição
multichave estão em R04-01 a R04-03. AOF e integração durável de shards permanecem
pendentes. O [contrato de shards](sharding.md) descreve a divisão fixa de quota.

A divisão em várias crates e otimizações de cópia, alocação ou hashing dependerão
de necessidades concretas e medições. Não há resultados de desempenho publicados
neste estágio.
