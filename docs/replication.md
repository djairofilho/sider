# Replicação Sider → Sider

O Sider replica assincronamente strings, hashes, listas, sets, sorted sets,
deadlines absolutos e lotes transacionais resolvidos. As instâncias precisam da
mesma versão do Sider, do formato de registros e da configuração de shards.
O transporte é próprio. Não aceita réplicas Redis, Redis Cluster ou failover automático.

O primário responde às escritas sem esperar por réplicas. Uma promoção pode perder
tudo que o primário confirmou e a réplica ainda não aplicou. Não existe limite
garantido de perda em segundos ou em operações durante uma desconexão. O status
mostra a posição aplicada e o último avanço anunciado pelo upstream; ele não prevê
escritas que ainda não foram observadas.

## Configuração

A replicação exige AOF nas duas instâncias. Cada processo usa seu próprio diretório
de dados. O listener interno atende replicação, exportação de snapshot e administração.
Ele não tem autenticação nem TLS; use loopback ou uma rede privada controlada.
A época identifica a continuidade de um fluxo, sem autenticar o operador ou a máquina.

Exemplo de primário em Linux, com quatro shards:

```sh
SIDER_ADDR=127.0.0.1:6379 \
SIDER_SHARDS=4 \
SIDER_AOF_DIR=./primary-aof \
SIDER_REPLICATION_ADDR=127.0.0.1:7380 \
./sider
```

Em outro terminal, configure a réplica:

```sh
SIDER_ADDR=127.0.0.1:6380 \
SIDER_SHARDS=4 \
SIDER_AOF_DIR=./replica-aof \
SIDER_REPLICATION_ADDR=127.0.0.1:7381 \
SIDER_REPLICA_OF=127.0.0.1:7380 \
./sider
```

No PowerShell, defina essas mesmas variáveis com `$env:NOME = 'valor'` no terminal
de cada processo e execute `sider.exe`. A variável `SIDER_REPLICA_OF` deve estar
ausente no primeiro primário. O endereço upstream exige IP literal e porta não nula.

| Variável | Padrão | Contrato |
| --- | --- | --- |
| `SIDER_REPLICATION_ADDR` | Ausente | Habilita o listener; aceita porta zero |
| `SIDER_REPLICA_OF` | Ausente | Upstream da réplica; também habilita o listener local em `127.0.0.1:0` |
| `SIDER_REPLICATION_READY_FILE` | Ausente | JSON separado com PID, host e porta interna efetiva |
| `SIDER_REPLICATION_BACKLOG_BYTES` | `134217728` | Bytes máximos de frames retidos no histórico do primário |
| `SIDER_REPLICATION_BACKLOG_BATCHES` | `4096` | Número máximo de lotes no histórico |
| `SIDER_REPLICATION_MAX_CONNECTIONS` | `4` | Sessões internas simultâneas, incluindo exportação e administração |
| `SIDER_REPLICATION_FRAME_TIMEOUT_MS` | `5000` | Prazo por leitura/escrita de frame, inclusive cabeçalho parcial |
| `SIDER_REPLICATION_SYNC_TIMEOUT_MS` | `30000` | Prazo de captura e prazo total da transferência de snapshot |
| `SIDER_REPLICATION_RECONNECT_MIN_MS` | `100` | Primeiro intervalo de reconexão |
| `SIDER_REPLICATION_RECONNECT_MAX_MS` | `5000` | Teto do backoff exponencial, limitado a 60000 ms |

O histórico deve comportar pelo menos um registro AOF máximo com o framing de
replicação. Prazos e capacidades precisam ser positivos; o prazo de sincronização
não pode ser menor que o de um frame. A configuração é validada antes da prontidão.
O receptor precisa aceitar os limites anunciados de registro, quantidade de mutações
e dataset do primário. Configuração incompatível falha antes de instalar dados.

`SIDER_REPLICATION_READY_FILE` usa o mesmo formato e a mesma publicação atômica do
[arquivo de prontidão RESP](network.md#arquivo-de-prontidão). Os dois caminhos devem
ser distintos. O JSON interno é publicado antes do JSON RESP, após inicializar os
workers e o papel durável. Porta zero é descoberta nesse arquivo, sem reservar e
fechar uma porta antes de iniciar o processo. Confira o PID do filho iniciado e
consulte `sider-replica --status`; `PING` é enviado somente ao listener RESP.

## Snapshot e histórico

Uma única sequência global vem do escritor AOF. A captura de snapshot bloqueia novas
mutações, aguarda pedidos aceitos e expirações em andamento, coleta todos os shards
e sincroniza a AOF na posição `S`. A assinatura do histórico começa em `S` sob a mesma
barreira. A transmissão ocorre depois de liberar as escritas do primário.

O receptor valida versão, limites, ordem binária das chaves, quantidade de entradas,
checksum, tipos e quotas antes de trocar o dataset. Uma transferência incompleta ou
inválida conserva o estado anterior. O snapshot recebido fica em memória limitada
pelo orçamento anunciado; o arquivo temporário AOF é criado durante a instalação.

A instalação publica uma nova geração AOF com dados, sequência, papel e época.
Somente depois troca todos os mapas sob a barreira global. Uma leitura aceita observa
o estado anterior ou o novo estado completo. Cancelar a conexão não abandona uma
instalação já aceita no meio dessa troca. No reinício, a recuperação seleciona a
geração publicada e remove temporários de instalação que não foram publicados.

Após o snapshot, o primário envia lotes de pós-imagens resolvidas em sequência.
A réplica confirma com ACK somente depois de persistir, aplicar e sincronizar o lote.
Essa sincronização ocorre mesmo com `SIDER_AOF_SYNC=everysec`. O TTL conserva seu
deadline absoluto, e a transação continua sendo um lote único. A repetição exata do
último frame na mesma sessão apenas repete o ACK; gaps e duplicatas divergentes
encerram a sessão sem aplicar o lote inválido.

O primário limita o histórico por bytes e lotes. Cada assinante mantém no máximo um
frame incremental fora desse histórico. Um assinante lento não segura escritas nem
outros assinantes. Se perder o histórico necessário, recebe `FULL` na reconexão.
Backlog, snapshots, índices, buffers e filas consomem memória além da quota lógica
do dataset. Essa quota não é uma garantia de RSS.

## Reconexão e somente leitura

A réplica reconecta com a época e a sequência recuperadas de sua própria AOF.
O primário responde `CONTINUE` apenas se a identidade e o histórico ainda cobrirem
essa posição. Época divergente ou histórico insuficiente exigem `FULL`. Cada início
de primário gera uma nova época; seu histórico em memória não sobrevive ao reinício.
Não há continuidade simulada usando apenas o número da sequência.

Enquanto o papel for réplica, escritas de clientes retornam `READONLY`. A verificação
ocorre na conexão e novamente no worker, inclusive para `MULTI`/`EXEC`. Leituras
ocultam chaves expiradas, mas a réplica não cria tombstones próprios nem executa
expiração ativa. O primário replica as remoções por expiração na ordem da AOF.
Os relógios das máquinas precisam permanecer sincronizados para interpretar deadlines
absolutos; a execução local usa relógio monotônico.

Pub/Sub continua efêmero e local à instância. Mensagens publicadas e assinaturas não
integram o dataset, o snapshot ou o histórico de replicação. `WATCH` também é local;
a instalação de um novo snapshot invalida observações do estado substituído.

## Status e promoção manual

Consulte o listener interno:

```sh
./sider-replica --addr 127.0.0.1:7381 --status
```

A saída JSON contém `role`, `epoch`, `sequence`, `connected`, `upstream_sequence`,
`backlog_bytes`, `full_syncs` e `partial_syncs`. Na réplica, `sequence` é a posição
aplicada e sincronizada. No primário, é a cabeça do journal, observada após append;
não comprova que todos os workers terminaram seu apply naquele instante.
O atraso em lotes só pode ser calculado com a réplica conectada e a posição upstream
conhecida na mesma época. Desconectada, o atraso é desconhecido, inclusive quando
o último número observado coincide com a posição local.

Para uma troca planejada, interrompa as escritas destinadas ao primário antigo,
espere as posições convergirem e redirecione os clientes após a promoção. Para uma
recuperação com upstream indisponível, aceite explicitamente a perda do sufixo ainda
não aplicado. O comando exige conexão de loopback:

```sh
./sider-replica --addr 127.0.0.1:7381 --promote
```

A resposta só chega depois de publicar o papel primário e uma nova época na AOF,
desligar a sessão antiga e liberar escritas. Um timeout do cliente pode deixar o
resultado desconhecido; consulte o status antes de decidir o próximo passo.
O papel promovido prevalece sobre um `SIDER_REPLICA_OF` antigo no reinício, impedindo
voltar a seguir aquele upstream. Uma AOF ainda marcada como réplica exige upstream
configurado para iniciar; retirar a variável não promove o dataset.

O primário antigo não é rebaixado automaticamente. Retire-o do tráfego de escrita
antes de reutilizá-lo, para evitar duas instâncias aceitando alterações independentes.
Para recriá-lo como réplica, pare o processo, preserve um backup do diretório antigo
e inicie com um diretório AOF novo e `SIDER_REPLICA_OF` apontando para o novo primário.
Esta versão não oferece rebaixamento online nem consenso entre primários.

## Durabilidade e validação

O cabeçalho AOF v3 registra papel e época na mesma geração que o snapshot. Registros
tipados mantêm seu formato; arquivos legados v1/v2 continuam aceitos. Um binário antigo
que não conhece v3 não deve abrir essa AOF. A política de sincronização do primário
continua sendo a do [guia AOF](persistence.md), independentemente do ACK de réplica.
Os testes de término forçado comprovam recuperação após crash de processo. No Windows,
não comprovam resistência a queda de energia ou sincronização de diretório equivalente
à disponível no Unix.

Os testes focados são:

```sh
cargo test --locked --lib replication_config
cargo test --locked --test replication_protocol
cargo test --locked --test replication_journal
cargo test --locked --test replication_storage
cargo test --locked --test replication_persistence
cargo test --locked --test replication_network replication_
```

`replication_network` executa cinco cenários com binários reais: tipos/TTL/transações
e exportação; transferência inválida e ACK seguido de crash; réplica lenta e perda
de histórico; reinício do primário; promoção atrasada com corte do upstream.
`replication_persistence` inclui seis kills controlados durante a instalação, nos
três pontos de publicação para cada papel. Os testes de storage verificam barreira,
cancelamento, quota e ausência de mistura entre shards.
Um ensaio adicional encerra primário e réplica conectados com um frame interno
parcial em andamento e confere a retirada dos arquivos de prontidão.

O runner `release_replication_gate` executa os cinco cenários de processos e registra
as medidas observadas de convergência e atraso antes da promoção. Só emite recibo
com contexto de release válido no SHA exato, conforme o [guia de releases](releases.md).
Testes locais não aprovam automaticamente os gates da candidata nem substituem
ensaios nos pacotes e na imagem distribuída.
