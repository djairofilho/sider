# Persistência AOF

O AOF registra o estado final de cada comando em lotes indivisíveis. Está disponível
no marco interno R03. O formato tem sua própria versão, independente da versão do
binário. Sem `SIDER_AOF_DIR`, o servidor continua operando somente em memória.

## Executar

```powershell
$env:SIDER_AOF_DIR = 'C:\dados\sider'
$env:SIDER_AOF_SYNC = 'always'
cargo run --locked
```

```sh
SIDER_AOF_DIR=./data SIDER_AOF_SYNC=always cargo run --locked
```

Use o mesmo diretório ao reiniciar. Um lock exclusivo do sistema operacional
impede dois escritores de abrirem esse diretório. A recuperação ocorre antes do
bind e da criação do arquivo de prontidão. Na API que recebe um listener já
aberto, `serve` recupera antes de aceitar conexões. `server::prepare` permite
recuperar explicitamente antes de abrir o listener.

O cabeçalho registra a quantidade de shards e a versão do roteamento. Divergência
impede a recuperação antes de qualquer reparo da cauda. O AOF legado v1 significa
um shard. Alterar o particionamento exige a [migração offline](aof-migration.md).

| Variável | Padrão | Contrato |
| --- | --- | --- |
| `SIDER_AOF_DIR` | Ausente | Diretório nativo; ativa a persistência |
| `SIDER_AOF_SYNC` | `always` | `always` ou `everysec` |
| `SIDER_AOF_QUEUE_CAPACITY` | `32` | Número máximo de pedidos aguardando o escritor |
| `SIDER_AOF_MAX_RECORD_BYTES` | `67108864` | Payload de um registro; entre 64 bytes e 64 MiB |
| `SIDER_AOF_MAX_DELTA_BYTES` | `16777216` | Bytes de registros capturados durante a compactação |
| `SIDER_AOF_COMPACT_AFTER_BYTES` | `67108864` | Append acumulado que solicita compactação; `0` desativa a automática |

As opções AOF são interpretadas quando o diretório está configurado. O limite de
registro deve comportar uma mutação completa e seu framing interno. Reduzir esse
limite pode impedir a leitura de um AOF existente ou a gravação de um valor que
antes cabia. Uma escrita que excede o limite recebe `ERR AOF record limit exceeded`,
sem append ou aplicação, mantendo a conexão disponível. O teto de 64 MiB continua
valendo para configurações com dataset maior.
O limite padrão de 100 mil mutações por lote também é conferido antes da alocação.

## Confirmação e falha

O worker prepara apenas as chaves tocadas pelo comando, compartilhando os bytes
imutáveis. Resolve condições de `SET`, incrementos, TTL e quota com uma leitura
consistente do relógio. Depois envia um `ResolvedBatch` ao escritor global, que
atribui uma sequência crescente. O worker aplica o estado preparado e responde
somente depois da confirmação desse escritor. O replay nunca reexecuta condições
como `NX`, `XX` ou incrementos.

| Política | O que a confirmação assegura | Perda possível |
| --- | --- | --- |
| `always` | `write_all` e `sync_all` terminaram antes da aplicação e resposta | A suíte de crash de processo exige recuperar todos os lotes confirmados; integridade física depende do filesystem e dispositivo |
| `everysec` | `write_all` terminou; o próximo sync é periódico | Tudo que ainda não alcançou um `sync_all` bem-sucedido pode ser perdido em falha do sistema |

`everysec` solicita sincronização a cada segundo, inclusive sem nova escrita. O
período não é um limite rígido de perda: atrasos de I/O e agendamento podem ampliar
a janela. Uma parada normal drena pedidos aceitos e sincroniza o escritor. O prazo
de drenagem não torna chamadas de filesystem canceláveis.

Falha de append ou sync impede uma resposta de sucesso e encerra a admissão do
worker. Um lote completamente escrito pode reaparecer no reinício mesmo que a
resposta tenha sido perdida. Timeout ou desconexão depois da aceitação não provam
ausência de efeitos e não autorizam repetição automática de um incremento.

Expiração ativa usa o mesmo caminho durável, com origem `Expiration`. Leituras que
encontram uma entrada vencida também produzem tombstones com essa origem. Essa distinção
permite distinguir manutenção de TTL de futuras escritas recebidas por réplicas.

## Formatos v1 e v2

Todos os inteiros usam little endian. Chaves e valores são bytes, sem exigência de
UTF-8. CRC-32/ISO-HDLC detecta corrupção acidental; não autentica os arquivos.

Novos arquivos e compactações usam o cabeçalho v2, com 32 bytes:

| Offset | Tamanho | Campo |
| --- | --- | --- |
| 0 | 8 | Magic ASCII `SIDERAOF` |
| 8 | 4 | Versão do formato, `2` |
| 12 | 8 | Sequência representada pelo snapshot inicial |
| 20 | 4 | Quantidade de shards, entre 1 e 256 |
| 24 | 4 | Versão do roteamento, `1` |
| 28 | 4 | CRC dos primeiros 28 bytes |

O leitor também aceita o cabeçalho v1, com 24 bytes: mesmo magic, versão `1`,
sequência e CRC dos primeiros 20 bytes no offset 20. Sua configuração implícita é
um shard com roteamento v1. A versão de roteamento 1 identifica FNV-1a64 sobre as
hash tags de `storage::routing`; versões desconhecidas são recusadas. Registros e
mutações preservam a mesma codificação nas duas versões do cabeçalho.

Cada registro tem comprimento de payload `u32`, seu complemento binário `u32`,
CRC do payload `u32` e o payload. Comprimento e complemento são conferidos antes
da alocação; checksum é conferido antes do decode. Campos internos precisam caber
inteiramente no registro. Tipos, origens e bytes extras desconhecidos são erros.

| Tag | Payload depois da tag |
| --- | --- |
| `1`, snapshot | Uma mutação `Put` |
| `2`, selo | Sequência `u64`, quantidade de entradas `u64`, digest `u32` |
| `3`, lote | Sequência `u64`, origem `u8`, quantidade `u32`, mutações |

As entradas de snapshot estão em ordem binária crescente, sem duplicatas. O digest
encadeia os CRCs dos registros completos com `snapshot_digest` e começa em zero.
Contagem e digest do selo detectam também a retirada de um registro completo. Um
arquivo só é recuperável depois de um selo válido. Os lotes seguintes precisam ter
sequências consecutivas a partir do snapshot.

Mutações iniciais usam tag `1` para `Put` de string e `2` para `Delete`. Ambas têm
comprimento de chave `u32` e chave. `Put` acrescenta comprimento de valor `u32`,
valor, indicador de TTL `u8` e deadline Unix em milissegundos `i64`. Sem TTL, ambos
os últimos campos são zero. Origem `1` indica cliente; `2`, expiração.

R05/R06 acrescentam postimages completas: tag `3` para hash, `4` para lista,
`5` para set e `6` para sorted set. Coleções não podem estar vazias no arquivo;
chaves/campos/membros continuam binários. Scores preservam os bits IEEE e recusam
NaN mesmo com checksum válido. O [contrato de persistência tipada](types-persistence.md)
descreve ordem, quota, TTL e ensaios de crash dessas famílias.

O parser e o armazenamento pré-validam o lote inteiro. Chaves duplicadas no mesmo
lote, quota excedida ou deadline não representável impedem sua aplicação. Na
recuperação, um `Put` já vencido remove o valor anterior e não volta a ser persistente.
A quota lógica e o índice de expiração são reconstruídos.

Cada lote precisa pertencer a um único shard na configuração gravada. A quota
total é dividida pelo número de shards, distribuindo o resto pelos primeiros
índices. A recuperação acompanha o uso de cada shard e recusa excesso local mesmo
quando o total global ainda cabe. Metadados recuperados ficam disponíveis antes
de iniciar o escritor.

## Recuperação e compactação

Os arquivos publicados têm nomes `generation-NNNNNNNNNNNNNNNNNNNN.aof`. O servidor
abre a maior geração. Corrupção nessa geração interrompe a inicialização; não há
fallback silencioso para uma geração antiga que possa perder escritas confirmadas.

Uma cauda com registro incompleto depois do selo válido é recuperável. Antes de
truncá-la, o servidor copia e sincroniza o original em `tail-*.bak`. Header inválido,
checksum incorreto, versão desconhecida ou corrupção interna preservam o AOF e
encerram a inicialização com erro. Para diagnóstico, trabalhe sobre uma cópia do
diretório com o servidor parado. Não renomeie uma geração antiga como atual sem
avaliar a perda de dados correspondente.

A compactação captura um snapshot consistente, escreve seus registros e selo em
um arquivo temporário e mantém os appends no arquivo atual. O escritor acumula um
delta limitado, incluindo expirações. Ao concluir o snapshot, grava o delta,
sincroniza o arquivo novo e publica uma geração com nome ainda não utilizado. Só
então muda o destino dos appends. O arquivo anterior permanece completo; a geração
que antecede esse backup é retirada na próxima compactação bem-sucedida.

Se o delta exceder o orçamento, a compactação é abortada e o arquivo atual continua
válido. O produtor termina antes de permitir outro snapshot, limitando a quantidade
de trabalho simultâneo. Falha antes da publicação também preserva o escritor atual.
Falha depois da publicação é fatal, impedindo que novas escritas continuem apenas
na geração antiga. Temporários interrompidos nunca são escolhidos para replay e
podem ser removidos com o servidor parado após conferir a geração atual.

Valores imutáveis são compartilhados pelo snapshot; os metadados do snapshot e o
delta consomem memória adicional. Quota lógica do dataset não é limite do RSS.
Com múltiplos workers, o integrador deve coordenar uma barreira global antes de
enfileirar `begin_compaction`; um snapshot isolado de um shard não representa o AOF
global.

## Garantias por plataforma

O lock usa `File::try_lock`, aberto para leitura e escrita, compatível com os
requisitos de locking do Windows. `sync_all` solicita a persistência do conteúdo e
dos metadados do arquivo. São os contratos da
[biblioteca padrão de Rust](https://doc.rust-lang.org/std/fs/struct.File.html).
O guard libera o lock explicitamente antes de fechar o arquivo. Em Linux, isso
evita que descritores duplicados por `fork`/`dup` prolonguem a propriedade depois
da parada do escritor, conforme a semântica de
[`flock`](https://man7.org/linux/man-pages/man2/flock.2.html).

No Linux, a publicação também sincroniza o diretório depois do rename. No Windows,
Rust não oferece essa sincronização de diretório de forma portátil. A implementação
publica em um nome novo, mantém a geração anterior e foi testada com término abrupto
de processos nas fases de troca. Isso não demonstra atomicidade contra queda de
energia. `rename` tem comportamento dependente do sistema, conforme sua
[documentação](https://doc.rust-lang.org/std/fs/fn.rename.html).

## Verificar

```sh
cargo test --locked --lib persistence
cargo test --locked --lib storage::mutation
cargo test --locked --test persistence
cargo test --locked --test aof_migration
```

A suíte interna independe de contexto de release e executa arquivos reais em
diretórios temporários. O pai inicia filhos da própria suíte e os termina somente
depois de um sinal explícito nos pontos de falha. Não encerra processos alheios.

Os runners `release_crash_gate`, `release_recovery_gate` e `release_migration_gate`
estão registrados em `releases/gates.json`. Eles exigem `GateContext`, repetem os
casos efetivos e publicam recibos somente após validar SHA, plataforma e contexto
do bundle. Os quatro testes ignorados da execução interna são esses três wrappers
e o helper de processo filho; sua ausência não conta como gate aprovado.

O gate inicial de migração lê a fixture fixa `tests/fixtures/aof-v1.hex`, compacta,
reabre e compara seu estado. Também recusa uma versão desconhecida preservando os
bytes. A fixture representa o primeiro formato AOF, sem alegar migração de uma
versão anterior publicada com persistência.

Validação de desenvolvimento do R03: a suíte `persistence` executou 20 testes com
sucesso no Windows e no Ubuntu via WSL, incluindo nove pontos de crash de processo.
Os arquivos dos ensaios Linux ficam em `/tmp`, no filesystem Linux. Esses resultados
são verificação funcional local; os recibos do bundle final exigem nova execução no
SHA congelado conforme o [guia de releases](releases.md).
