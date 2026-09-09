# Executar o Sider

Este pacote contém o servidor Sider, as CLIs `sider-backup`, `sider-aof-migrate`
e `sider-replica`, este README, a licença MIT do código próprio
e os avisos de terceiros no diretório `licenses/`, com seu inventário de hashes.
Confira a versão com `sider --version`. A release também fornece `SHA256SUMS`,
`release-manifest.json`, notas e evidências de validação.

## Iniciar

Linux x86_64 GNU, compilado e testado em Ubuntu 24.04:

```sh
./sider --version
./sider
```

Windows x86_64 MSVC, no PowerShell:

```powershell
.\sider.exe --version
.\sider.exe
```

O processo permanece em primeiro plano e escuta em `127.0.0.1:6379` por padrão.
Use `Ctrl+C` para encerrar. Para escolher outra porta no Linux:

```sh
SIDER_ADDR=127.0.0.1:6380 ./sider
```

No PowerShell, defina `$env:SIDER_ADDR = '127.0.0.1:6380'` antes de iniciar.
O endereço exige IP literal, não hostname. `--help` mostra a interface do binário.

## Experimentar

Em outro terminal, com `redis-cli` instalado separadamente:

```sh
redis-cli -2 -h 127.0.0.1 -p 6379 PING
redis-cli -2 -h 127.0.0.1 -p 6379 SET exemplo valor
redis-cli -2 -h 127.0.0.1 -p 6379 GET exemplo
redis-cli -2 -h 127.0.0.1 -p 6379 DEL exemplo
```

O núcleo atende `PING`, `ECHO`, `GET`, `SET`, `DEL`, `EXISTS`, `INCR`, `DECR`,
`MGET`, `MSET`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL` e `PERSIST`. SET aceita NX, XX,
EX, PX, GET e KEEPTTL. Requisições usam arrays RESP2 de bulk strings.
Também atende hashes, listas, sets, sorted sets e Pub/Sub.
Transações oferecem `MULTI`, `EXEC`, `DISCARD`, `WATCH` e `UNWATCH` dentro de
um shard. Erros individuais durante EXEC não desfazem os demais comandos.
Com mais de um shard, comandos multichave e transações devem usar chaves do
mesmo shard; uma hash tag comum, como `{conta}:saldo` e `{conta}:limite`, mantém
essas chaves juntas. Operações cruzadas retornam erro antes da execução.
Chaves e valores preservam bytes arbitrários. Não há modo inline ou RESP3.
Clientes que enviam comandos de inicialização adicionais podem ser incompatíveis.

## Persistência e diagnóstico

AOF é opcional. Para iniciar um primário persistente com quatro shards e
listener interno de backup/replicação, no Linux:

```sh
SIDER_ADDR=127.0.0.1:6379 SIDER_SHARDS=4 SIDER_AOF_DIR=./primary-aof \
SIDER_AOF_SYNC=always SIDER_REPLICATION_ADDR=127.0.0.1:6381 ./sider
```

No PowerShell, defina cada variável com `$env:NOME = 'valor'` e execute
`.\sider.exe`. Cada instância precisa de um diretório AOF próprio. A recuperação
termina antes da prontidão; corrupção ou layout de shards incompatível impede
a inicialização. `always` aguarda sincronização por lote. `everysec` permite
perda das escritas ainda não sincronizadas se o processo ou a máquina falhar.

`sider --diagnose` confere a configuração sem abrir listeners ou iniciar recovery.
Com o servidor em execução, `redis-cli -2 INFO` consulta métricas da instância.
Quota lógica do dataset e memória RSS são medidas distintas.

## Backup e administração

As CLIs operacionais estão no pacote e não exigem Cargo ou checkout. Consulte
`./sider-backup --help`, `./sider-aof-migrate --help` e `./sider-replica --help`.
No Windows, use `.\` e acrescente `.exe` ao nome do executável.

O primário precisa de `SIDER_AOF_DIR` e do listener interno em
`SIDER_REPLICATION_ADDR`, separado do endereço RESP. Mantenha-o em loopback ou
rede privada controlada. Com o listener em `127.0.0.1:6381`:

```sh
./sider-replica --addr 127.0.0.1:6381 --status
./sider-backup export --source 127.0.0.1:6381 --destination backup-novo --source-sha SHA_COMPLETO
./sider-backup verify --source backup-novo --shards 4 --routing 1
./sider-backup restore --source backup-novo --destination dados-novos --shards 4 --routing 1
```

Informe o SHA de 40 dígitos do manifesto de origem e o número real de shards.
Backup e restauração recusam destino existente; a restauração confere checksums,
formato, layout e quota. TTL mantém o vencimento absoluto, consumindo o tempo
transcorrido. Inicie os dados restaurados em uma instância isolada com a mesma
configuração e confira o resultado antes de utilizá-la.

Para iniciar o diretório restaurado do exemplo, use `SIDER_AOF_DIR=./dados-novos`
e `SIDER_SHARDS=4`. Para uma origem com um shard, use `--shards 1` nos comandos
verify/restore e `SIDER_SHARDS=1` na instância restaurada.

A migração offline altera explicitamente o layout em outro diretório. Pare a
instância de origem antes de executar, por exemplo:

```sh
./sider-aof-migrate --source dados-originais --source-shards 1 --source-routing 1 \
  --destination dados-quatro-shards --shards 4 --routing 1
```

O destino deve ser novo. A ferramenta confere o formato e a quota por shard e
preserva os arquivos da origem; trocar apenas `SIDER_SHARDS` não migra os dados.

## Réplica e promoção

No mesmo host do primário acima, inicie uma réplica com diretório separado:

```sh
SIDER_ADDR=127.0.0.1:6380 SIDER_SHARDS=4 SIDER_AOF_DIR=./replica-aof \
SIDER_REPLICATION_ADDR=127.0.0.1:6382 SIDER_REPLICA_OF=127.0.0.1:6381 ./sider
```

Em outro terminal, confira o status:

```sh
./sider-replica --addr 127.0.0.1:6382 --status
```

A replicação exige a mesma versão e layout; a réplica rejeita escritas de clientes.
Pub/Sub permanece local a cada instância.
Mantenha os relógios sincronizados, pois TTL usa deadlines absolutos.

`sider-replica --addr IP:PORTA --promote` exige loopback e promove explicitamente
a réplica após interromper a sessão upstream. Replicação é assíncrona: escritas
confirmadas pelo primário e ainda não aplicadas na réplica podem ser perdidas.
Para uma troca planejada, suspenda escritas no primário antigo, espere as posições
convergirem, promova e redirecione os clientes. O primário antigo não é rebaixado
automaticamente. Se a promoção retornar timeout, consulte o status antes de repetir.
Não há eleição ou failover automático.

## Limites e segurança

- Use somente em ambiente controlado. Não há autenticação, ACL ou TLS.
- Persistência é opcional por `SIDER_AOF_DIR`; sem AOF, dados se perdem ao terminar.
  AOF e replicação não substituem um backup preservado e verificado.
- TTL tem expiração passiva e limpeza ativa limitada. A quota lógica padrão é
  64 MiB (`SIDER_MAX_DATASET_BYTES`); crescimento excedente é rejeitado sem eviction.
  A contabilidade inclui chave, valor e taxa fixa de 128 bytes, não mede RSS.
- O padrão permite 32 conexões e 32 comandos na fila do worker, payloads de até
  1 MiB e frames/buffers de entrada de até 4 MiB. Isso não limita a memória total.
- Os prazos padrão são 10 segundos para formar um frame, 5 segundos para fila e
  resposta, 5 segundos para escrita e 5 segundos para drenagem no encerramento.
  Conexões ociosas sem frame parcial não têm timeout de ociosidade.
- Depois que um comando entra na fila, perder a conexão ou a resposta não desfaz
  sua execução. Após timeout, o resultado pode ser desconhecido para o cliente.
- Encerramento é best-effort; não existe garantia de drenagem após término forçado.

## Documentação da versão

O [repositório privado](https://github.com/djairofilho/sider) contém os guias de
configuração, rede, compatibilidade e testes em `docs/`. Use o SHA do manifesto ou
a tag da publicação para consultar o mesmo código
do pacote. Consulte `docs/replication.md`, `docs/backup.md`, `docs/persistence.md`,
`docs/metrics.md` e `docs/compatibility-matrix.md` nessa revisão. Os guias completos
não estão incluídos neste arquivo compactado.

O pacote não inclui Redis ou redis-cli. As dependências do Sider preservam suas
próprias licenças. A licença MIT não altera a visibilidade privada dos artefatos.
