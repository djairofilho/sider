# Backup consistente e restauração

`sider-backup` captura um ponto consistente do primário por TCP e produz um
snapshot AOF selado, um manifesto e checksums. A restauração verifica o conjunto
antes de criar um diretório de dados novo. Use o executável do
[pacote da mesma versão](packages.md), no Windows com sufixo `.exe`.

## Preparar a origem

O primário precisa de AOF e do listener interno de replicação/exportação.
Configure endereços separados para clientes RESP e exportação:

```sh
SIDER_ADDR=127.0.0.1:6379 SIDER_AOF_DIR=/dados/primario \
SIDER_AOF_SYNC=always SIDER_SHARDS=4 \
SIDER_REPLICATION_ADDR=127.0.0.1:6381 ./sider
```

No PowerShell, defina as mesmas variáveis `$env:SIDER_...` antes de iniciar
`.\sider.exe`. O endereço exige IP literal e porta. Esse listener não usa RESP
nem aceita clientes Redis. Ele não tem autenticação ou TLS; mantenha-o em
loopback ou na rede privada controlada do serviço.

Para portas efêmeras em ensaios, use `SIDER_REPLICATION_ADDR=127.0.0.1:0` e
`SIDER_REPLICATION_READY_FILE` com caminho exclusivo. O arquivo separado
publica PID, host e porta quando a preparação durável terminou. A prontidão
RESP continua em `SIDER_READY_FILE`.

## Exportar

Consulte o manifesto do build para obter o SHA completo da origem. Escolha um
diretório que ainda não existe, com pai existente:

```sh
./sider-backup export --source 127.0.0.1:6381 \
  --destination /backups/backup-001 \
  --source-sha SHA_COMPLETO_DE_40_HEXADECIMAIS
```

O cliente exige a mesma versão do Sider, confere layout e limites anunciados
no handshake e recebe `Hello`, `FullStart`, entradas ordenadas, `FullEnd` e EOF.
Cursor, contagem, ordem, comprimentos e digest precisam coincidir. Não há ACK
ou fluxo incremental nesta conexão.

O snapshot reúne todos os shards no mesmo corte durável, incluindo todos os
tipos e efeitos completos de transações. A barreira dos workers cobre a captura
e o corte de sequência. A transferência usa essa imagem imutável fora da
barreira; um cliente lento não mantém o bloqueio global durante o envio.
Escritas posteriores ao corte ficam fora desse backup.

Uma execução bem-sucedida retorna JSON em stdout e deixa três arquivos:

| Arquivo | Conteúdo |
| --- | --- |
| `snapshot.aof` | Cabeçalho v2 com layout/sequência, registros tipados e selo |
| `backup-manifest.json` | Versão, cursor, SHA declarado, limites, contagem, tamanho e hash |
| `SHA256SUMS` | SHA-256 do manifesto e do snapshot |

O cabeçalho v2 não copia papel de réplica nem identidade de replicação da
origem. O cursor completo fica no manifesto. `source_sha_declared` é declaração
do operador: o protocolo não atesta o SHA do binário remoto.
`validation_max_dataset_bytes` registra a quota escolhida para validar a
captura, não uma quota inferida da origem.

Os arquivos são sincronizados antes do retorno de sucesso. Erro ou cancelamento
remove os arquivos parciais pertencentes ao comando. Término forçado pode
deixar diretório parcial; ele nunca é aceito sem manifesto, checksums e selo
válidos. Não reutilize esse nome sem inspecionar o conteúdo. Destinos existentes
são recusados.

## Verificar e restaurar

Preserve o backup sem alterações enquanto os comandos o leem. Informe o layout
esperado e uma quota suficiente por shard. Estes exemplos usam quatro shards,
roteamento versão 1 e a quota total padrão de 64 MiB:

```sh
./sider-backup verify --source /backups/backup-001 --shards 4 --routing 1
./sider-backup restore --source /backups/backup-001 \
  --destination /dados/restaurado-001 --shards 4 --routing 1
```

`verify` não altera dados. `restore` confere manifesto, checksums, cabeçalho,
registros, ordem, selo e EOF antes de criar o destino. Valida novamente a cópia
antes de publicá-la como a geração inicial AOF. Links simbólicos nos arquivos
obrigatórios e no diretório selecionado são recusados. O destino deve ser novo
e ficar fora do backup; um diretório em uso não é sobrescrito.

As chaves mantêm o vencimento Unix absoluto. O tempo entre captura e restauração
consome o TTL: chaves já vencidas não voltam a ficar visíveis. A quota é repartida
pelo mesmo algoritmo do servidor; espaço livre em outro shard não compensa
estouro local. O relatório informa uso por shard e entradas ainda vivas.

Inicie a instância isolada com o mesmo layout e quota usados para restaurar:

```sh
SIDER_ADDR=127.0.0.1:6382 SIDER_SHARDS=4 \
SIDER_AOF_DIR=/dados/restaurado-001 SIDER_AOF_SYNC=always ./sider
```

Mudar o número de shards exige a [migração offline explícita](aof-migration.md)
depois da restauração. Não edite cabeçalho ou manifesto para simular outro
layout. Promova a instância após conferir seus dados e a prontidão.

## Limites e garantias

| Opção | Padrão e efeito |
| --- | --- |
| `--max-record-bytes` | 64 MiB por payload AOF, máximo de 64 MiB |
| `--max-mutations` | 100.000 por lote, máximo de 100.000 |
| `--max-snapshot-bytes` | 256 MiB, máximo de 2 GiB; limita frames acumulados e arquivo local |
| `--max-dataset-bytes` | 64 MiB de quota lógica total, repartida por shard |
| `--timeout-ms` | 120.000 ms para toda a conexão/exportação, incluindo EOF |

Os limites anunciados pela origem precisam caber no receptor, mesmo quando o
snapshot atual é pequeno. Limites inválidos são recusados antes da conexão.
A quota lógica não é um teto de RSS. A validação local processa uma entrada
por vez e não restaura o dataset inteiro em memória.

SHA-256 e checksums de registros detectam corrupção; não autenticam um backup
contra quem consegue substituir todos os arquivos e checksums. Mantenha a
origem e os arquivos sob controle de acesso do ambiente.

O prazo de rede não interrompe I/O bloqueante do filesystem. As garantias de
sync seguem a [persistência do Sider](persistence.md): testes de término de
processo não equivalem a ensaio de falta de energia. No Windows, não se promete
sync do diretório como no Linux. Um backup concluído também depende da
durabilidade do dispositivo ou volume onde foi gravado.

## Evidência reproduzível

Registre SHA do checkout limpo, versões dos executáveis, configuração da origem,
comandos, saídas, códigos de término e hashes. Preserve backup e dados originais
para a baseline interna R10 e a migração 1.0. Não transfira resultados para outro
SHA nem reescreva baselines anteriores.

O [runbook da baseline interna](internal-baseline.md) registra esse ciclo com
pacotes extraídos em um e quatro shards. Na migração, o backup R10 é restaurado
pela CLI congelada da origem antes de abrir os dados com o servidor novo.

`cargo test --locked --test backup` cobre a CLI real por TCP, os cinco tipos,
bytes binários, TTL absoluto, layout, corrupção, quotas, diretórios existentes,
EOF com prazo e cancelamento em Linux e Windows. Usa relógios injetados e portas
efêmeras. `cargo test --locked --test backup_process` usa primário e CLI reais:
mantém um receptor lento com buffer reduzido, confirma progresso de transações,
exporta outro snapshot e inicia a restauração isolada. Compara os cinco tipos,
corte indivisível de EXEC, quotas por shard e TTL consumido pelo tempo.

Para repetir esse cenário com os executáveis do arquivo realmente extraído:

```sh
SIDER_BACKUP_PACKAGE_DIR=/caminho/extraido/sider-vVERSAO-TARGET \
  cargo test --locked --test backup_process -- --ignored --exact extracted_package_backup_roundtrip_under_traffic --nocapture
```

O resultado registra caminhos selecionados e dados observados. A origem no
arquivo distribuído, o SHA e os hashes continuam exigindo evidência separada
de build, empacotamento e extração. O ensaio não emite aprovação de release.
