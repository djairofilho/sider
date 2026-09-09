# Migração offline do AOF

O particionamento pertence à configuração durável do diretório. A recuperação
confere a quantidade de shards e a versão do roteamento antes de reparar uma
cauda ou aceitar dados. Trocar `SIDER_SHARDS` não redistribui um AOF existente.

O cabeçalho v1 da R03 representa um shard. Novos diretórios e compactações usam
v2, que registra quantidade de shards e roteamento. A versão de roteamento 1 usa
FNV-1a64 e as mesmas hash tags do servidor. As versões dos registros e das
mutações não mudam nessa transição.

## Executar a migração

Pare o servidor de origem e escolha um diretório de destino que ainda não exista.
O pai desse diretório precisa existir, e o destino deve ficar fora da árvore da
origem. A origem deve conter seu `writer.lock`, criado pelo servidor. O migrador
adquire esse lock e recusa a operação se outro escritor ainda estiver ativo.

```sh
cargo build --locked --release --bin sider-aof-migrate
target/release/sider-aof-migrate \
  --source ./data-r03 --source-shards 1 --source-routing 1 \
  --destination ./data-r04 --shards 4 --routing 1
```

No PowerShell:

```powershell
& .\target\release\sider-aof-migrate.exe `
  --source 'C:\dados\sider-r03' --source-shards 1 --source-routing 1 `
  --destination 'C:\dados\sider-r04' --shards 4 --routing 1
```

As seis opções de diretório e identidade são obrigatórias. Caminhos preservam a
representação nativa do sistema. O parser é injetável e não altera o ambiente.
`cargo run --locked` continua iniciando o servidor; para executar o migrador pelo
Cargo, use `cargo run --locked --bin sider-aof-migrate -- ...`.

| Opção adicional | Padrão | Uso |
| --- | --- | --- |
| `--source-max-dataset-bytes` | `67108864` | Quota usada para recuperar a origem |
| `--max-dataset-bytes` | `67108864` | Quota total do destino |
| `--source-max-record-bytes` | `67108864` | Limite de registro aceito na origem |
| `--max-record-bytes` | `67108864` | Limite de registro escrito no destino |

A quota do destino é dividida de forma fixa: `total / shards`, mais um byte para
cada índice menor que `total % shards`. A migração recusa uma distribuição com
shard sobrecarregado, mesmo que o dataset caiba na soma. Nesse caso, escolha uma
quota que comporte a distribuição ou ajuste os dados antes de tentar novamente.

## O que é preservado

O migrador recupera a origem somente para leitura, sem truncar nem compactar seus
arquivos. Valores, chaves binárias, deadlines Unix e a última sequência completa
são preservados. Valores já expirados no instante da migração ficam ausentes.
Um par de relógios congelado torna a leitura e a validação do snapshot consistentes.

Uma cauda incompleta depois de um selo válido é ignorada no snapshot de destino,
mas permanece intacta na origem e aparece no relatório. Corrupção, versão
desconhecida, configuração divergente ou lote inválido interrompem a operação.

O snapshot é roteado pela implementação real do servidor. A contabilidade usa
um `Store` temporário com no máximo uma entrada, compartilhando o valor imutável;
não constrói um segundo dataset completo. Depois de conferir as quotas, o migrador
reserva o diretório novo, escreve um snapshot e selo completos e sincroniza o
arquivo. Reabre o temporário, confere cabeçalho, cada registro e fim de arquivo,
e só então publica a geração zero.

Se a escrita falhar, a limpeza retira somente os arquivos criados por essa operação.
O destino existente nunca é sobrescrito e a origem permanece utilizável. A publicação
e a sincronização obedecem às [garantias por plataforma](persistence.md#garantias-por-plataforma).

O CLI retorna JSON com formato de origem, sequência, quantidade de entradas,
identidades de origem e destino, uso por shard e bytes de cauda incompleta. Guarde
esse relatório com a configuração e os hashes dos diretórios. O migrador não troca
a configuração do servidor nem começa a atender conexões.

Após conferir o resultado, inicie o servidor com o novo diretório e a quantidade
de shards correspondente. A migração não modifica nomes de chaves: operações
multichave que antes usavam um único worker podem ser recusadas como `CROSSSHARD`
na nova distribuição. Hash tags permitem manter grupos de chaves juntos.

Para voltar à configuração anterior, pare o novo servidor e use a origem preservada
com seu binário e configuração compatíveis. Escritas feitas somente no destino
depois da migração não estarão nessa origem. O binário antigo da R03 não entende
cabeçalhos v2.

## API e verificação

`AofConfig.layout` define `DurableLayout { shard_count, routing_version }`.
`recover` entrega `Recovered.metadata` com identidade, sequência, versão do
cabeçalho, uso por shard e extensão válida/incompleta. O integrador pode distribuir
o snapshot pelos workers somente depois dessas verificações.

`migration::migrate_offline(MigrationOptions, Arc<dyn Clock>)` recebe configurações
e quotas separadas para origem e destino. `migration::options_from_args` é o
parser puro usado pelo CLI.

```sh
cargo test --locked --test aof_migration
cargo test --locked --test persistence
cargo test --locked --lib persistence
```

Os casos incluem v1/v2, truncamento e corrupção do cabeçalho, incompatibilidade de
configuração, lote entre shards, quota local, preservação de TTL e sequência,
lock de origem ativa, recusa de sobrescrita, limpeza após erro e CLI real.

A baseline interna R03 foi congelada no SHA
`c7b148abeff43fd354d29b1bbce36aea43ea8f3f`, com binário, AOF de strings/MSET/TTL,
configuração, logs e hashes. A migração real desse diretório de um para quatro
shards foi conferida com o migrador no SHA
`4739d596f8d80d0038a9a97838395c90be60ceff`: três entradas, sequência dois e hash
da origem idêntico antes e depois. Os artefatos locais ficam em
`target/baselines/r03-<SHA>` e `target/baselines/r04-migration-<SHA>`.
Esses registros são baselines internas, sem identidade ou publicação de release.
