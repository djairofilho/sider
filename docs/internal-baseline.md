# Baseline interna R10 e migração para a 1.0

A baseline R10 é um conjunto privado congelado por SHA e hashes, com pacote
`0.1.0`, dados e backups reais. O congelamento ocorre depois da integração do
runner, em checkout limpo, antes de alterar a versão para `1.0.0`. Não cria tag,
release ou recibo de publicação. Prepare um conjunto por plataforma suportada:
Windows MSVC x86_64 e Linux GNU x86_64.

## Preparar o pacote e registrar o build

No SHA que será congelado, execute `cargo build --locked --release --bins` e
empacote os quatro executáveis com README, licença e avisos, conforme o
[contrato dos pacotes](packages.md). Guarde a saída real do build, seu código de
término e os hashes observados. O runner de testes deve ser compilado no perfil
normal; não sobrescreva os quatro arquivos de `target/release` durante a coleta.

Imediatamente depois do build e do empacotamento, produza um sidecar JSON UTF-8.
Este contrato também serve ao pacote candidato, com sua versão e SHA próprios:

| Campo | Valor exigido |
| --- | --- |
| `schema_version` | `1` |
| `source_sha` | SHA completo, 40 hexadecimais minúsculos, do checkout limpo |
| `target` | `x86_64-pc-windows-msvc` ou `x86_64-unknown-linux-gnu` |
| `version` | `0.1.0` na origem; versão da candidata na migração |
| `compiler` | stdout completo de `rustc --version --verbose`, incluindo LF final |
| `command` | `cargo` |
| `args` | `["build","--locked","--release","--bins"]` |
| `exit_code` | `0`, observado no build |
| `source_clean_before`, `source_clean_after` | `true`, observado antes e depois do build |
| `binaries` | Quatro objetos `{ "path", "bytes", "sha256" }`, ordenados por `path` |
| `archive` | Objeto `{ "bytes", "sha256" }` do ZIP ou tar.gz produzido |

Os nomes em `binaries` são `sider-aof-migrate`, `sider-backup`, `sider-replica`
e `sider`, com `.exe` no Windows. Cada hash tem 64 hexadecimais minúsculos e
cada tamanho é medido em bytes. `args` também aceita `"--target", "TARGET"`
ao final, quando o build real usou esse argumento. Não há campos extras.

O sidecar é um registro observacional do operador. Ele vincula as declarações
aos arquivos conferidos; não autentica quem produziu o build. Preserve os logs
e o hash externo de `baseline.json` fora do diretório congelado.

## Congelar sem reabrir os dados originais

Defina caminhos absolutos. O diretório de saída deve ser novo e ter pai existente:

| Variável | Entrada |
| --- | --- |
| `SIDER_BASELINE_PACKAGE` | Arquivo ZIP ou tar.gz realmente produzido |
| `SIDER_BASELINE_BUILD_DIR` | Diretório com os quatro executáveis originais do build |
| `SIDER_BASELINE_PROVENANCE` | Sidecar de build descrito acima |
| `SIDER_INTERNAL_BASELINE_DIR` | Diretório novo para o conjunto congelado |
| `SIDER_BASELINE_LONG_TTL_MS` | Opcional: 604800000 ms, sete dias, por padrão |

O TTL longo aceita de um a 365 dias. Escolha um prazo que ainda esteja vivo na
validação da candidata. O TTL curto é de 60 segundos e precisa continuar vivo
ao concluir a parada do processo.

```sh
cargo test --locked --test persistence -- --ignored --exact freeze_internal_baseline --nocapture
```

O runner verifica checkout, toolchain, sidecar, pacote e hashes dos quatro
binários originais e extraídos. A extração limita nomes, inventário e bytes,
grava cada membro em um arquivo novo e não materializa links do arquivo
compactado. Cada servidor e cada CLI usado no ensaio vem desse pacote extraído.

Os cenários usam um e quatro shards, roteamento versão 1, quota total de 4 MiB,
registros AOF de até 65536 bytes, `always` e compactação automática desligada.
Cada shard recebe strings, hashes, listas, sets e sorted sets com bytes binários.
EXEC contém uma operação com WRONGTYPE e confirma a escrita posterior do lote.
São conferidos dados, ordem, membros e representação de scores extremos.

Depois dessas comparações, o runner semeia o TTL curto, exporta o backup, confere
os vencimentos absolutos e para o processo. No Linux, envia SIGTERM ao filho e
exige saída bem-sucedida e remoção dos arquivos de prontidão. No Windows, registra
explicitamente o término forçado do filho após o flush do backup e a política
`always`; isso não representa parada cooperativa por sinal do console.

O backup é verificado e restaurado em outro diretório, aberto por outro processo
do pacote R10. Os dados originais parados não voltam a ser abertos pelo servidor.
O manifesto só é salvo depois dessas verificações e da nova conferência da
identidade do checkout e dos arquivos do build.

O JSON de sucesso informa o caminho, o SHA da origem e `manifest_sha256`.
Guarde esse hash externamente. O conjunto contém:

- `baseline.json`, schema 1, task `R10`, identidade e inventário completo;
- `build-provenance.json`, arquivo do pacote e seus quatro executáveis extraídos;
- `datasets/shards-1` e `datasets/shards-4`, fechados na origem;
- `backups/shards-1` e `backups/shards-4`, produzidos pela CLI real.

Cada cenário registra configuração, tags por shard, digest das respostas,
vencimentos Unix, formato AOF, papel, época, sequência, método e horário da
parada. Arquivos extras, ausentes, alterados, links e caminhos não portáteis
fazem a validação falhar. Uma execução interrompida pode deixar um diretório
parcial; ele não é uma baseline concluída sem manifesto e hash externo válidos.

## Atualizar usando cópias e conferir a candidata

O gate usa a baseline da mesma plataforma, com TTL curto já expirado e TTL longo
ainda vivo. Preserve o conjunto original. Defina:

| Variável | Entrada |
| --- | --- |
| `SIDER_INTERNAL_BASELINE_DIR` | Conjunto R10 congelado |
| `SIDER_INTERNAL_BASELINE_SHA256` | Hash externo de `baseline.json` |
| `SIDER_MIGRATION_PACKAGE` | Arquivo realmente empacotado da candidata |
| `SIDER_MIGRATION_BUILD_DIR` | Quatro executáveis originais do build candidato |
| `SIDER_MIGRATION_PROVENANCE` | Sidecar desse build candidato |
| `SIDER_MIGRATION_OUTPUT_DIR` | Diretório novo, fora da baseline, para o ensaio |

Com o contexto `GateContext(migration)` do [build candidato](releases.md):

```sh
cargo test --locked --test persistence -- --ignored --exact release_migration_gate --nocapture
```

O gate chama diretamente os casos existentes de formato inicial, versão
desconhecida e migração tipada, além dos dois cenários R10. Só publica recibo
após sucesso completo e validação do contexto de publicação.

Para cada layout, o runner abre uma cópia dos dados com o executável novo e
compara os cinco tipos, os efeitos de EXEC, o TTL longo absoluto e a ausência
do TTL curto vencido durante a parada. Inicia uma réplica vazia da mesma versão
nova, confere snapshot, delta e recusa de escrita. Também produz e restaura um
backup da candidata. Cópias separadas com corrupção ou quantidade incompatível
de shards precisam ser recusadas sem prontidão nem alteração dos arquivos.

A rota de restauração do backup antigo usa a **CLI congelada 0.1.0** para criar
um diretório novo. Só depois esse diretório é aberto pelo **servidor candidato**.
`sider-backup` exige a mesma versão do manifesto do backup; não use a CLI nova
diretamente sobre o backup antigo. Essa regra preserva o contrato da ferramenta.
Se for preciso voltar à origem, restaure outra cópia com a CLI e o servidor
antigos, mantenha o mesmo layout e confira os dados antes de redirecionar clientes.
Escritas posteriores ao ponto do backup não fazem parte dessa recuperação.

Ao final, o runner verifica novamente todo o inventário congelado e registra os
hashes do pacote e da proveniência da candidata. Não testa nem promete replicação
entre versões distintas. Estes casos usam pacotes e processos reais, mas não
substituem os demais [ensaios operacionais](metrics.md) ou o gate de soak.

## Ensaiar o runner antes do congelamento oficial

Com pacote, build e proveniência `0.1.0` do checkout limpo, defina as três entradas
`SIDER_BASELINE_PACKAGE`, `SIDER_BASELINE_BUILD_DIR` e `SIDER_BASELINE_PROVENANCE`:

```sh
cargo test --locked --test persistence -- --ignored --exact rehearse_internal_baseline_migration --nocapture
```

Esse ensaio cria dados temporários com TTL curto de 30 segundos e longo de uma
hora, espera o vencimento real e executa a migração para a mesma versão do pacote.
O relatório identifica `same_version_short_rehearsal_not_frozen_baseline`.
Não emite recibo, não guarda uma baseline oficial e não prova atualização para
1.0. A evidência 0.1.0 para 1.0 vem da execução posterior do gate candidato.

Os testes nativos do inventário e da proveniência usam arquivos declarados como
fixtures artificiais; eles validam recusa de contratos e nunca são contados como
evidência de execução de pacotes reais.
