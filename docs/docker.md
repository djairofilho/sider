# Imagem privada de distribuição

A imagem Linux amd64 contém os mesmos executáveis `sider` e `sider-aof-migrate`
do pacote Linux validado. O [Dockerfile](../deploy/Dockerfile) copia os binários;
não executa Cargo nem instala pacotes. A base Ubuntu 24.04 é fixada por digest,
compatível com o ambiente usado para compilar o pacote GNU.

O processo usa UID/GID `10001:10001`, é o PID 1 e recebe `SIGTERM` diretamente.
O diretório `/var/lib/sider` pertence a esse usuário e é o volume do AOF. A
imagem usa endereço `0.0.0.0:6379`, um shard e sincronização `always` por padrão.
O número de shards persistido precisa coincidir com os próximos inícios.

## Entrada e identidade

Siga o [guia de pacotes](packages.md) e preserve evidências do SHA limpo, build,
hashes, empacotamento, extração e smoke. A imagem recebe o diretório **extraído**
com os dois executáveis, `README.md`, `LICENSE` e a árvore `licenses/`.

As labels OCI registram versão e SHA de origem; `io.sider.binary.sha256` e
`io.sider.migrator.sha256` registram os hashes dos executáveis. O build confere
hashes, `--version` e ajuda do migrador. SHA é uma declaração do empacotador,
comprovada pelo procedimento de build; o runner não deduz o commit dos bytes
de um executável sem essa informação embutida.

Os avisos do produto ficam em `/usr/share/doc/sider`. A base mantém os avisos
de seus próprios pacotes em `/usr/share/doc`. Reavalie a base, bibliotecas e
avisos antes da publicação quando esses componentes mudarem.

## Produzir e testar o arquivo

O runner Rust exige Linux GNU x86_64, daemon Docker Linux, CLI `docker`, `gzip`
e `sha256sum`. Não exige Buildx. O daemon pode precisar obter a base pública
fixada no primeiro build; as instruções de build não usam rede. O contexto
contém somente o pacote e o Dockerfile, sem checkout ou credenciais.

```sh
export SIDER_DOCKER_PACKAGE_DIR=/dados/extraido/sider-v1.0.0-x86_64-unknown-linux-gnu
export SIDER_DOCKER_OUTPUT_DIR=/dados/ensaio-docker-novo
export SIDER_DOCKER_SOURCE_SHA=SHA_COMPLETO_DO_BUILD
export SIDER_DOCKER_BINARY_SHA256=SHA256_REGISTRADO_DO_SIDER
export SIDER_DOCKER_MIGRATOR_SHA256=SHA256_REGISTRADO_DO_MIGRADOR
cargo test --locked --test docker_distribution -- --ignored --exact exported_image_runs_after_load --nocapture
```

A saída precisa ser um diretório novo, com pai existente. O runner confere
os binários, recusa links e constrói a imagem. Executa `docker save`, compacta
com `gzip -n -6`, remove sua tag temporária e carrega o `.tar.gz`. Compara Image
ID, plataforma, usuário, sinal, labels e hashes dentro da imagem recarregada.

Os oito cenários verificam exportação/recarga, hashes, versão/migrador, UID/PID 1,
TCP binário, AOF/TTL em volume reutilizado por outro contêiner, SIGTERM e
configuração inválida. O rootfs é somente leitura e o contêiner não recebe
capabilities adicionais. O teste confirma término normal e remove apenas seus
contêineres, volume e tag. Imagem exportada, contexto e logs permanecem na saída.

`docker-report.json` contém Image ID, tag, hashes, tamanho e cenários;
`commands.json` preserva argumentos, saídas, duração e código dos processos.
Falha ou timeout não produz relatório de sucesso. Entradas opt-in ignoradas
na suíte comum não contam como evidência do ensaio.

Em host Linux, o teste publica uma porta efêmera somente em `127.0.0.1`.
Se o runner também for um contêiner, configure `SIDER_DOCKER_RUNNER_ID` com
seu ID completo. O teste exige runner Linux ativo, rede privada e nenhuma porta
publicada. Usa `--network container:ID`, endereço `127.0.0.1:0` e arquivo de
prontidão com PID 1 para descobrir a porta. Esse modo registra a topologia
privada; não comprova encaminhamento de portas do host Docker Desktop.

O runner em contêiner precisa da CLI e do socket Docker. O socket dá controle
do daemon de testes e não deve ser montado na imagem do produto. A CLI envia
o contexto; seus caminhos internos não precisam existir no host do daemon.

## Manifesto da 1.0

O verificador schema 2 já exige `sider-v1.0.0-linux-amd64-image.tar.gz`. Copie
somente esse arquivo para a raiz dos assets. Inclua o objeto `artifact` do
relatório em `release-manifest.json.artifacts`, com nome, tamanho e SHA-256,
e a linha correspondente em `SHA256SUMS`. Relatório e logs entram no ZIP de
evidências e em `evidence_files`. Contexto e staging ficam fora da raiz dos assets.

Para o gate, use `release_docker_gate` com o contexto de [releases.md](releases.md).
Ele exige SHA e versão do build congelado, executa o mesmo ensaio e publica
o recibo após os oito casos. O pacote Cargo precisa estar na versão `1.0.0`.
RC e final usam o mesmo arquivo exportado, sem reconstruir, alterar a tag
interna, recomprimir ou executar outro `docker save` durante a promoção.
O procedimento é reproduzível, mas não promete bytes idênticos entre builds
independentes do daemon. Não há push de imagem, registry público ou CI automática.

## Executar o arquivo recebido

Confira `SHA256SUMS`, carregue o arquivo e use a tag registrada no relatório:
o comando Compose usa o exemplo do checkout, que também pode ser copiado
isoladamente para `deploy/compose.yaml`.

```sh
docker load --input sider-v1.0.0-linux-amd64-image.tar.gz
export SIDER_IMAGE=TAG_LOCAL_EXATA_DO_RELATORIO
docker image inspect "$SIDER_IMAGE"
docker run --rm --network none "$SIDER_IMAGE" --version
docker compose -f deploy/compose.yaml up -d
docker compose -f deploy/compose.yaml logs sider
docker compose -f deploy/compose.yaml stop
```

O [exemplo Compose](../deploy/compose.yaml) nunca busca imagem no registry,
publica somente em loopback e mantém o volume `sider-data`. Permite ajustar
porta, quota, shards e política AOF. Em volume inicializado, mudar shards
exige [migração offline](aof-migration.md). Bind mounts precisam de escrita
para UID/GID 10001; o processo não eleva privilégios para alterar diretórios.

`stop_grace_period: 10s` permite a drenagem normal configurada em cinco segundos.
I/O bloqueante pode ultrapassar o prazo do servidor; ao vencer o prazo Docker,
o daemon pode enviar `SIGKILL`, que não comprova drenagem. Preserve o volume ao
recriar o serviço. Backup e restauração seguem procedimentos próprios; copiar
um AOF ativo sem um ponto consistente não é backup validado.

## Ensaio interno registrado

O checkout limpo `676f4f978df00383794104f8b3f3f66441362cbf`, ainda com versão
Cargo `0.1.0`, passou nos oito cenários em 57,76 segundos no Linux Ubuntu 24.04
de `sider-dev:tests`, com Docker Desktop e namespace privado do runner. O smoke
do pacote Linux extraído também passou. Não foi emitido recibo de release.

O arquivo tem 30.701.303 bytes e SHA-256
`fa9467a983ad6da27bda783fb48e4d2291af5a8f60e25b4d9ebc02b405515017`.
Pacote, imagem, contexto e logs estão preservados em
`target/baselines/r10-docker-676f4f978df00383794104f8b3f3f66441362cbf/`.
Tentativas anteriores que identificaram ausência de Buildx e diferença de
namespace continuam separadas e não contam como aprovação.
