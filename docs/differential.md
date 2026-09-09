# Diferenciais e gates locais da 0.1

Os testes novos usam Rust/Cargo. A suíte envia os mesmos bytes ao binário Sider
e a um Redis descartável da imagem fixada em `releases/plan.json`. O leitor de
respostas em `tests/common/wire.rs` não importa o codec do produto: distingue
tipos, preserva os bytes completos e limita bytes, linhas, nós e profundidade.

## Cobertura

- Oito fixtures, 48 trocas sequenciais e repetição em pipelines, com respostas
  literais e sentinela para detectar bytes residuais.
- Seis seeds fixas, 256 operações por seed, repetidas sequencialmente e em
  pipelines de 16 comandos. Incluem os cinco comandos, chaves binárias/vazias,
  sobrescritas e duplicatas em `DEL`.
- Observação de todas as chaves após cada sequência; remoção somente das chaves
  do caso e conferência de ausência. Não há `FLUSHALL` nem endpoint Redis externo.
- Payloads de 0, 1, 127, 8.192 e 1.048.576 bytes em `ECHO`, `SET` e `GET`.
- Comparação de bytes e tipos das respostas, incluindo erros de aridade do
  subconjunto, seguida de half-close e EOF sem bytes adicionais.
- Nove chamadas separadas de `redis-cli -2 --raw` nos dois servidores, cobrindo os
  cinco comandos. A saída textual da CLI não substitui o oráculo binário.

Cada execução compara 3.588 respostas binárias. As nove chamadas CLI adicionais
só são contabilizadas no caminho Linux compartilhado. Opções de `SET`, comando
desconhecido e framing fora do subconjunto não são anunciados como equivalentes
ao Redis. Consulte a [matriz de compatibilidade](compatibility.md).

## Ciclo nativo, sem infraestrutura externa

```sh
cargo test --locked --test compatibility --test harness --test gate_contract
cargo clippy --locked --all-targets -- -D warnings
```

Os testes externos são explicitamente ignorados por padrão. Isso não aprova os
gates. Os testes nativos cobrem o leitor independente, o gerador reproduzível,
processos com deadline/saída limitada, prontidão inválida, contexto de release
divergente, recibo antigo e zero casos.

## Comparação binária no Windows

Com Docker Desktop em modo Linux e a imagem fixada disponível:

```powershell
docker --context desktop-linux pull --platform linux/amd64 redis:8.10.1@sha256:76961cd2a0f40ef6fdd334b6b1b3a76a2bad1848d89f3030ca30a7521d4a9493
cargo test --locked --test compatibility -- --ignored --exact sider_matches_redis --nocapture
```

Use o mesmo contexto Docker no pull e no processo de teste. O helper chama a CLI
Docker disponível no ambiente; este exemplo pressupõe `desktop-linux` já como
contexto corrente. Não altera o contexto global. Esse caminho executa o Sider
Windows e a referência Redis Linux, com porta efêmera publicada em loopback.
Ele não executa `redis-cli` contra o Sider nem aprova o gate Linux.

## Linux e redis-cli em rede isolada

`127.0.0.1` dentro de um container não é o host do Docker. Por isso, o Redis usa
`--network container:<runner-id>` e compartilha o loopback do runner Ubuntu.
O Sider usa porta efêmera; o Redis usa 6379. Não há portas publicadas, nem bind
do Sider fora do loopback. Execute uma referência por runner, sem paralelizar
os entrypoints externos. [Rede compartilhada do Docker](https://docs.docker.com/engine/network/#container-networks).

A receita [dev/test.Dockerfile](../dev/test.Dockerfile) fixa Ubuntu 24.04 e Docker
CLI por digest, Rust 1.97.1 e rustup com checksum.
Ela é uma ferramenta local, não a imagem de distribuição prevista para a 0.10.
Os pacotes Ubuntu vêm dos repositórios da distribuição; não há promessa de imagem
bit a bit idêntica em builds feitos em datas diferentes.

Exemplo PowerShell, executado na raiz do repositório:

```powershell
docker --context desktop-linux build --platform linux/amd64 --tag sider-dev:tests --file dev/test.Dockerfile dev
if ($LASTEXITCODE -ne 0) { throw 'Falha no build do ambiente' }

$siderRoot = (Get-Location).Path
$siderRunner = docker --context desktop-linux run --detach --rm --platform linux/amd64 --label dev.sider.purpose=local-tests --mount "type=bind,source=$siderRoot,target=/workspace,readonly" --mount 'type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock' --env DOCKER_HOST=unix:///var/run/docker.sock --env GIT_OPTIONAL_LOCKS=0 --workdir /workspace sider-dev:tests tail -f /dev/null
if ($LASTEXITCODE -ne 0 -or $siderRunner -cnotmatch '^[a-f0-9]{64}$') { throw 'Runner não identificado' }
try {
    docker --context desktop-linux exec --env "SIDER_TEST_RUNNER_CONTAINER=$siderRunner" $siderRunner cargo test --locked --test compatibility -- --ignored --exact sider_matches_redis_and_cli --nocapture
    if ($LASTEXITCODE -ne 0) { throw 'Diferencial/CLI falhou' }
} finally {
    docker --context desktop-linux stop $siderRunner
}
```

O socket Docker dá ao runner acesso administrativo ao daemon. Use somente fontes
e imagem de teste confiáveis, num ambiente de desenvolvimento controlado. Não use
esse comando para executar PRs externos não revisados. Os fontes ficam somente
para leitura; o build fica em `/tmp/sider-target` dentro do container. Um cache
opcional deve ser separado do target Windows e não deve ocultar `/opt/cargo/bin`.

O harness confere imagem, digest, versões Redis/CLI, IDs completos, estado dos
containers e rede antes dos testes. Prontidão do Sider exige versão, PID do filho
vivo, IP/porta e PING literal. No sucesso, confirma o recolhimento do filho e a
remoção do container Redis e dos arquivos temporários próprios. Em falhas normais,
os guards também tentam a limpeza. Matar o executor à força pode impedir o cleanup;
inspecione o ID registrado antes de remover um recurso. Nunca use prune global.

## Recibos de release

Os comandos em [releases/gates.json](../releases/gates.json) usam testes Cargo
explícitos. O processo precisa estar num checkout limpo do SHA escolhido, com
toolchain estável e target Linux GNU nativo. Além do ID do runner, forneça:

| Variável | Conteúdo |
| --- | --- |
| `SIDER_RELEASE_VERSION` | Versão exata do pacote compilado |
| `SIDER_RELEASE_SHA` | HEAD completo, verificado antes e depois da execução |
| `SIDER_RELEASE_TARGET` | `x86_64-unknown-linux-gnu` |
| `SIDER_REFERENCE_IMAGE` | Imagem Redis exata do manifesto |
| `SIDER_RELEASE_DIR` | Diretório absoluto existente para esta execução |

```sh
cargo test --locked --test compatibility -- --ignored --exact release_compatibility_gate --nocapture
```

Os recibos só são publicados após sucesso e cleanup, com casos positivos, duração,
seeds e ferramentas. A escrita usa arquivo temporário e hard link, sem substituir
destino; o filesystem de resultados precisa suportar hard links. Contexto ausente,
checkout alterado, target errado, recibo antigo ou execução filtrada sem casos não
produzem aprovação. Se o diretório estiver dentro do checkout, use `target/`, que
é ignorado pelo Git. Copie os resultados para fora do runner antes de removê-lo.

A candidata 1.0 exige novos recibos no próprio SHA, com versão de build `1.0.0`.
A final promove esses mesmos arquivos; não gera novos recibos. Resultados de uma
branch de trabalho não substituem a validação do build candidato. Ensaios internos
usam as entradas diretas da suíte e registram tarefa/SHA, sem contexto de release.
