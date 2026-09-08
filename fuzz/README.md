# Fuzz do codec RESP2

O alvo `resp_decoder` exercita o codec sem sockets ou armazenamento. Ele usa
libFuzzer com AddressSanitizer em Linux x86_64. O workspace é separado: instalar
o fuzzer ou compilar com nightly não é requisito para desenvolver o banco.

## Contratos exercitados

- Mesmos frames, consumo e erro ao entregar bytes de uma vez, byte a byte ou em
  fragmentos de tamanho variável.
- Nenhuma alteração do buffer em frame incompleto ou erro; repetir a chamada sem
  novos bytes é idempotente.
- Consumo de exatamente um frame completo, preservando o sufixo concatenado.
- Decoder permanentemente encerrado após erro, inclusive redução indevida de um
  buffer incompleto.
- Encoder e novo decoder preservam cada frame válido, incluindo bytes não UTF-8,
  valores nulos, arrays, inteiros e normalização de cabeçalhos.
- Bytes arbitrários também viram payloads de frames válidos gerados pelo harness,
  para exercitar materialização mesmo quando a entrada bruta não forma RESP.

As entradas têm até 4.096 bytes. Dois conjuntos de limites pequenos exercitam
fronteiras de frame, bulk, linha, nós e profundidade. Os
[27 seeds textuais](corpus-seeds.json) incluem frames válidos, truncados,
concatenados, conteúdo binário, comprimentos inválidos e limites excedidos.
O teste Rust converte o hexadecimal em corpus binário; não há script Python.

## Ferramentas fixadas

| Ferramenta | Versão |
| --- | --- |
| Rust do banco e testes comuns | Definida em `../rust-toolchain.toml` |
| Rust do alvo instrumentado | `nightly-2026-09-07` |
| cargo-fuzz | `0.13.2` |
| libfuzzer-sys | `0.4.13`, fixada no manifesto e lockfile próprios |
| Plataforma do gate | `x86_64-unknown-linux-gnu` |

O [guia oficial do cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz/guide.html)
descreve os alvos instrumentados. A versão da dependência foi conferida na
[documentação de libfuzzer-sys 0.4.13](https://docs.rs/libfuzzer-sys/0.4.13/libfuzzer_sys/).
Use um compilador C++ disponível no ambiente; o gate registra `clang++ --version`.

```sh
rustup toolchain install nightly-2026-09-07 --profile minimal --component rust-src
cargo install cargo-fuzz --version 0.13.2 --locked
```

## Ciclo curto sem recibo de release

Na raiz do repositório, os seeds e os contratos do harness também rodam com Rust
estável, sem instalar libFuzzer:

```sh
cargo test --locked --test fuzz_gate
cargo test --locked --test fuzz_gate -- --ignored --exact prepare_fuzz_corpus --nocapture
```

O segundo comando prepara os seeds em `target/sider-fuzz/corpus`, ou abaixo de
`CARGO_TARGET_DIR/sider-fuzz/corpus` quando essa variável estiver definida.
Ele não executa fuzz nem gera um recibo. Um seed existente com bytes diferentes
provoca erro; o comando não substitui evidência anterior.

Para compilar e executar uma amostra curta em um shell Linux:

```sh
FUZZ_BUILD="${CARGO_TARGET_DIR:-target}/sider-fuzz"
cargo +nightly-2026-09-07 metadata --locked --format-version 1 --manifest-path fuzz/Cargo.toml
cargo +nightly-2026-09-07 fuzz build resp_decoder --target x86_64-unknown-linux-gnu --sanitizer address --debug-assertions --target-dir "$FUZZ_BUILD"
"$FUZZ_BUILD/x86_64-unknown-linux-gnu/release/resp_decoder" "$FUZZ_BUILD/corpus" -runs=10000 -seed=1397310533 -max_len=4096 -timeout=5 -rss_limit_mb=2048 -print_final_stats=1
```

`cargo-fuzz 0.13.2` não oferece `--locked` em `build`. O gate executa primeiro
`cargo metadata --locked` e confere os dois lockfiles byte a byte após o build.
Ao preparar uma versão, atualize também a versão local de `sider` em
`fuzz/Cargo.lock`, sem mudar dependências involuntariamente.

## Gate obrigatório de publicação

Depois de preparar o checkout limpo e as variáveis de release descritas no
[guia de releases](../docs/releases.md#gates-do-produto), execute:

```sh
cargo test --locked --test fuzz_gate -- --ignored --exact release_fuzz_gate --nocapture
```

O gate exige Linux x86_64, versão e SHA correspondentes ao checkout limpo,
referência Redis fixada e um diretório de resultados novo. O build instrumentado
acontece antes do período medido. Pré-compile para que download e compilação não
consumam o prazo global de 1.200 segundos.

O próprio executável libFuzzer roda com seed `1397310533`,
`-max_total_time=901`, `-max_len=4096`, `-timeout=5`, `-rss_limit_mb=2048` e
`-print_final_stats=1`. Os parâmetros de tempo e estatísticas seguem a
[documentação oficial do libFuzzer](https://llvm.org/docs/LibFuzzer.html#options).

O teste confere duração real de pelo menos 900 segundos, duração informada pelo
fuzzer, execuções positivas, cobertura positiva, seed e resumo final coerentes.
Erro do processo, saída excessiva, timeout, panic ou diagnóstico de sanitizer
impedem o recibo. Um processo que termina cedo não é compensado com espera.

Cada execução usa um subdiretório exclusivo de `SIDER_RELEASE_DIR`. Corpus e
artefatos já gravados são preservados. stdout e stderr completos são salvos quando
o processo termina e a captura conclui dentro dos limites de tempo e de 1 MiB por
stream. Timeout, saída excessiva ou falha de captura podem impedir esses logs
completos; nesse caso, o teste grava um diagnóstico explícito da falha.
Somente após todas as verificações o teste publica `receipt-fuzz.json`, com
ferramentas, parâmetros e estatísticas.
Ele não cria release, tag ou qualquer publicação no GitHub.

Uma execução curta, um teste ignorado ou um fuzz executado em worktree modificada
pode ajudar no desenvolvimento, mas não substitui esse gate. Candidata e final
exigem novas evidências no SHA exato correspondente.

## Falhas e regressões

Preserve o input que causou a falha e os logs. Reproduza com o mesmo executável
instrumentado, passando o arquivo como argumento. Corrija a causa e acrescente
uma regressão determinística à suíte nativa antes de repetir o período completo.
Não apague corpus ou artefatos de uma execução usada como evidência.

Fuzz sem falhas não prova ausência de bugs. O alvo cobre framing e contratos do
codec; compatibilidade de comandos e comportamento TCP têm testes separados.
