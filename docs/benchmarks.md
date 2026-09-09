# Benchmarks do pacote candidato

O gate `benchmarks` executa o binário Linux GNU x86_64 extraído do pacote
candidato e produz medições reproduzíveis de loopback. Não estabelece
superioridade sobre Redis nem um compromisso de throughput ou latência.

A carga só começa quando o operador define `SIDER_BENCH_IDLE_MACHINE=1`.
Compile primeiro e reserve a máquina para o ensaio. A variável registra essa
declaração; o runner não consegue provar ausência de outros processos.
Não rode builds, testes, soak ou outra carga simultaneamente.

## Identidade da entrada

O [helper de entrada](../tests/common/release_input.rs) é compartilhável com
outros gates que executam o pacote. Ele exige checkout limpo no SHA declarado,
versão Cargo e artefato `1.0.0`, target nativo e política de publicação privada
do [contrato de release](releases.md).

Antes e depois do ensaio, o helper confere:

- Identidade do manifesto schema 2: versão, SHA, target e proveniência do checkout.
- Nome, tamanho e SHA-256 do tar.gz conforme o manifesto.
- Igualdade integral entre o executável extraído e o membro único correspondente
  do tar.gz, com limite de tamanho e rejeição de nomes de membros ambíguos.
- README e licença extraídos iguais aos arquivos do checkout.
- SHA-256 e tamanho do executável, preservados até o fim da medição.

A comparação lê o membro com [GNU tar --to-stdout](https://www.gnu.org/software/tar/manual/html_node/Writing-to-Standard-Output.html);
não extrai outros membros durante o gate. GNU tar e `sha256sum` precisam estar
disponíveis. O diretório de entrada deve permanecer sob controle do operador.
Hashes conferem identidade e integridade, sem autenticar a autoria dos arquivos.

O manifesto pode ser preliminar, ainda sem todos os recibos. O gate não exige
aprovação final antecipada, pois seu próprio recibo fará parte dessa aprovação.
O pacote selecionado e seus bytes já precisam estar fixos. Após todos os gates,
o bundle é congelado; candidata e final reutilizam os mesmos arquivos.

## Matriz e metodologia

O [runner](../tests/shard_benchmark.rs) executa 16 cenários em ordem fixa:

| Dimensão | Valores |
| --- | --- |
| Shards | 1 e 4 |
| Chaves | Uma chave compartilhada ou 256 chaves por cliente |
| Pipeline | 1 e 16 comandos por lote |
| Persistência | Desativada ou AOF `always`, compactação automática desativada |

Cada cenário tem três repetições, cada uma com processo e dataset novos.
Quatro conexões executam 128 operações de aquecimento por cliente e depois
2.048 operações medidas por cliente. A fase medida só começa após todos
concluírem o aquecimento. O recibo conta **393.216 operações medidas**;
as 24.576 operações de aquecimento são registradas separadamente.

O gerador usa seed `0x52404005 XOR client_id`, multiplicador LCG
`6364136223846793005`, incremento `1` e aritmética modular de 64 bits.
A fase medida continua a sequência usada no aquecimento. Pedidos são
pré-calculados; cada resposta INCR precisa ser um inteiro positivo. Ao terminar,
GET verifica a contagem de todas as chaves, somando aquecimento e medição.

O throughput divide as operações medidas pelo intervalo entre o primeiro
cliente iniciar e o último terminar. A latência é o **RTT do lote inteiro**:
da escrita do lote à leitura de todas as suas respostas. Inclui o trabalho do
cliente para configurar prazos, escrever e decodificar respostas. Pipeline 16
não divide esse intervalo por 16 para inventar latências individuais.
Os percentis p50/p95/p99 usam nearest rank nas amostras reais de RTT.

Uma thread lê `/proc/PID/status` durante a fase medida, com intervalo solicitado
de 1 ms, e converte VmRSS de KiB para bytes. O arquivo bruto preserva os tempos
observados; o agendamento do sistema pode alongar o intervalo. Só contam
amostras dentro da janela medida. Ausência de amostras faz o gate falhar.
O máximo observado não é garantia do pico real de memória.

As três taxas por cenário produzem mínimo, mediana, máximo, média, desvio
padrão populacional e coeficiente de variação. São estatísticas descritivas;
não há intervalo de confiança, descarte automático de outliers, limiar de
superioridade ou comparação de velocidade com Redis. O gate recebe a imagem
Redis fixada apenas como parte do contexto comum, sem iniciar Redis.

## Execução manual

Use o checkout congelado de publicação 1.0, com os pacotes já preparados.
Marcos internos não satisfazem o contrato desse gate. O diretório de evidências
deve conter o tar.gz e o manifesto preliminar, sem recibo ou arquivo bruto
anterior do benchmark.

Configure `SIDER_RELEASE_VERSION=1.0.0`, `SIDER_RELEASE_SHA`,
`SIDER_RELEASE_TARGET=x86_64-unknown-linux-gnu`, `SIDER_REFERENCE_IMAGE` com
o valor exato de `releases/plan.json` e `SIDER_RELEASE_DIR` com o caminho
absoluto das evidências. `SIDER_PACKAGE_DIR` aponta para o diretório absoluto
extraído que contém `sider`, `README.md` e `LICENSE`.

Compile antes de reservar a máquina:

```sh
cargo test --locked --release --test shard_benchmark --no-run
```

Quando as demais cargas tiverem terminado, execute:

```sh
SIDER_BENCH_IDLE_MACHINE=1 cargo test --locked --release --test shard_benchmark -- --ignored --exact release_benchmarks_gate --nocapture
```

A execução usa o target Cargo já compilado. Mudanças que exijam recompilação
pedem uma nova preparação antes da reserva. O executável do servidor vem sempre
de `SIDER_PACKAGE_DIR`; o runner não usa `CARGO_BIN_EXE_sider` como entrada.

O resultado separado `benchmarks-samples.json` contém hardware, sistema,
toolchain, load average antes/depois, configuração efetiva de cada repetição,
seeds e todas as amostras ordenadas de RTT e RSS. O diagnóstico de configuração
é coletado antes de iniciar o servidor; a prontidão e a porta efêmera acrescentadas
pelo helper são registradas como dados de runtime. O limite desse arquivo é
64 MiB. O recibo registra o hash e o tamanho do arquivo bruto, os hashes do
pacote/binário, resumos por repetição e variação por cenário.

Timeout, resposta inválida, estado final divergente, falha de observação ou
entrada alterada impedem a publicação do recibo. Uma execução interrompida não
é aprovada. Preserve o material incompleto para diagnóstico em outro diretório;
não sobrescreva evidências aprovadas para repetir o gate.

## Verificação durante implementação

```sh
cargo test --locked --test shard_benchmark benchmark_contract -- --nocapture
cargo test --locked --test shard_benchmark release_input -- --nocapture
cargo clippy --locked --test shard_benchmark -- -D warnings
```

Esses testes conferem matriz, contagens, cálculo dos percentis, dispersão,
unidades RSS e rejeições de identidade da entrada. Eles compilam o runner sem
iniciar a medição. A implementação foi validada dessa forma no Windows; o
ensaio completo Linux do pacote permanece reservado ao build candidato em
máquina livre.

A [medição exploratória R04-05](sharding.md#medição-exploratória-r04-05)
permanece como evidência histórica do SHA registrado. Ela usava outro volume,
não tinha aquecimento/repetições e não satisfaz este gate.

