# Ensaio prolongado da candidata

O runner `tests/soak.rs::release_soak_gate` executa pelo menos 3600 segundos
de carga em processos Linux reais do pacote extraído. Confere o executável
contra o membro do `.tar.gz` e o manifesto preliminar antes e depois do ensaio.
O recibo só é publicado após verificar os invariantes e recolher os processos.

## Carga e limites declarados

São oito conjuntos de chaves binárias, distribuídos por hash tags em quatro
shards, com seed `0x511e1103`. Cada iteração confirma um EXEC de oito comandos:
duas strings pareadas, hash, lista, set e sorted set. O modelo independente
mantém o valor esperado de cada conjunto e verifica bytes, cardinalidade,
ordem da lista e score. A réplica deve convergir e preservar o mesmo estado.

A carga é limitada a 20 iterações por segundo, com quota de 4 MiB por instância,
AOF `always` e compactação a partir de 128 KiB. Isso é a configuração deste
ensaio de duração, sem promessa de throughput do produto. A quota mede memória
lógica do dataset; o RSS é lido separadamente em `/proc/PID/status`. O envelope
do ensaio é 512 MiB de RSS por processo, incluindo runtime e buffers.

A cada segundo o runner registra RSS, filas, dataset, expiração, AOF e estado
de replicação em `soak-samples.jsonl`. Filas não podem exceder sua capacidade,
o dataset não pode ultrapassar a quota e falhas de worker/AOF são bloqueantes.
Os resultados incluem configuração, duração monotônica, contagens e estado
final. A amostragem não promete capturar picos entre observações.

## Falhas e progresso

A cada minuto são verificados WATCH abortado, expiração real de TTL, reconexão
RESP e isolamento de um assinante lento. O assinante saudável precisa receber
128 mensagens de 64 KiB em ordem; o lento deve ser expulso pela fila limitada.

A cada cinco minutos a réplica é interrompida e reiniciada. Alternadamente,
o primário também é interrompido, recupera os dados confirmados e inicia uma
nova época. O relatório exige observar tanto CONTINUE quanto FULL. Há escritas
durante a indisponibilidade da réplica e verificação de todos os tipos após
recuperação. Os cortes acontecem entre lotes confirmados; interrupções dentro
de append, sync e publicação pertencem aos testes de falha e ao gate `crash`.

Os diretórios de dados e os registros do ensaio são preservados em uma saída
nova. Nenhum diretório existente é sobrescrito. Uma falha interrompe o teste
sem recibo de sucesso; as amostras já gravadas continuam disponíveis.

## Execução

Prepare o contexto de [releases](releases.md), com manifesto preliminar e
pacote Linux na raiz de `SIDER_RELEASE_DIR`. `SIDER_PACKAGE_DIR` aponta para
o diretório realmente extraído. Execute explicitamente:

```sh
cargo test --locked --test soak -- --ignored --exact release_soak_gate --nocapture
```

O runner cria `SIDER_RELEASE_DIR/soak`; esse caminho precisa estar ausente.
O recibo inclui os hashes dos registros, além da identidade do pacote. Os
arquivos entram no conjunto de evidências da candidata. A final promove
exatamente esse conjunto, sem repetir a hora de carga.

Para testar o próprio runner durante desenvolvimento, existe um ensaio de
25 segundos com eventos acelerados, sem recibo de release:

```sh
SIDER_SOAK_BINARY=/caminho/absoluto/sider \
SIDER_SOAK_OUTPUT_DIR=/caminho/absoluto/saida-nova \
cargo test --locked --test soak -- --ignored --exact internal_soak_rehearsal --nocapture
```

Esse ensaio curto exige o binário integrado com replicação e métricas. Ele
não aprova a duração, o pacote ou o gate da candidata. Uma entrada ignorada
na suíte comum também não é evidência de aprovação.
