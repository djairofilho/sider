# Ensaios operacionais do pacote extraído

O teste opt-in `tests/operational_package.rs::operational_extracted_package`
confere diagnóstico e limites por interfaces públicas dos executáveis
distribuídos. Recebe um diretório extraído, executa casos curtos e grava JSON
com observações reais. Não cria recibo de release nem substitui a verificação
do manifesto e dos hashes do pacote.

## Entrada e execução

O diretório deve conter os quatro executáveis da mesma compilação:
`sider`, `sider-aof-migrate`, `sider-backup` e `sider-replica`, com sufixo
`.exe` no Windows. Todos precisam ser arquivos regulares. O runner calcula
SHA-256, verifica a ajuda de cada CLI e confirma a versão exposta por `sider`
e `sider-backup`. A CLI de migração e a de replicação não oferecem
`--version` nessa interface. Os mesmos hashes são conferidos ao terminar.

Primeiro confira o pacote conforme o [guia de releases](releases.md). Use
`SIDER_OPERATIONAL_PACKAGE_DIR` com o caminho absoluto do diretório extraído
e `SIDER_OPERATIONAL_OUTPUT_DIR` com um caminho absoluto ainda inexistente.
A versão do runner precisa corresponder à versão do binário.

No Linux:

```sh
SIDER_OPERATIONAL_PACKAGE_DIR=/caminho/pacote-extraido \
SIDER_OPERATIONAL_OUTPUT_DIR=/caminho/evidencia-nova \
cargo test --locked --test operational_package -- --ignored --exact operational_extracted_package --nocapture
```

No PowerShell:

```powershell
$env:SIDER_OPERATIONAL_PACKAGE_DIR = 'C:/caminho/pacote-extraido'
$env:SIDER_OPERATIONAL_OUTPUT_DIR = 'C:/caminho/evidencia-nova'
cargo test --locked --test operational_package -- --ignored --exact operational_extracted_package --nocapture
```

O teste usa apenas os executáveis desse diretório. Não consulta
`CARGO_BIN_EXE_sider` para escolher o servidor. O ambiente dos filhos é
limpo de opções `SIDER_*` herdadas e recebe apenas a configuração de cada
cenário. As alterações não atingem o ambiente global do processo de teste.

## Casos observados

| Caso | Condição e resultado exigido |
| --- | --- |
| Configuração e diagnóstico | Com a porta RESP e a porta interna ocupadas, `--diagnose` termina com sucesso sem criar AOF ou prontidão. Uma configuração inválida termina com erro e identifica a opção sem repetir o valor sensível recebido. |
| Quota | Em quatro shards e quota total de 4 KiB, SET grande é recusado com OOM. GET preserva o valor anterior e INFO preserva uso/chaves. DEL libera o consumo; novo SET e PING funcionam. |
| Conexões | Quatro vagas são confirmadas por PING. A conexão excedente fecha e aumenta `rejected_connections`; uma conexão admitida continua consultando INFO. Depois de liberar uma vaga, outra conexão executa PING. |
| Cliente lento | Dois assinantes recebem publicações binárias. Um não drena o socket; o outro confirma cada frame em ordem. A fila limitada remove o lento sem bloquear o saudável, e PING continua funcionando. Após fechar os assinantes, as inscrições chegam a zero. |
| Abertura AOF e recuperação | Um arquivo regular obstrui o diretório pai da AOF. A inicialização termina com erro, preserva esse arquivo e não publica prontidão. Após remover somente a obstrução criada pelo teste, o servidor confirma uma escrita com AOF `always`. Uma segunda instância com a mesma AOF é recusada; a primeira continua atendendo. Após recolher a primeira, a reabertura recupera exatamente o valor binário e a sequência confirmada. |

A carga do cliente lento tem limite de 512 mensagens de 64 KiB e fila de uma
mensagem por assinante. A confirmação do leitor saudável ordena cada publicação;
não há espera fixa usada como prova de processamento. Convergência de métricas,
operações de rede e processos têm prazos limitados.

A obstrução testa um **erro de abertura de caminho**. Ela não simula ENOSPC,
remoção de permissão durante append, falha de escrita em um arquivo já aberto
ou atomicidade sob escrita curta. Esses caminhos continuam nas suítes nativas
de injeção e nos testes de persistência. Não interprete esse ensaio como
evidência de falha de disco cheio.

O teste de baseline cobre migração, backup/restauração, tipos, TTL, réplica e
parada cooperativa onde suportada. Esses cenários não são repetidos aqui.
Este runner recolhe apenas os PIDs de sua propriedade; a reabertura AOF ocorre
após uma escrita confirmada em política `always`. Esse recolhimento não
comprova shutdown cooperativo no Windows.

## Evidências e limites

Cada caso concluído é gravado em `observed-cases.jsonl`. O relatório final
`operational-report.json` reúne plataforma, versão, duração, hashes das quatro
CLIs, códigos de saída, diagnósticos e contagens observadas. Somente aparece
quando todos os casos passam e os executáveis permanecem iguais. Uma falha
preserva os casos anteriores e os diretórios de dados para inspeção, sem
publicar sucesso.

A saída exige diretório novo. Os dados e as evidências são preservados; o teste
não remove uma fonte de dados existente. As únicas remoções de arquivo da
carga atingem a obstrução criada pelo próprio cenário. O helper de processo
remove sua prontidão e seus arquivos temporários próprios ao recolher o filho.

Para conferir a compilação sem executar processos do ensaio:

```sh
cargo clippy --locked --test operational_package -- -D warnings
```

Uma execução com binários de desenvolvimento serve para depurar o runner.
A implementação passou nesse modo no Windows, com os cinco casos, incluindo
a recusa de uma conexão excedente, remoção do assinante lento e erro de abertura
de caminho AOF. Isso não é evidência de distribuição.
A evidência operacional de distribuição exige repetir o teste com os pacotes
realmente extraídos de Windows e Linux, depois de conferir sua identidade.
Uma entrada ignorada na suíte nativa não equivale a aprovação.
