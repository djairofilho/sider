# Testes do Sider

Os testes do banco e da referência são escritos em Rust. Python não é necessário
para os comandos deste documento. A CI permanece desligada até a 1.0 inclusive.

## Índice

- [Ciclo local](#ciclo-local)
- [Referência Redis descartável](#referência-redis-descartável)
- [O que as fixtures cobrem](#o-que-as-fixtures-cobrem)
- [Execução registrada](#execução-registrada)
- [Limites desta evidência](#limites-desta-evidência)

## Ciclo local

Na raiz do repositório:

```sh
cargo fmt --check
cargo check --locked --all-targets
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
```

O teste externo aparece explicitamente como `ignored` no ciclo normal. Isso não
significa aprovação da referência. Para concluir R01-01, execute o teste externo
abaixo e confira seu resultado, além dos testes locais.

## Referência Redis descartável

Requisitos: Rust da toolchain fixada, Docker CLI e daemon Linux amd64 ativos.
No Windows, o Docker Desktop em modo Linux é suficiente. Use o mesmo contexto
Docker no pull e no teste. Um daemon instalado dentro do WSL pode ser diferente
do Docker Desktop; imagens de um não ficam automaticamente disponíveis no outro.

Confira o ambiente e faça o pull exato:

```sh
docker version
docker context show
docker pull --platform linux/amd64 redis:8.10.1@sha256:76961cd2a0f40ef6fdd334b6b1b3a76a2bad1848d89f3030ca30a7521d4a9493
cargo test --locked --test reference -- --ignored --nocapture
```

O teste lê versão, imagem, digest e plataforma diretamente de
[`releases/plan.json`](../releases/plan.json). Não aceita um endpoint Redis externo
nem limpa bancos existentes. A infraestrutura:

1. Confere disponibilidade da imagem, digest, plataforma e ID imutável.
2. Cria seu próprio container sem persistência, com porta efêmera publicada
   somente em `127.0.0.1`.
3. Confere imagem usada, versão do servidor e versão do `redis-cli`.
4. Aguarda conectividade com prazo e compara respostas brutas por TCP.
5. Executa separadamente os cinco comandos com `redis-cli -2 --raw`.
6. Remove somente o container criado pelo teste, inclusive em falhas normais de
   setup ou de uma asserção. Não há limpeza global de containers ou volumes.

Docker ou imagem ausentes, versão divergente, timeout e respostas inesperadas
fazem o teste falhar. Nenhum desses casos é convertido em sucesso ou skip.
Comandos Docker e operações de socket têm prazos limitados.

O encerramento forçado do processo de teste pode impedir o cleanup. Nesse caso,
use o ID exato registrado pelo harness para inspecionar o container e confirmar
sua propriedade antes de removê-lo. Não use `docker system prune` para esta tarefa.

## O que as fixtures cobrem

As requisições e respostas em
[`tests/common/resp_fixtures.rs`](../tests/common/resp_fixtures.rs) são literais
Rust de bytes. Nenhum encoder ou decoder do Sider gera a resposta esperada.
Os testes locais conferem os comprimentos das requisições por um leitor separado,
restrito a arrays de bulk strings, antes de consultar a referência.

| Caso | Contrato verificado na referência |
| --- | --- |
| `ping` | Simple string sem argumento; mensagem, vazio e binário como bulk |
| `echo` | Preservar ASCII, vazio, NUL, CRLF e bytes não UTF-8 |
| `strings_and_missing` | Ausente, criação, leitura, sobrescrita por vazio e remoção |
| `empty_key_and_binary_value` | Chave vazia e valor binário |
| `binary_key` | Chave com NUL, CRLF e byte não UTF-8 |
| `del_duplicates` | Contar remoções efetivas; ignorar duplicatas e ausentes |
| `ascii_command_case_and_distinct_keys` | Comandos sem distinguir caixa ASCII; chaves distinguem caixa |
| `arity_errors_preserve_connection_and_state` | Aridade dos cinco comandos, conexão reutilizável e valor preservado |

Cada caso é repetido em pipeline, comparando a concatenação literal das respostas.
Uma chamada `PING` após cada pipeline detecta bytes residuais antes do caso seguinte.
Ao terminar, o cliente fecha sua escrita e exige EOF sem bytes adicionais.
Todos os casos terminam sem suas chaves e podem ser repetidos na mesma instância.

Os contratos de tipos seguem a [especificação RESP oficial](https://redis.io/docs/latest/develop/reference/protocol-spec/).
As aridades e respostas são verificadas executando a versão fixada, não inferidas
somente da documentação.

## Execução registrada

Em 8 de setembro de 2026, o teste externo passou no Windows x86_64, com Rust
1.97.1 e Redis Linux amd64 no Docker Desktop. Foram verificados oito casos,
48 trocas sequenciais, oito pipelines, EOF sem bytes extras e os cinco comandos
via `redis-cli`. Servidor e CLI reportaram 8.10.1; a imagem correspondeu ao digest
fixado acima. A remoção do container foi confirmada pelo harness.

Os testes locais também incluem um processo filho sem Docker no `PATH` para
comprovar que infraestrutura ausente resulta em falha explícita. Esse ambiente
é configurado somente no processo filho, sem alterar o ambiente global dos testes.

## Limites desta evidência

R01-01 comprova a referência e suas fixtures. O Sider ainda não implementa o codec,
comandos ou serviço TCP, portanto esses testes não são uma suíte diferencial
Sider versus Redis. A comparação com o produto será adicionada em R01-05.

Opções de `SET`, comando desconhecido, requisições fora do subconjunto e limites
próprios do Sider não entram como igualdade implícita com Redis. As diferenças
intencionais estão na [matriz de compatibilidade](compatibility.md).

Redis rodando em Linux dentro do Docker não comprova que o binário Sider foi
compilado e testado nativamente em Linux. Esse gate exige execução própria em
Ubuntu 24.04 antes da publicação, além da execução Windows.
