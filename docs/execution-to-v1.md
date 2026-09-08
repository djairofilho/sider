# Execução até a v1.0

Este plano resume a ordem de trabalho após a implementação do worker e TCP R01-04.
Os IDs, dependências e critérios completos continuam em
[releases/plan.json](../releases/plan.json). O [ROADMAP](../ROADMAP.md) apresenta
as tarefas; as issues do GitHub registram seu estado operacional.

## Ponto de partida

- Fundação Rust, licença MIT e backlog versionado estão implementados.
- R01-01 a R01-03 foram integradas: fixtures Redis, codec, parser e armazenamento.
- R01-04 conecta o núcleo ao worker e TCP, com limites, prazos e prontidão.
  A integração desta entrega passa pelo PR da branch `feat/server-worker-tcp`.
- Ainda não há persistência, quota ou release funcional publicada.
- CI e publicação automática ficam desligadas até a 1.0 inclusive. Reativá-las
  depois disso será uma entrega própria, não um efeito automático da versão.

## Próxima entrega: fechar a 0.1

1. Validar e integrar R01-04 após revisão, registrando testes e limitações reais.
2. Implementar R01-05: comparação Sider versus Redis 8.10.1, uso de `redis-cli`,
   corpus de fuzz e runners dos gates da capacidade, todos em Rust.
3. Executar R01-GATE: validar o SHA da candidata, testar pacotes extraídos nos dois
   sistemas e publicar `v0.1.0-rc.1` como prerelease privada.
4. Corrigir falhas com outra RC quando houver mudança funcional. Depois da
   candidata aprovada, recompilar e revalidar a final `v0.1.0`.

O [plano técnico da 0.1](../PLANO.md) detalha os contratos de rede e ciclo de vida.

## Sequência das releases

Cada linha depende da conclusão da anterior. Cada versão inclui sua própria
candidata, validação cumulativa e publicação final.

| Versão | Ordem de implementação | Critério central |
| --- | --- | --- |
| `0.1.0` | Finalizar worker/TCP; diferenciais e fuzz | Cinco comandos via TCP e CLI, com limites e ordenação testados |
| `0.2.0` | Strings adicionais; opções de `SET`; TTL passivo/ativo; quota | Overflow e expiração corretos; rejeições preservam estado; sem eviction |
| `0.3.0` | AOF versionado/checksum; append/fsync; recuperação; compactação; crashes | Nenhum registro parcial aplicado; durabilidade e substituição de arquivos testadas nos dois sistemas |
| `0.4.0` | Hash estável/hash tags; workers; multichave; AOF; medições | Rejeitar operações entre shards antes de qualquer alteração |
| `0.5.0` | Valores tipados; hashes; listas; sets; integração | `WRONGTYPE`, TTL, quota e recuperação verificados para cada tipo |
| `0.6.0` | Sorted sets; comandos; persistência e limites | Scores, desempate binário, índices negativos e ordenação corretos |
| `0.7.0` | Estado transacional; lotes; `WATCH`; AOF atômico | Um shard por transação; conflitos/expiração invalidam `WATCH`; replay sem meio lote |
| `0.8.0` | Assinaturas; publicação; modo assinante; clientes lentos | Filas limitadas, inscrições liberadas e assinantes sem bloquear o banco |
| `0.9.0` | Protocolo Sider→Sider; sincronização completa; incremental; reconexão; promoção | Réplicas somente leitura, sincronização consistente, TTL e transações preservados |
| `0.10.0` | Métricas; diagnóstico; backup/restauração; Docker; ensaios | Restauração reproduzível e imagem privada executada, testada e exportada |
| `1.0.0` | Congelar escopo; auditoria diferencial; carga; migração; benchmarks/documentação | Matriz completa, uma hora de carga e nenhuma falha conhecida de corrupção ou perda além das garantias declaradas |

## Ciclo de execução

1. Escolher a próxima issue desbloqueada do milestone atual.
2. Implementar em branch própria, com testes e commits pequenos por responsabilidade.
3. Executar a validação local relevante e abrir PR vinculado à issue.
4. Revisar e integrar por merge commit, registrando resultados e limitações reais.
5. Atualizar compatibilidade, changelog e backlog sem duplicar issues ou apagar
   comentários humanos.
6. Preparar a RC somente quando todas as tarefas funcionais estiverem concluídas.
7. Encerrar o milestone apenas após conferir a release final publicada e seus assets.

Para acelerar o ciclo, executar os testes focados durante a implementação e a
suíte local antes de integrar. Os ensaios longos continuam obrigatórios antes de
publicar, mas não precisam rodar a cada edição. Não aguardar CI nesta fase.

## Validação e distribuição

- Banco e testes novos usam Rust/Cargo. Os helpers Python já existentes são
  opcionais para backlog/releases e não fazem parte do ciclo de testes do banco.
- Desde a 0.1: validação nativa em Linux GNU x86_64 (Ubuntu 24.04) e Windows MSVC
  x86_64; teste TCP dos pacotes extraídos, diferenciais e fuzz.
- Cada candidata exige pelo menos 15 minutos de fuzz sem falhas novas. A final
  também é recompilada e revalidada, incluindo fuzz.
- Desde a 0.3: crashes, recuperação e migração nos dois sistemas. Cada capacidade
  posterior acrescenta seus testes aos gates anteriores.
- Na 1.0: carga contínua por uma hora e benchmarks com throughput, p50/p95/p99,
  memória, pipelines, hot keys e diferentes quantidades de shards.
- Publicação manual do SHA exato do merge de release, com notas, manifesto,
  checksums e licença MIT nos pacotes. RC nunca é latest.
- Repositório e artefatos continuam privados; `publish = false` permanece ativo.
  Desde a 0.10, a imagem Docker é exportada como asset privado.
- Correções posteriores usam patch com candidata. Novas capacidades usam minor;
  incompatibilidades anteriores à 1.0 ficam nas minors e aparecem nas notas.

Não publicar se qualquer gate obrigatório estiver ausente, ignorado, cancelado
ou com falha. O [guia de releases](releases.md) define o procedimento completo.

## Fora do escopo até a 1.0

Redis Cluster, Sentinel, failover automático, resharding online, RESP3, Lua,
operações bloqueantes, transações entre shards, TLS e ACL. O ambiente suportado
permanece controlado; a replicação é assíncrona entre instâncias Sider da mesma
versão e configuração de shards, com promoção manual.
