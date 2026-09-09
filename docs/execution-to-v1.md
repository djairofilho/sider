# Execução até a v1.0

Este plano resume a ordem de trabalho após a implementação do núcleo RESP2 da 0.1.
Os IDs, dependências e critérios completos continuam em
[releases/plan.json](../releases/plan.json). O [ROADMAP](../ROADMAP.md) apresenta
as tarefas; as issues do GitHub registram seu estado operacional.

## Ponto de partida

- Fundação Rust, licença MIT e backlog versionado estão implementados.
- R01-01 a R01-04 foram integradas: fixtures Redis, codec, parser, armazenamento,
  worker e TCP, com limites, prazos e prontidão.
- R01-05 integrou os diferenciais e CLI pelo PR #68.
  A validação local passou nos dois sistemas. R01-GATE publicou a candidata
  [v0.1.0-rc.1](https://github.com/djairofilho/sider/releases/tag/v0.1.0-rc.1).
  As notas e evidências dessa RC preservam o fuzz executado sob a política anterior.
- Ainda não há persistência ou quota. A final continua pendente. A migração para
  ferramentas Rust e a remoção do fuzz alteraram ferramentas e gates, exigindo
  uma nova candidata antes de publicar essas mudanças.
- CI e publicação automática ficam desligadas até a 1.0 inclusive. Reativá-las
  depois disso será uma entrega própria, não um efeito automático da versão.

## Próxima entrega: fechar a 0.1

1. Integrar e validar a remoção do fuzz e os ajustes de ferramentas e documentação.
2. Preparar uma nova candidata da 0.1 com esses ajustes e validar seus gates no
   SHA exato. Preservar as evidências anteriores como histórico, sem transferi-las
   para outro SHA nem aplicar a política nova à final anteriormente preparada.
3. Preparar e revalidar a final a partir da nova candidata, seguindo as conferências
   do [guia de releases](releases.md).
4. Publicar e conferir a final; então fechar o gate e o milestone e iniciar
   `R02-01`, os comandos adicionais de strings.

O [plano técnico da 0.1](../PLANO.md) detalha os contratos de rede e ciclo de vida.

## Sequência das releases

Cada linha depende da conclusão da anterior. Cada versão inclui sua própria
candidata, validação cumulativa e publicação final.

| Versão | Ordem de implementação | Critério central |
| --- | --- | --- |
| `0.1.0` | Nova candidata com os ajustes de ferramentas e gates; validar e publicar a final | Cinco comandos via TCP e CLI, com limites e ordenação testados |
| `0.2.0` | Strings adicionais; opções de `SET`; TTL passivo/ativo; quota | Overflow e expiração corretos; rejeições preservam estado; sem eviction |
| `0.3.0` | AOF versionado/checksum; append/fsync; recuperação; compactação; crashes | Nenhum registro parcial aplicado; durabilidade e substituição de arquivos testadas nos dois sistemas |
| `0.4.0` | Hash estável/hash tags; workers; multichave; AOF; medições | Rejeitar operações entre shards antes de qualquer alteração |
| `0.5.0` | Valores tipados; hashes; listas; sets; integração | `WRONGTYPE`, TTL, quota e recuperação verificados para cada tipo |
| `0.6.0` | Sorted sets; comandos; persistência e limites | Scores, desempate binário, índices negativos e ordenação corretos |
| `0.7.0` | Estado transacional; lotes; `WATCH`; AOF atômico | Um shard; conflitos/expiração invalidam `WATCH`; replay sem meio lote; erros individuais sem rollback |
| `0.8.0` | Assinaturas; publicação; modo assinante; clientes lentos | Filas limitadas, inscrições liberadas e assinantes sem bloquear o banco |
| `0.9.0` | Protocolo Sider→Sider; sincronização completa; incremental; reconexão; promoção | Réplicas somente leitura; TTL/transações preservados; histórico insuficiente exige sincronização completa |
| `0.10.0` | Métricas; diagnóstico; backup/restauração; Docker; ensaios | Restauração reproduzível e imagem privada executada, testada e exportada |
| `1.0.0` | Congelar escopo; auditoria diferencial; carga; migração da 0.10; benchmarks/documentação | Matriz completa, uma hora de carga e nenhuma falha conhecida de corrupção ou perda além das garantias declaradas |

## Ciclo de execução

1. Escolher a próxima issue desbloqueada do milestone atual.
2. Implementar em branch própria, com testes e commits pequenos por responsabilidade.
3. Executar a validação local relevante e abrir PR vinculado à issue.
4. Revisar e integrar por merge commit, registrando resultados e limitações reais.
5. Atualizar compatibilidade, changelog e backlog sem duplicar issues ou apagar
   comentários humanos.
6. Preparar a RC somente quando todas as tarefas funcionais estiverem concluídas.
7. Encerrar o milestone apenas após conferir a release final publicada e seus assets.

Para acelerar, usar testes focados durante a implementação e `cargo xtask check`
uma vez sobre o diff final antes de integrar. Não repetir verificação sem mudança
nos arquivos relevantes. `cargo xtask check --tools` fica reservado a mudanças no
utilitário e no plano. Os ensaios longos são executados quando relevantes à tarefa
ou obrigatórios para publicar, não a cada edição. Não aguardar CI nesta fase.
Separar tarefas independentes em paralelo e conservar os caches Cargo.

## Validação e distribuição

- Banco, testes e ferramentas próprias usam Rust/Cargo. Os helpers Python e os
  workflows arquivados foram removidos. Não há um publicador automático para manter.
- Desde a 0.1: validação nativa em Linux GNU x86_64 (Ubuntu 24.04) e Windows MSVC
  x86_64; teste TCP dos pacotes extraídos e diferenciais.
- Cada candidata exige aprovação dos gates no próprio SHA. A final também é
  recompilada e revalidada no SHA exato do seu merge de release.
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
