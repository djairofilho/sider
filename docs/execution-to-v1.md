# Execução acelerada até a v1.0

O escopo funcional da v1 permanece completo. Os marcos `R01` a `R10` são internos;
somente `R11` publica candidata e final. A final promove exatamente o SHA e os
arquivos aprovados na candidata. Fuzz permanece removido, e CI e publicação
automática continuam desligadas.

Os IDs, dependências e critérios estão em [releases/plan.json](../releases/plan.json).
O [ROADMAP](../ROADMAP.md) é gerado; as issues registram o estado operacional.
O [guia de releases](releases.md) define os contratos de evidência e publicação.

O PR de preparação identifica o pacote como `1.0.0`. A origem selecionada para
a baseline interna R10 é o SHA `0021d875dde9da6cbbe9b5b84cd640681128e6ea`, ainda
com pacote `0.1.0`. Seus arquivos, hashes e ensaios são registrados separadamente;
a migração e os demais gates da candidata precisam executar no SHA do merge de
preparação. O bump de versão não encerra esses critérios.

## Checkpoints internos

- Preservar IDs, issues e milestones existentes. O campo `publication` distingue
  marcos internos de releases publicáveis.
- Fechar `R01-GATE` a `R10-GATE` após cumprir implementação e validação do marco,
  sem candidata, empacotamento ou publicação como requisito do checkpoint.
- Registrar resultados pelo ID da tarefa e SHA de origem. Não alterar a versão do
  pacote a cada checkpoint nem atribuir resultados históricos a outro SHA.
- Concluir `R01-GATE` com as entregas já verificadas. A candidata histórica
  [v0.1.0-rc.1](https://github.com/djairofilho/sider/releases/tag/v0.1.0-rc.1)
  permanece intacta; não haverá outra candidata ou final da 0.1 neste fluxo.
- Cada checkpoint depende das tarefas locais. `R11-GATE` depende de todos os
  checkpoints internos e das tarefas finais.

## Execução por dependências técnicas

| Etapa | Trabalho | Condição de conclusão |
| --- | --- | --- |
| Fundação | R02: strings, opções de SET, TTL e quota | Semântica, overflow, expiração e rejeições sem mutação comprovados |
| Frente A | R03: AOF; depois R04: shards e integração durável | Replay, compactação, falhas de escrita e operações entre shards verificados |
| Frente B | R05: valores tipados e coleções; depois R06: sorted sets | Comandos, WRONGTYPE, TTL, quota e persistência integrados |
| Frente C | R08: Pub/Sub; depois R07: transações | Clientes lentos isolados; WATCH e execução/persistência atômica dos lotes |
| Integração final | R09: replicação, em paralelo com R10: operação e distribuição | Snapshot, retomada, promoção manual, backup/restauração e pacotes funcionais |
| Estabilização | R11: auditoria, migração, carga e benchmarks | Escopo completo comprovado no build candidato |

A ordem no JSON não cria dependências. O `xtask` valida referências e ciclos,
mas só considera os vínculos técnicos explícitos; não injeta o gate anterior.

Um integrador coordena interfaces e merges; até três agentes trabalham em
worktrees separados. Coleções começam após TTL/quota e sua integração durável
espera o formato e replay do AOF. Pub/Sub pode usar a rede atual. Transações
aguardam roteamento/execução por worker e, para concluir, lotes duráveis.
Replicação precisa de sequência de mutações, snapshots e shards; sua aprovação
cobre todos os tipos e transações.

Métricas e diagnóstico acompanham seus subsistemas. Backup depende de snapshot
consistente; ensaios operacionais completos aguardam a integração. O integrador
concentra contratos compartilhados: arrays/erros, entradas com TTL e contabilidade,
mutações resolvidas, lotes e modos da conexão. Cada abstração tem consumidor real.

Usar PRs por entrega coesa, podendo fechar várias issues relacionadas. Manter
commits pequenos por responsabilidade com implementação e testes necessários
juntos; integrar por merge commit após validação local.

## Validação proporcional

| Momento | Validação |
| --- | --- |
| Durante implementação | Testes focados no comportamento alterado |
| Antes de integrar cada PR do banco | Uma execução de `cargo xtask check` sobre o diff final |
| Ferramentas/plano | `cargo xtask check --tools` |
| Novos comandos ou semântica | Diferenciais da família afetada contra a referência fixada |
| Persistência, shards e transações | Falha, replay, migração e atomicidade afetados; Windows/Linux para filesystem |
| Replicação e operação | Snapshot interrompido, sequências, reconexão, TTL, lotes, backup e restauração |
| Candidata 1.0 | Matriz completa, pacotes extraídos, Docker, migração, soak de 3600 segundos e benchmarks |

Executar uma bateria integrada ao concluir o núcleo durável com shards e
transações. A próxima bateria completa será no build candidato da 1.0. Ensaios
internos chamam as suítes diretamente e registram tarefa/SHA, sem exigir recibos
ou pacotes de release. Implementar runners futuros junto das funcionalidades.

Documentação isolada recebe revisão de texto, links e comandos. Interfaces e
build também exigem `cargo doc --locked --no-deps` e
`cargo build --locked --release`. Preservar caches e não repetir verificações
aprovadas sem mudanças relevantes. Benchmarks rodam sem builds ou carga
concorrentes na mesma máquina. Gate ausente, ignorado ou com falha fica pendente.

## Baseline e publicação da 1.0

1. Congelar uma baseline interna de R10 com executável, dados, configuração,
   formato AOF, SHA e hashes. Ela substitui a exigência de uma 0.10 final publicada
   no ensaio de migração para 1.0.
2. Concluir documentação/notas, fixar o pacote em `1.0.0` e integrar o PR de
   preparação. Construir e validar seu SHA exato.
3. Produzir pacotes e recibos com versão `1.0.0`. O manifesto de artefatos v2 usa
   `artifact_version: "1.0.0"`; tag e status no GitHub identificam `v1.0.0-rc.N`.
4. Publicar a candidata privada, conferir downloads e registrar sua aprovação.
5. Criar a tag final no mesmo SHA e publicar os mesmos arquivos, incluindo
   manifesto, evidências e checksums. Não recompilar, reempacotar ou repetir o soak.
6. Registrar a conferência da promoção fora do conjunto imutável. Qualquer mudança
   nesse conjunto exige outra candidata.

O verificador aceita RC e final para o mesmo build e rejeita divergências de
versão, SHA e hashes. Conferir a origem dos bytes e a aprovação remota continua
parte da publicação manual, não uma autorização inferida do verificador local.

A execução termina quando todas as capacidades estiverem comprovadas, a final
1.0 publicada e seus arquivos conferidos. Repositório e artefatos permanecem
privados; `publish = false`, Rust, RESP2 binário, workers proprietários, canais
limitados e suporte Linux/Windows permanecem obrigatórios.

## Fora do escopo até a 1.0

Redis Cluster, Sentinel, failover automático, resharding online, RESP3, Lua,
operações bloqueantes, transações entre shards, TLS e ACL. A replicação é
assíncrona entre instâncias Sider da mesma versão e configuração de shards,
com promoção manual em ambiente controlado.
