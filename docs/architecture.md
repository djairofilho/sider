# Arquitetura do Sider

O Sider começa como uma única crate Rust, com biblioteca testável e um binário
pequeno. O [plano de implementação](../PLANO.md) define os contratos completos da
versão 0.1; este documento resume as fronteiras e identifica o que já existe.

## Implementado

| Parte | Responsabilidade atual |
| --- | --- |
| Biblioteca | Expor a configuração reutilizável pelo binário e pelos testes |
| Codec RESP2 | Representar, validar, codificar e decodificar frames com limites |
| Configuração | Ler e validar `SIDER_ADDR`, com padrão `127.0.0.1:6379` |
| Erros de configuração | Representar falhas de validação com tipos explícitos |
| Binário | Processar ajuda e versão; validar a configuração na execução normal |
| Ferramentas de desenvolvimento | Fixar toolchain e dependências; verificar formatação, lint, testes e build |

A execução normal informa que o servidor TCP ainda não foi implementado. Não há
listener, parser de comandos, mapa de dados ou tarefas assíncronas neste estágio.
As dependências de produção são `thiserror`, para erros tipados, e `bytes`, para
buffers e payloads binários. O código próprio usa `#![forbid(unsafe_code)]`.

`ServerConfig` contém `bind_addr: SocketAddr`. `from_env` lê o ambiente do
processo; `from_lookup` permite fornecer valores explícitos nos testes, sem alterar
o ambiente global. O binário trata argumentos desconhecidos e configuração
inválida como falhas, com código de saída diferente de zero.

## Fronteiras planejadas para a versão 0.1

| Componente | Conhece | Não precisa conhecer |
| --- | --- | --- |
| Codec RESP2 | Frames, bytes e limites do protocolo | Sockets, comandos e armazenamento |
| Parser de comandos | Frames e contratos dos comandos | I/O e estado do banco |
| Armazenamento | Comandos tipados e mapa de chaves e valores | RESP e tarefas de rede |
| Worker | Armazenamento, fila de pedidos e respostas | Framing e buffers de conexão |
| Conexão | Buffer, codec, parser, handle do worker e socket | Acesso direto ao mapa |
| Servidor | Listener, configuração, tarefas e encerramento | Detalhes de cada comando |

Além do codec, esses componentes serão criados nas respectivas etapas. A estrutura não
antecipa módulos vazios nem várias crates sem consumidores independentes.

### Propriedade do armazenamento

Um único worker possuirá o `HashMap` e executará cada comando de forma síncrona.
Conexões enviarão pedidos por um canal `mpsc` limitado e receberão a resposta por
um canal `oneshot`. Tokio e as dependências de observabilidade serão adicionados
quando esses componentes forem implementados. `bytes` já é usado pelo codec.

A fila define a ordem de execução entre conexões. Cada conexão aguardará sua
resposta antes de despachar o próximo comando. Assim, a versão 0.1 manterá um
pedido em voo por cliente e preservará a ordem dos comandos concatenados daquele
cliente, sem prometer justiça estrita entre clientes.

### Protocolo e dados binários

O decoder é incremental: sua entrada pode conter um fragmento, um frame completo
ou vários frames. Ele consome exatamente um frame completo, preserva o sufixo do
buffer e mantém estado limitado enquanto aguarda mais bytes.

O codec representa os cinco tipos RESP2. O parser aceitará como requisição apenas
arrays não vazios de bulk strings não nulas. Chaves e valores preservarão os bytes,
inclusive conteúdo vazio e sequências que não sejam UTF-8.

Payloads de entrada são copiados para alocações independentes após validação completa.
Isso evita que uma chave pequena retenha um buffer grande de rede. A decisão poderá
ser revista com medições. O [guia do codec](resp.md) detalha contratos e limites.

### Limites e ciclo de vida

Os limites planejados abrangem conexões, fila, tamanho dos frames, buffers,
profundidade, número de elementos e prazos de operação. A configuração deve ser
validada antes de abrir o listener. Os valores propostos estão no
[plano](../PLANO.md#limites-e-ciclo-de-vida); somente o endereço pertence à
configuração implementada no bootstrap.

O envio concluído à fila será a fronteira de aceitação. Depois dele, o worker
executará o comando mesmo se o cliente desconectar. Perder a resposta poderá
deixar o resultado desconhecido para o cliente; não haverá repetição automática.

O encerramento deverá parar novas conexões e pedidos, aguardar o trabalho aceito
por um prazo limitado e supervisionar as tarefas. Esses contratos ainda precisam
de implementação e testes.

## Evolução

A versão 0.1 terá apenas dados em memória e um worker. TTL, quota do dataset e
`EXISTS` estão previstos para a 0.2; AOF para a 0.3; múltiplos shards para a 0.4.
Essas versões exigem decisões adicionais de semântica, durabilidade e ordenação.

A divisão em várias crates e otimizações de cópia, alocação ou hashing dependerão
de necessidades concretas e medições. Não há resultados de desempenho publicados
neste estágio.
