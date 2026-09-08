# Compatibilidade com Redis

Nenhum comando Redis está implementado no bootstrap do Sider. O binário valida
configuração, mas ainda não oferece serviço TCP. Esta matriz registra o alvo da
versão 0.1 e será atualizada conforme os testes produzirem evidências.

## Matriz da versão 0.1

| Forma do comando | Comportamento alvo | Implementação | Verificação contra Redis |
| --- | --- | --- | --- |
| `PING` | Responder com simple string `PONG` | Não implementada | Pendente |
| `PING mensagem` | Devolver a mensagem como bulk string | Não implementada | Pendente |
| `ECHO mensagem` | Devolver exatamente os bytes da mensagem | Não implementada | Pendente |
| `GET chave` | Devolver bulk string ou bulk string nula se ausente | Não implementada | Pendente |
| `SET chave valor` | Criar ou substituir; responder com simple string `OK` | Não implementada | Pendente |
| `DEL chave [chave ...]` | Contar apenas as chaves efetivamente removidas | Não implementada | Pendente |

A versão exata do Redis de referência, a versão do `redis-cli` e o digest da
imagem de teste ainda precisam ser selecionados e registrados. Não há comparação
diferencial executada nem suporte verificado com `redis-cli` neste estágio.

## Subconjunto alvo

O Sider aceitará requisições RESP2 formadas por arrays não vazios de bulk strings
não nulas. Os nomes dos comandos serão comparados sem distinguir maiúsculas de
minúsculas ASCII. Chaves e valores serão binários, sem exigir UTF-8.

O codec representará os cinco tipos RESP2, mas isso não significará que todos os
tipos serão aceitos como argumentos de comandos. Ausência de chave, valor vazio,
array nulo e array vazio terão representações distintas.

Frames fragmentados e comandos concatenados serão tratados desde a versão 0.1.
Cada conexão processará os comandos em sequência, com um único pedido em voo.

## Limitações e divergências planejadas

| Área | Contrato alvo da versão 0.1 |
| --- | --- |
| Opções de `SET` | Aceitar somente `SET chave valor`; argumentos adicionais produzirão `ERR unsupported SET options`, sem alterar estado |
| Comando desconhecido | Responder `ERR unknown command`, com texto simplificado que não reproduz os argumentos |
| Aridade dos comandos suportados | Produzir resposta compatível com a versão de Redis selecionada, após verificação |
| Formato de requisição inválido | Rejeitar e fechar a conexão; sem promessa de equivalência com Redis fora do subconjunto declarado |
| Protocolo | RESP2; sem RESP3 e sem comandos inline |
| Banco lógico | Somente o banco padrão; sem `SELECT` |
| Handshake e autenticação | Sem `AUTH`, `HELLO`, `COMMAND` ou `CLIENT`; clientes que exigem esses comandos não estarão cobertos |
| Tipos de dados | Somente chaves e valores binários; sem listas, hashes, sets ou sorted sets |
| Expiração e contagem | Sem TTL e sem `EXISTS` na versão 0.1 |
| Persistência e replicação | Sem AOF, snapshots, replicação ou Redis Cluster |
| Memória do dataset | Sem quota ou eviction; limites de rede não limitam o tamanho do banco |
| Uso operacional | Protótipo para desenvolvimento local e testes, com endereço padrão em loopback |

Os limites de entrada e os prazos propostos estão no
[plano de implementação](../PLANO.md#limites-e-ciclo-de-vida). Eles serão limites
próprios do Sider e não uma reprodução dos valores padrão do Redis.

## Evidência necessária

Para declarar uma forma de comando verificada, registre o teste reproduzível e
a versão de referência. A suíte diferencial deverá comparar respostas brutas e
estado observado usando instâncias descartáveis. Sua leitura das respostas não
poderá depender apenas do codec sob teste.

Inclua casos com bytes não UTF-8, chaves e valores vazios, chaves ausentes,
sobrescrita e chaves repetidas em `DEL`. Verifique também caixa dos comandos,
aridade, rejeição sem mutação e continuidade após erros recuperáveis.

O teste com `redis-cli` será uma evidência de integração separada: sua saída
textual não substitui a comparação dos bytes no protocolo. Uma ferramenta ausente
ou um teste ignorado deve permanecer registrado como pendente.
