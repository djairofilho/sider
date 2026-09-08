# Executar o Sider

Este pacote contém o servidor Sider, este README e a licença MIT do código próprio.
Confira a versão com `--version`. A release também fornece `SHA256SUMS`,
`release-manifest.json`, notas e evidências de validação.

## Iniciar

Linux x86_64 GNU, compilado e testado em Ubuntu 24.04:

```sh
./sider --version
./sider
```

Windows x86_64 MSVC, no PowerShell:

```powershell
.\sider.exe --version
.\sider.exe
```

O processo permanece em primeiro plano e escuta em `127.0.0.1:6379` por padrão.
Use `Ctrl+C` para encerrar. Para escolher outra porta no Linux:

```sh
SIDER_ADDR=127.0.0.1:6380 ./sider
```

No PowerShell, defina `$env:SIDER_ADDR = '127.0.0.1:6380'` antes de iniciar.
O endereço exige IP literal, não hostname. `--help` mostra a interface do binário.

## Experimentar

Em outro terminal, com `redis-cli` instalado separadamente:

```sh
redis-cli -2 -h 127.0.0.1 -p 6379 PING
redis-cli -2 -h 127.0.0.1 -p 6379 SET exemplo valor
redis-cli -2 -h 127.0.0.1 -p 6379 GET exemplo
redis-cli -2 -h 127.0.0.1 -p 6379 DEL exemplo
```

A versão 0.1 atende `PING [mensagem]`, `ECHO mensagem`, `GET chave`,
`SET chave valor` básico e `DEL chave [chave ...]`, em arrays RESP2 de bulk strings.
Chaves e valores preservam bytes arbitrários. Não há modo inline ou RESP3.
Clientes que enviam comandos de inicialização adicionais podem ser incompatíveis.

## Limites e segurança

- Use somente em ambiente controlado. Não há autenticação, ACL ou TLS.
- Não há persistência, TTL ou quota do dataset na 0.1. Todos os dados são perdidos
  ao terminar o processo; escritas podem esgotar a memória disponível.
- O padrão permite 32 conexões e 32 comandos na fila do worker, payloads de até
  1 MiB e frames/buffers de entrada de até 4 MiB. Isso não limita a memória total.
- Os prazos padrão são 10 segundos para formar um frame, 5 segundos para fila e
  resposta, 5 segundos para escrita e 5 segundos para drenagem no encerramento.
  Conexões ociosas sem frame parcial não têm timeout de ociosidade.
- Depois que um comando entra na fila, perder a conexão ou a resposta não desfaz
  sua execução. Após timeout, o resultado pode ser desconhecido para o cliente.
- Encerramento é best-effort; não existe garantia de drenagem após término forçado.

## Documentação da versão

O [repositório privado](https://github.com/djairofilho/sider) contém os guias de
configuração, rede, compatibilidade e testes em `docs/`. Use a tag indicada nas
notas e no manifesto da release, não a branch `main`, para consultar o mesmo código
do pacote. Os guias completos não estão incluídos neste arquivo compactado.

O pacote não inclui Redis ou redis-cli. As dependências do Sider preservam suas
próprias licenças. A licença MIT não altera a visibilidade privada dos artefatos.
