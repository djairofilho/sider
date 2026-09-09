# Pacotes locais e smoke do executável extraído

Antes de publicar a candidata 1.0, compile e teste o SHA exato do merge de
preparação no sistema nativo. Use diretórios novos para staging, arquivo compactado
e extração. RC e final usam os mesmos pacotes, já identificados como `1.0.0`.
A promoção confere os mesmos bytes; não recompila nem reempacota.

## Conteúdo

Cada arquivo contém um diretório `sider-vVERSAO-TARGET/` com:

- `sider` no Linux GNU ou `sider.exe` no Windows MSVC;
- `sider-aof-migrate` no Linux ou `sider-aof-migrate.exe` no Windows, para
  [migração offline explícita](aof-migration.md);
- `sider-backup` no Linux ou `sider-backup.exe` no Windows, para
  [exportar, verificar e restaurar backups](backup.md);
- `sider-replica` no Linux ou `sider-replica.exe` no Windows, para consultar
  estado e executar promoção manual pelo listener interno;
- `README.md`, copiado de [releases/README.md](../releases/README.md);
- `LICENSE`, copiado integralmente da raiz do checkout;
- `licenses/`, cópia integral de [releases/licenses/](../releases/licenses/),
  com avisos upstream e inventário de arquivos/hashes.

Use `.tar.gz` para `x86_64-unknown-linux-gnu` e `.zip` para
`x86_64-pc-windows-msvc`. A compilação Linux ocorre em Ubuntu 24.04. O README
de distribuição ensina a executar o binário sem exigir Cargo ou o checkout.

Antes de empacotar, confira os hashes e tamanhos de todos os arquivos do inventário
de avisos. Depois de extrair, compare também a árvore completa de `licenses/` com
o staging e o checkout. Reavalie a coleção quando dependências de produção ou a
toolchain mudarem; ela não é uma declaração sobre licenças de dependências futuras.
Compile ambos com `cargo build --locked --release --bins`. Preserve os dois
executáveis Linux como `0755` e documentos como `0644`.

## Smoke manual

O teste Rust `extracted_package_runs_version_and_tcp` recebe `SIDER_PACKAGE_DIR`
com o caminho absoluto do diretório realmente extraído. Ele não extrai o arquivo,
não seleciona o binário de build e não produz aprovação de release sozinho.
O operador deve comprovar extração, SHA, target e hashes na evidência da publicação.
O smoke exige os quatro executáveis, README e licença, e executa também `--help`
do migrador e da CLI de replicação, além de `--version` da CLI de backup.
Os avisos são conferidos separadamente
pelo inventário e pela comparação de todos os arquivos extraídos.

```sh
SIDER_PACKAGE_DIR=/caminho/extraido/sider-vVERSAO-TARGET cargo test --locked --test package -- --ignored --exact extracted_package_runs_version_and_tcp --nocapture
```

No PowerShell, defina `SIDER_PACKAGE_DIR` para o diretório extraído antes de chamar
o mesmo comando Cargo. Preserve e restaure o valor anterior ao terminar a sessão
de validação. O teste lê o ambiente sem alterá-lo.

O smoke exige arquivos regulares, README e licença corretos, versão compilada
exata, processo próprio vivo e prontidão com PID/porta conferidos. Executa TCP
literal, com dados binários, pipeline, leitura após remoção e EOF sem resposta
residual. Confirma o recolhimento do filho e só então imprime seu resultado JSON.
Ausência de configuração, arquivo inválido, timeout ou resposta divergente falha.

O ciclo padrão testa as validações do harness, mas ignora explicitamente a entrada
externa. Testar uma cópia do binário de desenvolvimento não conta como smoke do
pacote extraído. Execute a entrada opt-in nos dois sistemas para cada build
candidato. A final reutiliza essa evidência somente para o mesmo SHA e arquivos.

## Evidência e publicação

Registre os comandos de build, empacotamento, extração e smoke; a saída e o código
de término; hashes dos arquivos e do pacote; sistema, compilador e SHA do checkout.
Inclua essas evidências no manifesto local e anexe os logs junto da release privada.

O [procedimento de release](releases.md) acrescenta os gates cumulativos, candidata,
downloads de conferência e fechamento do milestone somente após a final publicada.

A [imagem Docker](docker.md) copia esses mesmos executáveis Linux, confere os
hashes e testa o arquivo exportado após `docker load`. Não recompila o produto
durante o build da imagem nem altera o formato do manifesto schema 2.

## Ensaio inicial registrado

Em 8 de setembro de 2026, no commit `9768b1f`, o ensaio criou um ZIP Windows e um
tar.gz Linux, extraiu cada um em outro diretório e passou pelo smoke opt-in com
um teste aprovado e nenhum ignorado por sistema. Os hashes dos executáveis
extraídos coincidiram com os builds originais; README e LICENSE foram conferidos.
Esses ensaios ainda tinham somente os três arquivos principais, antes da inclusão
de `licenses/`, e não são gates de release. Os artefatos foram preservados localmente
em `target/package-rehearsal-9768b1f/` e `target/package-rehearsal-linux-9768b1f/`.

A suíte nativa ampliada passou com 241 testes no Windows e 243 no Linux, mais um
doctest em cada sistema. Sete entradas opt-in permanecem explicitamente ignoradas
na execução comum. Formatação, check, Clippy, documentação e build também passaram.
Uma execução Windows anterior falhou ao abrir um listener com erro `10055`.
O evento System/Tcpip `4231`, às 01:40:49 locais, registrou esgotamento do pool
global de portas efêmeras; não identifica o processo causador. Após a recuperação
observada dos recursos, o caso isolado, os 11 testes TCP e a suíte completa passaram
sem alterações de código ou supressão de cenários. A tentativa com falha não foi
usada como aprovação de release.
