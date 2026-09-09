# Requisitos de execução

Estas instruções gerais se aplicam ao servidor `sider` e às CLIs
`sider-aof-migrate`, `sider-backup` e `sider-replica`. Rust e Cargo não são
necessários para executar os pacotes. Redis e `redis-cli` não acompanham o pacote;
um cliente RESP2 é necessário apenas para interagir com o servidor.

## Ambientes suportados

Os ensaios de desenvolvimento usam os seguintes ambientes nativos:

| Sistema | Arquitetura e target do pacote |
| --- | --- |
| Windows 11 x64 | `x86_64-pc-windows-msvc` |
| Ubuntu 24.04 x86_64 GNU | `x86_64-unknown-linux-gnu` |

Esses ambientes definem o suporte testado, sem declarar uma versão mínima de
Windows ou glibc por inferência dos cabeçalhos do executável. Não há declaração
de suporte a Windows anteriores, outras distribuições Linux, musl ou ARM64.

## Windows

Os executáveis MSVC dependem de `VCRUNTIME140.dll`, do Universal CRT e de APIs
do Windows. É necessário ter o
[Microsoft Visual C++ v14 Redistributable x64](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist)
compatível com a toolchain do build e manter os componentes do Windows atualizados.
As DLLs da Microsoft não são incluídas no ZIP do Sider.

Os ensaios registrados ocorreram em Windows com o runtime já instalado. Isso
não representa validação em uma instalação limpa do sistema. A versão do runtime
observada em uma máquina de teste não estabelece, sozinha, a versão mínima exigida.

## Linux GNU

Os executáveis são vinculados dinamicamente. O ambiente precisa oferecer o loader
GNU para x86_64, glibc e as demais bibliotecas importadas pelo build, incluindo
`libgcc_s.so.1` e, quando importada, `libm.so.6`. O pacote não inclui essas
bibliotecas do sistema e não é um pacote estático ou destinado a musl.

O ambiente de build e teste é Ubuntu 24.04. A maior versão de símbolo `GLIBC_*`
encontrada na inspeção não comprova suporte a todas as distribuições que tenham
essa versão de glibc; loader, demais bibliotecas e APIs também participam da execução.

## Evidência de cada candidata

Este texto é a fonte versionada de `runtime-requirements.md` no conjunto de assets.
Ele não identifica nem aprova um build. O manifesto e as evidências da candidata
devem registrar o SHA exato, a toolchain, o sistema e os hashes dos quatro
executáveis realmente extraídos dos pacotes.

Antes de publicar, inspecione arquitetura e DLLs importadas dos quatro executáveis
Windows com `objdump -f`/`objdump -p` ou ferramenta equivalente. No Linux, confira
arquitetura, loader, bibliotecas e versões de símbolos com `readelf -h -l -d -V`,
e a resolução das dependências dos binários próprios com `ldd`.

Preserve as saídas por executável e o ambiente observado. Execute `--version`
do servidor e do backup, `--help` do migrador e da CLI de réplica e o smoke do
pacote nas duas plataformas. Resultados de builds anteriores não substituem
essas verificações. A promoção da candidata para a final preserva os mesmos
assets, requisitos e evidências, sem recompilar os executáveis.
