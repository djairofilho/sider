# Codec RESP2

O módulo `sider::resp` representa e codifica os cinco tipos RESP2, sem conhecer
comandos, sockets ou armazenamento. Representar um frame não significa aceitá-lo
como requisição: essa validação pertence ao parser de comandos.

## Índice

- [Tipos e uso](#tipos-e-uso)
- [Limites](#limites)
- [Decodificação incremental](#decodificação-incremental)
- [Codificação atômica](#codificação-atômica)
- [Testes e escopo](#testes-e-escopo)

## Tipos e uso

`Frame` distingue `Simple`, `Error`, `Integer`, `Bulk` e `Array`. Os payloads usam
`Bytes`, inclusive simple strings e erros; não há conversão obrigatória para
UTF-8. `Bulk(None)` e `Array(None)` são nulos, não valores ou arrays vazios.

```rust
use bytes::BytesMut;
use sider::resp::{Decoder, Frame, RespLimits, encode};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let limits = RespLimits::default();
    let mut decoder = Decoder::new(limits)?;
    let mut input = BytesMut::from(&b"$5\r\nhello"[..]);
    assert!(decoder.decode(&mut input)?.is_none());
    assert_eq!(&input[..], b"$5\r\nhello");

    input.extend_from_slice(b"\r\n+OK\r\n");
    let frame = decoder.decode(&mut input)?.expect("frame completo");
    assert!(matches!(frame, Frame::Bulk(Some(_))));
    assert_eq!(&input[..], b"+OK\r\n");

    let mut output = BytesMut::new();
    encode(&frame, &mut output, limits)?;
    assert_eq!(&output[..], b"$5\r\nhello\r\n");
    Ok(())
}
```

Inteiros aceitam sinal `+` ou `-` opcional e dígitos decimais, sem espaços.
O valor precisa caber em `i64`. Comprimentos de bulk/array aceitam dígitos sem
sinal ou exatamente `-1` para nulo. Outros negativos e sinal `+` nesses
comprimentos são inválidos. Zeros à esquerda podem ser normalizados na saída.

Simple strings e erros não aceitam CR ou LF no conteúdo. Bulk strings aceitam
qualquer byte, inclusive CRLF, NUL e sequências inválidas em UTF-8. Os delimitadores
continuam exigindo CRLF exato. O codec não aceita prefixos RESP3 ou comandos inline.
Esses tipos e delimitadores seguem a [especificação RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/).

## Limites

| Campo de `RespLimits` | Padrão | O que conta |
| --- | --- | --- |
| `max_frame_bytes` | 4 MiB | Um frame inteiro, incluindo framing |
| `max_bulk_bytes` | 1 MiB | Um payload bulk |
| `max_line_bytes` | 1 KiB | Linha inteira, incluindo prefixo e CRLF |
| `max_nodes` | 1.024 | Raiz, arrays e todos os seus elementos |
| `max_depth` | 16 | Níveis de arrays; array raiz conta como 1 |

Arrays nulos e vazios também contam como nós e níveis. Um escalar na raiz tem
profundidade zero. Todos os limites devem ser positivos; bulk e linha não podem
exceder o limite do frame. A profundidade configurável tem teto de 128 para
limitar também a destruição recursiva da árvore de `Frame`.

Esses limites não medem o RSS, a capacidade de todas as alocações ou o dataset.
A futura conexão ainda precisa limitar seu buffer, fila, tempo e quantidade de
pedidos. Um buffer com dois frames válidos pode ultrapassar `max_frame_bytes`
no total: o decoder limita o frame atual, não rejeita um sufixo válido por existir.

## Decodificação incremental

`Decoder::new` valida os limites. Uma instância pertence a um único fluxo:

- `Ok(None)`: frame incompleto; bytes do buffer permanecem intactos.
- `Ok(Some(frame))`: consome exatamente um frame; preserva o sufixo e reinicia
  o estado de parsing para o próximo frame.
- `Err(...)`: formato inválido ou limite excedido; o decoder fica inutilizável.
  Não tente recuperar sincronização no mesmo fluxo.

Enquanto um frame estiver incompleto, o chamador só pode acrescentar bytes ao
final do mesmo buffer. Não remova nem altere seu prefixo já entregue. Uma redução
detectável gera `BufferChanged`; não há hash do prefixo para detectar alterações
arbitrárias feitas pelo próprio chamador.

O scanner preserva cursor, pilha de arrays e metadados dos elementos já lidos.
As leituras seguintes não refazem o parsing de todo o prefixo. Comprimentos,
offsets, número de nós e profundidade são validados antes de reservar payloads.
Os payloads são copiados para `Bytes` independentes somente após validar o frame
completo. Uma chave pequena não deve reter o buffer inteiro da conexão.

## Codificação atômica

`encode` primeiro valida toda a árvore e calcula o tamanho com aritmética
verificada. Só então reserva espaço e acrescenta os bytes. Um erro de conteúdo,
configuração ou limite deixa os bytes e a capacidade de `dst` intactos.

O orçamento é do novo frame, não do prefixo existente em `dst`. A camada de rede
deverá impor seu próprio limite de resposta. O encoder não cria uma resposta
parcial para um array cujo último elemento é inválido.

Como no restante do projeto, não há promessa de recuperar falta de memória do
processo. Os erros retornados cobrem entradas e limites, não falha do alocador.

## Testes e escopo

```sh
cargo test --locked --lib resp::
cargo test --locked --test resp_codec
```

Os testes usam frames literais dos cinco tipos, fixtures já verificadas contra
Redis, todos os pontos de fragmentação curtos, entrega byte a byte, sufixos,
limites e entradas inválidas. Propriedades geram árvores válidas e bytes
arbitrários. Um contador de trabalho exclusivo dos testes do decoder detecta
reprocessamento quadrático sem medir tempo de parede.

Isso valida o codec isolado, não o servidor TCP nem a semântica dos comandos.
Os testes de integração e os gates de publicação estão no
[guia de testes](testing.md) e no [guia de releases](releases.md).
