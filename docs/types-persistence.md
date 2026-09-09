# Persistência de coleções e sorted sets

O teste `typed_migration_from_frozen_r04_binary_output_preserves_shards_and_elapsed_ttl`
lê `tests/fixtures/aof-r04-four-shards.hex`, cópia exata do AOF produzido pelo
migrador `4739d596f8d80d0038a9a97838395c90be60ceff` a partir da baseline real R03
`c7b148abeff43fd354d29b1bbce36aea43ea8f3f`. SHA256 dos bytes:
`b6be7a45ad5e57eb7957d10136afddec6488c60f7aa59bb1c5522388ba4a246f`.
Com relógio injetado, confere quatro shards, sequência dois, dados antigos e TTL;
acrescenta cada tipo, compacta e recupera após o deadline. É evidência do arquivo
congelado e do leitor atual; não atribui os resultados ao executável histórico.

Hashes, listas, sets e sorted sets usam o mesmo writer AOF das strings. A gravação
recebe a pós-imagem tipada completa e o prazo absoluto resolvido. Em `always`, o
armazenamento aplica essa imagem depois de o writer confirmar o sync do lote.

O codec AOF v1 mantém as tags anteriores de string e remoção. As tags `3`, `4`,
`5` e `6` identificam hash, lista, set e sorted set. O decoder limita o registro
antes de alocar o corpo e valida contagens, duplicatas e scores. A compactação
preserva a sequência e reaplica o delta antes de publicar a nova geração.

## Limites e expiração

O registro AOF tem limite configurável de até 64 MiB, também usado como padrão.
Um comando cujo resultado cabe na quota mas excede esse registro retorna
`ERR AOF record limit exceeded`. Ele não muda dados, sequência ou arquivo e
permite continuar usando o worker. A quota lógica segue os custos definidos nos
guias de [coleções](collections.md) e [sorted sets](sorted-sets.md).

Leituras que encontram coleções expiradas preparam tombstones com origem
`Expiration`. A remoção passa pelo AOF antes de ser aplicada. Após essa remoção
durável, voltar o relógio de parede durante o restart não ressuscita a chave.
O replay descarta também pós-imagens cujo prazo absoluto já venceu e restaura
a quota somente dos valores ainda presentes.

## Evidência executada

```powershell
cargo test --locked --test persistence typed_
```

Cinco testes passam no Windows, cada um cobrindo as quatro famílias:

- Roundtrip e compactação restauram dados completos, bits dos scores, TTL e
  quota. Um relógio injetado verifica prazo de 250 ms e remoção no vencimento.
- Erros de tipo, quota e limite de registro preservam o valor, a sequência AOF
  e o atendimento de `PING`.
- Atualizações enquanto um snapshot está pausado aparecem no delta recuperado.
  Uma expiração passiva posterior persiste seu tombstone antes do restart.
- Trinta e seis processos são interrompidos nos nove pontos de append, sync,
  resposta, snapshot e publicação. O recovery aceita o estado anterior ou novo
  completo quando não houve confirmação; após sync ou confirmação, exige o novo.
- A fixture fixa de strings AOF v1 pode receber cada tipo novo, passar por
  compactação e ser recuperada sem perder a string anterior.

O helper `typed_aof_process_child` é ignorado na execução comum e iniciado
explicitamente pelos testes pais com variáveis restritas ao processo filho.
Nenhum teste altera o ambiente global ou depende de portas fixas.

## Limites da migração verificada

A baseline interna R05 foi gerada pelo executável do checkout limpo
`a615f705266c7562eece5e751d43d94b2b0eb363`, com quatro shards, strings, hashes,
listas, sets e TTL. O processo confirmou cinco mutações com `always`; uma
reinicialização do mesmo binário conferiu os quatro tipos. O binário, a
configuração, os comandos, os logs e os hashes ficam preservados em
`target/baselines/r05-a615f705266c7562eece5e751d43d94b2b0eb363/`.
O executável tem SHA-256
`7eb68f536d9ba8c6e41f430fb7ed66cb04b2e0dab38dcb8f6e6401b20da42aba`;
os 403 bytes AOF têm SHA-256
`87d12a8fc88698266882a6dce88506249fdd7dac3ffe594fc12e42cded47242b`.

`sorted_set_migration_from_frozen_r05_binary_output_preserves_collections`
usa esses bytes em `tests/fixtures/aof-r05-collections.hex`, recupera os tipos
anteriores, acrescenta e atualiza um sorted set, compacta e reinicia com relógio
injetado após o vencimento do hash. Os demais tipos e a ordenação permanecem.
O ensaio passou no Windows e Linux; o SHA histórico identifica a origem dos
dados, sem atribuir a ele a validação do código novo.

`tests/fixtures/aof-v1.hex` foi introduzida no commit
`ea86917` antes dos tipos novos. O SHA-256 do arquivo textual é
`9236640383afe34c7733dddc5cbf30153b0be8b1f7108c584bff3133f8b10862`.
O ensaio verifica a evolução do formato preservado, incluindo leitura, nova
escrita e compactação. Ele não substitui a migração entre executáveis de
baselines internas congeladas por SHA e hashes, nem comprova execução nativa
Linux ou aprovação dos artefatos da candidata 1.0.
