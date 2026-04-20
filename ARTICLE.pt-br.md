# DataRift: construindo um data pipeline assíncrono com Medallion Architecture, rate limiting adaptativo e Delta Lake — sem JVM

## Introdução

Quem trabalha com dados de jogos sabe que APIs de plataformas como a Riot Games são generosas em conteúdo mas exigentes em limites de requisição. A ideia do DataRift nasceu de uma necessidade concreta: extrair dados ranqueados de League of Legends (league entries, perfis de summoner, contas, detalhes de partida e timelines), armazenar tudo de forma reproduzível e transformar em tabelas analíticas — tudo isso respeitando os rate limits da API e sem depender de infraestrutura pesada.

O resultado é um pipeline que segue a Medallion Architecture (Bronze → Silver), usa Polars + delta-rs como motor de processamento e armazenamento, Dagster como orquestrador, e um rate limiter in-house que entende os headers da Riot API. Nenhuma JVM envolvida — o stack inteiro roda em Python e Rust.

Neste artigo a gente vai dissecar as decisões por trás do DataRift: por que Medallion, como o rate limiter funciona por dentro, a filosofia de tratamento de erros, a escolha de Polars em vez de Spark, e a superfície de DevOps que mantém tudo observável.

Se quiser acompanhar no código, o repositório está no GitHub e o [README](README.md) tem um setup progressivo que vai do smoke test (sem API key) até o pipeline completo.

---

## Medallion Architecture: Bronze e Silver

A Medallion Architecture divide o pipeline em camadas com responsabilidades claras. No DataRift a gente usa duas: Bronze e Silver.

### Bronze — a camada de preservação

A camada Bronze armazena as respostas cruas da API como Delta tables. Cada resposta JSON é gravada inteira no campo `raw_json`, junto com metadados de ingestão: endpoint chamado, status code HTTP, região e timestamp UTC.

São 6 tabelas Bronze:

| Tabela | Chave primária | Conteúdo |
|--------|---------------|----------|
| `league_entries_raw` | `puuid` | Entradas de liga (Challenger, Grandmaster, Master) |
| `accounts_raw` | `puuid` | Dados de conta (gameName, tagLine) |
| `summoners_raw` | `puuid` | Perfis de summoner (level, ícone, datas) |
| `match_ids_raw` | `puuid` | IDs de partida por jogador |
| `match_details_raw` | `match_id` | Detalhes completos de cada partida |
| `match_timelines_raw` | `match_id` | Timeline frame-a-frame de cada partida |

A classe `BronzeWriter` em `src/datarift/bronze_writer.py` cuida da escrita. Ela usa `deltalake.write_deltalake` no modo append e monta DataFrames Polars com as colunas padronizadas antes de converter pra Arrow:

```python
df = pl.DataFrame({
    self._primary_key_col: [r[self._primary_key_col] for r in records],
    "raw_json": [r["raw_json"] for r in records],
    "ingested_at": [now] * len(records),
    "endpoint": [endpoint] * len(records),
    "status_code": [status_code] * len(records),
    "region": [region] * len(records),
})
write_deltalake(self.table_path, df.to_arrow(), mode="append")
```

O BronzeWriter também suporta **anti-join resume**: o método `existing_keys()` lê as chaves primárias já gravadas na tabela Delta, permitindo que a extração pule registros que já foram ingeridos. Isso é essencial para retomada após interrupções — SIGINT no meio de uma extração longa não joga trabalho fora.

### Por que preservar o JSON cru?

A tentação é já normalizar na ingestão. Mas guardar o JSON original tem vantagens concretas:

1. **Replay**: se a Riot mudar a estrutura da resposta, a gente pode re-processar os dados históricos sem chamar a API de novo
2. **Auditoria**: dá pra comparar o que a API retornou com o que o Silver produziu
3. **Schema evolution**: campos novos aparecem automaticamente no Bronze sem precisar mexer na ingestão

### Silver — a superfície analítica

A camada Silver pega o JSON cru do Bronze e transforma em 11 tabelas relacionais normalizadas:

**Do match_details_raw:**
- `matches` — metadados da partida (duração, versão, modo de jogo)
- `match_participants` — estatísticas individuais de cada jogador (kills, deaths, assists, +125 campos de challenges)
- `match_teams` — resultado por time
- `match_teams_bans` — bans de cada time
- `match_teams_objectives` — objetivos por time (barão, dragão, torre, etc.)

**Do match_timelines_raw:**
- `match_timeline_frames` — snapshots do estado do jogo a cada minuto
- `match_timeline_participant_frames` — posição, gold e XP de cada jogador por frame
- `match_timeline_events` — eventos individuais (kills, compras, ward placement)

**Do league_entries_raw + summoners_raw + accounts_raw:**
- `league_entries` — tier, rank, LP, wins/losses
- `summoners` — nível, ícone, data de revisão
- `accounts` — gameName, tagLine

A extração de campos do JSON usa uma combinação de `json_path_match` (pra campos escalares simples) e `json.loads` com traversal em Python (pra estruturas aninhadas como times, participantes e challenges). Essa decisão foi pragmática: o Polars 1.x exige dtype explícito pra `json_decode` no nível de expressão, então pra arrays e structs complexos o parsing via Python é mais confiável.

Os campos de challenge dos participantes são extraídos **dinamicamente** — em vez de hardcodar 125 nomes de campo, o código lê as chaves presentes nos dados e gera colunas automaticamente. Isso significa que se a Riot adicionar um novo campo de challenge, ele aparece no Silver sem precisar alterar código.

---

## Rate Limiter: por dentro do RiotRateLimiter

A Riot API tem um sistema de rate limit em duas camadas: limites no nível da aplicação (app-level) e limites por método (per-method). Os dois são comunicados via headers de resposta e funcionam como sliding windows. Nenhuma biblioteca pronta lida com isso direito, então o DataRift implementa um rate limiter customizado na classe `RiotRateLimiter` em `src/datarift/riot_client.py`.

### Arquitetura do rate limiter

```
Requisição
    │
    ▼
asyncio.Semaphore (max_concurrent=10)
    │
    ▼
_should_wait() — verifica janelas app + method
    │
    ├── usage >= 80%? → preemptive delay
    │
    ▼
httpx.AsyncClient.request()
    │
    ▼
_update_windows_from_headers()
    │
    ├── Lê X-App-Rate-Limit / X-App-Rate-Limit-Count
    ├── Lê X-Method-Rate-Limit / X-Method-Rate-Limit-Count
    │
    ▼
Resposta
    ├── 200 + JSON → retorna
    ├── 200 + não-JSON → retry (content-type guard)
    ├── 401/403 → raise imediato (nunca retry)
    ├── 429 → Retry-After + jitter → retry
    └── 5xx → exponential backoff → retry
```

### Sliding window dual-layer

O rate limiter mantém duas coleções de janelas:

- **`_app_windows`**: limites globais da aplicação. Inicializados com os defaults de dev key (20 req/1s, 100 req/2min) e atualizados a cada resposta via headers `X-App-Rate-Limit` e `X-App-Rate-Limit-Count`.
- **`_method_windows`**: limites por endpoint. Chave derivada dos três primeiros segmentos do path (ex: `/lol/summoner/v4/...` → `lol.summoner.v4`). Atualizados via `X-Method-Rate-Limit` e `X-Method-Rate-Limit-Count`.

Cada `RateWindow` guarda: `calls` (limite), `seconds` (duração da janela), `count` (uso atual) e `reset_at` (quando a janela reseta, em `time.monotonic`).

### Threshold preemptivo de 80%

Em vez de esperar bater o limite e receber um 429, o método `_should_wait` verifica se alguma janela está com uso >= 80%:

```python
@staticmethod
def _should_wait(windows: list[RateWindow]) -> float:
    now = time.monotonic()
    max_wait = 0.0
    for w in windows:
        if w.calls == 0:
            continue
        usage = w.count / w.calls
        if usage >= 0.8:
            if w.reset_at is not None and w.reset_at > now:
                wait = w.reset_at - now
            else:
                wait = float(w.seconds)
            max_wait = max(max_wait, wait)
    return max_wait
```

Esse delay preemptivo reduz drasticamente a quantidade de 429s que o pipeline recebe. Na prática, com a dev key (20 req/s), o limiter começa a frear a partir de 16 requisições na janela de 1 segundo.

### Estratégias de retry

O limiter implementa três estratégias de retry, todas dentro do mesmo loop de até 5 tentativas (`_MAX_RETRIES = 5`):

1. **429 (Rate Limited)**: lê o header `Retry-After`, adiciona jitter aleatório entre 0.1 e 0.5 segundos, e espera. O jitter evita thundering herd quando múltiplas coroutines recebem 429 ao mesmo tempo.

2. **5xx (Server Error)**: exponential backoff com base de 1 segundo e cap de 60 segundos. Fórmula: `min(1.0 * 2^(attempt-1), 60.0)`. Na prática: 1s → 2s → 4s → 8s → 16s.

3. **200 com content-type errado**: a API da Riot ocasionalmente retorna HTML em vez de JSON (geralmente durante manutenção). O content-type guard detecta respostas 200 que não são `application/json` e faz retry com a mesma política de backoff dos 5xx.

Erros 401/403 nunca fazem retry — indicam chave expirada ou inválida e o pipeline falha imediatamente com uma mensagem clara.

### Controle de concorrência

O `asyncio.Semaphore(max_concurrent=10)` limita quantas requisições HTTP podem estar em vôo simultaneamente. Isso complementa o rate limiter temporal — mesmo que as janelas permitam mais, no máximo 10 requisições concorrem ao mesmo tempo, protegendo contra rajadas que poderiam saturar a conexão.

---

## Filosofia de tratamento de erros

O DataRift segue três princípios de tratamento de erros que foram decisões arquiteturais explícitas:

### 1. Retry owner único

Toda lógica de retry vive dentro do `RiotRateLimiter`. O Dagster `RetryPolicy` está **explicitamente desabilitado** em todos os assets. Sem isso, a gente teria retries de retries: o limiter tenta 5 vezes, falha, o asset faz retry, o limiter tenta mais 5 vezes... Em um cenário de 429 storm, isso pode loopear por horas.

Com retry centralizado, o comportamento é diagnosável: se o limiter exauriu 5 tentativas e falhou, o asset falha de forma terminal. Re-execução é manual via Dagster UI.

### 2. Schema drift assimétrico

A política de schema drift (D007) reconhece que nem toda mudança na API merece a mesma reação:

- **Mudança de tipo** (ex: um campo que era string vira number) → **falha**. Mudanças de tipo corrompem queries downstream silenciosamente.
- **Campo desapareceu** → **warn**. A Silver layer aceita o campo como null. Se mais de 95% dos registros tiverem um campo null, o metadata do Dagster reporta um `null_rate` alto.
- **Campo novo apareceu** → **silently drop**. Campos desconhecidos são ignorados. Na próxima vez que alguém atualizar o schema Silver, o campo pode ser adicionado.

Essa assimetria é intencional: alguém que clonar o repo e rodar contra outra região não deveria ter uma falha só porque a Riot adicionou um campo opcional. Ao mesmo tempo, uma mudança de tipo é um sinal de que algo mudou de forma incompatível e precisa de atenção.

### 3. SIGINT graceful shutdown

O `runner.py` implementa shutdown graceful em resposta a SIGINT. São 6 estágios de extração que checam um `shutdown_event`:

```python
shutdown_event = asyncio.Event()

loop = asyncio.get_running_loop()
loop.add_signal_handler(signal.SIGINT, lambda: shutdown_event.set())

# Cada estágio checa o evento antes de prosseguir
if not shutdown_event.is_set():
    await extract_accounts(...)
```

Quando o usuário pressiona Ctrl+C:

1. O handler seta `shutdown_event`
2. O estágio atual flushes o batch em andamento pro Delta table
3. Estágios downstream são pulados
4. O pipeline registra `extraction_interrupted` e encerra limpo

Isso significa que um SIGINT **nunca** perde dados — tudo que foi extraído até o momento está salvo no Bronze. Na próxima execução, o anti-join resume do BronzeWriter pula os registros já ingeridos e continua de onde parou.

A granularidade da checagem varia por extractor: league entries checa entre páginas (cada chamada API = uma página), enquanto accounts, summoners e match extractors checam por item individual, dando responsividade mais fina ao sinal de shutdown.

---

## Polars + delta-rs: por que não Spark?

A escolha de Polars + delta-rs no lugar de Spark foi uma decisão do projeto desde o início (D001) e tem um motivo simples: **nenhuma JVM**.

### O problema com Spark pra esse caso de uso

Spark é excelente pra processar terabytes distribuídos. Mas pra um pipeline que:
- Roda em uma única máquina
- Processa milhares (não bilhões) de registros por run
- Precisa de startup rápido pra desenvolvimento iterativo
- Vai rodar em Docker (python:3.12-slim)

...o overhead de uma JVM é difícil de justificar. O tempo de startup do SparkSession sozinho já seria maior do que a transformação Silver inteira.

### O que Polars traz

**Flattening de JSON aninhado**: a API da Riot retorna JSONs profundamente aninhados. Polars tem `explode()` e `unnest()` nativos que transformam arrays de structs em linhas de DataFrame de forma declarativa. No Silver match, por exemplo, a lista de participantes de uma partida é explodida em linhas individuais com `explode("participants")`.

**Lazy evaluation**: as transformações Silver usam expressões Polars que são otimizadas antes de executar. O query planner do Polars elimina colunas não usadas e reordena operações automaticamente.

**Zero-copy com Arrow**: Polars usa Apache Arrow internamente, e delta-rs aceita Arrow tables diretamente. A conversão entre os dois é zero-copy — os dados não são serializados/deserializados no meio do caminho.

**Tipo-segurança no nível de coluna**: cada coluna tem um dtype explícito. Quando o Silver faz cast de `json_path_match` (que retorna Utf8) pra `Int64` ou `Float64`, erros de tipo são detectados na transformação, não em uma query downstream três meses depois.

### O que delta-rs traz

**Delta Lake sem Spark**: `write_deltalake` e `DeltaTable` do pacote `deltalake` (Python bindings pra delta-rs em Rust) dão leitura, escrita, versionamento e schema enforcement sem precisar do Spark runtime.

**Append mode + time travel**: cada `write_batch` do BronzeWriter faz um append que cria uma nova versão no transaction log do Delta. Se algo der errado, a gente pode voltar pra versão anterior.

**Anti-join resume**: `DeltaTable.to_pyarrow_table(columns=[pk_col])` lê só a coluna de chave primária, eficiente mesmo pra tabelas grandes. O BronzeWriter usa isso pra construir o set de chaves existentes e skippar registros duplicados.

O resultado é um stack que instala com `pip install`, roda em segundos, e cabe em um container Docker de ~200MB. Sem JVM, sem YARN, sem configuração de cluster.

---

## DevOps: observabilidade e infraestrutura

### Dagster software-defined assets

O pipeline é exposto como 4 Dagster assets definidos em `src/datarift/definitions.py`:

1. **`bronze_extraction`** — roda a extração completa contra a API da Riot
2. **`silver_matches`** — transforma Bronze match data em 5 tabelas Silver (depende de `bronze_extraction`)
3. **`silver_timelines`** — transforma Bronze timelines em 3 tabelas Silver (depende de `bronze_extraction`)
4. **`silver_league`** — transforma Bronze league/summoner/account em 3 tabelas Silver (depende de `bronze_extraction`)

Cada asset retorna `MaterializeResult` com metadata estruturado:

```python
return MaterializeResult(
    metadata={
        "succeeded": succeeded,  # contagem de tabelas Bronze gravadas
        "failed": failed,
        "rate_limit_hits": 0,
        "total_wall_time": wall_time,
    },
)
```

Os assets Silver retornam `row_counts` — a contagem de linhas por tabela — no metadata, dando visibilidade imediata no Dagster UI sobre quantos registros cada transformação produziu.

### Logging com dual-sink structlog

O módulo `src/datarift/logging.py` configura structlog com dois sinks:

1. **stdout**: JSON lines capturado pelo Dagster UI. Aparece na aba de logs de cada asset materializado.
2. **Arquivo**: `data/_logs/<run_id>/<asset_name>.jsonl`. Persistente, queryável com `pl.read_ndjson()`.

Todo log event carrega 7 campos obrigatórios:

| Campo | Descrição |
|-------|-----------|
| `run_id` | ID do Dagster run (ou UUID curto pra invocação direta) |
| `asset_name` | Nome do asset que gerou o log |
| `endpoint` | Endpoint da API sendo chamado |
| `severity` | Nível: DEBUG, INFO, WARNING, ERROR |
| `event_type` | Tipo semântico do evento (ex: `rate_limit.throttle`) |
| `message` | Mensagem legível |
| `ts` | Timestamp ISO 8601 |

Esses campos são injetados automaticamente pelo processor `_inject_context`. Qualquer chamada a `structlog.get_logger()` em qualquer módulo do pipeline já inclui esses campos sem o caller precisar saber disso.

Os logs JSONL persistentes são particularmente úteis pra pós-mortem: se um run falhou às 3h da manhã, a gente pode carregar o arquivo com Polars e filtrar por severity, endpoint ou timestamp sem precisar do Dagster UI rodando.

### Docker e containerização

O Dockerfile usa `python:3.12-slim` como base — sem JDK, sem Hadoop, sem nada além do Python e as dependências pip:

```dockerfile
FROM python:3.12-slim AS base
WORKDIR /app
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir .
ENTRYPOINT ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "datarift.definitions"]
```

O `docker-compose.yaml` sobe o Dagster UI na porta 3000 com o diretório `data/` montado como volume (pra persistir Delta tables entre restarts) e lê as variáveis de ambiente do `.env`:

```yaml
services:
  dagster:
    build: .
    ports:
      - "3000:3000"
    volumes:
      - ./data:/app/data
    env_file: .env
    stop_signal: SIGINT
```

Note o `stop_signal: SIGINT` — quando o Docker manda o container parar, ele envia SIGINT em vez de SIGTERM, ativando o graceful shutdown do runner.

### Smoke test offline

O comando `make smoke` roda um smoke test que não precisa de API key:

```bash
make smoke
```

Ele semeia tabelas Bronze a partir de fixtures JSON commitadas no repositório e executa todas as transformações Silver. É a primeira coisa que qualquer pessoa rodando o projeto pela primeira vez deveria executar — valida que o ambiente está configurado corretamente e que todas as 11 tabelas Silver são geradas sem erro.

O README segue uma escada progressiva: smoke (sem chave) → API real com config pequena → pipeline completo. Isso garante que o usuário tenha um sinal de sucesso antes de precisar configurar credenciais externas.

---

## Conclusão

O DataRift é um pipeline de dados que nasceu de restrições concretas — rate limits rigorosos, ausência de cluster, necessidade de reprodutibilidade — e as transformou em decisões de design:

- **Medallion Architecture** separa preservação (Bronze) de análise (Silver), permitindo replay e evolução de schema independentes
- **Rate limiter adaptativo** respeita os dois níveis de limite da Riot API com preempção em 80%, jitter contra thundering herd e retry inteligente
- **Retry centralizado** evita loops cascata e torna falhas diagnosticáveis
- **Polars + delta-rs** entregam performance de processamento e storage Delta Lake com zero dependência de JVM
- **Observabilidade estruturada** via structlog dual-sink e metadata no Dagster dá visibilidade end-to-end

O projeto é open source e o setup completo está documentado no [README](README.md). A escada de verificação (smoke → API → Docker) foi desenhada pra que qualquer pessoa consiga rodar o pipeline em minutos.

Uma nota: o pipeline completo com dados reais da API da Riot requer uma chave de desenvolvedor ativa e, dependendo do volume de dados configurado (tiers, região), pode levar de minutos a horas. O smoke test e as transformações Silver, porém, rodam instantaneamente com os dados de fixture — ideal pra explorar a arquitetura sem esperar pela API.
