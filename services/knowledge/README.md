# AI Advokat Knowledge Service

Изолированный поисковый слой поверх данных `ai-advokat-parser` для двух корпусов:

| корпус | источник | таблицы | очередь индексации | индекс Elasticsearch | id фрагмента |
|---|---|---|---|---|---|
| `legal_act` | PRG.ZANGER | `documents` + `document_outputs` | `search_index_jobs` | `ELASTICSEARCH_INDEX` (`ai_advokat_chunks_v1`) | `doc_id:номер` |
| `judicial_decision` | PRG.SOT | `sot_decisions` + `sot_decision_outputs` | `sot_search_index_jobs` | `SOT_ELASTICSEARCH_INDEX` (`ai_advokat_sot_chunks_v1`) | `prg_sot:ключ:номер` |

Корпуса делят один Postgres и один кластер Elasticsearch, но не пересекаются ни
в очередях, ни в индексах, ни в id. Существующие строки и фрагменты
legal-корпуса не мигрируются и не переписываются: `search_index_jobs`,
`documents` и id вида `doc_id:n` остаются ровно как были; судебный конвейер
только добавляет свои таблицы `sot_*`.

```text
ai-advokat-parser ─┬─ documents/document_outputs ──────▶ search_index_jobs ──────▶ ai_advokat_chunks_v1
                   └─ sot_decisions/sot_decision_outputs ▶ sot_search_index_jobs ▶ ai_advokat_sot_chunks_v1
```

Индексатор создаёт задания только для строк со статусом `exported` и выходом
`txt`. Задания берутся через `FOR UPDATE SKIP LOCKED`, поэтому каждый сервис
можно масштабировать репликами Railway без двойной обработки. Зависшие
`processing` возвращаются в очередь по lease, `failed` повторяются один раз при
следующем деплое, изменение SHA-256 текста ставит строку на переиндексацию.

## Режимы и переменные

Один Docker-образ поддерживает два режима через `KNOWLEDGE_MODE`:

- `indexer` — непрерывная индексация;
- `mcp` — Streamable HTTP MCP на `/mcp`.

Обязательные переменные:

```text
DATABASE_URL=postgresql://...
ELASTICSEARCH_URL=http://elasticsearch:9200
KNOWLEDGE_MODE=indexer|mcp
```

Корпуса и индексы:

```text
ELASTICSEARCH_INDEX=ai_advokat_chunks_v1        # legal, по умолчанию
SOT_ELASTICSEARCH_INDEX=ai_advokat_sot_chunks_v1 # judicial, по умолчанию
```

Имена обязаны быть непустыми и разными — сервис отказывается стартовать,
если их совместить в один индекс. Индексатор обрабатывает только тот корпус,
который выбран `KNOWLEDGE_CORPUS`:

```text
KNOWLEDGE_CORPUS=legal_act          # только PRG.ZANGER (по умолчанию)
KNOWLEDGE_CORPUS=judicial_decision  # только PRG.SOT
KNOWLEDGE_CORPUS=all                # оба корпуса
```

MCP по умолчанию ищет в обоих корпусах; `MCP_DEFAULT_CORPUS` может закрепить
один корпус без изменения индексатора:

```text
MCP_DEFAULT_CORPUS=all|legal_act|judicial_decision   # по умолчанию all
```

Для публичного MCP задаётся `MCP_API_KEY`. Клиент передаёт его как:

```text
Authorization: Bearer <key>
```

Подключение MCP-клиента:

```text
Transport: Streamable HTTP
URL: https://ai-advokat-mcp-production.up.railway.app/mcp
Header: Authorization: Bearer <MCP_API_KEY>
```

## Поиск по двум корпусам

`search_documents(query, corpus, limit)` принимает `corpus=legal_act`,
`judicial_decision` или `all` (по умолчанию `all`). Для `all` запрашиваются оба
индекса, и результаты сливаются в один список: BM25-очки несравнимы между
индексами с разными коллекциями, поэтому каждый хит сначала нормализуется на
лучший очк своего индекса и только потом списки чередуются; ничьи решаются по
сырому score и doc_id. Каждый результат несет:

- `source_system` — `prg_zanger` или `prg_sot`;
- `corpus_type` — `legal_act` или `judicial_decision`;
- для судебных решений дополнительно `decision_id`, `case_number`, `court`,
  `judge`, `region`, `instance`, `proceeding_type`, `decision_date`, `parties`;
- для legal-документов — `pages`.

Ответ также содержит `corpus` (итоговый фильтр) и `sources` (сколько
результатов пришло из каждой системы).

`get_document(doc_id)` читает исходный текст частями: legal-документы — по
обычному `doc_id`, судебные решения — по ключу из результатов поиска
(`prg_sot:...`). Оба ответа несут `source_system` и `corpus_type`.

`get_related_documents(doc_id)` работает с графом ссылок legal-корпуса; для
ключа `prg_sot:` возвращает пустой `related` с провенансом и
`"supported": false`, ничего не ломая.

## Railway: два безопасных сервиса

1. **MCP** — один сервис:

```text
KNOWLEDGE_MODE=mcp
MCP_DEFAULT_CORPUS=all
MCP_API_KEY=...
```

2. **Индексатор второго корпуса** — отдельный сервис из того же образа
(`services/knowledge/railway.toml`, `startCommand = "python -m knowledge_service"`,
`restartPolicyType = "ON_FAILURE"`):

```text
KNOWLEDGE_MODE=indexer
KNOWLEDGE_CORPUS=judicial_decision
```

Это безопасно как второй сервис: очередь забирается через
`FOR UPDATE SKIP LOCKED`, зависшие `processing` возвращаются по
`INDEXER_LEASE_SECONDS`, а рестарт по `ON_FAILURE` не дублирует работу.
Индексатор legal-корпуса можно оставить на существующем сервисе с
`KNOWLEDGE_CORPUS=legal_act` (по умолчанию) или объединить через `all`.

## Доступные инструменты

- `search_documents` — поиск по `legal_act`, `judicial_decision` или `all` с лучшим фрагментом документа;
- `get_document` — чтение точного исходного текста частями (`doc_id` или `prg_sot:` ключ);
- `get_related_documents` — ссылки legal-документов и их статусы;
- `get_collection_status` — обе очереди индексации, обе коллекции, число фрагментов в каждом индексе и здоровье Elasticsearch.
