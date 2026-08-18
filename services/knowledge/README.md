# AI Advokat Knowledge Service

Изолированный поисковый слой поверх данных `ai-advokat-parser`:

- Postgres хранит очередь индексации и исходные документы;
- индексатор режет `txt` на фрагменты и пишет их в Elasticsearch;
- MCP отдаёт поиск, документ, связанные документы и состояние коллекции.

```text
ai-advokat-parser -> Postgres documents/document_outputs
                  |
                  v
          search_index_jobs
          queued -> processing -> indexed/failed
                  |
                  v
  ai-advokat-indexer -> Elasticsearch ai_advokat_chunks_v1
                              |
                              v
                 ai-advokat-mcp /mcp
```

Индексатор создаёт задания только для документов со статусом `exported` и
выходом `txt`. Задания берутся через `FOR UPDATE SKIP LOCKED`, поэтому сервис
можно масштабировать несколькими Railway-репликами без двойной обработки.
Зависшие `processing` возвращаются в очередь по lease, а `failed` повторяются
один раз при следующем деплое. Изменение SHA-256 текста автоматически ставит
документ на переиндексацию.

Один Docker-образ поддерживает два режима через `KNOWLEDGE_MODE`:

- `indexer` — непрерывная индексация;
- `mcp` — Streamable HTTP MCP на `/mcp`.

Обязательные переменные:

```text
DATABASE_URL=postgresql://...
ELASTICSEARCH_URL=http://elasticsearch:9200
KNOWLEDGE_MODE=indexer|mcp
```

Для публичного MCP также задаётся `MCP_API_KEY`. Клиент передаёт его как:

```text
Authorization: Bearer <key>
```

Подключение MCP-клиента:

```text
Transport: Streamable HTTP
URL: https://ai-advokat-mcp-production.up.railway.app/mcp
Header: Authorization: Bearer <MCP_API_KEY>
```

Доступные инструменты:

- `search_documents` — полнотекстовый поиск с лучшим фрагментом документа;
- `get_document` — чтение точного исходного текста частями;
- `get_related_documents` — ссылки на связанные документы и их статусы;
- `get_collection_status` — состояние Postgres-очереди и Elasticsearch.
