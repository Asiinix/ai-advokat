# PRG Parser

Локальный парсер бесплатных документов PRG.ZANGER.

Проект умеет:

- брать `doc_id` со страниц списка документов;
- скачивать документ через внутренний API чанков;
- склеивать все части документа, включая те, которые сайт показывает только при прокрутке;
- переписывать ссылки на другие документы PRG в локальные ссылки;
- при необходимости докачивать документы из этих ссылок;
- сохранять результат в `html`, `txt`, `json`, опционально `pdf`;
- вести состояние в SQLite, чтобы продолжать работу после остановки.

## Быстрый старт

```bash
cd /Users/asiin/prg-parser
python3 -m prg_parser
```

Откроется cmd-панель:

```text
PRG Parser CMD
1. Скачать диапазон страниц списка
2. Скачать документы из файла doc_id/URL
3. Скачать один doc_id
4. Показать doc_id на странице списка
5. Статус
6. Повторить failed
7. Настройки
0. Выход
```

## Примеры команд

Скачать один документ:

```bash
cd /Users/asiin/prg-parser
python3 -m prg_parser --formats html,txt,json doc 35502996
```

Скачать один документ и все документы, на которые он ссылается:

```bash
python3 -m prg_parser --formats html,txt,json --follow-links-depth 1 doc 35502996
```

То же самое, но не больше 20 связанных документов:

```bash
python3 -m prg_parser --formats html,txt,json --follow-links-depth 1 --max-linked-docs 20 doc 35502996
```

Скачать документы со страниц списка 1-3:

```bash
python3 -m prg_parser --formats html,txt,json range --from-page 1 --to-page 3
```

Скачать только первые 10 документов из диапазона:

```bash
python3 -m prg_parser --formats html,txt range --from-page 1 --to-page 10 --max-docs 10
```

Скачать документы из файла:

```bash
python3 -m prg_parser --formats html,txt,json file --input examples/doc_ids.txt
```

Показать, какие документы есть на странице списка:

```bash
python3 -m prg_parser list --page 1
```

Показать статистику:

```bash
python3 -m prg_parser status
```

Повторить документы, которые упали с ошибкой:

```bash
python3 -m prg_parser --formats html,txt,json retry
```

## Где лежат результаты

По умолчанию все сохраняется в:

```text
/Users/asiin/prg-parser/data
```

Структура:

```text
data/
  state.sqlite3
  documents/
    35502996/
      document.html
      document.txt
      document.json
      meta.json
```

Внутренние ссылки в `document.html` ведут на соседние локальные файлы:

```text
../35582732/document.html
../32936917/document.html#SUB10000
```

Если запускаешь без `--follow-links-depth`, ссылки будут переписаны на локальные пути, но связанные документы могут еще не существовать. Для полной локальной коллекции используй `--follow-links-depth 1`.

Можно указать другую папку:

```bash
python3 -m prg_parser --out /Users/asiin/Downloads/prg-data doc 35502996
```

## Railway и Postgres

Локально парсер по умолчанию пишет файлы в `data/` и состояние в `data/state.sqlite3`.

Если в окружении есть `DATABASE_URL` или `PRG_DATABASE_URL`, парсер автоматически включает Postgres-хранилище:

- статусы документов и страниц списка пишутся в Postgres;
- `document.html`, `document.txt`, `document.json`, `document.pdf` и `meta.json` сохраняются в таблицу `document_outputs`;
- найденные связи документов сохраняются в `document_links`;
- локальный экспорт в `--out` остается как временная копия.

Для Railway используется worker:

```bash
python -m prg_parser.railway_worker
```

Он держит контейнер живым. Чтобы запустить парсер на деплое, задай переменную `PRG_COMMAND`, например:

```text
--out /tmp/prg-data --formats html,txt,json doc 35502996
```

Для больших запусков лучше начинать с маленьких диапазонов и обязательно включать `json`, чтобы raw-слой попал в Postgres.

### Очередь документов

Для больших объемов можно разделить обход страниц и скачивание документов.
Сначала страницы списка только складывают документы в очередь:

```text
--out /tmp/prg-data --formats html,txt,json --follow-links-depth 1 --enqueue-only range --from-page 1 --to-page 14845
```

Потом один или несколько воркеров разбирают очередь:

```text
--out /tmp/prg-data --formats html,txt,json --follow-links-depth 1 --workers 3 --delay 0 work --idle-seconds 60
```

В Postgres документы идут по статусам `queued -> processing -> exported/failed`.
Воркеры берут задачи атомарно через `FOR UPDATE SKIP LOCKED`, поэтому несколько Railway-реплик могут работать с одной очередью и не должны скачивать один и тот же `doc_id`.
Если контейнер умер на статусе `processing`, следующий запуск вернет зависшие задачи обратно в `queued` после `--lease-seconds`.

## Форматы

Доступные форматы:

- `html` - красивый HTML для просмотра в браузере;
- `txt` - чистый текст;
- `json` - полный сырой ответ API PRG со всеми чанками;
- `pdf` - PDF из HTML через установленный Chrome/Chromium.

PDF пример:

```bash
python3 -m prg_parser --formats html,pdf doc 35502996
```

Если Chrome не найден, укажи путь:

```bash
PRG_CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
python3 -m prg_parser --formats html,pdf doc 35502996
```

## Файл с документами

Файл может содержать просто `doc_id`:

```text
35502996
1005029
33587966
```

Или ссылки:

```text
https://prg.kz/lawyer/document/?doc_id=35502996
```

Пустые строки и строки с `#` игнорируются.

## Настройки нагрузки

Для аккуратной загрузки используй задержку:

```bash
python3 -m prg_parser --delay 1.5 range --from-page 1 --to-page 10
```

Параллельная загрузка документов:

```bash
python3 -m prg_parser --workers 2 --delay 1.0 range --from-page 1 --to-page 5
```

Для больших объемов лучше начинать с маленького теста:

```bash
python3 -m prg_parser --formats html,txt range --from-page 1 --to-page 1 --max-docs 3
```

Потом расширять диапазон.

## Локальные ссылки между документами

По умолчанию HTML-экспорт переписывает ссылки вида:

```text
https://prg.kz/lawyer/document/?doc_id=35582732
?doc_id=35582732#sub_id=10000
```

в локальные:

```text
../35582732/document.html
../35582732/document.html#SUB10000
```

Чтобы парсер не только переписал ссылку, но и скачал связанные документы, включи глубину:

```bash
python3 -m prg_parser --formats html,txt,json --follow-links-depth 1 doc 35502996
```

Глубина:

- `0` - не докачивать ссылки;
- `1` - скачать документы, на которые ссылаются выбранные документы;
- `2` - скачать еще и документы из ссылок связанных документов.

Осторожно с глубиной `2+`: граф ссылок может быстро стать очень большим.

Можно ограничить количество документов, добавленных именно из ссылок:

```bash
python3 -m prg_parser --follow-links-depth 1 --max-linked-docs 100 range --from-page 1 --to-page 1
```

## Важное

Парсер рассчитан на бесплатные документы. По умолчанию он пропускает документы, которые API не помечает как бесплатные. Не используй его для обхода авторизации, платного доступа или ограничений сайта.
