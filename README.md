# AI Advokat Parser

Парсер и очередь загрузки юридических документов для AI Advokat. Два изолированных конвейера: исходные документы PRG.ZANGER (законодательство) и судебные акты PRG.SOT.

Проект умеет:

- брать `doc_id` со страниц списка документов;
- скачивать документ через внутренний API чанков;
- склеивать все части документа, включая те, которые сайт показывает только при прокрутке;
- переписывать ссылки на другие документы источника в локальные ссылки;
- при необходимости докачивать документы из этих ссылок;
- сохранять результат в `html`, `txt`, `json`, опционально `pdf`;
- вести состояние в SQLite, чтобы продолжать работу после остановки.

## Быстрый старт

```bash
cd ai-advokat
python3 -m ai_advokat_parser
```

Откроется cmd-панель:

```text
AI Advokat Parser
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
cd ai-advokat
python3 -m ai_advokat_parser --formats html,txt,json doc 35502996
```

Скачать один документ и все документы, на которые он ссылается:

```bash
python3 -m ai_advokat_parser --formats html,txt,json --follow-links-depth 1 doc 35502996
```

То же самое, но не больше 20 связанных документов:

```bash
python3 -m ai_advokat_parser --formats html,txt,json --follow-links-depth 1 --max-linked-docs 20 doc 35502996
```

Скачать документы со страниц списка 1-3:

```bash
python3 -m ai_advokat_parser --formats html,txt,json range --from-page 1 --to-page 3
```

Скачать только первые 10 документов из диапазона:

```bash
python3 -m ai_advokat_parser --formats html,txt range --from-page 1 --to-page 10 --max-docs 10
```

Скачать документы из файла:

```bash
python3 -m ai_advokat_parser --formats html,txt,json file --input examples/doc_ids.txt
```

Показать, какие документы есть на странице списка:

```bash
python3 -m ai_advokat_parser list --page 1
```

Показать статистику:

```bash
python3 -m ai_advokat_parser status
```

Повторить документы, которые упали с ошибкой:

```bash
python3 -m ai_advokat_parser --formats html,txt,json retry
```

## Где лежат результаты

По умолчанию все сохраняется в:

```text
ai-advokat/data
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
python3 -m ai_advokat_parser --out /Users/asiin/Downloads/ai-advokat-data doc 35502996
```

## Вход в PRG.ZANGER

Часть документов доступна только авторизованным пользователям. Если в окружении заданы обе переменные, парсер сам логинится через `auth.zakon.kz` и переиспользует cookie сессии во всех потоках:

```bash
export AI_ADVOCAT_PRG_USERNAME="логин"
export AI_ADVOCAT_PRG_PASSWORD="пароль"
```

- если переменных нет, парсер работает как раньше, анонимно;
- если задана только одна из двух, запуск падает с понятной ошибкой;
- логин выполняется один раз, при истечении сессии парсер перелогинивается и повторяет запрос;
- логин, пароль, cookie и anti-forgery токен никогда не попадают в логи и в тексты ошибок.

Храни учетные данные только в переменных окружения (в Railway - в Variables), не добавляй их в `AI_ADVOCAT_COMMAND`.

## Railway и Postgres

Локально парсер по умолчанию пишет файлы в `data/` и состояние в `data/state.sqlite3`.

Если в окружении есть `DATABASE_URL` или `AI_ADVOCAT_DATABASE_URL`, парсер автоматически включает Postgres-хранилище:

- статусы документов и страниц списка пишутся в Postgres;
- `document.html`, `document.txt`, `document.json`, `document.pdf` и `meta.json` сохраняются в таблицу `document_outputs`;
- найденные связи документов сохраняются в `document_links`;
- локальный экспорт в `--out` остается как временная копия.

Для Railway используется worker:

```bash
python -m ai_advokat_parser.railway_worker
```

Он держит контейнер живым. Чтобы запустить парсер на деплое, задай переменную `AI_ADVOCAT_COMMAND`, например:

```text
--out /tmp/ai-advokat-data --formats html,txt,json doc 35502996
```

Для больших запусков лучше начинать с маленьких диапазонов и обязательно включать `json`, чтобы raw-слой попал в Postgres.

### Очередь документов

Для больших объемов можно разделить обход страниц и скачивание документов.
Если нужен один Railway-сервис, запускай pipeline: он одновременно складывает страницы в очередь и параллельно качает документы несколькими потоками:

```text
--out /tmp/ai-advokat-data --formats html,txt,json --follow-links-depth 1 --workers 6 --delay 0 pipeline --from-page 1 --to-page 14845 --idle-seconds 300
```

Для будущего масштабирования на несколько сервисов можно разделить процесс.
Сначала страницы списка только складывают документы в очередь:

```text
--out /tmp/ai-advokat-data --formats html,txt,json --follow-links-depth 1 --enqueue-only range --from-page 1 --to-page 14845
```

Потом один или несколько воркеров разбирают очередь:

```text
--out /tmp/ai-advokat-data --formats html,txt,json --follow-links-depth 1 --workers 3 --delay 0 work --idle-seconds 60
```

В Postgres документы идут по статусам `queued -> processing -> exported/failed`.
Воркеры берут задачи атомарно через `FOR UPDATE SKIP LOCKED`, поэтому несколько Railway-реплик могут работать с одной очередью и не должны скачивать один и тот же `doc_id`.
Если контейнер умер на статусе `processing`, следующий запуск вернет зависшие задачи обратно в `queued` после `--lease-seconds`.

Failed-документы, которые оказались платными, можно отдельно обогатить заголовками.
Команда берет только `documents.status='failed'` с пустым `title`, делает один легкий metadata-запрос к источнику и оставляет статус `failed`.
Новые платные документы основной парсер сохраняет с `title` сразу, этот режим нужен для старых строк без заголовка.

```text
--out /tmp/ai-advokat-data --workers 1 --delay 0 enrich-failed-titles
```

## Полный обход каталога (catalog-scan)

`catalog-scan` проходит весь каталог источника, а не только бесплатную выборку.
Он ходит по `/lawyer/documents` с `onlyFreeDocuments=false`, поэтому в скан попадают и платные документы.
Обычные команды (`range`, `pipeline`, `list`) продолжают использовать прежний бесплатный список.

```text
--out /tmp/ai-advokat-data --formats html,txt,json --delay 0 catalog-scan --scan-id catalog-2026-08
```

Что делает скан:

- `--scan-id` обязателен и задает один долгий проход. Повтор той же команды продолжает этот же скан;
- страницы списка читаются последовательно, состав каждой страницы пишется в `catalog_scan_documents`;
- документы ставятся в общую очередь и скачиваются после того, как перечисление закончилось;
- уже готовые документы (`exported` со всеми запрошенными форматами) не скачиваются повторно;
- документы, которые раньше упали как платные, недоступные или сломанные, ставятся в очередь заново;
- недоступные документы не роняют скан: по каждому пишется JSON-заглушка;
- когда скан завершен, та же команда ничего не делает и сразу выходит.

Состояние скана живет в двух таблицах, отдельно от старых `listing_pages`/`listing_documents`:

- `catalog_scans` - фаза (`pending`, `enumerating`, `draining`, `paused`, `completed`, `aborted`), общий размер каталога, размер страницы, курсор `next_page` и счетчики;
- `catalog_scan_documents` - состав скана и итог по каждому документу: `done`, `inaccessible`, `not_found`, `failed`.

Итоги по документам:

- `done` - документ выгружен во все запрошенные форматы;
- `inaccessible` - источник не отдает содержимое авторизованной сессии: платный документ, HTTP 402/403, логин-стена после успешного перелогина или ответ без страниц;
- `not_found` - HTTP 404;
- `failed` - остальные ошибки запроса, разбора или экспорта после исчерпания повторов.

Все, кроме `done`, получает заглушку: `scan_id`, `doc_id`, `page`, `position`, `title`, `source_url`, `outcome`, `failure_kind`, `http_status`, короткий `detail` и время.
В заглушку не попадают тела ответов, cookie, токены и учетные данные.
Если документ позже скачался, заглушка снимается, а итог становится `done`.

Ошибка авторизации PRG - фатальная: скан переходит в `aborted`, взятый в работу документ возвращается в очередь и не помечается как `failed`, процесс выходит с ненулевым кодом.
После исправления учетных данных достаточно повторить ту же команду.

Прогресс и заглушки:

```bash
python3 -m ai_advokat_parser --out /tmp/ai-advokat-data catalog-status --scan-id catalog-2026-08
python3 -m ai_advokat_parser --out /tmp/ai-advokat-data catalog-stubs --scan-id catalog-2026-08 --output stubs.json
```

`catalog-stubs` без `--output` печатает JSON в stdout.

Ограничители для проверочных запусков:

```text
--out /tmp/ai-advokat-data --formats html,txt,json --delay 0 catalog-scan --scan-id catalog-2026-08 --max-pages 2
```

`--max-pages` и `--max-docs` не закрывают скан: он останавливается в фазе `paused`, а следующий запуск с тем же `--scan-id` продолжает с той же страницы.
Поэтому маленький smoke-run нельзя перепутать с полностью пройденным каталогом.

Ограничения:

- `catalog-scan` не ходит по ссылкам документов, запуск с `--follow-links-depth` больше нуля отклоняется;
- если страница списка не отдала общее число документов, скан падает и не пытается угадать размер каталога;
- sitemap источника пока не используется: он нужен как будущий источник сверки, а не как источник истины.

### Railway

Команда для `AI_ADVOCAT_COMMAND` (учетные данные остаются в отдельных переменных):

```text
--out /tmp/ai-advokat-data --formats html,txt,json --workers 3 --delay 0 catalog-scan --scan-id catalog-2026-08
```

После перезапуска контейнера Railway снова выполнит ту же строку: скан продолжится с сохраненной страницы, а зависшие `processing` документы этого скана вернутся в очередь.

## PRG.SOT (судебные акты)

Второй конвейер собирает судебные акты PRG.SOT (`sb.prg.kz`) и живет рядом с PRG.ZANGER, нигде с ним не пересекаясь:

- локально — отдельный файл состояния `sot_state.sqlite3` и таблицы `sot_scans`, `sot_decisions`, `sot_decision_outputs`, `sot_scan_decisions`;
- в Postgres — те же таблицы `sot_*` в общей базе рядом с таблицами ZANGER; таблицы `documents`/`document_outputs` не читаются и не переписываются;
- ключ каждого решения начинается с `prg_sot:`, поэтому он не может совпасть с `doc_id`;
- поисковый слой (`services/knowledge`) индексирует корпуса в разные таблицы очереди и разные индексы Elasticsearch.

### Конфигурация источника

Контракт PRG.SOT подтвержден живым read-only probe 26 августа 2026 года. Он остается в Variables, а не зашивается в код: если поставщик изменит маршрут или форму ответа, валидация остановит скан до первой записи вместо тихой потери документов.

```text
AI_ADVOCAT_SOT_USERNAME=...
AI_ADVOCAT_SOT_PASSWORD=...
AI_ADVOCAT_SOT_BASE_URL=https://sb.prg.kz
AI_ADVOCAT_SOT_SEARCH_URL_TEMPLATE=https://sb.prg.kz/api/Lawsuit/get-lawsuits
AI_ADVOCAT_SOT_SEARCH_METHOD=POST
AI_ADVOCAT_SOT_DECISION_URL_TEMPLATE=https://sb.prg.kz/api/Lawsuit/find-lawsuit?Id={decision_id}
AI_ADVOCAT_SOT_RESULTS_PATH=list
AI_ADVOCAT_SOT_TOTAL_PATH=total
AI_ADVOCAT_SOT_ID_PATH=ordinalId
AI_ADVOCAT_SOT_TEXT_PATH=documents.*.htmlText
```

`AI_ADVOCAT_SOT_SEARCH_BODY_TEMPLATE` содержит подтвержденный JSON фильтра и пагинацию `{page}`/`{page_size}`. `AI_ADVOCAT_SOT_FIELD_MAP` — JSON вида `поле -> путь` для `case_number`, `court`, `judge`, `region`, `instance`, `proceeding_type`, `decision_date`, `title`, `parties`; для объединения истца и ответчика поддерживается путь `plaintiff|defendant`.

В проверенном ответе `total=6 443 276` — число судебных дел, а `totalDoc=16 454 818` — число вложенных документов. Скан страниц идет по делам (`total`), затем забирает все `documents[].htmlText`, очищает HTML, объединяет тексты для поиска и сохраняет исходный JSON со всеми документами. Поэтому второй и последующие документы дела не теряются. Шаблоны обязаны указывать на `AI_ADVOCAT_SOT_BASE_URL` — иначе сессионный cookie ушел бы чужому origin. Пока контракт не заполнен полностью, `sot-scan` падает до первой записи скана; `sot-status` и проверка только логина остаются доступны.

### Egress-партиции (ротация IP после квоты)

PRG явно разрешил ротацию исходящего IP и параллельные сессии после исчерпания квоты. Пул egress-партиций описывается JSON-дескрипторами; по умолчанию (переменная не задана) работает одна прямая партиция. Логика чтения остается прежней, но ambient-прокси теперь намеренно отключены: маршрут задается только этим пулом.

```text
AI_ADVOCAT_SOT_EGRESS_PARTITIONS=[{"id":"direct"},{"id":"px1","proxy_env":"AI_ADVOCAT_SOT_PROXY_PX1"}]
AI_ADVOCAT_SOT_PROXY_PX1=http://user:pass@proxy-host:3128
```

Дескриптор содержит только безопасный `id` (буквы, цифры, `._-`), имя переменной `proxy_env` и необязательный `enabled`. Сам URL прокси (он может содержать учетные данные прокси) живет только в отдельной переменной окружения, никогда не попадает в дескриптор, логи, ошибки и диагностику — наружу выходят только id партиции и состояние квоты. Незнакомые ключи дескриптора (например, вписанный напрямую `proxy_url`) отклоняются до первой записи.

Каждая партиция получает независимый `SourceClient`: свой cookie jar, свой вход в PRG.SOT и, при наличии `proxy_env`, свой HTTP(S)-прокси. Прямая партиция явно отключает ambient `http_proxy`/`https_proxy`, а явный прокси партиции игнорирует ambient `NO_PROXY`/`no_proxy` — случайная переменная контейнера не может ни завернуть трафик в чужой прокси, ни тихо пустить его мимо своего. Запросы держатся одной партиции, пока источник не сообщит, что ее квота исчерпана — HTTP 429 (в том числе на логине) или успешный ответ с `remaining=0`. Тогда партиция отдыхает ровно до собственного reset (или 60 секунд, если reset не назван; более короткая отметка никогда не сокращает уже записанный отдых), а пул прозрачно продолжает на следующей включенной. Сбой собственного egress-пути партиции — сетевая ошибка без HTTP-статуса или HTTP 407 от прокси — отправляет ее в карантин на 300 секунд с тем же переключением; обычные ошибки контента (404, 500, неверный JSON) наружу проходят без ротации. Когда отдыхают все, наверх поднимается один агрегированный HTTP 429 с самым ранним reset, и скан ставится на паузу как раньше. Лизы, resume и защита от дублей в Postgres/SQLite не меняются: пул работает ниже уровня очереди решений. `sot-status` и `sot-probe-auth` показывают партиции (id, имя переменной прокси, состояние квоты); `sot-probe-auth` дополнительно логинит каждую партицию, подтверждая каждый egress-путь.

Для Railway в `egress_proxy/` лежит отдельный минимальный CONNECT-proxy. Он не
расшифровывает TLS, принимает только `CONNECT` к `auth.zakon.kz:443` и
`sb.prg.kz:443`, не пишет access-логи и не получает публичный домен. SOT-сервис
подключается к нему по private network:

```text
AI_ADVOCAT_SOT_EGRESS_PARTITIONS=[{"id":"railway-eu-01","proxy_env":"AI_ADVOCAT_SOT_PROXY_EU_01"}]
AI_ADVOCAT_SOT_PROXY_EU_01=http://${{ai-advokat-sot-egress-01.RAILWAY_PRIVATE_DOMAIN}}:${{ai-advokat-sot-egress-01.PORT}}
```

Railway сам задает `PORT`; reference variable выше связывает proxy URL с
фактическим private domain и портом сервиса. Дополнительные настройки
имеют безопасные значения по умолчанию: `EGRESS_PROXY_MAX_CONNECTIONS=16`,
`EGRESS_PROXY_HEADER_TIMEOUT_SECONDS=5`, `EGRESS_PROXY_CONNECT_TIMEOUT_SECONDS=15`
и `EGRESS_PROXY_IDLE_TIMEOUT_SECONDS=120`. `EGRESS_PROXY_ALLOWED_HOSTS` можно
сужать, но нельзя добавлять произвольные порты: production-сервис разрешает
только встроенные PRG-хосты по HTTPS/443.

### Живая проверка перед сканом (гейт валидации)

`sot-probe-auth` — единственная команда, которая обращается к источнику и ничего не записывает. Без `--page` код выхода `0` подтверждает только успешный вход в PRG.SOT. С `--page` читается ровно одна страница, и код `0` дополнительно означает, что снятый контракт совпал с ответом источника.

```bash
python3 -m ai_advokat_parser sot-probe-auth
python3 -m ai_advokat_parser sot-probe-auth --page 1
```

### Команды

```text
--out /tmp/ai-advokat-sot sot-scan --scan-id sot-2026-08
```

Скан перечисляет страницы поиска, ставит решения в очередь и докачивает их; повтор той же команды продолжает тот же скан. `--max-pages`/`--max-decisions` дают smoke-run в фазе `paused`, `--retry-failed` явно возвращает в очередь failed/inaccessible/not_found. HTTP 429 останавливает запуск по `Retry-After` вместо продавливания лимита подписки; зависшие `processing` возвращаются в очередь по `--lease-seconds`.

```bash
python3 -m ai_advokat_parser --out /tmp/ai-advokat-sot sot-status --scan-id sot-2026-08
python3 -m ai_advokat_parser --out /tmp/ai-advokat-sot sot-stubs --scan-id sot-2026-08 --output sot-stubs.json
```

`sot-stubs` без `--output` печатает JSON в stdout; заглушки недоступных решений не содержат cookie, токенов и учетных данных.

### Railway: безопасный второй сервис

Тот же образ и та же команда `python -m ai_advokat_parser.railway_worker`, но отдельный Railway-сервис. Сначала безопасный режим, который не обращается к источнику:

```text
AI_ADVOCAT_COMMAND=--out /tmp/ai-advokat-sot sot-status
```

После проверки логина и двухстраничного smoke-run команда полного прохода:

```text
AI_ADVOCAT_COMMAND=--out /tmp/ai-advokat-sot --delay 0 sot-scan --scan-id sot-2026-08 --retry-failed
```

Учетные данные и контракт источника задаются только в Variables и никогда не попадают в `AI_ADVOCAT_COMMAND`. Для Postgres добавь `DATABASE_URL` (или `AI_ADVOCAT_DATABASE_URL`): таблицы `sot_*` создадутся рядом с таблицами ZANGER, а рестарт контейнера продолжит скан с сохраненной страницы. Начинай с `--max-pages 2` и расширяй после проверки `sot-status`. Скан работает в пределах подписки: по умолчанию один worker и остановка по лимиту источника.

## Форматы

Доступные форматы:

- `html` - красивый HTML для просмотра в браузере;
- `txt` - чистый текст;
- `json` - полный сырой ответ API источника со всеми чанками;
- `pdf` - PDF из HTML через установленный Chrome/Chromium.

PDF пример:

```bash
python3 -m ai_advokat_parser --formats html,pdf doc 35502996
```

Если Chrome не найден, укажи путь:

```bash
AI_ADVOCAT_CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
python3 -m ai_advokat_parser --formats html,pdf doc 35502996
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
python3 -m ai_advokat_parser --delay 1.5 range --from-page 1 --to-page 10
```

Параллельная загрузка документов:

```bash
python3 -m ai_advokat_parser --workers 2 --delay 1.0 range --from-page 1 --to-page 5
```

Для больших объемов лучше начинать с маленького теста:

```bash
python3 -m ai_advokat_parser --formats html,txt range --from-page 1 --to-page 1 --max-docs 3
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
python3 -m ai_advokat_parser --formats html,txt,json --follow-links-depth 1 doc 35502996
```

Глубина:

- `0` - не докачивать ссылки;
- `1` - скачать документы, на которые ссылаются выбранные документы;
- `2` - скачать еще и документы из ссылок связанных документов.

Осторожно с глубиной `2+`: граф ссылок может быстро стать очень большим.

Можно ограничить количество документов, добавленных именно из ссылок:

```bash
python3 -m ai_advokat_parser --follow-links-depth 1 --max-linked-docs 100 range --from-page 1 --to-page 1
```

## Важное

Парсер рассчитан на бесплатные документы. По умолчанию он пропускает документы, которые API не помечает как бесплатные. Не используй его для обхода авторизации, платного доступа или ограничений сайта.
