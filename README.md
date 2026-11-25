# Граф аккаунтов для антифрод-анализа

Сервис загружает события переводов из Kafka в JanusGraph и обеспечивает просмотр графа, поиск аккаунтов и просмотр их переводов в браузере и через HTTP API. HTTP-сервер и Kafka consumer работают в одном Go-процессе и используют общий репозиторий графа. В графе аккаунты представлены вершинами `account`, а каждый перевод — отдельным направленным ребром `transfer` от отправителя к получателю.

## Использованный стек

- **Go, Gin, Viper** — HTTP API и конфигурация сервиса.
- **React, Cytoscape.js, Vite** — интерактивный граф, поиск и история переводов.
- **Apache Kafka 3.9.1, kafka-go** — доставка и обработка событий переводов. Брокер работает в режиме KRaft.
- **JanusGraph 1.1.0, Gremlin** — модель графа и выборка переводов.
- **Apache Cassandra 4.0.20** — постоянное хранилище графа. JanusGraph подключается к Cassandra по CQL.
- **Elasticsearch 8.15.3** — поисковый индекс имён аккаунтов. JanusGraph обновляет индекс при записи свойств вершины.
- **Apache Hadoop 3.4.2 / HDFS** — хранение исходных сообщений Kafka для пакетной обработки.
- **Apache Spark 3.5.8 / PySpark** — расчёт суточных признаков с публикацией результатов в Parquet.
- **Apache Airflow 3.4.0** — запуск и контроль суточного расчёта.
- **Docker, Kubernetes, Helm** — сборка образа API и развёртывание сервиса и базы. Для локального запуска используется Minikube.

## API сервиса

### Полный граф

`GET /api/v1/graph` возвращает объект с массивами `accounts` и `transfers`. Аккаунты без имени отображаются по ID. В массиве переводов сохраняются отдельные рёбра между одними и теми же аккаунтами и переводы себе. При превышении 250 аккаунтов или 500 переводов API возвращает `422 Unprocessable Entity`. [Запрос Gremlin](services/graph-service/internal/repositories/gremlin/graph_snapshot.groovy)

### Поиск аккаунтов

`GET /api/v1/accounts?q={prefix}` возвращает до 20 аккаунтов, в имени которых есть слово с заданным началом. Поиск учитывает латиницу и кириллицу, регистр игнорируется. Параметр `q` должен содержать один префикс от 2 до 100 символов. При неверном параметре сервис возвращает `400 Bad Request`, при отсутствии совпадений — пустой массив `[]`. [Запрос Gremlin](services/graph-service/internal/repositories/gremlin/search_accounts.groovy)

Например, префикс `ан` даёт такой ответ

```json
[{"account_id": "anton", "name": "Антон"}]
```

### История переводов

`GET /api/v1/accounts/{account_id}/transfers` возвращает до 20 последних переводов. Сортировка — по времени от новых к старым, при совпадении времени — по `transaction_id` по возрастанию. Перевод самому себе попадает в ответ один раз. Для неизвестного аккаунта или аккаунта без переводов сервис возвращает `200 OK` и пустой массив `[]`. [Запрос Gremlin](services/graph-service/internal/repositories/gremlin/account_history.groovy) · [Тест выборки](tests/gremlin/account_history_test.groovy)

Пример ответа для `GET /api/v1/accounts/boris/transfers` на демонстрационных данных

```json
[
  {
    "transaction_id": "t5",
    "from_account_id": "anton",
    "to_account_id": "boris",
    "time": "2026-11-26T12:00:00Z",
    "amount_kopecks": 10000
  }
]
```

Время указано в UTC, сумма — в копейках. При ошибке чтения графа сервис возвращает `502 Bad Gateway`, при истечении таймаута HTTP-клиента — `504 Gateway Timeout`.

## Локальный запуск

Для запуска нужны Docker Desktop в режиме Linux-контейнеров, Minikube, Helm и kubectl. Команды выполняются в PowerShell из корня репозитория.

```powershell
minikube start -p wb-graph-lab --driver=docker --kubernetes-version=v1.35.1 --keep-context
docker build -t wb-graph-api:0.7.0 services/graph-service
minikube -p wb-graph-lab image load wb-graph-api:0.7.0
helm --kube-context wb-graph-lab upgrade --install wb-graph ./deployment/helm/wb-graph --namespace wb-graph --create-namespace --wait --timeout 4m
kubectl --context wb-graph-lab -n wb-graph get pods,svc,pvc
kubectl --context wb-graph-lab -n wb-graph port-forward service/wb-graph-api 18080:8080
```

Пока работает `port-forward`, интерфейс доступен по адресу [127.0.0.1:18080](http://127.0.0.1:18080/). На вкладке «Граф» можно менять масштаб, выбирать аккаунты и отдельные переводы, открывать историю выбранного аккаунта и обновлять данные. На вкладке «История» запрос `ве` открывает историю Веры.

HTTP API доступен на том же порту. Примеры запросов из второго терминала

```powershell
Invoke-RestMethod http://127.0.0.1:18080/api/v1/accounts/boris/transfers | Format-Table
$prefix = [uri]::EscapeDataString('ан')
Invoke-RestMethod "http://127.0.0.1:18080/api/v1/accounts?q=$prefix" | Format-Table
```

## Конфигурация сервиса

Viper читает настройки HTTP, Gremlin и Kafka из `config.json` при запуске. В Kubernetes Helm задаёт сетевые адреса и параметры топика через переменные окружения, которые имеют приоритет над JSON. [Файл конфигурации](services/graph-service/config/config.json) · [Загрузка настроек](services/graph-service/internal/config/config.go)

| Переменная | Параметр в JSON |
| --- | --- |
| `WB_GRAPH_HTTP_ADDR` | `http.addr` |
| `WB_GRAPH_GREMLIN_URL` | `gremlin.url` |
| `WB_GRAPH_KAFKA_BROKER` | `kafka.broker` |
| `WB_GRAPH_KAFKA_TOPIC` | `kafka.topic` |
| `WB_GRAPH_KAFKA_GROUP_ID` | `kafka.groupId` |
| `WB_GRAPH_KAFKA_ARCHIVE_GROUP_ID` | `kafka.archiveGroupId` |
| `WB_GRAPH_HDFS_URL` | `hdfs.url` |
| `WB_GRAPH_HDFS_USER` | `hdfs.user` |
| `WB_GRAPH_HDFS_ROOT` | `hdfs.root` |
| `WB_GRAPH_HDFS_TIMEOUT` | `hdfs.timeout` |

## Разработка интерфейса

В каталоге `services/graph-service/frontend` можно запустить `npm ci` и `npm run dev` на Node.js 22.12 или новее. Vite перенаправляет запросы `/api` на локальный Go-сервис на порту 18080. Сборка Docker помещает готовые файлы React в тот же образ, из которого запускаются HTTP API и Kafka consumer.

## Загрузка переводов из Kafka

Топик `transfers.v1` принимает JSON-события о создании перевода. Поля совпадают с форматом ответа API. Идентификатор операции и идентификаторы аккаунтов обязательны, сумма должна быть положительной, время поддерживает точность до миллисекунды. Consumer создаёт отсутствующие аккаунты при записи перевода.

Consumer читает сообщения последовательно из одной партиции и сохраняет offset после подтверждения записи в JanusGraph. Повтор с тем же `transaction_id` и теми же данными подтверждается без вставки. При ошибке, в том числе при некорректном событии, consumer повторяет чтение с сохранённой позиции после паузы. HTTP-сервер продолжает обслуживать запросы. Проверка на дубли рассчитана на одного пишущего consumer. [Обработчик](services/graph-service/internal/consumer/consumer.go) · [Запись в граф](services/graph-service/internal/repositories/gremlin/record_transfer.groovy)

Пример публикации двух копий одного события из PowerShell

```powershell
$event = '{"transaction_id":"kafka-demo-1","from_account_id":"demo-sender","to_account_id":"demo-recipient","time":"2026-11-26T13:00:00Z","amount_kopecks":75000}'
$event, $event | kubectl --context wb-graph-lab -n wb-graph exec -i wb-graph-kafka-0 -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server wb-graph-kafka:9092 --topic transfers.v1 --sync
kubectl --context wb-graph-lab -n wb-graph logs deployment/wb-graph-api -c graph-service
Invoke-RestMethod http://127.0.0.1:18080/api/v1/accounts/demo-recipient/transfers | Format-Table
```

При первом запуске в логе consumer появляются `created` и `replayed` с разными offset. В истории получателя появляется одна операция `kafka-demo-1`.

## Архив сообщений в HDFS

Go-сервис читает `transfers.v1` двумя группами. `graph-ingestion` загружает переводы в JanusGraph, а `graph-archive` сохраняет каждое исходное сообщение в HDFS, включая повторы и сообщения с некорректным содержимым. При первом запуске архива группа читает сообщения с начала доступной истории Kafka.

Каждому offset соответствует файл `/archive/kafka/<topic>/partition-<номер>/<offset>.json`. В нём одна JSON-запись с топиком, партицией, offset, временем Kafka и исходными ключом и значением в Base64. Сервис загружает временный файл через WebHDFS, публикует его переименованием и затем подтверждает offset. Повторная обработка того же offset использует уже опубликованный файл. В стенде на одно сообщение приходится один файл. [Код архива](services/graph-service/internal/archive/hdfs.go)

Пример просмотра файлов и декодирования сообщения с offset 0 в PowerShell

```powershell
$hdfs = @('--context', 'wb-graph-lab', '-n', 'wb-graph', 'exec', 'wb-graph-hdfs-namenode-0', '--', 'hdfs', 'dfs')
kubectl @hdfs -ls /archive/kafka/transfers.v1/partition-0
$raw = kubectl @hdfs -cat /archive/kafka/transfers.v1/partition-0/00000000000000000000.json
$event = $raw | ConvertFrom-Json
[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($event.value_base64))
```

NameNode и DataNode используют отдельные PVC. На одном DataNode коэффициент репликации равен единице. Адрес архива внутри кластера — `hdfs://wb-graph-hdfs-namenode:8020/archive/kafka`. [Шаблон HDFS](deployment/helm/wb-graph/templates/hdfs.yaml)

## Пакетный расчёт в Spark

Spark считает `unique_recipients` по каждому отправителю за календарные сутки UTC. Дата берётся из времени перевода в исходном событии. Повторно доставленные записи удаляются, переводы себе исключаются. Если один ID перевода соответствует разным данным и хотя бы одна его версия попадает в расчётные сутки, запуск завершается ошибкой до публикации.

Результат содержит `feature_date`, `account_id` и `unique_recipients`. Каждый запуск публикует отдельный каталог Parquet с маркером `_SUCCESS` в HDFS. Spark сначала пишет во временный каталог и затем публикует готовый набор переименованием. Повторный запуск с той же датой и ID использует уже опубликованный результат. Для пересчёта с учётом новых событий указывается новый ID. [Расчёт PySpark](deployment/helm/wb-graph/files/spark/daily_recipients.py) · [Шаблон Job](deployment/helm/wb-graph/templates/spark-job.yaml)

Ручной запуск Kubernetes Job на работающем стенде

```powershell
helm template wb-graph ./deployment/helm/wb-graph --namespace wb-graph --show-only templates/spark-job.yaml --set spark.run.enabled=true --set-string spark.run.id=manual-1 --set-string spark.run.date=2026-11-26 | kubectl --context wb-graph-lab -n wb-graph create -f -
kubectl --context wb-graph-lab -n wb-graph wait --for=condition=complete job/wb-graph-spark-manual-1 --timeout=5m
kubectl --context wb-graph-lab -n wb-graph logs job/wb-graph-spark-manual-1
kubectl --context wb-graph-lab -n wb-graph exec wb-graph-hdfs-namenode-0 -- hdfs dfs -ls /features/unique-recipients/2026-11-26/manual-1
```

Spark использует два потока в одном поде (`local[2]`). В демонстрационном архиве повторные сообщения о переводах от `demo-sender` дают трёх уникальных получателей за 26 ноября 2026 года.

## Оркестрация в Airflow

DAG `daily_unique_recipients` ежедневно в 01:00 UTC запускает расчёт за предыдущие календарные сутки UTC. Airflow создаёт Kubernetes Job из общего шаблона Spark, дожидается его завершения и проверяет наличие `_SUCCESS` в HDFS. У каждой попытки свой Job, а ID опубликованного набора сохраняется в пределах одного запуска DAG. [Код DAG](deployment/helm/wb-graph/files/airflow/daily_recipients_dag.py) · [Общий шаблон Spark Job](deployment/helm/wb-graph/templates/_spark.tpl)

Ручной запуск с явно заданной датой расчёта

```powershell
$af = @('--context', 'wb-graph-lab', '-n', 'wb-graph', 'exec', 'wb-graph-airflow-0', '--', 'airflow')
kubectl @af dags trigger daily_unique_recipients --run-id manual-2026-11-26 --conf '{"feature_date":"2026-11-26"}'
kubectl @af tasks states-for-dag-run daily_unique_recipients manual-2026-11-26
```

Airflow работает в режиме `standalone` с SQLite на отдельном PVC. Расписание задаётся параметром `airflow.schedule` в Helm. Веб-интерфейс можно открыть через проброс порта на [127.0.0.1:18081](http://127.0.0.1:18081/). Airflow генерирует пароль пользователя `admin` при первом запуске.

```powershell
kubectl --context wb-graph-lab -n wb-graph exec wb-graph-airflow-0 -- cat /opt/airflow/state/simple_auth_manager_passwords.json.generated
kubectl --context wb-graph-lab -n wb-graph port-forward service/wb-graph-airflow 18081:8080
```

## Хранение данных

Cassandra развёрнута как одноузловой StatefulSet. Для её каталога данных Kubernetes выделяет PersistentVolume через PVC `data-wb-graph-cassandra-0`. Граф сохраняется при пересоздании подов JanusGraph и Cassandra, пока существует том. Cassandra передаёт драйверу адрес Kubernetes Service `wb-graph-cql`, который сохраняется при смене IP пода. [Шаблон Cassandra](deployment/helm/wb-graph/templates/cassandra.yaml)

При запуске JanusGraph создаёт mixed index `accountsByName` для свойства `account_name`. Если включён `janusgraph.seedDemo`, в пустой граф загружаются три аккаунта и пять переводов. В существующем графе скрипт заполняет отсутствующие имена этих трёх аккаунтов. Аккаунты, созданные из событий Kafka, получают только ID и появляются в поиске после заполнения имени. Elasticsearch обновляет поисковую выдачу после обновления индекса. [Схема индекса](deployment/helm/wb-graph/files/account_search_schema.groovy) · [Демонстрационные данные](deployment/helm/wb-graph/files/seed_history.groovy)

Elasticsearch развёрнут как одноузловой StatefulSet с постоянным томом для индекса. Сервис Elasticsearch доступен внутри локального Kubernetes, встроенная аутентификация отключена в настройках стенда. [Шаблон Elasticsearch](deployment/helm/wb-graph/templates/elasticsearch.yaml)

При изменении Go-кода образ нужно собрать с новым тегом, загрузить в Minikube и передать в `helm upgrade` параметр `--set api.image=wb-graph-api:<тег>`. После замены пода API потребуется снова запустить `port-forward`.

## План развития

- **Обработка событий.** Добавить отдельный топик для некорректных сообщений и обработку изменений статуса перевода.
