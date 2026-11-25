# Граф аккаунтов для антифрод-анализа

Сервис загружает события переводов из Kafka в JanusGraph и обеспечивает доступ к входящим и исходящим переводам аккаунта через HTTP API. HTTP-сервер и Kafka consumer работают в одном Go-процессе и используют общий репозиторий графа. В графе аккаунты представлены вершинами `account`, а каждый перевод — отдельным направленным ребром `transfer` от отправителя к получателю.

## Использованный стек

- **Go, Gin, Viper** — HTTP API и конфигурация сервиса.
- **Apache Kafka 3.9.1, kafka-go** — доставка и обработка событий переводов. Брокер работает в режиме KRaft.
- **JanusGraph 1.1.0, Gremlin** — модель графа и выборка переводов.
- **Apache Cassandra 4.0.20** — постоянное хранилище графа. JanusGraph подключается к Cassandra по CQL.
- **Docker, Kubernetes, Helm** — сборка образа API и развёртывание сервиса и базы. Для локального запуска используется Minikube.

## API сервиса

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
docker build -t wb-graph-api:0.3.0 services/graph-service
minikube -p wb-graph-lab image load wb-graph-api:0.3.0
helm --kube-context wb-graph-lab upgrade --install wb-graph ./deployment/helm/wb-graph --namespace wb-graph --create-namespace --wait --timeout 4m
kubectl --context wb-graph-lab -n wb-graph get pods,svc,pvc
kubectl --context wb-graph-lab -n wb-graph port-forward service/wb-graph-api 18080:8080
```

Пока работает `port-forward`, API доступен на порту 18080. Пример запроса из второго терминала

```powershell
Invoke-RestMethod http://127.0.0.1:18080/api/v1/accounts/boris/transfers | Format-Table
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

## Хранение данных

Cassandra развёрнута как одноузловой StatefulSet. Для её каталога данных Kubernetes выделяет PersistentVolume через PVC `data-wb-graph-cassandra-0`. Граф сохраняется при пересоздании подов JanusGraph и Cassandra, пока существует том. Cassandra передаёт драйверу адрес Kubernetes Service `wb-graph-cql`, который сохраняется при смене IP пода. [Шаблон Cassandra](deployment/helm/wb-graph/templates/cassandra.yaml)

При запуске JanusGraph загружает три аккаунта и пять переводов, если граф пуст. Параметр `--set janusgraph.seedDemo=false` отключает эту загрузку. При обновлении с версии стенда с хранилищем в памяти Cassandra создаётся с новым графом, а демонстрационные данные загружаются из скрипта. [Демонстрационные данные](deployment/helm/wb-graph/files/seed_history.groovy) · [Настройки чарта](deployment/helm/wb-graph/values.yaml)

При изменении Go-кода образ нужно собрать с новым тегом, загрузить в Minikube и передать в `helm upgrade` параметр `--set api.image=wb-graph-api:<тег>`. После замены пода API потребуется снова запустить `port-forward`.

## План развития

- **Обработка событий.** Добавить отдельный топик для некорректных сообщений и обработку изменений статуса перевода.
- **Elasticsearch и React.** Настроить поиск аккаунтов по имени через индекс JanusGraph и показать связи найденного аккаунта в браузере.
- **Hadoop HDFS, Spark и Airflow.** Сохранять события в HDFS, считать в Spark число уникальных получателей по каждому аккаунту за сутки и запускать расчёт по расписанию через Airflow.
