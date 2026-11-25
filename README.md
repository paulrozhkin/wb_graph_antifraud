# Граф аккаунтов для антифрод-анализа

Сервис обеспечивает доступ к входящим и исходящим переводам аккаунта через HTTP API. В JanusGraph аккаунты представлены вершинами `account`, а каждый перевод — отдельным направленным ребром `transfer` от отправителя к получателю.

## Использованный стек

- **Go, Gin** — HTTP API.
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
docker build -t wb-graph-api:0.1.0 services/graph-service
minikube -p wb-graph-lab image load wb-graph-api:0.1.0
helm --kube-context wb-graph-lab upgrade --install wb-graph ./deployment/helm/wb-graph --namespace wb-graph --create-namespace --wait --timeout 4m
kubectl --context wb-graph-lab -n wb-graph get pods,svc,pvc
kubectl --context wb-graph-lab -n wb-graph port-forward service/wb-graph-api 18080:8080
```

Пока работает `port-forward`, API доступен на порту 18080. Запрос из второго терминала:

```powershell
Invoke-RestMethod http://127.0.0.1:18080/api/v1/accounts/boris/transfers | Format-Table
```

## Хранение данных

Cassandra развёрнута как одноузловой StatefulSet. Для её каталога данных Kubernetes выделяет PersistentVolume через PVC `data-wb-graph-cassandra-0`. Граф сохраняется при пересоздании подов JanusGraph и Cassandra, пока существует том. Cassandra передаёт драйверу адрес Kubernetes Service `wb-graph-cql`, который сохраняется при смене IP пода. [Шаблон Cassandra](deployment/helm/wb-graph/templates/cassandra.yaml)

При запуске JanusGraph загружает три аккаунта и пять переводов, если граф пуст. Параметр `--set janusgraph.seedDemo=false` отключает эту загрузку. При обновлении с версии стенда с хранилищем в памяти Cassandra создаётся с новым графом, а демонстрационные данные загружаются из скрипта. [Демонстрационные данные](deployment/helm/wb-graph/files/seed_history.groovy) · [Настройки чарта](deployment/helm/wb-graph/values.yaml)

При изменении Go-кода образ нужно собрать с новым тегом, загрузить в Minikube и передать в `helm upgrade` параметр `--set api.image=wb-graph-api:<тег>`. После замены пода API потребуется снова запустить `port-forward`.

## План развития

- **Kafka.** Принимать события переводов и обеспечить идемпотентную запись в граф при повторной доставке.
- **Elasticsearch и React.** Настроить поиск аккаунтов по имени через индекс JanusGraph и показать связи найденного аккаунта в браузере.
- **Hadoop HDFS, Spark и Airflow.** Сохранять события в HDFS, считать в Spark число уникальных получателей по каждому аккаунту за сутки и запускать расчёт по расписанию через Airflow.
