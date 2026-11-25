# Граф аккаунтов для антифрод-анализа

Я собрал этот стенд, чтобы разобраться, как использовать графовую базу в антифроде. Для начала хотел по одному аккаунту увидеть, кому он отправлял деньги и от кого их получал. На синтетических данных я описал связи в JanusGraph, написал обход на Gremlin и подключил его к HTTP API на Go. Сервис и базу развернул в локальном Kubernetes через Helm.

## Как устроен запрос

Аккаунт в графе — вершина `account`. Каждый перевод — отдельное ребро `transfer` от отправителя к получателю. На ребре хранятся идентификатор операции, время и сумма в копейках.

`GET /api/v1/accounts/{account_id}/transfers` возвращает последние 20 операций аккаунта. Для этого я использовал шаг Gremlin `bothE('transfer')`, который выбирает входящие и исходящие рёбра. При переводе самому себе одно ребро встречается с двух сторон. Следующий шаг `dedup()` убирает повтор. Две операции на одну сумму между теми же аккаунтами сохраняются в выдаче. Такое поведение и порядок результатов при одинаковом времени я проверил в тесте. [Запрос Gremlin](services/graph-service/internal/repositories/gremlin/account_history.groovy) · [Тест на JanusGraph](tests/gremlin/account_history_test.groovy)

## Локальный запуск

Я проверил стенд на Kubernetes 1.35.1 в Minikube с Docker Desktop в режиме Linux-контейнеров. Команды выполняются в PowerShell из корня репозитория. Понадобятся Docker, Minikube, Helm и kubectl.

```powershell
minikube start -p wb-graph-lab --driver=docker --kubernetes-version=v1.35.1 --keep-context
docker build -t wb-graph-api:0.1.0 services/graph-service
minikube -p wb-graph-lab image load wb-graph-api:0.1.0
helm --kube-context wb-graph-lab upgrade --install wb-graph ./deployment/helm/wb-graph --namespace wb-graph --create-namespace --wait --timeout 4m
kubectl --context wb-graph-lab -n wb-graph get pods,svc
kubectl --context wb-graph-lab -n wb-graph port-forward service/wb-graph-api 18080:8080
```

Пока работает `port-forward`, в другом терминале можно запросить тестовый аккаунт:

```powershell
Invoke-RestMethod http://127.0.0.1:18080/api/v1/accounts/vera/transfers | Format-Table
```

Ожидаемый результат — четыре строки в порядке `t3, t4, t2, t1`. У `t4` отправитель и получатель совпадают. У `t2` и `t1` совпадают участники и сумма, но разные идентификаторы операций.

## Данные стенда

JanusGraph 1.1.0 хранит граф в памяти. При старте контейнера загружаются три аккаунта и пять переводов. Добавленные вручную данные живут до перезапуска JanusGraph. Для запуска с пустым графом к команде Helm можно добавить `--set janusgraph.seedDemo=false`. [Тестовые данные](deployment/helm/wb-graph/files/seed_history.groovy) · [Настройки чарта](deployment/helm/wb-graph/values.yaml)

При изменении Go-кода образ нужно собрать с новым тегом, загрузить в Minikube и передать в `helm upgrade` параметр `--set api.image=wb-graph-api:<тег>`. После замены пода API потребуется снова запустить `port-forward`.

## Что хочу проверить дальше

- **Cassandra и Kafka.** Подключить Cassandra как хранилище JanusGraph, загружать события из Kafka и проверить, что происходит при повторной доставке перевода.
- **Elasticsearch и React.** Настроить поиск аккаунтов по имени через индекс JanusGraph и показать связи найденного аккаунта в браузере.
- **Hadoop HDFS, Spark и Airflow.** Сохранять события в HDFS, считать в Spark число уникальных получателей по каждому аккаунту за сутки и запускать расчёт по расписанию через Airflow.
