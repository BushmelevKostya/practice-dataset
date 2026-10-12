# Практическое занятие 2.

**Группа Р4150**

- Буров С.А.
- Бушмелёв К.А.
- Садовая А.Р.
- Халдин Д.А.

### 1. Интерфейсы

![Разработанные интерфейсы](src/main/resources/telegram-cloud-photo-size-2-5361807567576834527-x.jpg)

### 2. Загрузка и предварительная обработка данных

![Разработанные интерфейсы](src/main/resources/img.png)

## Запуск

Сначала собрать jar (из корня проекта):

```bash
./gradlew jar
```

После любого изменения кода jar нужно пересобрать.

### Через Docker Compose

Spark устанавливать не нужно:

```bash
docker compose up --exit-code-from spark-submit
```

Поднимаются Spark master и worker, затем на кластере выполняется задача. После её завершения всё останавливается. Результаты выводятся в строках `spark-submit-1` вместе с логами Spark.

Чтобы видеть только результаты:

```bash
docker compose up --exit-code-from spark-submit 2>&1 | grep '^spark-submit' | grep -vE ' (INFO|WARN) '
```

### Через spark-submit

Нужен установленный Spark 4.x на Java 21:

```bash
spark-submit --class org.itmo.Main --master "local[*]" build/libs/practice-dataset-1.0-SNAPSHOT.jar data.csv
```

Последний аргумент — путь к CSV-файлу с данными.

Запуск `Main` напрямую из IDE не работает: Spark подключён как `compileOnly` и запускается через `spark-submit`.
