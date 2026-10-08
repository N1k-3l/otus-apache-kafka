# Homework 5

Развернул Kafka Connect и настроил интеграцию с PostgreSQL с использованием Debezium PostgreSQL CDC Source Connector. Вся инфра в Docker. Коннектор успешно отслеживает WAL-лог базы данных и передаёт разницу из тестовой таблицы `customers` в топик `cdc.public.customers` в формате JSON-событий в реальном времени.

