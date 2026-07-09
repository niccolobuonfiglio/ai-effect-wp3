import os

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "tef-clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "V0ltF3@t5?store")
