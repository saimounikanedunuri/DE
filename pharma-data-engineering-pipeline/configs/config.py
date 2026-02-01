# config.py

PIPELINE = {
    "layer": "landing",
    "run_date": "2025-01-01",

    "database": {
        "type": "postgres",
        "host": "db.company.internal",
        "port": 5432,
        "database": "orders_db",
        "user": "etl_user",
        "password": "******",
        "tables": ["orders", "customers"]
    },

    "files": {
        "format": "csv",
        "path": "/raw/files",
        "header": True
    },

    "paths": {
        "landing": "data/landing",
        "silver": "data/silver",
        "gold": "data/gold"
    },

    "load": {
        "mode": "FULL"
    },

    "schema": {
        "orders": {
            "order_id": "string",
            "customer_id": "string",
            "amount": "decimal(10,2)",
            "created_date": "date"
        },
        "customers": {
            "customer_id": "string",
            "country": "string"
        }
    },

    "quality": {
        "required_columns": ["order_id", "customer_id", "amount", "created_date"],
        "not_null": ["order_id", "customer_id"]
    }
}

# Paths (used across jobs)
LANDING_PATHS = {
    "orders": "data/landing/orders",
    "customers": "data/landing/customers"
}

SILVER_PATHS = {
    "orders": "data/silver/orders",
    "customers": "data/silver/customers"
}

GOLD_PATHS = {
    "country_metrics": "data/gold/country_metrics",
    "order_segments": "data/gold/order_segments"
}

SCHEMA_METADATA = PIPELINE["schema"]

# Ingestion configs
FILE_PATHS = {
    "orders": "/raw/files/orders.csv",
    "customers": "/raw/files/customers.csv"
}

DB_CONFIG = {
    "url": "jdbc:postgresql://db.company.internal:5432/orders_db",
    "user": "etl_user",
    "password": "******",
    "tables": ["orders", "customers"]
}

KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "topics": ["orders_topic"]
}
