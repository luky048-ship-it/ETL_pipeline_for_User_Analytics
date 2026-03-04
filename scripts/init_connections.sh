#!/bin/bash
# Скрипт для создания Airflow Connections после инициализации

echo "Waiting for Airflow to be ready..."
sleep 10

echo "Creating MongoDB connection..."
airflow connections add 'mongo_source' \
  --conn-type 'mongo' \
  --conn-login 'mongo_admin' \
  --conn-password 'mongo_pass' \
  --conn-host 'mongodb' \
  --conn-port '27017' \
  --conn-schema 'etl_source_data' \
  --conn-extra '{"authSource": "admin"}' || echo "Connection already exists"

echo "Creating PostgreSQL DWH connection..."
airflow connections add 'postgres_dwh' \
  --conn-type 'postgres' \
  --conn-login 'dwh_admin' \
  --conn-password 'dwh_pass' \
  --conn-host 'postgres_dwh' \
  --conn-port '5432' \
  --conn-schema 'analytics_dwh' || echo "Connection already exists"

echo "Connections created successfully!"
