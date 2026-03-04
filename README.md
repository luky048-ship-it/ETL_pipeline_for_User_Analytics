# ETL_pipeline_for_User_Analytics


# 1. Клонировать/создать структуру проекта
mkdir -p airflow-etl-project/{dags,config,plugins,scripts}
cd airflow-etl-project

# 2. Создать файлы (.env, requirements.txt, Dockerfile, docker-compose.yml)

# 3. Сгенерировать Fernet-ключ и добавить в .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 4. Запустить стек
docker compose up -d --build

# 5. Проверить статус
docker compose ps

# 6. Войти в Airflow UI
# http://localhost:8080
# Логин: airflow_admin / Пароль: из .env

# 7. (Опционально) Запустить Mongo Express для отладки
docker compose --profile dev up -d mongo-express
# http://localhost:8081
