FROM apache/superset:latest

USER root

RUN pip install --no-cache-dir --target /app/.venv/lib/python3.10/site-packages \
    "trino[sqlalchemy]==0.328.0" \
    "sqlalchemy-trino==0.5.0" \
    "psycopg2-binary==2.9.9"

RUN mkdir -p /app/superset_home && \
    chown -R superset:superset /app/superset_home && \
    chmod -R 775 /app/superset_home

RUN mkdir -p /app/pythonpath && \
    echo "SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://postgres:postgres@postgres:5432/superset'" \
    > /app/pythonpath/superset_config.py
    # echo "SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'" \
    # > /app/pythonpath/superset_config.py && \
    # chown -R superset:superset /app/pythonpath

USER superset