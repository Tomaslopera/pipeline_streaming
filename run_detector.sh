#!/bin/bash
# Lanza detector.py dentro del contenedor "spark" (Caso B: e-commerce).
# Requiere que `docker compose up -d` ya este corriendo.
set -e

COMPOSE="docker compose"
if ! docker compose version >/dev/null 2>&1; then
  COMPOSE="docker-compose"
fi

$COMPOSE exec spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /app/detector.py
