#!/bin/sh
set -e

if [ "${RUN_MIGRATIONS:-0}" = "1" ]; then
  echo "Applying Alembic migrations"
  alembic upgrade head
fi

exec "$@"
