#!/bin/bash
if [ -z "$1" ]; then
    echo "Usage: $0 \"your query text\""
    exit 1
fi

echo "Searching for query: $1"

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

if [ ! -f /app/.venv.tar.gz ]; then
    echo "Error: Archive /app/.venv.tar.gz does not exist."
    exit 1
fi

spark-submit --master yarn \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 \
  --conf spark.cassandra.connection.host=cassandra-host \
  --archives /app/.venv.tar.gz#.venv \
  query.py "$1"
