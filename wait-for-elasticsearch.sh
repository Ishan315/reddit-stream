#!/bin/bash

echo "Waiting for Elasticsearch..."
until curl -s http://elasticsearch:9200 >/dev/null; do
    echo "Elasticsearch is unavailable - sleeping"
    sleep 1
done
echo "Elasticsearch is up - executing command"
exec "$@"