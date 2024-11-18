#!/bin/bash

echo "Waiting for Kafka to be ready..."
until nc -z kafka 9093; do
    echo "Kafka is unavailable - sleeping"
    sleep 1
done
echo "Kafka is up - executing command"
exec "$@" 