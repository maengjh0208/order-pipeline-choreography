#!/bin/sh
set -e

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:19092}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"

create_topic() {
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$1" --partitions "$PARTITIONS" --replication-factor 1
}

create_topic order.events

echo "--- topics ---"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
