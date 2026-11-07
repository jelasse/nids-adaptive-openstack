#!/usr/bin/env bash
# Start Kafka in the VM with the protocol's 512 MB heap (never start it with kafka-server-start.sh alone:
# that gives the default 1 GB). Usage (laptop): ssh -i ~/.ssh/testbed_key ubuntu@<VM> 'bash ~/nids/stream/kafka_start.sh'
K=/opt/kafka/bin
if ss -ltn | grep -q ':9092 '; then
  pgrep -af kafka.Kafka | grep -q -- '-Xmx512M' && { echo "Kafka already running with 512 MB"; exit 0; }
  echo "Kafka running with another heap: stopping"; $K/kafka-server-stop.sh
  for i in $(seq 1 30); do ss -ltn | grep -q ':9092 ' || break; sleep 2; done
fi
KAFKA_HEAP_OPTS="-Xmx512M -Xms512M" $K/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
for i in $(seq 1 30); do ss -ltn | grep -q ':9092 ' && break; sleep 2; done
pgrep -af kafka.Kafka | grep -o -- '-Xmx[0-9]*[MG]' | head -1 | sed 's/^/Kafka started with heap /'
