#!/usr/bin/env bash
# Interactive demo of the Kafka features. Run step by step:
#   ./scripts/demo.sh topics | groups | scale | broker-down | broker-up | dlq | api | lag
set -euo pipefail
K="docker exec kafka-1"
BS="kafka-1:29092"

case "${1:-help}" in
  topics)      # partitions, leaders, replicas, ISR
    $K kafka-topics --bootstrap-server $BS --describe --topic transactions.raw ;;

  groups)      # which scorer instance owns which partition + lag
    $K kafka-consumer-groups --bootstrap-server $BS --describe --group fraud-scorer
    $K kafka-consumer-groups --bootstrap-server $BS --describe --group fraud-alert-notifier ;;

  scale)       # dynamic membership: add consumers, watch the REBALANCE logs
    N="${2:-6}"
    docker compose up -d --scale scorer="$N" --no-recreate
    sleep 15
    docker compose logs --since 30s scorer | grep REBALANCE || true
    $K kafka-consumer-groups --bootstrap-server $BS --describe --group fraud-scorer ;;

  broker-down) # fault tolerance: kill one broker, system keeps running
    docker stop kafka-2
    sleep 10
    docker exec kafka-1 kafka-topics --bootstrap-server $BS --describe --topic transactions.raw
    echo ">>> Leaders moved to kafka-1/kafka-3, ISR shrank to 2 = min.insync.replicas -> writes still OK"
    docker compose logs --since 20s producer | tail -5 ;;

  broker-up)
    docker start kafka-2
    sleep 20
    $K kafka-topics --bootstrap-server $BS --describe --topic transactions.raw
    echo ">>> kafka-2 caught up and is back in the ISR" ;;

  dlq)         # send a poison message, watch it land in the DLQ
    echo 'C000000001|{"this is": "not a valid transaction"}' | \
      docker exec -i kafka-1 kafka-console-producer --bootstrap-server $BS \
        --topic transactions.raw --property parse.key=true --property key.separator='|'
    sleep 3
    docker exec kafka-1 kafka-console-consumer --bootstrap-server $BS --topic transactions.dlq \
      --from-beginning --max-messages 1 --property print.key=true --property print.headers=true ;;

  api)         # ingest through the API; same account always -> same partition
    for i in 1 2 3; do
      curl -s -X POST localhost:8000/v1/transactions -H 'content-type: application/json' -d '{
        "step": 30, "type": "TRANSFER", "amount": 250000, "nameOrig": "C123456789",
        "oldbalanceOrg": 250000, "newbalanceOrig": 0, "nameDest": "C987654321",
        "oldbalanceDest": 0, "newbalanceDest": 0}'; echo
    done ;;

  lag)
    watch -n 2 "docker exec kafka-1 kafka-consumer-groups --bootstrap-server $BS --describe --group fraud-scorer" ;;

  *)
    sed -n '2,4p' "$0" ;;
esac
