#!/bin/bash

# Usage check
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <resource-group> <namespace-name>"
    exit 1
fi

RESOURCE_GROUP="$1"
NAMESPACE="$2"
MAX_PARALLEL=10

echo "Starting deletion of queues and topics in namespace: $NAMESPACE"

# Helper to control parallel job count
function wait_for_jobs() {
    while (( $(jobs -r | wc -l) >= MAX_PARALLEL )); do
        sleep 1
    done
}

# Delete queues in parallel
queues=$(az servicebus queue list --resource-group "$RESOURCE_GROUP" --namespace-name "$NAMESPACE" --query "[].name" -o tsv)

for queue in $queues; do
    wait_for_jobs
    echo "Scheduling deletion of queue: $queue"
    (
        az servicebus queue delete --resource-group "$RESOURCE_GROUP" --namespace-name "$NAMESPACE" --name "$queue" &&
        echo "✅ Deleted queue: $queue" || echo "❌ Failed to delete queue: $queue"
    ) &
done

# Delete topics in parallel
topics=$(az servicebus topic list --resource-group "$RESOURCE_GROUP" --namespace-name "$NAMESPACE" --query "[].name" -o tsv)

for topic in $topics; do
    wait_for_jobs
    echo "Scheduling deletion of topic: $topic"
    (
        az servicebus topic delete --resource-group "$RESOURCE_GROUP" --namespace-name "$NAMESPACE" --name "$topic" &&
        echo "✅ Deleted topic: $topic" || echo "❌ Failed to delete topic: $topic"
    ) &
done

# Wait for all deletions to complete
wait

echo "🎉 All queues and topics deleted from namespace '$NAMESPACE'."