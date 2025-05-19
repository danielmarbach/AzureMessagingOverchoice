
param location string = resourceGroup().location
param namespaceName string = 'ndcosloservicebus1prem'
param queueName string = 'queue'
param topicName string = 'topic'
param subscriptionName string = 'subscription'
// or https://xyz.ngrok-free.app
param endpointUrl string = 'https://tidy-fog-1xpvksk.euw.devtunnels.ms:8080/api/EventGridEventHandler'

resource ServiceBus 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' existing = {
  name: namespaceName
}

resource queue 'Microsoft.ServiceBus/namespaces/queues@2022-10-01-preview' = {
  parent: ServiceBus
  name: queueName
  properties: {
    maxSizeInMegabytes: 1024
  }
}

resource EventGridTopic 'Microsoft.EventGrid/systemTopics@2023-06-01-preview' = {
  properties: {
    source: ServiceBus.id
    topicType: 'Microsoft.ServiceBus.Namespaces'
  }
  identity: {
    type: 'None'
  }
  location: location
  tags: {}
  name: topicName
}

resource EventGridSubscription 'Microsoft.EventGrid/systemTopics/eventSubscriptions@2023-06-01-preview' = {
  properties: {
    destination: {
      endpointType: 'WebHook'
      properties: {
        maxEventsPerBatch: 1
        preferredBatchSizeInKilobytes: 64
        endpointUrl: endpointUrl
        deliveryAttributeMappings: [
          {
            name: 'X-Tunnel-Authorization'
            properties: {
              isSecret: true
              value: 'tunnel eyJhbGciOiJFUzI1NiIsImtpZCI6IkM3NDYxNEM5OTE0NjUwNzI2REI1RUZBM0M1OTBDQzdGNjJFOUI4QzQiLCJ0eXAiOiJKV1QifQ.eyJjbHVzdGVySWQiOiJldXciLCJ0dW5uZWxJZCI6InRpZHktZm9nLTF4cHZrc2siLCJzY3AiOiJjb25uZWN0IiwiZXhwIjoxNzQ3MTYyOTAyLCJpc3MiOiJodHRwczovL3R1bm5lbHMuYXBpLnZpc3VhbHN0dWRpby5jb20vIiwibmJmIjoxNzQ3MDc1NjAyfQ.bp63yHM_2YeJiyDaA9hp268iZcldpVxNmTBn9jI52M8V5INpEvKbUcGPKxy73DGLj3FchK4d2MrrTEoWncxy9Q'
            }
            type: 'Static'
          }
        ]
      }
    }
    filter: {
      includedEventTypes: [
        'Microsoft.ServiceBus.ActiveMessagesAvailableWithNoListeners'
      ]
      enableAdvancedFilteringOnArrays: true
      advancedFilters: [
        {
          values: [
            queueName
          ]
          operatorType: 'StringIn'
          key: 'data.QueueName'
        }
      ]
    }
    eventDeliverySchema: 'CloudEventSchemaV1_0'
    retryPolicy: {
      maxDeliveryAttempts: 30
      eventTimeToLiveInMinutes: 1440
    }
  }
  name: subscriptionName
  parent: EventGridTopic
}
