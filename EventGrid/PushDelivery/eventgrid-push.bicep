
param location string = 'UK South'
param namespaceName string = 'ReliableMessagingServiceBus1'
param queueName string = 'ReliableMessagingServiceBus1Queue1'
param topicName string = 'ReliableMessagingServiceBus1Topic1'
param subscriptionName string = 'ReliableMessagingServiceBus1TopicSubscription'
// or https://xyz.ngrok-free.app
param endpointUrl string = 'https://giant-dog-gtqq5wx.devtunnels.ms:8080/api/EventGridEventHandler'

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
              value: 'tunnel eyJhbGciOiJFUzI1NiIsImtpZCI6IkZCM0U2NTMwNjlDQ0I5MUFCQUUxRTNFQjk1RDc5NzdERDQxODM1QjYiLCJ0eXAiOiJKV1QifQ.eyJjbHVzdGVySWQiOiJ1a3MxIiwidHVubmVsSWQiOiJnaWFudC1kb2ctZ3RxcTV3eCIsInNjcCI6ImNvbm5lY3QiLCJleHAiOjE3MzgxMDg5NDAsImlzcyI6Imh0dHBzOi8vdHVubmVscy5hcGkudmlzdWFsc3R1ZGlvLmNvbS8iLCJuYmYiOjE3MzgwMjE2NDB9.-YRQ7gFKCZZFfe5WWy0ansFu_HZnX3YavgAioL7FM7AvL35kEUZlcmgxbozYUr0GN6mPyZ8_FsO7gFancAp38Q'
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
