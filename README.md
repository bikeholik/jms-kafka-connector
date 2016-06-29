# jms-kafka-connector

## Concept

Drop main jar (jms-kafka-connect.jar) together with broker specific connection provider jar to kafka lib directory, configure the tasks specifying, among other settings, the package which should be scanned by *Spring* in order to find ConnectionFactory bean.

Sample source connector configuration:
```
name=jms-source
connector.class=com.github.bikeholik.kafka.connector.jms.source.JmsSourceConnector
tasks.max=3
topics=connect-jms-test
component-packages=com.github.bikeholik.kafka.connector.hornetq
jms.topicToJmsQueue.connect-jms-test=test
jms.messageReceiverClass=com.github.bikeholik.kafka.connector.jms.source.ListeningContainerMessageReceiver
jms.sessionTransacted=true
hornetq.cluster=localhost:5457
hornetq.user=admin
hornetq.password=changeme
queues=test

```
 
## Known issues
 - Transactional source connector will not  work with kafka 0.9.0.x because of https://issues.apache.org/jira/browse/KAFKA-3225