# jms-kafka-connector

## Concept

Drop main jar (jms-kafka-connect.jar) together with broker specific connection provider jar to kafka lib directory, configure the tasks specifying, among other settings, the package which should be scanned by *Spring* in order to find ConnectionFactory bean. 