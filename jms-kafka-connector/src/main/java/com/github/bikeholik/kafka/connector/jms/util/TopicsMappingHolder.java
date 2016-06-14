package com.github.bikeholik.kafka.connector.jms.util;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.github.bikeholik.kafka.connector.jms.JmsConnectorConfigurationProperties;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.destination.DestinationResolver;
import org.springframework.stereotype.Component;

@Component
public class TopicsMappingHolder {
    private final Map<String, Destination> topicsMapping;
    private final Map<String, String> jmsDestinationsMapping;

    @Autowired
    TopicsMappingHolder(
            JmsTemplate jmsTemplate,
            DestinationResolver destinationResolver,
            JmsConnectorConfigurationProperties properties) {
        // TODO delay resolution in case broker is not available ?
        topicsMapping = jmsTemplate.execute(session -> {
            return properties.getTopicToJmsQueue().entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> getDestination(destinationResolver, session, e.getValue(), false)));
        });
        jmsDestinationsMapping = properties.getTopicToJmsQueue().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        LoggerFactory.getLogger(getClass()).info("mappings={}", topicsMapping);
    }

    private Destination getDestination(DestinationResolver destinationResolver, Session session, String destinationName, boolean pubSubDomain) {
        try {
            return destinationResolver.resolveDestinationName(session, destinationName, pubSubDomain);
        } catch (JMSException e1) {
            throw new IllegalArgumentException(e1);
        }
    }

    public Optional<Destination> getDestination(String topicName) {
        return Optional.ofNullable(topicsMapping.get(topicName));
    }

    public Optional<String> getTopic(String queueName) {
        return Optional.ofNullable(jmsDestinationsMapping.get(queueName));
    }
}
