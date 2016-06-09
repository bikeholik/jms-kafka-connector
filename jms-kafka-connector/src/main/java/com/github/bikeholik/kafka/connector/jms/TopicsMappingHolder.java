package com.github.bikeholik.kafka.connector.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.destination.DestinationResolver;
import org.springframework.stereotype.Component;

/**
 * TODO comment
 */
@Component
class TopicsMappingHolder {
    private final Map<String, Destination> topicsMapping;

    @Autowired
    TopicsMappingHolder(
            JmsTemplate jmsTemplate,
            DestinationResolver destinationResolver,
            JmsConnectorConfigurationProperties properties) {
        topicsMapping = jmsTemplate.execute(session -> {
            return properties.getTopicToJmsQueue().entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> getDestination(destinationResolver, session, e.getKey(), false)));
        });
        LoggerFactory.getLogger(getClass()).info("mappings={}", topicsMapping);
    }

    private Destination getDestination(DestinationResolver destinationResolver, Session session, String destinationName, boolean pubSubDomain) {
        try {
            return destinationResolver.resolveDestinationName(session, destinationName, pubSubDomain);
        } catch (JMSException e1) {
            throw new IllegalArgumentException(e1);
        }
    }
}
