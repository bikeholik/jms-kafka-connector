package com.github.bikeholik.kafka.connector.jms;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.destination.DestinationResolver;

@Configuration
@ComponentScan
public class JmsConnectorConfig {
    @Autowired
    private ConnectionFactory connectionFactory;

    @Bean
    JmsTemplate jmsTemplate(JmsConnectorConfigurationProperties properties) {
        JmsTemplate jmsTemplate = new JmsTemplate(connectionFactory);
        jmsTemplate.setSessionTransacted(properties.isSessionTransacted());
        // TODO QoS
        return jmsTemplate;
    }

    private static class TopicsMappingHolder {
        private final Map<String, Destination> topicsMapping;

        @Autowired
        private TopicsMappingHolder(
                JmsTemplate jmsTemplate,
                DestinationResolver destinationResolver,
                JmsConnectorConfigurationProperties properties) {
            topicsMapping = jmsTemplate.execute(session -> {
                return properties.getTopicToJmsQueue().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> getDestination(destinationResolver, session, e.getKey(), false)));
            });
        }

        private Destination getDestination(DestinationResolver destinationResolver, Session session, String destinationName, boolean pubSubDomain) {
            try {
                return destinationResolver.resolveDestinationName(session, destinationName, pubSubDomain);
            } catch (JMSException e1) {
                throw new IllegalArgumentException(e1);
            }
        }
    }
}
