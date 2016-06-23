package com.github.bikeholik.kafka.connector.jms.source;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.github.bikeholik.kafka.connector.jms.JmsConnectorConfigurationProperties;
import com.github.bikeholik.kafka.connector.jms.util.TopicsMappingHolder;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class SimpleMessageReceiver implements AutoCloseable, MessageReceiver {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Session session;
    private final JmsConnectorConfigurationProperties connectorConfigurationProperties;
    private final TopicsMappingHolder topicsMappingHolder;
    private final String destinationName;
    private final MessageConsumer messageConsumer;
    private final Connection connection;

    @Autowired
    SimpleMessageReceiver(ConnectionFactory connectionFactory, JmsConnectorConfigurationProperties connectorConfigurationProperties, TopicsMappingHolder topicsMappingHolder, String destinationName) throws JMSException {
        this.connectorConfigurationProperties = connectorConfigurationProperties;
        this.topicsMappingHolder = topicsMappingHolder;
        this.destinationName = destinationName;
        this.connection = connectionFactory.createConnection();
        // TODO ack type
        this.connection.start();
        this.session = connection.createSession(connectorConfigurationProperties.isSessionTransacted(), 0);
        this.messageConsumer = topicsMappingHolder.getTopic(destinationName)
                .flatMap(topicsMappingHolder::getDestination)
                .map(destination -> {
                    try {
                        return this.session.createConsumer(destination);
                    } catch (JMSException e) {
                        throw new RuntimeException(e);
                    }
                })
                .orElseThrow(() -> new IllegalStateException("Unknown destination: " + destinationName));
        logger.info("op=started for={}", destinationName);
    }

    @Override
    public List<SourceRecord> poll() {
        logger.trace("op=pool from={}", destinationName);
        // receive up to X messages
        return IntStream.range(0, 10)
                .mapToObj(i -> Optional.ofNullable(getMessage()))
                .filter(Optional::isPresent)
                .map(opt -> (TextMessage) opt.get())
                .map(message -> new SourceRecord(Collections.<String, Object>emptyMap(), Collections.<String, Object>emptyMap(),
                        topicsMappingHolder.getTopic(destinationName).get(),
                        STRING_SCHEMA, MessageReceiver.getText(message)))
                .collect(Collectors.toList());
    }

    private Message getMessage() {
        try {
            Message message = messageConsumer.receive(10);
            if (message == null) {
                logger.debug("op=noMessage");
            }
            return message;
        } catch (JMSException e) {
            logger.error("op=errorOnReceive", e);
            return null;
        }
    }

    @Override
    public void commitIfNecessary() {
        if (connectorConfigurationProperties.isSessionTransacted()) {
            try {
                session.commit();
            } catch (JMSException e) {
                logger.error("Error when onCommit", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        connection.stop();
        messageConsumer.close();
        session.close();
        connection.close();
    }
}
