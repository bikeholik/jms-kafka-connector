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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.github.bikeholik.kafka.connector.jms.JmsConnectorConfigurationProperties;
import com.github.bikeholik.kafka.connector.jms.util.ApplicationContextHolder;
import com.github.bikeholik.kafka.connector.jms.util.TopicsMappingHolder;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * TODO comment
 */
public class JmsSourceConnector extends SourceConnector {
    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {

    }

    @Override
    public Class<? extends Task> taskClass() {
        return JmsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return null;
    }

    @Override
    public void stop() {

    }

    public static class JmsSourceTask extends SourceTask {

        private MessageReceiver messageReceiver;

        @Override
        public String version() {
            return null;
        }

        @Override
        public void start(Map<String, String> props) {
            // get session & consumer (per one queue -> meaning multiply tasks by queues count)
            // TODO or is t one connector per topic ? -> source record has a topic
            String destinationName = props.get("destinationName");
            ApplicationContext context = ApplicationContextHolder.getApplicationContext();
            messageReceiver = context
                    .getBean(MessageReceiver.class,
                            context.getBean(ConnectionFactory.class),
                            context.getBean(JmsConnectorConfigurationProperties.class),
                            context.getBean(TopicsMappingHolder.class),
                            destinationName);
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            //
            // read up to X messages
            // can source partition/offset be empty ?
            return messageReceiver.poll();
        }

        @Override
        public void stop() {
            // stop session
        }

        @Override
        public void commit() throws InterruptedException {
            // TODO commit session
        }
    }

    @Component
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    static class MessageReceiver {
        private final Session session;
        private final TopicsMappingHolder topicsMappingHolder;
        private final String destinationName;
        private MessageConsumer messageConsumer;

        @Autowired
        MessageReceiver(ConnectionFactory connectionFactory, JmsConnectorConfigurationProperties connectorConfigurationProperties, TopicsMappingHolder topicsMappingHolder, String destinationName) throws JMSException {
            this.topicsMappingHolder = topicsMappingHolder;
            this.destinationName = destinationName;
            Connection connection = connectionFactory.createConnection();
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
        }

        public List<SourceRecord> poll() {
            // receive up to X messages
            return IntStream.range(0, 10)
                    .mapToObj(i -> {
                        try {
                            return Optional.ofNullable(getMessage());
                        } catch (Exception e) {
                            return Optional.empty();
                        }
                    })
                    .filter(Optional::isPresent)
                    .map(opt -> (TextMessage)opt.get())
                    .map(message -> new SourceRecord(Collections.<String, Object>emptyMap(), Collections.<String, Object>emptyMap(),
                            topicsMappingHolder.getTopic(destinationName).get(),
                            STRING_SCHEMA, getText(message)))
                    .collect(Collectors.toList());
        }

        public String getText(TextMessage message)  {
            try {
                return message.getText();
            } catch (JMSException e) {
                return null;
            }
        }

        public Message getMessage() {
            try {
                return messageConsumer.receiveNoWait();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
