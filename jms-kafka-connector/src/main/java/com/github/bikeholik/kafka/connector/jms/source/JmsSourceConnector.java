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
import java.util.HashMap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

public class JmsSourceConnector extends SourceConnector {

    private static final String TASK_DESTINATION_NAME = "destinationName";
    private String[] queues;

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        ApplicationContextHolder.startApplicationContext(props);
        queues = Optional.ofNullable(props.get("queues"))
                .map(queueNames -> queueNames.split(","))
                .orElseThrow(IllegalArgumentException::new);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JmsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // #tasks > #queues
        return IntStream.range(0, maxTasks)
                .mapToObj(this::buildTask)
                .collect(Collectors.toList());
    }

    private Map<String, String> buildTask(int i) {
        HashMap<String, String> map = new HashMap<>();
        map.put(TASK_DESTINATION_NAME, queues[i % queues.length]);
        map.put("task-id", String.valueOf(i));
        return map;
    }

    @Override
    public void stop() {
        ApplicationContextHolder.closeApplicationContext();
    }

    public static class JmsSourceTask extends SourceTask {

        private MessageReceiver messageReceiver;

        @Override
        public String version() {
            return null;
        }

        @Override
        public void start(Map<String, String> props) {
            LoggerFactory.getLogger(getClass()).info("op=start props={}", props);
            // get session & consumer (per one queue -> meaning multiply tasks by queues count)
            // TODO or is t one connector per topic ? -> source record has a topic
            String destinationName = props.get(TASK_DESTINATION_NAME);
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
            messageReceiver.stop();
        }

        @Override
        public void commit() throws InterruptedException {
            // TODO commit session
            messageReceiver.commitIfNecessary();
        }
    }

    @Component
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    static class MessageReceiver implements AutoCloseable {
        private final Logger logger = LoggerFactory.getLogger(getClass());
        private final Session session;
        private final JmsConnectorConfigurationProperties connectorConfigurationProperties;
        private final TopicsMappingHolder topicsMappingHolder;
        private final String destinationName;
        private final MessageConsumer messageConsumer;
        private final Connection connection;

        @Autowired
        MessageReceiver(ConnectionFactory connectionFactory, JmsConnectorConfigurationProperties connectorConfigurationProperties, TopicsMappingHolder topicsMappingHolder, String destinationName) throws JMSException {
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

        List<SourceRecord> poll() {
            logger.info("op=pool from={}", destinationName);
            // receive up to X messages
            return IntStream.range(0, 10)
                    .mapToObj(i -> Optional.ofNullable(getMessage()))
                    .filter(Optional::isPresent)
                    .map(opt -> (TextMessage) opt.get())
                    .map(message -> new SourceRecord(Collections.<String, Object>emptyMap(), Collections.<String, Object>emptyMap(),
                            topicsMappingHolder.getTopic(destinationName).get(),
                            STRING_SCHEMA, getText(message)))
                    .collect(Collectors.toList());
        }

        String getText(TextMessage message) {
            try {
                return message.getText();
            } catch (JMSException e) {
                logger.error("op=errorOnGetText", e);
                return null;
            }
        }

        Message getMessage() {
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

        void commitIfNecessary() {
            if (connectorConfigurationProperties.isSessionTransacted()) {
                try {
                    session.commit();
                } catch (JMSException e) {
                    logger.error("Error when onCommit", e);
                }
            }
        }

        void stop() {
            try {
                connection.stop();;
                messageConsumer.close();
                session.close();
                connection.close();
            } catch (JMSException _) {

            }
        }

        @Override
        public void close() throws Exception {
            stop();
        }
    }
}
