package com.github.bikeholik.kafka.connector.jms;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import javafx.util.Pair;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.ProducerCallback;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.stereotype.Component;

public class JmsSinkTask extends SinkTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Sender sender;

    private String id;

    public String version() {
        return JmsSinkConnector.getVersion(getClass());
    }

    public void start(Map<String, String> map) {
        logger.info("operation=start properties={}", map);
        id = map.getOrDefault(JmsSinkConnector.PROPERTY_TASK_ID, "task-" + UUID.randomUUID().toString());
        this.sender = JmsSinkConnector.getApplicationContext().getBean(Sender.class);
    }

    public void put(Collection<SinkRecord> collection) {
        logger.info("operation=put id={} size={}", id, collection.size());
        sender.sendMessages(collection);
    }

    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // session commit (call onCompleted ?) or NOP ?
        logger.info("operation=flush properties={}", map);
    }

    public void stop() {
        logger.info("operation=stop id={}", id);
    }

    @Component
    static class Sender {
        private final Logger logger = LoggerFactory.getLogger(getClass());
        private final JmsTemplate jmsTemplate;
        private final MessageConverter messageConverter;
        private final TopicsMappingHolder topicsMappingHolder;

        @Autowired
        public Sender(JmsTemplate jmsTemplate, TopicsMappingHolder topicsMappingHolder) {
            this.jmsTemplate = jmsTemplate;
            this.messageConverter = jmsTemplate.getMessageConverter();
            this.topicsMappingHolder = topicsMappingHolder;
        }

        private static void call(JmsAction callback, Function<Exception, RuntimeException> exceptionMapper) {
            try {
                callback.execute();
            } catch (Exception e) {
                throw exceptionMapper.apply(e);
            }
        }

        void sendMessages(final Collection<SinkRecord> collection) {
            jmsTemplate.execute((ProducerCallback<Void>) (session, messageProducer) -> {
                collection.stream()
                        .peek(record -> logger.debug("operation=nextMessage topic={}", record.topic()))
                        .map(sinkRecord -> topicsMappingHolder.getDestination(sinkRecord.topic())
                                .flatMap(destination -> toMessage(sinkRecord.value(), session)
                                        .map(message -> new Pair<>(destination, message))))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .forEach(message -> call(() -> messageProducer.send(message.getKey(), message.getValue()), RetriableException::new));
                if(jmsTemplate.isSessionTransacted()) {
                    call(session::commit, RetriableException::new);
                }
                return null;
            });
        }

        private Optional<Message> toMessage(Object o, Session session) {
            try {
                return Optional.of(messageConverter.toMessage(o, session));
            } catch (JMSException e) {
                e.printStackTrace();
                return Optional.empty();
            }
        }

        private interface JmsAction {
            void execute() throws JMSException;
        }
    }
}
