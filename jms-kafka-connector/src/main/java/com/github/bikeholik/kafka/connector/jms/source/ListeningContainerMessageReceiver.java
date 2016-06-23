package com.github.bikeholik.kafka.connector.jms.source;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.github.bikeholik.kafka.connector.jms.JmsConnectorConfigurationProperties;
import com.github.bikeholik.kafka.connector.jms.util.TopicsMappingHolder;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Scope;
import org.springframework.jms.connection.JmsTransactionManager;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@ConditionalOnProperty(name = "jms.consumer.type", havingValue = "container")
public class ListeningContainerMessageReceiver implements MessageReceiver, MessageListener {

    private final BlockingMessageListenerContainer listenerContainer;
    private final TransferQueue<Message> messagesQueue = new LinkedTransferQueue<>();

    private final TopicsMappingHolder topicsMappingHolder;
    private final String destinationName;
    private final JmsTransactionManager transactionManager;

    public ListeningContainerMessageReceiver(ConnectionFactory connectionFactory, JmsConnectorConfigurationProperties connectorConfigurationProperties, TopicsMappingHolder topicsMappingHolder, String destinationName) {
        this.topicsMappingHolder = topicsMappingHolder;
        this.destinationName = destinationName;
        this.listenerContainer = new BlockingMessageListenerContainer();
        this.listenerContainer.setConnectionFactory(connectionFactory);
        this.listenerContainer.setMaxConcurrentConsumers(1);
        this.listenerContainer.start();
        this.listenerContainer.setMessageListener(this);
        if (connectorConfigurationProperties.isSessionTransacted()) {
            this.transactionManager = new JmsTransactionManager(connectionFactory);
            this.listenerContainer.setTransactionManager(transactionManager);
        } else {
            this.transactionManager = null;
        }
    }

    @Override
    public List<SourceRecord> poll() {
        return IntStream.range(0, 10)
                .filter(i -> this.listenerContainer.allowConsumption())
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
            return messagesQueue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override
    public void commitIfNecessary() {
        // TODO each message committed separately at the moment
    }

    @Override
    public void close() throws Exception {
        this.listenerContainer.stop();
        this.listenerContainer.destroy();
    }

    @Override
    public void onMessage(Message message) {
        try {
            messagesQueue.transfer(message);
        } catch (InterruptedException e) {
            // TODO should trigger rollback ?
            throw new RuntimeException(e);
        }
    }

    private static class BlockingMessageListenerContainer extends DefaultMessageListenerContainer {
        private final TransferQueue<Boolean> permissionQueue = new LinkedTransferQueue<>();

        @Override
        protected boolean receiveAndExecute(Object invoker, Session session, MessageConsumer consumer) throws JMSException {
            try {
                // TODO should match connector polling time
                if (!permissionQueue.poll(30, TimeUnit.SECONDS)) {
                    return false;
                }
            } catch (InterruptedException e) {
                throw new JMSException("Failed to get permission");
            }
            return super.receiveAndExecute(invoker, session, consumer);
        }

        @Override
        protected void commitIfNecessary(Session session, Message message) throws JMSException {
            if (!isSessionTransacted()) {
                super.commitIfNecessary(session, message);
            }
        }

        public boolean allowConsumption() {
            try {
                // TODO time should be configurable
                return permissionQueue.tryTransfer(Boolean.TRUE, 10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                return false;
            }
        }
    }
}
