package com.github.bikeholik.kafka.connector.jms.source;

import static java.util.Collections.singletonMap;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.github.bikeholik.kafka.connector.jms.JmsConnectorConfigurationProperties;
import com.github.bikeholik.kafka.connector.jms.util.TopicsMappingHolder;
import org.aopalliance.aop.Advice;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.container.jms.BatchMessageListenerContainer;
import org.springframework.batch.repeat.RepeatContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.repeat.interceptor.RepeatOperationsInterceptor;
import org.springframework.batch.repeat.listener.RepeatListenerSupport;
import org.springframework.batch.repeat.policy.CompletionPolicySupport;
import org.springframework.batch.repeat.support.RepeatTemplate;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.jms.connection.JmsTransactionManager;
import org.springframework.jms.connection.TransactionAwareConnectionFactoryProxy;
import org.springframework.stereotype.Component;
import org.springframework.transaction.interceptor.MatchAlwaysTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;

@Component
@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ListeningContainerMessageReceiver implements MessageReceiver, MessageListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final BatchMessageListenerContainer listenerContainer;
    private final TransferQueue<Optional<Message>> messagesQueue = new LinkedTransferQueue<>();

    private final TopicsMappingHolder topicsMappingHolder;
    private final String destinationName;
    private final BlockingRepeatListener repeatListener;

    public ListeningContainerMessageReceiver(ConnectionFactory connectionFactory, JmsConnectorConfigurationProperties connectorConfigurationProperties, TopicsMappingHolder topicsMappingHolder, String destinationName) {
        this.topicsMappingHolder = topicsMappingHolder;
        this.destinationName = destinationName;
        this.listenerContainer = new BatchMessageListenerContainer();
        this.listenerContainer.setMaxConcurrentConsumers(1);
        this.listenerContainer.setMessageListener(this);
        RepeatOperationsInterceptor repeatOperationsInterceptor = new RepeatOperationsInterceptor();
        RepeatTemplate repeatTemplate = new RepeatTemplate();
        repeatListener = new BlockingRepeatListener();
        repeatTemplate.registerListener(repeatListener);
        repeatTemplate.setCompletionPolicy(new CompletionPolicySupport() {
            @Override
            public boolean isComplete(RepeatContext context, RepeatStatus result) {
                return context.isTerminateOnly();
            }
        });
        repeatOperationsInterceptor.setRepeatOperations(repeatTemplate);
        if (connectorConfigurationProperties.isSessionTransacted()) {
            logger.debug("op=withTransactionInterceptor");
            TransactionAwareConnectionFactoryProxy proxy = new TransactionAwareConnectionFactoryProxy(connectionFactory);
            this.listenerContainer.setConnectionFactory(proxy);
            JmsTransactionManager transactionManager = new JmsTransactionManager(proxy);
            transactionManager.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ALWAYS);
            this.listenerContainer.setTransactionManager(transactionManager);
            this.listenerContainer.setAdviceChain(new Advice[]{new TransactionInterceptor(transactionManager, new MatchAlwaysTransactionAttributeSource()), repeatOperationsInterceptor});
        } else {
            this.listenerContainer.setAdviceChain(new Advice[]{repeatOperationsInterceptor});
            this.listenerContainer.setConnectionFactory(connectionFactory);
        }
        this.listenerContainer.setDestination(topicsMappingHolder.getTopic(destinationName).flatMap(topicsMappingHolder::getDestination).orElseThrow(() -> new IllegalStateException(destinationName + " not found")));
        this.listenerContainer.start();
        this.listenerContainer.afterPropertiesSet();
    }

    @Override
    public List<SourceRecord> poll() {
        return IntStream.range(0, 10)
                .peek(i -> logger.trace("op=requestMessage i={}", i))
                .filter(i -> this.repeatListener.allowConsumption())
                .peek(i -> logger.trace("op=waitForMessage i={}", i))
                .mapToObj(i -> getMessage())
                .filter(Optional::isPresent)
                .map(opt -> (TextMessage) opt.get())
                .peek(msg -> logger.debug("op=prepareRecord hash={}", msg.hashCode()))
                .map(message -> new SourceRecord(
                        singletonMap("destination", destinationName),
                        singletonMap(destinationName, getJmsMessageID(message)),
                        topicsMappingHolder.getTopic(destinationName).get(),
                        STRING_SCHEMA, MessageReceiver.getText(message)))
                .peek(record -> logger.trace("op=collect record={}", record))
                .collect(Collectors.toList());
    }

    private String getJmsMessageID(Message message) {
        try {
            return message.getJMSMessageID();
        } catch (JMSException e) {
            logger.error("op=errorOnGettingMessageID", e);
            return UUID.randomUUID().toString();
        }
    }

    private Optional<Message> getMessage() {
        try {
            return Optional.ofNullable(messagesQueue.poll(10, TimeUnit.SECONDS))
                    .orElseGet(() -> {
                        logger.warn("op=timeoutWaitingForMessage");
                        return Optional.empty();
                    });
        } catch (InterruptedException e) {
            return Optional.empty();
        }
    }

    @Override
    public void commitIfNecessary() {
        logger.debug("op=commit");
        this.repeatListener.markCompleted();
    }

    @Override
    public void close() throws Exception {
        // TODO does it interrupt threads ?
        logger.info("op=stop");
        this.listenerContainer.stop();
        this.repeatListener.markTerminated();
        this.listenerContainer.destroy();
    }

    @Override
    public void onMessage(Message message) {
        Optional<Message> messageWrapper = Optional.ofNullable(message);
        logger.trace("op=onMessage hash={}", messageWrapper.map(Object::hashCode).map(String::valueOf).orElse("NONE"));
        try {
            if (!messagesQueue.tryTransfer(messageWrapper, 5, TimeUnit.SECONDS)) {
                throw new RuntimeException("Requested message not taken");
            }
        } catch (InterruptedException e) {
            // TODO should trigger rollback ?
            throw new RuntimeException(e);
        }
    }

    private static class BlockingRepeatListener extends RepeatListenerSupport {
        private final TransferQueue<Boolean> permissionQueue = new LinkedTransferQueue<>();
        private final AtomicReference<RepeatContext> currentContext = new AtomicReference<>(null);
        private final AtomicBoolean terminated = new AtomicBoolean(false);

        @Override
        public void open(RepeatContext context) {
            currentContext.set(context);
            super.open(context);
        }

        @Override
        public void before(RepeatContext context) {
            try {
                while (permissionQueue.poll(500, TimeUnit.MILLISECONDS) == null && !context.isCompleteOnly() && !terminated.get()) {
                    // NOP wait for permission
                }
                if (terminated.get()) {
                    LoggerFactory.getLogger(getClass()).info("op=terminated");
                    throw new RuntimeException("Terminated");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("Failed to get permission", e);
            }
        }

        boolean allowConsumption() {
            try {
                // TODO time should be configurable
                return permissionQueue.tryTransfer(Boolean.TRUE, 10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                return false;
            }
        }

        void markCompleted() {
            Optional.ofNullable(currentContext.get()).ifPresent(RepeatContext::setCompleteOnly);
        }

        void markTerminated() {
            terminated.set(true);
        }
    }
}
