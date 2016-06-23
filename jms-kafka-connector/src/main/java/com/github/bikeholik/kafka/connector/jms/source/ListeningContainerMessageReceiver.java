package com.github.bikeholik.kafka.connector.jms.source;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.github.bikeholik.kafka.connector.jms.JmsConnectorConfigurationProperties;
import com.github.bikeholik.kafka.connector.jms.util.TopicsMappingHolder;
import org.aopalliance.aop.Advice;
import org.apache.kafka.connect.source.SourceRecord;
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
import org.springframework.stereotype.Component;
import org.springframework.transaction.interceptor.TransactionInterceptor;

@Component
@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ListeningContainerMessageReceiver implements MessageReceiver, MessageListener {

    private final BatchMessageListenerContainer listenerContainer;
    private final TransferQueue<Message> messagesQueue = new LinkedTransferQueue<>();

    private final TopicsMappingHolder topicsMappingHolder;
    private final String destinationName;
    private final BlockingRepeatListener repeatListener;

    public ListeningContainerMessageReceiver(ConnectionFactory connectionFactory, JmsConnectorConfigurationProperties connectorConfigurationProperties, TopicsMappingHolder topicsMappingHolder, String destinationName) {
        this.topicsMappingHolder = topicsMappingHolder;
        this.destinationName = destinationName;
        this.listenerContainer = new BatchMessageListenerContainer();
        this.listenerContainer.setConnectionFactory(connectionFactory);
        this.listenerContainer.setMaxConcurrentConsumers(1);
        this.listenerContainer.setMessageListener(this);
        RepeatOperationsInterceptor repeatOperationsInterceptor = new RepeatOperationsInterceptor();
        RepeatTemplate repeatTemplate = new RepeatTemplate();
        repeatListener = new BlockingRepeatListener();
        repeatTemplate.registerListener(repeatListener);
        repeatTemplate.setCompletionPolicy(new CompletionPolicySupport(){
            @Override
            public boolean isComplete(RepeatContext context, RepeatStatus result) {
                return context.isTerminateOnly();
            }
        });
        repeatOperationsInterceptor.setRepeatOperations(repeatTemplate);
        if (connectorConfigurationProperties.isSessionTransacted()) {
            this.listenerContainer.setAdviceChain(new Advice[]{new TransactionInterceptor(new JmsTransactionManager(connectionFactory), new Properties()), repeatOperationsInterceptor});
        } else {
            this.listenerContainer.setAdviceChain(new Advice[]{repeatOperationsInterceptor});
        }
        this.listenerContainer.setDestination(topicsMappingHolder.getTopic(destinationName).flatMap(topicsMappingHolder::getDestination).orElseThrow(() -> new IllegalStateException(destinationName + " not found")));
        this.listenerContainer.start();
        this.listenerContainer.afterPropertiesSet();
    }

    @Override
    public List<SourceRecord> poll() {
        return IntStream.range(0, 10)
                .filter(i -> this.repeatListener.allowConsumption())
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
            return messagesQueue.poll(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override
    public void commitIfNecessary() {
        this.repeatListener.markCompleted();
    }

    @Override
    public void close() throws Exception {
        // TODO does it interrupt threads ?
        this.repeatListener.markTerminated();
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

    private static class BlockingRepeatListener extends RepeatListenerSupport {
        private final TransferQueue<Boolean> permissionQueue = new LinkedTransferQueue<>();
        private final AtomicReference<RepeatContext> currentContext = new AtomicReference<>(null);

        @Override
        public void open(RepeatContext context) {
            currentContext.set(context);
            super.open(context);
        }

        @Override
        public void before(RepeatContext context) {
            try {
                while (permissionQueue.poll(500, TimeUnit.MILLISECONDS) == null && !context.isCompleteOnly() && !context.isTerminateOnly()) {
                    // NOP wait for permission
                }
                if (context.isTerminateOnly()) {
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
            Optional.ofNullable(currentContext.get()).ifPresent(RepeatContext::setTerminateOnly);
        }
    }
}
