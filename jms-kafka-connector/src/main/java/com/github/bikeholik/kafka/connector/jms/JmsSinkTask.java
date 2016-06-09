package com.github.bikeholik.kafka.connector.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.ProducerCallback;
import org.springframework.jms.support.converter.MessageConverter;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

public class JmsSinkTask extends SinkTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<String, Destination> topicToJmsDestinationMapping = new HashMap<>();

    private JmsTemplate jmsTemplate = new JmsTemplate();

    private MessageConverter messageConverter = jmsTemplate.getMessageConverter();
    private String id;

    public String version() {
        return JmsSinkConnector.getVersion(getClass());
    }

    public void start(Map<String, String> map) {
        logger.info("operation=start properties={}", map);
        id = map.getOrDefault(JmsSinkConnector.PROPERTY_TASK_ID, "x");

    }

    public void put(Collection<SinkRecord> collection) {
        logger.info("operation=put size={}", collection.size());
        jmsTemplate.execute(topicToJmsDestinationMapping.get("x"), new ProducerCallback<Void>() {
            @Override
            public Void doInJms(Session session, MessageProducer messageProducer) throws JMSException {
                Observable
                        .from(collection)
                        .map(new Func1<SinkRecord, Optional<Message>>() {
                            @Override
                            public Optional<Message> call(SinkRecord sinkRecord) {
                                try {
                                    return Optional.of(messageConverter.toMessage(sinkRecord.value(), session));
                                } catch (JMSException e) {
                                    return Optional.empty();
                                }
                            }
                        })
                        .filter(Optional::isPresent)
                        .map(message -> message.get()) // TODO error ?
                        .subscribe(new Subscriber<Message>() {
                            @Override
                            public void onCompleted() {
                                call(session::commit, RetriableException::new);
                            }

                            @Override
                            public void onError(Throwable e) {
                                throw new RetriableException(e);
                            }

                            @Override
                            public void onNext(Message message) {
                                call(() -> {
                                    // TODO destination
                                    messageProducer.send(message);
                                }, RuntimeException::new);
                            }
                        });
                return null;
            }
        });
    }

    private interface JmsAction {
        void execute() throws JMSException;
    }

    private static void call(JmsAction callback, Function<Exception, RuntimeException> exceptionMapper) {
        try {
            callback.execute();
        } catch (Exception e) {
            throw exceptionMapper.apply(e);
        }
    }

    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // session commit (call onCompleted ?) or NOP ?
        logger.info("operation=flush properties={}", map);
    }

    public void stop() {
        logger.info("operation=stop id={}", id);
    }
}
