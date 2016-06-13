package com.github.bikeholik.kafka.connector.jms.sink;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Collections;
import java.util.Optional;

import com.github.bikeholik.kafka.connector.jms.sink.JmsSinkTask;
import com.github.bikeholik.kafka.connector.jms.util.TopicsMappingHolder;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageType;

@RunWith(MockitoJUnitRunner.class)
public class SenderTest {

    private static final String TEST_TOPIC = "testTopic";
    private JmsSinkTask.Sender sender;

    @Mock
    private TopicsMappingHolder topicsMapping;
    @Mock
    private ConnectionFactory connectionFactory;
    @Mock
    private SinkRecord sinkRecord;
    @Mock
    private Connection connection;
    @Mock
    private Session session;
    @Mock
    private MessageProducer producer;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        JmsTemplate jmsTemplate = new JmsTemplate(connectionFactory);
        jmsTemplate.setSessionTransacted(true);
        MappingJackson2MessageConverter messageConverter = new MappingJackson2MessageConverter();
        messageConverter.setTargetType(MessageType.TEXT);
        jmsTemplate.setMessageConverter(messageConverter);
        sender = new JmsSinkTask.Sender(jmsTemplate, topicsMapping);
        when(connectionFactory.createConnection()).thenReturn(connection);
        when(connection.createSession(anyBoolean(), anyInt())).thenReturn(session);
        when(session.createProducer(any(Destination.class))).thenReturn(producer);
        when(session.createTextMessage(anyString())).thenReturn(mock(TextMessage.class));
        when(sinkRecord.topic()).thenReturn(TEST_TOPIC);
        when(sinkRecord.value()).thenReturn("test");
        when(topicsMapping.getDestination(TEST_TOPIC)).thenReturn(Optional.of(mock(Destination.class)));
    }

    @Test
    public void sendMessages() throws Exception {
        sender.sendMessages(Collections.singletonList(sinkRecord));

        verify(producer).send(any(Destination.class), any(Message.class));
        verify(session).commit();
    }

    @Test
    public void sendRetryOnCommit() throws Exception {
        doThrow(new JMSException("test")).
                when(producer).send(any(Destination.class), any(Message.class));

        expectedException.expect(RetriableException.class);

        sender.sendMessages(Collections.singletonList(sinkRecord));

        verify(producer).send(any(Destination.class), any(Message.class));
    }
}