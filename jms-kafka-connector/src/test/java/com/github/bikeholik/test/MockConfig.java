package com.github.bikeholik.test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jms.support.destination.DestinationResolver;

@Configuration
public class MockConfig {
    @Bean
    ConnectionFactory connectionFactory() throws JMSException {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        when(connectionFactory.createConnection()).thenReturn(connection);
        Session session = mock(Session.class);
        when(connection.createSession(anyBoolean(), anyInt())).thenReturn(session);
        when(session.createConsumer(any(Destination.class))).thenReturn(mock(MessageConsumer.class));
        return connectionFactory;
    }

    @Bean
    @Primary
    DestinationResolver destinationResolver() throws JMSException {
        DestinationResolver destinationResolver = mock(DestinationResolver.class);
        when(destinationResolver.resolveDestinationName(any(Session.class), anyString(), anyBoolean())).thenReturn(mock(Destination.class));
        return destinationResolver;
    }

}
