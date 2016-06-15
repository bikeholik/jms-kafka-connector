package com.github.bikeholik.kafka.connector.hornetq;

import javax.jms.ConnectionFactory;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HornetQConnectionFactoryConfig {

    @Bean
    @ConditionalOnProperty(name = "hornetq.connectionFactory.type", havingValue = "auth", matchIfMissing = true)
    ConnectionFactory authenticatingConnectionFactory(HornetQConnectionFactoryProperties props) {
        return newBuilder(props)
                .password(props.getPassword())
                .user(props.getUser())
                .buildAuthenticatingConnectionFactory();
    }

    private static HornetQConnectionFactoryBuilder newBuilder(HornetQConnectionFactoryProperties props) {
        return HornetQConnectionFactoryBuilder.forCluster(props.getCluster())
                .connectionTtl(props.getConnectionTtl())
                .failureCheckPeriod(props.getClientFailureCheckPeriod())
                .reconnectAttempts(props.getReconnectAttempts())
                .retryInterval(props.getRetryInterval())
                ;
    }

    @Bean
    @ConditionalOnProperty(name = "hornetq.connectionFactory.type", havingValue = "basic")
    ConnectionFactory basicConnectionFactory(HornetQConnectionFactoryProperties props) {
        return newBuilder(props)
                .buildAuthenticatingConnectionFactory();
    }

    @Bean
    @ConditionalOnProperty(name = "hornetq.connectionFactory.type", havingValue = "cache")
    ConnectionFactory cachingConnectionFactory(HornetQConnectionFactoryProperties props) {
        return newBuilder(props)
                .sessionCached(props.getSessionsCached())
                .cacheProducers(props.isCacheProducers())
                .cacheConsumers(props.isCacheConsumers())
                .buildCachingConnectionFactory();
    }
}
