package com.github.bikeholik.kafka.connector.hornetq;

import javax.jms.ConnectionFactory;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HornetQConnectionFactoryConfig {

    @Bean
    ConnectionFactory connectionFactory(HornetQConnectionFactoryProperties props) {
        return HornetQConnectionFactoryBuilder.forCluster(props.getCluster())
                .password(props.getPassword())
                .user(props.getUser())
                .buildAuthenticatingConnectionFactory();
    }
}
