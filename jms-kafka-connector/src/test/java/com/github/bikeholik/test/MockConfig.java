package com.github.bikeholik.test;

import javax.jms.ConnectionFactory;

import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MockConfig {
    @Bean
    ConnectionFactory connectionFactory(){
        return Mockito.mock(ConnectionFactory.class);
    }
}
