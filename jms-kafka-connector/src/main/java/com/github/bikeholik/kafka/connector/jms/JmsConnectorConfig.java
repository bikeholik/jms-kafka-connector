package com.github.bikeholik.kafka.connector.jms;

import javax.jms.ConnectionFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.SearchStrategy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.destination.DestinationResolver;
import org.springframework.jms.support.destination.DynamicDestinationResolver;

@Configuration
@EnableConfigurationProperties
@ComponentScan
public class JmsConnectorConfig {
    @Autowired
    private ConnectionFactory connectionFactory;

    @Bean
    JmsTemplate jmsTemplate(JmsConnectorConfigurationProperties properties) {
        JmsTemplate jmsTemplate = new JmsTemplate(connectionFactory);
        jmsTemplate.setSessionTransacted(properties.isSessionTransacted());
        // TODO QoS
        return jmsTemplate;
    }

    @Bean
    @ConditionalOnMissingBean(search = SearchStrategy.ALL)
    DestinationResolver destinationResolver(){
        return new DynamicDestinationResolver();
    }

    // TODO
//    messageConverter ?

}
