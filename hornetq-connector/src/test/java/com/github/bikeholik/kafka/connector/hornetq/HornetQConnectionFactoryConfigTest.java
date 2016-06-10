package com.github.bikeholik.kafka.connector.hornetq;

import javax.jms.ConnectionFactory;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@Ignore
public class HornetQConnectionFactoryConfigTest {

    @Configuration
    @ComponentScan
    @EnableConfigurationProperties
    @PropertySource("test.properties")
    static class TestConfig {

    }

    @Autowired
    private ConnectionFactory connectionFactory;

    @Test
    public void testConnectionFactory() throws Exception {
        Assert.assertNotNull(connectionFactory);

        new JmsTemplate(connectionFactory).convertAndSend("test", "test");
    }
}