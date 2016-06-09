package com.github.bikeholik.kafka.connector.jms;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * TODO comment
 */
@Configuration
@ConfigurationProperties(prefix = "jms")
public class JmsConnectorConfigurationProperties {
    private boolean sessionTransacted;

    private Map<String, String> topicToJmsQueue;

    public boolean isSessionTransacted() {
        return sessionTransacted;
    }

    public void setSessionTransacted(boolean sessionTransacted) {
        this.sessionTransacted = sessionTransacted;
    }

    public Map<String, String> getTopicToJmsQueue() {
        return topicToJmsQueue;
    }

    public void setTopicToJmsQueue(Map<String, String> topicToJmsQueue) {
        this.topicToJmsQueue = topicToJmsQueue;
    }
}
