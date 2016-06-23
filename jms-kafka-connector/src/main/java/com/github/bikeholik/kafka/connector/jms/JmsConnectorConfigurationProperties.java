package com.github.bikeholik.kafka.connector.jms;

import java.util.HashMap;
import java.util.Map;

import com.github.bikeholik.kafka.connector.jms.source.MessageReceiver;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "jms")
public class JmsConnectorConfigurationProperties {
    private boolean sessionTransacted;

    private Map<String, String> topicToJmsQueue = new HashMap<>();
    private Class<? extends MessageReceiver> messageReceiverClass;

    public boolean isSessionTransacted() {
        return sessionTransacted;
    }

    public void setSessionTransacted(boolean sessionTransacted) {
        this.sessionTransacted = sessionTransacted;
    }

    public Map<String, String> getTopicToJmsQueue() {
        return topicToJmsQueue;
    }

    public Class<? extends MessageReceiver> getMessageReceiverClass() {
        return messageReceiverClass;
    }

    public void setMessageReceiverClass(Class<? extends MessageReceiver> messageReceiverClass) {
        this.messageReceiverClass = messageReceiverClass;
    }
}
