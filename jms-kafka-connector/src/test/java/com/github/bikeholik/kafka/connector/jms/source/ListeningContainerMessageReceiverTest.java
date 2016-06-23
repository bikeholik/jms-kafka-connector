package com.github.bikeholik.kafka.connector.jms.source;

import javax.jms.ConnectionFactory;
import java.util.HashMap;
import java.util.Map;

import com.github.bikeholik.kafka.connector.jms.JmsConnectorConfigurationProperties;
import com.github.bikeholik.kafka.connector.jms.sink.JmsSinkConnector;
import com.github.bikeholik.kafka.connector.jms.util.ApplicationContextHolder;
import com.github.bikeholik.kafka.connector.jms.util.TopicsMappingHolder;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

public class ListeningContainerMessageReceiverTest {
    private static Map<String, String> testProperties = new HashMap<>();

    private static final String TEST = "test";

    static {
        testProperties.put(ApplicationContextHolder.PACKAGES, "com.github.bikeholik.test");
        testProperties.put("jms.sessionTransacted", "true");
        testProperties.put("jms.topicToJmsQueue.testTopic", "testQueue");
        testProperties.put("jms.messageReceiverClass", ListeningContainerMessageReceiver.class.getName());
        testProperties.put("test.name", TEST);
    }

    @Test
    public void testPoll() throws Exception {
        JmsSinkConnector connector = new JmsSinkConnector();
        connector.start(testProperties);


        ApplicationContext context = ApplicationContextHolder.getApplicationContext();
        ListeningContainerMessageReceiver messageReceiver = context
                .getBean(ListeningContainerMessageReceiver.class,
                        context.getBean(ConnectionFactory.class),
                        context.getBean(JmsConnectorConfigurationProperties.class),
                        context.getBean(TopicsMappingHolder.class),
                        "testQueue");

        Assert.assertNotNull(messageReceiver.poll());

        messageReceiver.commitIfNecessary();

        connector.stop();
    }

}