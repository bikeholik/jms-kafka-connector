package com.github.bikeholik.kafka.connector.jms;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class JmsSinkConnectorTest {
    private static Map<String, String> testProperties = new HashMap<>();

    static {
        testProperties.put(JmsSinkConnector.PACKAGES, "com.github.bikeholik.test");
        testProperties.put("jms.sessionTransacted", "true");
        testProperties.put("jms.topicToJmsQueue.testTopic", "testQueue");
    }

    @Test
    public void start() throws Exception {
        JmsSinkConnector connector = new JmsSinkConnector();
        connector.start(testProperties);
        connector.stop();
    }

}