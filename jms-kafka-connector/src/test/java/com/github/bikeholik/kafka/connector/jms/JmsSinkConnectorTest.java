package com.github.bikeholik.kafka.connector.jms;

import java.util.HashMap;
import java.util.Map;

import com.github.bikeholik.test.TestProperties;
import org.junit.Assert;
import org.junit.Test;

public class JmsSinkConnectorTest {
    private static Map<String, String> testProperties = new HashMap<>();

    private static final String TEST = "test";

    static {
        testProperties.put(JmsSinkConnector.PACKAGES, "com.github.bikeholik.test");
        testProperties.put("jms.sessionTransacted", "true");
        testProperties.put("jms.topicToJmsQueue.testTopic", "testQueue");
        testProperties.put("test.name", TEST);
    }

    @Test
    public void start() throws Exception {
        JmsSinkConnector connector = new JmsSinkConnector();
        connector.start(testProperties);


        TestProperties testProperties = JmsSinkConnector.getApplicationContext().getBean(TestProperties.class);
        Assert.assertEquals(testProperties.getName(), TEST);

        connector.stop();
    }

}