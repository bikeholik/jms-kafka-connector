package com.github.bikeholik.kafka.connector.jms.sink;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.github.bikeholik.kafka.connector.jms.JmsConnectorConfig;
import com.github.bikeholik.kafka.connector.jms.util.ApplicationContextHolder;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsSinkConnector extends SinkConnector {

    static final String PROPERTY_TASK_ID = "task-id";
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public String version() {
        return getVersion(getClass());
    }

    public static String getVersion(Class<?> aClass) {
        return aClass.getPackage().getImplementationVersion();
    }

    public void start(Map<String, String> map) {
        logger.info("operation=start properties={}", map);

        // create context
        Class<JmsConnectorConfig> jmsConnectorConfigClass = JmsConnectorConfig.class;
        ApplicationContextHolder.startApplicationContext(map, jmsConnectorConfigClass);
    }

    public Class<? extends Task> taskClass() {
        return JmsSinkTask.class;
    }

    public List<Map<String, String>> taskConfigs(int i) {
        return IntStream.range(0, i)
                .mapToObj(this::createTaskProperties)
                .collect(Collectors.toList());
    }

    private Map<String, String> createTaskProperties(int taskId) {
        return Collections.singletonMap(PROPERTY_TASK_ID, String.valueOf(taskId));
    }

    public void stop() {
        logger.info("operation=stop");
        ApplicationContextHolder.closeApplicationContext();
    }

}
