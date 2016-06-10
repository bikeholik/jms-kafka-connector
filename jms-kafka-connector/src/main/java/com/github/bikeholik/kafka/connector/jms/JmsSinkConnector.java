package com.github.bikeholik.kafka.connector.jms;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class JmsSinkConnector extends SinkConnector {

    static final String PROPERTY_TASK_ID = "task-id";
    public static final String PACKAGES = "component-packages";
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static ConfigurableApplicationContext CONTEXT_REFERENCE;

    public String version() {
        return getVersion(getClass());
    }

    public static String getVersion(Class<?> aClass) {
        return aClass.getPackage().getImplementationVersion();
    }

    public void start(Map<String, String> map) {
        logger.info("operation=start properties={}", map);


        // create context
        ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(JmsConnectorConfig.class)
                .web(false)
                .properties(map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                .parent(new AnnotationConfigApplicationContext(Optional.ofNullable(map.get(PACKAGES)).map(s -> s.split(",")).orElseGet(() -> new String[0]))) // error ?
                .build()
                .run();

        synchronized (JmsSinkConnector.class) {
            CONTEXT_REFERENCE = applicationContext;
            JmsSinkConnector.class.notifyAll();
        }
    }

    public static ApplicationContext getApplicationContext() {
        synchronized (JmsSinkConnector.class) {
            while (CONTEXT_REFERENCE == null) {
                try {
                    JmsSinkConnector.class.wait();
                } catch (InterruptedException e) {
                    // NOP
                }
            }
            return CONTEXT_REFERENCE;
        }
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
        Optional.ofNullable(CONTEXT_REFERENCE)
                .ifPresent(ConfigurableApplicationContext::close);
    }

}
