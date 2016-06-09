package com.github.bikeholik.kafka.connector.jms;

import java.util.HashMap;
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

    private static final String PROPERTY_PREFIX = "jms.";
    static final String PROPERTY_TASK_ID = "task-id";
    public static final String PACKAGES = "component-packages";
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static ConfigurableApplicationContext CONTEXT_REFERENCE;

    private Map<String, String> properties;

    public String version() {
        return getVersion(getClass());
    }

    public static String getVersion(Class<?> aClass) {
        return aClass.getPackage().getImplementationVersion();
    }

    public void start(Map<String, String> map) {
        logger.info("operation=start properties={}", map);
        properties = map.entrySet().stream()
                .filter(e -> e.getKey().startsWith(PROPERTY_PREFIX))
                .collect(Collectors.toMap(e -> e.getKey().substring(PROPERTY_PREFIX.length()), Map.Entry::getValue));

        // create context
        ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(JmsConnectorConfig.class)
                .web(false)
                .parent(new AnnotationConfigApplicationContext(Optional.ofNullable(map.get(PACKAGES)).map(s -> s.split(",")).orElseGet(() -> new String[0]))) // error ?
                .build()
                .run();

        synchronized (JmsSinkConnector.class) {
            CONTEXT_REFERENCE = applicationContext;
            JmsSinkConnector.class.notifyAll();
        }

//        build.
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
        Map<String, String> taskProperties = new HashMap<>(properties);
        taskProperties.put(PROPERTY_TASK_ID, String.valueOf(taskId));
        return taskProperties;
    }

    public void stop() {
        logger.info("operation=stop");
        Optional.ofNullable(CONTEXT_REFERENCE)
                .ifPresent(ConfigurableApplicationContext::close);
    }

}
