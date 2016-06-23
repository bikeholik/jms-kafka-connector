package com.github.bikeholik.kafka.connector.jms.util;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.github.bikeholik.kafka.connector.jms.JmsConnectorConfig;
import com.github.bikeholik.kafka.connector.jms.sink.JmsSinkConnector;
import org.springframework.boot.Banner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

public class ApplicationContextHolder {
    public static final String PACKAGES = "component-packages";
    private static final AtomicReference<ConfigurableApplicationContext> CONTEXT_REFERENCE = new AtomicReference<>();

    public static void startApplicationContext(Map<String, String> map) {
        ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(JmsConnectorConfig.class)
                .web(false)
                .bannerMode(Banner.Mode.OFF)
                .properties(map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                .sources(getClientBasePackages(map))
                .run();

        synchronized (JmsSinkConnector.class) {
            CONTEXT_REFERENCE.compareAndSet(null, applicationContext);
            JmsSinkConnector.class.notifyAll();
        }
    }

    private static Object[] getClientBasePackages(Map<String, String> map) {
        return Optional.ofNullable(map.get(PACKAGES)).map(s -> s.split(",")).orElseGet(() -> new String[0]);
    }

    public static ApplicationContext getApplicationContext() {
        synchronized (JmsSinkConnector.class) {
            while (CONTEXT_REFERENCE.get() == null) {
                try {
                    JmsSinkConnector.class.wait();
                } catch (InterruptedException e) {
                    // NOP
                }
            }
            return CONTEXT_REFERENCE.get();
        }
    }

    public static void closeApplicationContext() {
        Optional.ofNullable(CONTEXT_REFERENCE.get()).ifPresent(ConfigurableApplicationContext::close);
        CONTEXT_REFERENCE.set(null);
    }
}
