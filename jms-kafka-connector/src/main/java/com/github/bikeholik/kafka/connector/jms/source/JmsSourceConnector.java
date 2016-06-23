package com.github.bikeholik.kafka.connector.jms.source;

import javax.jms.ConnectionFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.github.bikeholik.kafka.connector.jms.JmsConnectorConfigurationProperties;
import com.github.bikeholik.kafka.connector.jms.util.ApplicationContextHolder;
import com.github.bikeholik.kafka.connector.jms.util.TopicsMappingHolder;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

public class JmsSourceConnector extends SourceConnector {

    private static final String TASK_DESTINATION_NAME = "destinationName";
    private String[] queues;

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        ApplicationContextHolder.startApplicationContext(props);
        queues = Optional.ofNullable(props.get("queues"))
                .map(queueNames -> queueNames.split(","))
                .orElseThrow(IllegalArgumentException::new);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JmsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // #tasks > #queues
        return IntStream.range(0, maxTasks)
                .mapToObj(this::buildTask)
                .collect(Collectors.toList());
    }

    private Map<String, String> buildTask(int i) {
        HashMap<String, String> map = new HashMap<>();
        map.put(TASK_DESTINATION_NAME, queues[i % queues.length]);
        map.put("task-id", String.valueOf(i));
        return map;
    }

    @Override
    public void stop() {
        ApplicationContextHolder.closeApplicationContext();
    }

    public static class JmsSourceTask extends SourceTask {

        private MessageReceiver messageReceiver;

        @Override
        public String version() {
            return null;
        }

        @Override
        public void start(Map<String, String> props) {
            LoggerFactory.getLogger(getClass()).info("op=start props={}", props);
            // get session & consumer (per one queue -> meaning multiply tasks by queues count)
            // TODO or is t one connector per topic ? -> source record has a topic
            String destinationName = props.get(TASK_DESTINATION_NAME);
            ApplicationContext context = ApplicationContextHolder.getApplicationContext();
            messageReceiver = context
                    .getBean(SimpleMessageReceiver.class,
                            context.getBean(ConnectionFactory.class),
                            context.getBean(JmsConnectorConfigurationProperties.class),
                            context.getBean(TopicsMappingHolder.class),
                            destinationName);
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            //
            // read up to X messages
            // can source partition/offset be empty ?
            return messageReceiver.poll();
        }

        @Override
        public void stop() {
            try {
                messageReceiver.close();
            } catch (Exception e) {
                LoggerFactory.getLogger(getClass()).info("op=errorOnStop", e);
            }
        }

        @Override
        public void commit() throws InterruptedException {
            // TODO commit session
            messageReceiver.commitIfNecessary();
        }
    }

}
