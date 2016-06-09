package com.github.bikeholik.kafka.connector.jms;

import java.util.Collections;
import java.util.HashMap;
import java.util.stream.IntStream;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class JmsSinkTaskTest {

    private final JmsSinkTask task = new JmsSinkTask();

    @Before
    public void setUp() throws Exception {
        task.start(new HashMap<>());

    }

    @Test
    public void put() throws Exception {
        IntStream.range(1, 4)
                .forEach(i -> task.put(Collections.nCopies(i, Mockito.mock(SinkRecord.class))));
    }

}