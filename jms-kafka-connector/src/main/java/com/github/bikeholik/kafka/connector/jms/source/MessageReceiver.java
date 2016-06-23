package com.github.bikeholik.kafka.connector.jms.source;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;

public interface MessageReceiver {
    List<SourceRecord> poll();

    void commitIfNecessary();

    void close() throws Exception;

    static String getText(TextMessage message){
        try {
            return message.getText();
        } catch (JMSException e) {
            return null;
        }
    }
}
