package com.github.bikeholik.kafka.connector.hornetq;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "hornetq", ignoreInvalidFields = true, exceptionIfInvalid = false)
public class HornetQConnectionFactoryProperties {

    private String cluster;

    private String user;

    private String password;

    private Integer sessionsCached;

    private Integer connectionCacheSize;

    private Long clientFailureCheckPeriod;

    private Long connectionTtl;

    private Long retryInterval;

    private Integer reconnectAttempts;

    private Long callTimeout;

    private Long callFailoverTimeout;

    private Boolean blockOnDurableSend;

    private Boolean blockOnNonDurableSend;

    private Boolean compressLargeMessage;

    private Integer producerWindowSize;

    private Integer consumerWindowSize;

    private Integer confirmationWindowSize;
    private boolean cacheProducers;
    private boolean cacheConsumers;

    public Long getCallTimeout() {
        return callTimeout;
    }

    public void setCallTimeout(Long callTimeout) {
        this.callTimeout = callTimeout;
    }

    public Long getCallFailoverTimeout() {
        return callFailoverTimeout;
    }

    public void setCallFailoverTimeout(Long callFailoverTimeout) {
        this.callFailoverTimeout = callFailoverTimeout;
    }

    public Boolean isBlockOnDurableSend() {
        return blockOnDurableSend;
    }

    public void setBlockOnDurableSend(Boolean blockOnDurableSend) {
        this.blockOnDurableSend = blockOnDurableSend;
    }

    public Boolean isBlockOnNonDurableSend() {
        return blockOnNonDurableSend;
    }

    public void setBlockOnNonDurableSend(Boolean blockOnNonDurableSend) {
        this.blockOnNonDurableSend = blockOnNonDurableSend;
    }

    public Boolean isCompressLargeMessage() {
        return compressLargeMessage;
    }

    public void setCompressLargeMessage(Boolean compressLargeMessage) {
        this.compressLargeMessage = compressLargeMessage;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getSessionsCached() {
        return sessionsCached;
    }

    public void setSessionsCached(Integer sessionsCached) {
        this.sessionsCached = sessionsCached;
    }

    public Long getClientFailureCheckPeriod() {
        return clientFailureCheckPeriod;
    }

    public void setClientFailureCheckPeriod(Long clientFailureCheckPeriod) {
        this.clientFailureCheckPeriod = clientFailureCheckPeriod;
    }

    public Long getConnectionTtl() {
        return connectionTtl;
    }

    public void setConnectionTtl(Long connectionTtl) {
        this.connectionTtl = connectionTtl;
    }

    public Long getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(Long retryInterval) {
        this.retryInterval = retryInterval;
    }

    public Integer getReconnectAttempts() {
        return reconnectAttempts;
    }

    public void setReconnectAttempts(Integer reconnectAttempts) {
        this.reconnectAttempts = reconnectAttempts;
    }

    public Integer getConnectionCacheSize() {
        return connectionCacheSize;
    }

    public void setConnectionCacheSize(Integer connectionCacheSize) {
        this.connectionCacheSize = connectionCacheSize;
    }

    public Integer getProducerWindowSize() {
        return producerWindowSize;
    }

    public Integer getConsumerWindowSize() {
        return consumerWindowSize;
    }

    public Integer getConfirmationWindowSize() {
        return confirmationWindowSize;
    }

    public void setProducerWindowSize(Integer producerWindowSize) {
        this.producerWindowSize = producerWindowSize;
    }

    public void setConsumerWindowSize(Integer consumerWindowSize) {
        this.consumerWindowSize = consumerWindowSize;
    }

    public void setConfirmationWindowSize(Integer confirmationWindowSize) {
        this.confirmationWindowSize = confirmationWindowSize;
    }


    public boolean isCacheProducers() {
        return cacheProducers;
    }

    public void setCacheProducers(boolean cacheProducers) {
        this.cacheProducers = cacheProducers;
    }

    public boolean isCacheConsumers() {
        return cacheConsumers;
    }

    public void setCacheConsumers(boolean cacheConsumers) {
        this.cacheConsumers = cacheConsumers;
    }
}
