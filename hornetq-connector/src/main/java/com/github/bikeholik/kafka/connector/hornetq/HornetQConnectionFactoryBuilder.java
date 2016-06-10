package com.github.bikeholik.kafka.connector.hornetq;

import static org.hornetq.api.core.client.HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;
import static org.hornetq.api.core.client.HornetQClient.DEFAULT_CONNECTION_TTL;
import static org.hornetq.api.core.client.HornetQClient.DEFAULT_RECONNECT_ATTEMPTS;
import static org.hornetq.api.core.client.HornetQClient.DEFAULT_RETRY_INTERVAL;
import static org.springframework.util.Assert.hasLength;

import javax.jms.ConnectionFactory;
import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.jms.client.HornetQJMSConnectionFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.connection.UserCredentialsConnectionFactoryAdapter;

class HornetQConnectionFactoryBuilder {

    private static final String HOST_TOKEN = ",";

    private static final String URI_TOKEN = ":";

    private static final boolean WITH_HA = true;

    private final String clusterAddress;

    private String user = "user";

    private String password = "pass";

    private int sessionsCached = 1;

    private long clientFailureCheckPeriod = DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;

    private long connectionTtl = DEFAULT_CONNECTION_TTL;

    private long retryInterval = DEFAULT_RETRY_INTERVAL;

    private int reconnectAttempts = DEFAULT_RECONNECT_ATTEMPTS;

    private HornetQConnectionFactoryBuilder(String clusterAddress) {

        this.clusterAddress = clusterAddress;
    }

    public static HornetQConnectionFactoryBuilder forCluster(String clusterAddress) {
        hasLength(clusterAddress, "The hornetq cluster address cannot be empty");

        return new HornetQConnectionFactoryBuilder(clusterAddress);
    }

    public HornetQConnectionFactoryBuilder user(String user) {
        this.user = user;

        return this;
    }

    public HornetQConnectionFactoryBuilder password(String password) {
        this.password = password;

        return this;
    }

    public HornetQConnectionFactoryBuilder sessionCached(int sessionsCached) {
        this.sessionsCached = sessionsCached;

        return this;
    }

    public HornetQConnectionFactoryBuilder failureCheckPeriod(long clientFailureCheckPeriod) {
        this.clientFailureCheckPeriod = clientFailureCheckPeriod;

        return this;
    }

    public HornetQConnectionFactoryBuilder connectionTtl(long connectionTtl) {
        this.connectionTtl = connectionTtl;

        return this;
    }

    public HornetQConnectionFactoryBuilder retryInterval(long retryInterval) {
        this.retryInterval = retryInterval;

        return this;
    }

    public HornetQConnectionFactoryBuilder reconnectAttempts(int reconnectAttempts) {
        this.reconnectAttempts = reconnectAttempts;

        return this;
    }

    ConnectionFactory buildCachingConnectionFactory() {

        UserCredentialsConnectionFactoryAdapter userCredentialsAdapter = buildAuthenticatingConnectionFactory();

        CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(userCredentialsAdapter);
        cachingConnectionFactory.setSessionCacheSize(sessionsCached);

        return cachingConnectionFactory;
    }

    UserCredentialsConnectionFactoryAdapter buildAuthenticatingConnectionFactory() {
        UserCredentialsConnectionFactoryAdapter userCredentialsAdapter = new UserCredentialsConnectionFactoryAdapter();
        userCredentialsAdapter.setTargetConnectionFactory(hornetQJMSConnectionFactory());
        userCredentialsAdapter.setUsername(user);
        userCredentialsAdapter.setPassword(password);
        return userCredentialsAdapter;
    }

    HornetQJMSConnectionFactory hornetQJMSConnectionFactory() {

        HornetQJMSConnectionFactory connectionFactory =
                new HornetQJMSConnectionFactory(WITH_HA, hornetQTransportConfigurations(clusterAddress));

        connectionFactory.setClientFailureCheckPeriod(clientFailureCheckPeriod);
        connectionFactory.setConnectionTTL(connectionTtl);
        connectionFactory.setRetryInterval(retryInterval);
        connectionFactory.setReconnectAttempts(reconnectAttempts);

        return connectionFactory;
    }

    private TransportConfiguration[] hornetQTransportConfigurations(final String clusterAddress) {

        String[] hosts = clusterAddress.split(HOST_TOKEN);

        TransportConfiguration[] configurations = new TransportConfiguration[hosts.length];

        for (int i = 0; i < hosts.length; i++) {
            String uri = hosts[i];
            String[] tokens = uri.split(URI_TOKEN);
            Map<String, Object> params = new HashMap<String, Object>();
            params.put(TransportConstants.HOST_PROP_NAME, tokens[0]);
            params.put(TransportConstants.PORT_PROP_NAME, Integer.parseInt(tokens[1]));

            configurations[i] = new TransportConfiguration(NettyConnectorFactory.class.getName(), params);
        }

        return configurations;
    }
}
