package org.apache.activemq.artemis.core.server.federation;

import java.util.HashMap;
import java.util.Map;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.federation.address.FederatedAddress;
import org.apache.activemq.artemis.core.server.federation.address.FederatedAddressConfig;
import org.apache.activemq.artemis.core.server.federation.queue.FederatedQueue;
import org.apache.activemq.artemis.core.server.federation.queue.FederatedQueueConfig;

public class FederationManager {

    private final ActiveMQServer server;

    private Map<String, FederationConnection> connections = new HashMap<>();

    private Map<String, FederatedQueue> federatedQueueMap = new HashMap<>();
    private Map<String, FederatedAddress> federatedAddressMap = new HashMap<>();

    public FederationManager(final ActiveMQServer server) {
        this.server = server;
    }

    public boolean deploy(FederationConnectionConfiguration federationConnectionConfiguration) {
        if (connections.containsKey(federationConnectionConfiguration.getName())) {
            return false;
        }
        FederationConnection federationConnection = new FederationConnection();
        federationConnection.init(federationConnectionConfiguration, server.getConfiguration());
        connections.put(federationConnectionConfiguration.getName(), federationConnection);
        return true;
    }

    public FederationConnection get(String name) {
        return connections.get(name);
    }

    public boolean deploy(FederatedQueueConfig federatedQueueConfig) {
        if (federatedQueueMap.containsKey(federatedQueueConfig.getName())) {
            return false;
        }
        FederatedQueue federatedQueue = new FederatedQueue(federatedQueueConfig, server, getConnection(federatedQueueConfig.getConnectionName()));
        federatedQueueMap.put(federatedQueueConfig.getName(), federatedQueue);
        server.registerBrokerPlugin(federatedQueue);
        return true;

    }

    public boolean deploy(FederatedAddressConfig federatedAddressConfig) {
        if (federatedAddressMap.containsKey(federatedAddressConfig.getName())) {
            return false;
        }
        FederatedAddress federatedAddress = new FederatedAddress(federatedAddressConfig, server, getConnection(federatedAddressConfig.getConnectionName()));
        federatedAddressMap.put(federatedAddressConfig.getName(), federatedAddress);
        server.registerBrokerPlugin(federatedAddress);
        return true;

    }

    private FederationConnection getConnection(String connectionName) {
        FederationConnection federationConnection = connections.get(connectionName);
        if (federationConnection == null) {
            throw new IllegalArgumentException(connectionName + " does not exist");
        }
        return federationConnection;
    }


}
