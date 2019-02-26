package org.apache.activemq.artemis.core.server.federation;

import java.util.HashMap;
import java.util.Map;
import org.apache.activemq.artemis.api.core.ActiveMQException;
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

    public boolean deploy(FederationConnectionConfiguration federationConnectionConfiguration) throws ActiveMQException {
        if (connections.containsKey(federationConnectionConfiguration.getName())) {
            return false;
        }
        FederationConnection federationConnection = new FederationConnection();
        federationConnection.init(federationConnectionConfiguration, server.getConfiguration());
        connections.put(federationConnectionConfiguration.getName(), federationConnection);

        if (federationConnectionConfiguration.getAddressConfig() != null) {
            deploy(federationConnectionConfiguration.getName(), federationConnectionConfiguration.getAddressConfig());
        }
        if (federationConnectionConfiguration.getQueueConfig() != null) {
            deploy(federationConnectionConfiguration.getName(), federationConnectionConfiguration.getQueueConfig());
        }

        return true;
    }

    public FederationConnection get(String name) {
        return connections.get(name);
    }

    public boolean deploy(String name, FederatedQueueConfig federatedQueueConfig) throws ActiveMQException {
        if (federatedQueueMap.containsKey(name)) {
            return false;
        }
        FederatedQueue federatedQueue = new FederatedQueue(federatedQueueConfig, server, getConnection(name));
        federatedQueueMap.put(name, federatedQueue);
        server.registerBrokerPlugin(federatedQueue);
        return true;

    }

    public boolean deploy(String name, FederatedAddressConfig federatedAddressConfig) throws ActiveMQException {
        if (federatedAddressMap.containsKey(name)) {
            return false;
        }
        FederatedAddress federatedAddress = new FederatedAddress(federatedAddressConfig, server, getConnection(name));
        federatedAddressMap.put(name, federatedAddress);
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
