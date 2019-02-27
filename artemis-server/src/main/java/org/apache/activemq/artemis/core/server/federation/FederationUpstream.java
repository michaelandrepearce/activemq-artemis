package org.apache.activemq.artemis.core.server.federation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.federation.*;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.federation.address.FederatedAddress;
import org.apache.activemq.artemis.core.server.federation.queue.FederatedQueue;

public class FederationUpstream {

    private final ActiveMQServer server;
    private final FederationManager federationManager;
    private final SimpleString name;
    private FederationConnection connection;
    private FederationUpstreamConfiguration config;
    private Map<String, FederatedQueue> federatedQueueMap = new HashMap<>();
    private Map<String, FederatedAddress> federatedAddressMap = new HashMap<>();

    public FederationUpstream(ActiveMQServer server, FederationManager federationManager, String name, FederationUpstreamConfiguration config) {
        this.server = server;
        this.federationManager = federationManager;
        this.name = SimpleString.toSimpleString(config.getName());
        this.config = config;
        this.connection = new FederationConnection(server.getConfiguration(), name, config.getConnectionConfiguration());

    }

    public synchronized void start() {
        connection.start();
        for (FederatedQueue federatedQueue : federatedQueueMap.values()) {
            federatedQueue.start();
        }
        for (FederatedAddress federatedAddress : federatedAddressMap.values()) {
            federatedAddress.start();
        }
    }

    public synchronized void stop() {
        for (FederatedAddress federatedAddress : federatedAddressMap.values()) {
            federatedAddress.stop();
        }
        federatedAddressMap.clear();

        for (FederatedQueue federatedQueue : federatedQueueMap.values()) {
            federatedQueue.stop();
        }
        federatedQueueMap.clear();

        connection.stop();
    }

    public void deploy(Set<String> policyRefsToDeploy, Map<String, FederationPolicy> policyMap) throws ActiveMQException {
        deployPolicyRefs(policyRefsToDeploy, policyMap, 0);
    }

    private void deployPolicyRefs(Set<String> policyRefsToDeploy, Map<String, FederationPolicy> policyMap, int recursionDepth) throws ActiveMQException {
        for(String policyRef : policyRefsToDeploy) {
            FederationPolicy policy = policyMap.get(policyRef);
            if (policy != null) {
                if (policy instanceof FederationPolicySet) {
                    FederationPolicySet federationPolicySet = (FederationPolicySet) policy;
                    if (recursionDepth < 10) {
                        deployPolicyRefs(federationPolicySet.getPolicyRefs(), policyMap, ++recursionDepth);
                    } else {
                        ActiveMQServerLogger.LOGGER.federationAvoidStackOverflowPolicyRef(name.toString(), policyRef);
                    }
                } else if (policy instanceof FederationQueuePolicyConfiguration) {
                    deploy((FederationQueuePolicyConfiguration) policy);
                } else if (policy instanceof FederationAddressPolicyConfiguration) {
                    deploy((FederationAddressPolicyConfiguration) policy);
                } else {
                    ActiveMQServerLogger.LOGGER.federationUnknownPolicyType(name.toString(), policyRef);
                }
            } else {
                ActiveMQServerLogger.LOGGER.federationCantFindPolicyRef(name.toString(), policyRef);
            }
        }
    }

    public synchronized boolean deploy(FederationQueuePolicyConfiguration federatedQueueConfig) throws ActiveMQException {
        String name = federatedQueueConfig.getName();
        FederatedQueue existing = federatedQueueMap.get(name);
        if (existing == null || !existing.getConfig().equals(federatedQueueConfig)) {
            undeployQueue(name);

            FederatedQueue federatedQueue = new FederatedQueue(federationManager, federatedQueueConfig, server, this);
            federatedQueueMap.put(name, federatedQueue);
            federationManager.register(federatedQueue);
            if (connection.isStarted()) {
                federatedQueue.start();
            }
            return true;
        }
        return false;

    }

    public synchronized boolean deploy(FederationAddressPolicyConfiguration federatedAddressConfig) throws ActiveMQException {
        String name = federatedAddressConfig.getName();
        FederatedAddress existing = federatedAddressMap.get(name);
        if (existing == null || !existing.getConfig().equals(federatedAddressConfig)) {
            undeployAddress(name);

            FederatedAddress federatedAddress = new FederatedAddress(federationManager, federatedAddressConfig, server, this);
            federatedAddressMap.put(name, federatedAddress);
            federationManager.register(federatedAddress);
            if (connection.isStarted()) {
                federatedAddress.start();
            }
            return true;
        }
        return false;
    }

    private void undeployAddress(String name) {
        FederatedAddress federatedAddress = federatedAddressMap.remove(name);
        if (federatedAddress != null) {
            federatedAddress.stop();
            federationManager.unregister(federatedAddress);
        }
    }

    private void undeployQueue(String name) {
        FederatedQueue federatedQueue = federatedQueueMap.remove(name);
        if (federatedQueue != null) {
            federatedQueue.stop();
            federationManager.unregister(federatedQueue);
        }
    }

    public FederationUpstreamConfiguration getConfig() {
        return config;
    }

    private Exception circuitBreakerException;
    private long lastCreateClientSessionFactoryExceptionTimestamp;

    public SimpleString getName() {
        return name;
    }

    public FederationConnection getConnection() {
        return connection;
    }
}
