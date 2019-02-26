package org.apache.activemq.artemis.core.server.federation;

import java.util.List;
import org.apache.activemq.artemis.core.server.federation.address.FederatedAddressConfig;
import org.apache.activemq.artemis.core.server.federation.queue.FederatedQueueConfig;

public class FederationConnectionConfiguration {

    private final String name;
    private boolean isHA;
    private String discoveryGroupName;
    private List<String> staticConnectors;

    private FederatedAddressConfig addressConfig;

    private FederatedQueueConfig queueConfig;

    public FederationConnectionConfiguration(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getDiscoveryGroupName() {
        return discoveryGroupName;
    }

    public void setDiscoveryGroupName(String discoveryGroupName) {
        this.discoveryGroupName = discoveryGroupName;
    }

    public List<String> getStaticConnectors() {
        return staticConnectors;
    }

    public void setStaticConnectors(List<String> staticConnectors) {
        this.staticConnectors = staticConnectors;
    }

    public boolean isHA() {
        return isHA;
    }

    public void setHA(boolean HA) {
        isHA = HA;
    }

    public FederatedAddressConfig getAddressConfig() {
        return addressConfig;
    }

    public void setAddressConfig(FederatedAddressConfig addressConfig) {
        this.addressConfig = addressConfig;
    }

    public FederatedQueueConfig getQueueConfig() {
        return queueConfig;
    }

    public void setQueueConfig(FederatedQueueConfig queueConfig) {
        this.queueConfig = queueConfig;
    }
}
