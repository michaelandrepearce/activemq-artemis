package org.apache.activemq.artemis.core.config.federation;

import java.util.List;

public class FederationConnectionConfiguration {

    private boolean isHA;
    private String discoveryGroupName;
    private List<String> staticConnectors;
    private long circuitBreakTimeout = 30000;

    public String getDiscoveryGroupName() {
        return discoveryGroupName;
    }

    public FederationConnectionConfiguration setDiscoveryGroupName(String discoveryGroupName) {
        this.discoveryGroupName = discoveryGroupName;
        return this;
    }

    public List<String> getStaticConnectors() {
        return staticConnectors;
    }

    public FederationConnectionConfiguration setStaticConnectors(List<String> staticConnectors) {
        this.staticConnectors = staticConnectors;
        return this;
    }

    public boolean isHA() {
        return isHA;
    }

    public FederationConnectionConfiguration setHA(boolean HA) {
        isHA = HA;
        return this;
    }

    public long getCircuitBreakTimeout() {
        return circuitBreakTimeout;
    }

    public FederationConnectionConfiguration setCircuitBreakTimeout(long circuitBreakTimeout) {
        this.circuitBreakTimeout = circuitBreakTimeout;
        return this;
    }

}
