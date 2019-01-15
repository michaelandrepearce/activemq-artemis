package org.apache.activemq.artemis.core.server.plugin.impl;

import java.util.List;

public class LinkConfiguration {

    private String name;
    private boolean isHA;
    private String discoveryGroupName;
    private List<String> staticConnectors;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
}
