package org.apache.activemq.artemis.core.config.federation;

import java.util.HashSet;
import java.util.Set;

public class FederationUpstreamConfiguration {

    private String name;

    private FederationConnectionConfiguration connectionConfiguration = new FederationConnectionConfiguration();
    private Set<String> policyRefs = new HashSet<>();

    public String getName() {
        return name;
    }

    public FederationUpstreamConfiguration setName(String name) {
        this.name = name;
        return this;
    }

    public Set<String> getPolicyRefs() {
        return policyRefs;
    }

    public FederationUpstreamConfiguration addPolicyRef(String name) {
        policyRefs.add(name);
        return this;
    }

    public FederationConnectionConfiguration getConnectionConfiguration() {
        return connectionConfiguration;
    }
}
