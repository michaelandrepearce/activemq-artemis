package org.apache.activemq.artemis.core.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.activemq.artemis.core.config.federation.FederationPolicy;
import org.apache.activemq.artemis.core.config.federation.FederationUpstreamConfiguration;

public class FederationConfiguration {

    private List<FederationUpstreamConfiguration> upstreamConfigurations = new ArrayList<>();

    private Map<String, FederationPolicy> federationPolicyMap = new HashMap<>();

    public List<FederationUpstreamConfiguration> getUpstreamConfigurations() {
        return upstreamConfigurations;
    }

    public FederationConfiguration addUpstreamConfiguration(FederationUpstreamConfiguration federationUpstreamConfiguration) {
        this.upstreamConfigurations.add(federationUpstreamConfiguration);
        return this;
    }

    public FederationConfiguration addFederationPolicy(FederationPolicy federationPolicy) {
        federationPolicyMap.put(federationPolicy.getName(), federationPolicy);
        return this;
    }

    public Map<String, FederationPolicy> getFederationPolicyMap() {
        return federationPolicyMap;
    }
}
