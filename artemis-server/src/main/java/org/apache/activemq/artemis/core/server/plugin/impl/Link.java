package org.apache.activemq.artemis.core.server.plugin.impl;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;

public class Link {

    private String name;
    private ServerLocator serverLocator;
    private volatile ClientSessionFactory clientSessionFactory;

    public void init(LinkConfiguration config, Configuration configuration) {
        this.name = config.getName();
        if (config.getDiscoveryGroupName() != null) {
            DiscoveryGroupConfiguration discoveryGroupConfiguration = configuration.getDiscoveryGroupConfigurations().get(config.getDiscoveryGroupName());
            if (discoveryGroupConfiguration == null) {
                ActiveMQServerLogger.LOGGER.bridgeNoDiscoveryGroup(config.getDiscoveryGroupName());
                return;
            }

            if (config.isHA()) {
                serverLocator = ActiveMQClient.createServerLocatorWithHA(discoveryGroupConfiguration);
            } else {
                serverLocator = ActiveMQClient.createServerLocatorWithoutHA(discoveryGroupConfiguration);
            }

        } else {
            TransportConfiguration[] tcConfigs = configuration.getTransportConfigurations(config.getStaticConnectors());

            if (tcConfigs == null) {
                ActiveMQServerLogger.LOGGER.bridgeCantFindConnectors(config.getName());
                return;
            }

            if (config.isHA()) {
                serverLocator = ActiveMQClient.createServerLocatorWithHA(tcConfigs);
            } else {
                serverLocator = ActiveMQClient.createServerLocatorWithoutHA(tcConfigs);
            }
        }
    }

    public final ClientSessionFactory clientSessionFactory() throws Exception {
        ClientSessionFactory clientSessionFactory = this.clientSessionFactory;
        if (clientSessionFactory != null && !clientSessionFactory.isClosed()) {
            return clientSessionFactory;
        } else {
            return createClientSessionFactory();
        }
    }

    private synchronized ClientSessionFactory createClientSessionFactory() throws Exception {
        ClientSessionFactory clientSessionFactory = this.clientSessionFactory;
        if (clientSessionFactory != null && !clientSessionFactory.isClosed()) {
            return clientSessionFactory;
        } else {
            clientSessionFactory = serverLocator.createSessionFactory();
            this.clientSessionFactory = clientSessionFactory;
            return clientSessionFactory;
        }
    }

    public String getName() {
        return name;
    }
}
