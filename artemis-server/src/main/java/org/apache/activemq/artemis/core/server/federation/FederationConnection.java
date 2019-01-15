package org.apache.activemq.artemis.core.server.federation;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;

public class FederationConnection {

    private SimpleString name;
    private ServerLocator serverLocator;
    private volatile ClientSessionFactory clientSessionFactory;

    public void init(FederationConnectionConfiguration config, Configuration configuration) {
        this.name = SimpleString.toSimpleString(config.getName());
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
            return circuitBreakerCreateClientSessionFactory();
        }
    }

    private Exception circuitBreakerException;
    private long lastCreateClientSessionFactoryExceptionTimestamp;

    private synchronized ClientSessionFactory circuitBreakerCreateClientSessionFactory() throws Exception {
        if (circuitBreakerException == null || lastCreateClientSessionFactoryExceptionTimestamp < System.currentTimeMillis()) {
            try {
                circuitBreakerException = null;
                return createClientSessionFactory();
            } catch (Exception e) {
                circuitBreakerException = e;
                lastCreateClientSessionFactoryExceptionTimestamp = System.currentTimeMillis() + 30000;
                throw e;
            }
        } else {
            throw circuitBreakerException;
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

    public SimpleString getName() {
        return name;
    }
}
