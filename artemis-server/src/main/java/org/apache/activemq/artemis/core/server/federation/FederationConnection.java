package org.apache.activemq.artemis.core.server.federation;

import org.apache.activemq.artemis.api.core.*;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.federation.FederationConnectionConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;

public class FederationConnection {

    private final FederationConnectionConfiguration config;
    private final ServerLocator serverLocator;
    private final long circuitBreakTimeout;
    private volatile ClientSessionFactory clientSessionFactory;
    private volatile boolean started;

    public FederationConnection(Configuration configuration, String name, FederationConnectionConfiguration config) {
        this.config = config;
        this.circuitBreakTimeout = config.getCircuitBreakTimeout();
        if (config.getDiscoveryGroupName() != null) {
            DiscoveryGroupConfiguration discoveryGroupConfiguration = configuration.getDiscoveryGroupConfigurations().get(config.getDiscoveryGroupName());
            if (discoveryGroupConfiguration == null) {
                ActiveMQServerLogger.LOGGER.bridgeNoDiscoveryGroup(config.getDiscoveryGroupName());
                serverLocator = null;
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
                ActiveMQServerLogger.LOGGER.bridgeCantFindConnectors(name);
                serverLocator = null;
                return;
            }

            if (config.isHA()) {
                serverLocator = ActiveMQClient.createServerLocatorWithHA(tcConfigs);
            } else {
                serverLocator = ActiveMQClient.createServerLocatorWithoutHA(tcConfigs);
            }
        }
    }

    public synchronized void start() {
        started = true;
    }

    public synchronized void stop() {
        started = false;
        ClientSessionFactory clientSessionFactory = this.clientSessionFactory;
        if (clientSessionFactory != null) {
            clientSessionFactory.cleanup();
            clientSessionFactory.close();
            this.clientSessionFactory = null;
        }
    }

    public boolean isStarted() {
        return started;
    }

    public final ClientSessionFactory clientSessionFactory() throws Exception {
        ClientSessionFactory clientSessionFactory = this.clientSessionFactory;
        if (started) {
            if (clientSessionFactory != null && !clientSessionFactory.isClosed()) {
                return clientSessionFactory;
            } else {
                return circuitBreakerCreateClientSessionFactory();
            }
        } else {
            throw new ActiveMQSessionCreationException();
        }
    }

    public FederationConnectionConfiguration getConfig() {
        return config;
    }

    private Exception circuitBreakerException;
    private long lastCreateClientSessionFactoryExceptionTimestamp;

    private synchronized ClientSessionFactory circuitBreakerCreateClientSessionFactory() throws Exception {
        if (circuitBreakTimeout < 0 || circuitBreakerException == null || lastCreateClientSessionFactoryExceptionTimestamp < System.currentTimeMillis()) {
            try {
                circuitBreakerException = null;
                return createClientSessionFactory();
            } catch (Exception e) {
                circuitBreakerException = e;
                lastCreateClientSessionFactoryExceptionTimestamp = System.currentTimeMillis() + circuitBreakTimeout;
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
}
