package org.apache.activemq.artemis.core.server.federation;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.config.federation.*;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.config.FederationConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;

public class FederationManager implements ActiveMQComponent {

    private final ActiveMQServer server;

    private Map<String, FederationUpstream> upstreams = new HashMap<>();
    private String federationUser;
    private String federationPassword;
    private State state;

    enum State {
        STOPPED,
        STOPPING,
        /**
         * Deployed means {@link FederationManager#deploy()} was called but
         * {@link FederationManager#start()} was not called.
         * <p>
         * We need the distinction if {@link FederationManager#stop()} is called before 'start'. As
         * otherwise we would leak locators.
         */
        DEPLOYED, STARTED,
    }


    public FederationManager(final ActiveMQServer server) {
        this.server = server;
    }

    public synchronized void start() throws ActiveMQException {
        if (state == State.STARTED) return;
        deploy();
        for (FederationUpstream connection : upstreams.values()) {
            connection.start();
        }
        state = State.STARTED;
    }

    public synchronized void stop() {
        if (state == State.STOPPED) return;
        state = State.STOPPING;


        for (FederationUpstream connection : upstreams.values()) {
            connection.stop();
        }
        upstreams.clear();
        state = State.STOPPED;
    }

    @Override
    public boolean isStarted() {
        return state == State.STARTED;
    }

    public synchronized void deploy() throws ActiveMQException {
        federationUser = server.getConfiguration().getFederationUser();
        federationPassword = server.getConfiguration().getFederationPassword();
        for(FederationConfiguration federationConfiguration : server.getConfiguration().getFederationConfigurations()) {
            deploy(federationConfiguration);
        }
        if (state != State.STARTED) {
            state = State.DEPLOYED;
        }
    }

    public synchronized boolean undeploy(String name) {
        FederationUpstream federationConnection = upstreams.remove(name);
        if (federationConnection != null) {
            federationConnection.stop();
        }
        return true;
    }



    public synchronized boolean deploy(FederationConfiguration federationConfiguration) throws ActiveMQException {
        Collection<FederationUpstreamConfiguration> upstreamConfigurationSet = federationConfiguration.getUpstreamConfigurations();
        for (FederationUpstreamConfiguration upstreamConfiguration : upstreamConfigurationSet) {
            String name = upstreamConfiguration.getName();
            FederationUpstream upstream = upstreams.get(name);

            //If connection has changed we will need to do a full undeploy and redeploy.
            if (upstream == null) {
                undeploy(name);
                upstream = deploy(name, upstreamConfiguration);
            } else if (!upstream.getConnection().getConfig().equals(upstreamConfiguration.getConnectionConfiguration())) {
                undeploy(name);
                upstream = deploy(name, upstreamConfiguration);
            }

            upstream.deploy(upstreamConfiguration.getPolicyRefs(), federationConfiguration.getFederationPolicyMap());
        }
        return true;
    }

    private synchronized FederationUpstream deploy(String name, FederationUpstreamConfiguration upstreamConfiguration) {
        FederationUpstream upstream = null;
        upstream = new FederationUpstream(server, this, name, upstreamConfiguration);
        upstreams.put(name, upstream);
        if (state == State.STARTED) {
            upstream.start();
        }
        return upstream;
    }

    public FederationUpstream get(String name) {
        return upstreams.get(name);
    }



    public void register(FederatedAbstract federatedAbstract) {
        server.registerBrokerPlugin(federatedAbstract);
    }

    public void unregister(FederatedAbstract federatedAbstract) {
        server.unRegisterBrokerPlugin(federatedAbstract);
    }

    String getFederationPassword() {
        return federationPassword;
    }

    String getFederationUser() {
        return federationUser;
    }

}
