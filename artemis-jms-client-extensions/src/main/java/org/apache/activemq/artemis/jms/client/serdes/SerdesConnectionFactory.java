package org.apache.activemq.artemis.jms.client.serdes;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;

import org.apache.activemq.artemis.jms.client.serdes.internal.ForwardingObject;


public class SerdesConnectionFactory extends ForwardingObject<ConnectionFactory> implements javax.jms.ConnectionFactory {

    private final Serdes serdes;

    public SerdesConnectionFactory(javax.jms.ConnectionFactory connectionFactory){
        this(connectionFactory, null);
    }

    public SerdesConnectionFactory(javax.jms.ConnectionFactory connectionFactory, Serdes serdes){
        super(connectionFactory);
        this.serdes = serdes;
    }

    public Connection createConnection() throws JMSException {
        return serdes == null ? delegate().createConnection() : new org.apache.activemq.artemis.jms.client.serdes.internal.Connection(serdes, delegate().createConnection());
    }

    public Connection createConnection(String userName, String password) throws JMSException {
        return serdes == null ? delegate().createConnection(userName, password) : new org.apache.activemq.artemis.jms.client.serdes.internal.Connection(serdes, delegate().createConnection(userName, password));
    }

    public JMSContext createContext() {
        return serdes == null ? delegate().createContext() : new org.apache.activemq.artemis.jms.client.serdes.internal.JMSContext(serdes, delegate().createContext());
    }

    public JMSContext createContext(String userName, String password) {
        return serdes == null ? delegate().createContext(userName, password) : new org.apache.activemq.artemis.jms.client.serdes.internal.JMSContext(serdes, delegate().createContext(userName, password));
    }

    public JMSContext createContext(String userName, String password, int sessionMode) {
        return serdes == null ? delegate().createContext(userName, password, sessionMode) : new org.apache.activemq.artemis.jms.client.serdes.internal.JMSContext(serdes, delegate().createContext(userName, password, sessionMode));
    }

    public JMSContext createContext(int sessionMode) {
        return serdes == null ? delegate().createContext(sessionMode) : new org.apache.activemq.artemis.jms.client.serdes.internal.JMSContext(serdes, delegate().createContext(sessionMode));
    }

    public Serdes getSerdes() {
        return serdes;
    }

}
