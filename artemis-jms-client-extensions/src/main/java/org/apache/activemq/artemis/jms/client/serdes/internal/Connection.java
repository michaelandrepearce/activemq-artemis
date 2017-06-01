package org.apache.activemq.artemis.jms.client.serdes.internal;

import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Topic;

import org.apache.activemq.artemis.jms.client.serdes.Serdes;


public class Connection<T extends javax.jms.Connection> extends ForwardingObject<T> implements javax.jms.Connection {

    private Serdes serdes;
    
    public Connection(Serdes serdes, T connection){
        super(connection);
        this.serdes = serdes;
    }
    
    public javax.jms.Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return new Session(serdes, delegate().createSession(transacted, acknowledgeMode));
    }

    public javax.jms.Session createSession(int sessionMode) throws JMSException {
        return new Session(serdes, delegate().createSession(sessionMode));
    }

    public javax.jms.Session createSession() throws JMSException {
        return new Session(serdes, delegate().createSession());
    }

    public String getClientID() throws JMSException {
        return delegate().getClientID();
    }

    public void setClientID(String clientID) throws JMSException {
        delegate().setClientID(clientID);
    }

    public ConnectionMetaData getMetaData() throws JMSException {
        return delegate().getMetaData();
    }

    public ExceptionListener getExceptionListener() throws JMSException {
        return delegate().getExceptionListener();
    }

    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        delegate().setExceptionListener(listener);
    }

    public void start() throws JMSException {
        delegate().start();
    }

    public void stop() throws JMSException {
        delegate().stop();
    }

    public void close() throws JMSException {
        delegate().start();
    }

    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return delegate().createConnectionConsumer(destination, messageSelector, sessionPool, maxMessages);
    }

    public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return delegate().createSharedConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages);
    }

    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return delegate().createDurableConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages);
    }

    public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return delegate().createSharedDurableConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages);
    }
}
