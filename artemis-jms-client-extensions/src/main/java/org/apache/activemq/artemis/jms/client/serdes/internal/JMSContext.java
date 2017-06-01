package org.apache.activemq.artemis.jms.client.serdes.internal;

import javax.jms.BytesMessage;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.Serializable;

import org.apache.activemq.artemis.jms.client.serdes.Serdes;

public class JMSContext<T extends javax.jms.JMSContext> extends ForwardingObject<T> implements javax.jms.JMSContext {

    private final Serdes serdes;

    public JMSContext(Serdes serdes, T jmsContext){
        super(jmsContext);
        this.serdes = serdes;
    }
    
    public javax.jms.JMSContext createContext(int sessionMode) {
        return new JMSContext(serdes, delegate().createContext(sessionMode));
    }

    public javax.jms.JMSProducer createProducer() {
        return new JMSProducer(serdes, delegate().createProducer());
    }

    public String getClientID() {
        return delegate().getClientID();
    }

    public void setClientID(String clientID) {
        delegate().setClientID(clientID);
    }

    public ConnectionMetaData getMetaData() {
        return delegate().getMetaData();
    }

    public ExceptionListener getExceptionListener() {
        return delegate().getExceptionListener();
    }

    public void setExceptionListener(ExceptionListener listener) {
        delegate().setExceptionListener(listener);
    }

    public void start() {
        delegate().start();
    }

    public void stop() {
        delegate().stop();
    }

    public void setAutoStart(boolean autoStart) {
        delegate().setAutoStart(autoStart);
    }

    public boolean getAutoStart() {
        return delegate().getAutoStart();
    }

    public void close() {
        delegate().close();
    }

    public BytesMessage createBytesMessage() {
        return delegate().createBytesMessage();
    }

    public MapMessage createMapMessage() {
        return delegate().createMapMessage();
    }

    public Message createMessage() {
        return delegate().createMessage();
    }

    public ObjectMessage createObjectMessage() {
        return new SerdesObjectMessage(serdes, createBytesMessage());
    }

    public ObjectMessage createObjectMessage(Serializable object) {
        SerdesObjectMessage
            objectMessage = new SerdesObjectMessage(serdes, createBytesMessage());
        objectMessage.setObject(object);
        return objectMessage;
    }

    public StreamMessage createStreamMessage() {
        return delegate().createStreamMessage();
    }

    public TextMessage createTextMessage() {
        return delegate().createTextMessage();
    }

    public TextMessage createTextMessage(String text) {
        return delegate().createTextMessage(text);
    }

    public boolean getTransacted() {
        return delegate().getTransacted();
    }

    public int getSessionMode() {
        return delegate().getSessionMode();
    }

    public void commit() {
        delegate().commit();
    }

    public void rollback() {
        delegate().rollback();
    }

    public void recover() {
        delegate().recover();
    }

    public javax.jms.JMSConsumer createConsumer(Destination destination) {
        return new JMSConsumer(serdes, delegate().createConsumer(destination));
    }

    public javax.jms.JMSConsumer createConsumer(Destination destination, String messageSelector) {
        return new JMSConsumer(serdes, delegate().createConsumer(destination, messageSelector));
    }

    public javax.jms.JMSConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) {
        return new JMSConsumer(serdes, delegate().createConsumer(destination, messageSelector, noLocal));
    }

    public Queue createQueue(String queueName) {
        return delegate().createQueue(queueName);
    }

    public Topic createTopic(String topicName) {
        return delegate().createTopic(topicName);
    }

    public javax.jms.JMSConsumer createDurableConsumer(Topic topic, String subscriptionName) {
        return  new JMSConsumer(serdes,delegate().createDurableConsumer(topic, subscriptionName));
    }

    public javax.jms.JMSConsumer createDurableConsumer(Topic topic, String subscriptionName, String messageSelector, boolean noLocal) {
        return new JMSConsumer(serdes,delegate().createDurableConsumer(topic, subscriptionName, messageSelector, noLocal));
    }

    public javax.jms.JMSConsumer createSharedDurableConsumer(Topic topic, String subscriptionName) {
        return new JMSConsumer(serdes,delegate().createSharedDurableConsumer(topic, subscriptionName));
    }

    public javax.jms.JMSConsumer createSharedDurableConsumer(Topic topic, String subscriptionName, String messageSelector) {
        return new JMSConsumer(serdes, delegate().createSharedDurableConsumer(topic, subscriptionName, messageSelector));
    }

    public javax.jms.JMSConsumer createSharedConsumer(Topic topic, String subscriptionName) {
        return new JMSConsumer(serdes,delegate().createSharedConsumer(topic, subscriptionName));
    }

    public javax.jms.JMSConsumer createSharedConsumer(Topic topic, String subscriptionName, String messageSelector) {
        return new JMSConsumer(serdes,delegate().createSharedConsumer(topic, subscriptionName, messageSelector));
    }

    public QueueBrowser createBrowser(Queue queue) {
        return delegate().createBrowser(queue);
    }

    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        return delegate().createBrowser(queue, messageSelector);
    }

    public TemporaryQueue createTemporaryQueue() {
        return delegate().createTemporaryQueue();
    }

    public TemporaryTopic createTemporaryTopic() {
        return delegate().createTemporaryTopic();
    }

    public void unsubscribe(String name) {
        delegate().unsubscribe(name);
    }

    public void acknowledge() {
        delegate().acknowledge();
    }
}