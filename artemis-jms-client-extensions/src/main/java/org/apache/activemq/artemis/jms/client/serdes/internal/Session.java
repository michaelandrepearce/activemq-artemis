package org.apache.activemq.artemis.jms.client.serdes.internal;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
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


public class Session<T extends javax.jms.Session> extends ForwardingObject<T> implements javax.jms.Session {

    private Serdes serdes;
    
    public Session(Serdes serdes, T session){
        super(session);
        this.serdes = serdes;
    }
    
    public BytesMessage createBytesMessage() throws JMSException {
        return delegate().createBytesMessage();
    }

    public MapMessage createMapMessage() throws JMSException {
        return delegate().createMapMessage();
    }

    public Message createMessage() throws JMSException {
        return delegate().createMessage();
    }

    public ObjectMessage createObjectMessage() throws JMSException {
        return new SerdesObjectMessage(serdes, createBytesMessage());
    }

    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        ObjectMessage objectMessage = createObjectMessage();
        objectMessage.setObject(object);
        return objectMessage;
    }

    public StreamMessage createStreamMessage() throws JMSException {
        return delegate().createStreamMessage();
    }

    public TextMessage createTextMessage() throws JMSException {
        return delegate().createTextMessage();
    }

    public TextMessage createTextMessage(String text) throws JMSException {
        return delegate().createTextMessage(text);
    }

    public boolean getTransacted() throws JMSException {
        return delegate().getTransacted();
    }

    public int getAcknowledgeMode() throws JMSException {
        return delegate().getAcknowledgeMode();
    }

    public void commit() throws JMSException {
        delegate().commit();
    }

    public void rollback() throws JMSException {
        delegate().rollback();
    }

    public void close() throws JMSException {
        delegate().close();
    }

    public void recover() throws JMSException {
        delegate().recover();
    }

    public javax.jms.MessageListener getMessageListener() throws JMSException {
        return delegate().getMessageListener();
    }

    public void setMessageListener(javax.jms.MessageListener messageListener) throws JMSException {
        delegate().setMessageListener(new MessageListener(serdes, messageListener));
    }

    public void run() {
        delegate().run();
    }

    public javax.jms.MessageProducer createProducer(Destination destination) throws JMSException {
        return new MessageProducer(delegate().createProducer(destination));
    }

    public javax.jms.MessageConsumer createConsumer(Destination destination) throws JMSException {
        return new MessageConsumer(serdes, delegate().createConsumer(destination));
    }

    public javax.jms.MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        return new MessageConsumer(serdes, delegate().createConsumer(destination, messageSelector));
    }

    public javax.jms.MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        return new MessageConsumer(serdes, delegate().createConsumer(destination, messageSelector, noLocal));
    }

    public javax.jms.MessageConsumer createSharedConsumer(Topic topic, String subscriptionName) throws JMSException {
        return new MessageConsumer(serdes, delegate().createSharedConsumer(topic, subscriptionName));
    }

    public javax.jms.MessageConsumer createSharedConsumer(Topic topic, String subscriptionName, String messageSelector) throws JMSException {
        return new MessageConsumer(serdes, delegate().createSharedConsumer(topic, subscriptionName, messageSelector));
    }

    public Queue createQueue(String queueName) throws JMSException {
        return delegate().createQueue(queueName);
    }

    public Topic createTopic(String topicName) throws JMSException {
        return delegate().createTopic(topicName);
    }

    public javax.jms.TopicSubscriber createDurableSubscriber(Topic topic, String subscriptionName) throws JMSException {
        return new TopicSubscriber(serdes, delegate().createDurableSubscriber(topic, subscriptionName));
    }

    public javax.jms.TopicSubscriber createDurableSubscriber(Topic topic, String subscriptionName, String messageSelector, boolean noLocal) throws JMSException {
        return new TopicSubscriber(serdes, delegate().createDurableSubscriber(topic, subscriptionName, messageSelector, noLocal));
    }

    public javax.jms.MessageConsumer createDurableConsumer(Topic topic, String subscriptionName) throws JMSException {
        return new MessageConsumer(serdes, delegate().createDurableConsumer(topic, subscriptionName));
    }

    public javax.jms.MessageConsumer createDurableConsumer(Topic topic, String subscriptionName, String messageSelector, boolean noLocal) throws JMSException {
        return new MessageConsumer(serdes, delegate().createDurableConsumer(topic, subscriptionName, messageSelector, noLocal));
    }

    public javax.jms.MessageConsumer createSharedDurableConsumer(Topic topic, String subscriptionName) throws JMSException {
        return new MessageConsumer(serdes, delegate().createSharedDurableConsumer(topic, subscriptionName));
    }

    public javax.jms.MessageConsumer createSharedDurableConsumer(Topic topic, String subscriptionName, String messageSelector) throws JMSException {
        return new MessageConsumer(serdes, delegate().createSharedDurableConsumer(topic, subscriptionName, messageSelector));
    }

    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return delegate().createBrowser(queue);
    }

    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        return delegate().createBrowser(queue, messageSelector);
    }

    public TemporaryQueue createTemporaryQueue() throws JMSException {
        return delegate().createTemporaryQueue();
    }

    public TemporaryTopic createTemporaryTopic() throws JMSException {
        return delegate().createTemporaryTopic();
    }

    public void unsubscribe(String name) throws JMSException {
        delegate().unsubscribe(name);
    }
}
