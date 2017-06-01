package org.apache.activemq.artemis.jms.client.serdes.internal;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;


public class TopicPublisher<T extends javax.jms.TopicPublisher> extends MessageProducer<T> implements javax.jms.TopicPublisher {

    public TopicPublisher(T topicPublisher){
        super(topicPublisher);
    }

    public Topic getTopic() throws JMSException {
        return delegate().getTopic();
    }

    public void publish(Message message) throws JMSException {
        delegate().publish(onSend(null, message));
    }

    public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        delegate().publish(onSend(null, message), deliveryMode, priority, timeToLive);
    }

    public void publish(Topic topic, Message message) throws JMSException {
        delegate().publish(topic, onSend(null, message));
    }

    public void publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        delegate().publish(topic, onSend(topic, message), deliveryMode, priority, timeToLive);
    }

}
