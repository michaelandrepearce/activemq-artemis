package org.apache.activemq.artemis.jms.client.serdes.internal;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;


public class QueueSender<T extends javax.jms.QueueSender> extends MessageProducer<T> implements javax.jms.QueueSender {

    public QueueSender(T queueSender){
        super(queueSender);
    }

    public Queue getQueue() throws JMSException {
        return delegate().getQueue();
    }

    public void send(Queue queue, Message message) throws JMSException {
        delegate().send(queue, onSend(queue, message));
    }

    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        delegate().send(queue, onSend(queue, message), deliveryMode, priority, timeToLive);
    }
}
