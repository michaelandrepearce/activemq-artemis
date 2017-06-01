package org.apache.activemq.artemis.jms.client.serdes.internal;

import javax.jms.JMSException;
import javax.jms.Queue;

import org.apache.activemq.artemis.jms.client.serdes.Serdes;


public class QueueReceiver<T extends javax.jms.QueueReceiver> extends MessageConsumer<T> implements javax.jms.QueueReceiver {

    public QueueReceiver(Serdes serdes, T queueReceiver){
        super(serdes, queueReceiver);
    }

    public Queue getQueue() throws JMSException {
        return delegate().getQueue();
    }
}
