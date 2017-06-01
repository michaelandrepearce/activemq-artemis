package org.apache.activemq.artemis.jms.client.serdes.internal;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq.artemis.jms.client.serdes.Serdes;


public class MessageConsumer<T extends javax.jms.MessageConsumer> extends ForwardingObject<T> implements javax.jms.MessageConsumer {

    private final Serdes serdes;
    
    public MessageConsumer(Serdes serdes, T messageConsumer){
        super(messageConsumer);
        this.serdes = serdes;
    }

    public Message onReceive(Message message) {
        return message instanceof BytesMessage ? new SerdesObjectMessage(serdes, (BytesMessage) message) : message;
    }

    public String getMessageSelector() throws JMSException {
        return delegate().getMessageSelector();
    }

    public javax.jms.MessageListener getMessageListener() throws JMSException {
        return delegate().getMessageListener();
    }

    public void setMessageListener(javax.jms.MessageListener listener) throws JMSException {
        delegate().setMessageListener(new MessageListener(serdes, listener));
    }

    public Message receive() throws JMSException {
        return onReceive(delegate().receive());
    }

    public Message receive(long timeout) throws JMSException {
        return onReceive(delegate().receive(timeout));

    }

    public Message receiveNoWait() throws JMSException {
        return onReceive(delegate().receiveNoWait());
    }

    public void close() throws JMSException {
        delegate().close();
    }


}
