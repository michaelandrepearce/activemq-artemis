package org.apache.activemq.artemis.jms.client.serdes.internal;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;


public class MessageProducer<T extends javax.jms.MessageProducer> extends ForwardingObject<T> implements javax.jms.MessageProducer {
    
    public MessageProducer(T messageProducer){
        super(messageProducer);
    }

    public Message onSend(Destination destination, Message message) throws JMSException {
        if (message instanceof javax.jms.ObjectMessage) {
            if (message instanceof SerdesObjectMessage) {
                return ((SerdesObjectMessage) message).onSend(defaultIfNull(destination));
            } else {
                throw new javax.jms.IllegalStateException("ObjectMessage not of expected type=" + SerdesObjectMessage.class + ", was=" + message.getClass());
            }
        }
        return message;
    }
        
    private final Destination defaultIfNull(final Destination destination) throws JMSException {
        Destination result = destination;
        if (result == null) {
            result = getDestination();
            if (result == null) {
                throw new UnsupportedOperationException("Destination must be specified on send with an anonymous producer");
            }
        }
        return result;
    }
    
    public void setDisableMessageID(boolean value) throws JMSException {
        delegate().setDisableMessageID(value);
    }

    public boolean getDisableMessageID() throws JMSException {
        return delegate().getDisableMessageID();
    }

    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        delegate().setDisableMessageTimestamp(value);
    }

    public boolean getDisableMessageTimestamp() throws JMSException {
        return delegate().getDisableMessageTimestamp();
    }

    public void setDeliveryMode(int deliveryMode) throws JMSException {
        delegate().setDeliveryMode(deliveryMode);
    }

    public int getDeliveryMode() throws JMSException {
        return delegate().getDeliveryMode();
    }

    public void setPriority(int defaultPriority) throws JMSException {
        delegate().setPriority(defaultPriority);
    }

    public int getPriority() throws JMSException {
        return delegate().getPriority();
    }

    public void setTimeToLive(long timeToLive) throws JMSException {
        delegate().setTimeToLive(timeToLive);
    }

    public long getTimeToLive() throws JMSException {
        return delegate().getTimeToLive();
    }

    public void setDeliveryDelay(long deliveryDelay) throws JMSException {
        delegate().setDeliveryDelay(deliveryDelay);
    }

    public long getDeliveryDelay() throws JMSException {
        return delegate().getDeliveryDelay();
    }

    public Destination getDestination() throws JMSException {
        return delegate().getDestination();
    }

    public void close() throws JMSException {
        delegate().close();
    }

    public void send(Message message) throws JMSException {
        delegate().send(onSend(null, message));
    }

    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        delegate().send(onSend(null, message), deliveryMode, priority, timeToLive);
    }

    public void send(Destination destination, Message message) throws JMSException {
        delegate().send(destination, onSend(destination, message));
    }

    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        delegate().send(destination, onSend(destination, message), deliveryMode, priority, timeToLive);
    }

    public void send(Message message, javax.jms.CompletionListener completionListener) throws JMSException {
        delegate().send(onSend(null, message), completionListener);
    }

    public void send(Message message, int deliveryMode, int priority, long timeToLive, javax.jms.CompletionListener completionListener) throws JMSException {
        delegate().send(onSend(null, message), deliveryMode, priority, timeToLive, completionListener);
    }

    public void send(Destination destination, Message message, javax.jms.CompletionListener completionListener) throws JMSException {
        delegate().send(destination, onSend(destination, message), completionListener);
    }

    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive, javax.jms.CompletionListener completionListener) throws JMSException {
        delegate().send(destination, onSend(destination, message), deliveryMode, priority, timeToLive, completionListener);
    }
}
