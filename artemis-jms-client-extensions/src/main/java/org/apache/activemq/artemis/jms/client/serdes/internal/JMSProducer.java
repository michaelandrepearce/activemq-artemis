package org.apache.activemq.artemis.jms.client.serdes.internal;

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Topic;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.jms.client.serdes.Serdes;


public class JMSProducer<T extends javax.jms.JMSProducer> extends ForwardingObject<T> implements javax.jms.JMSProducer
{
    private Serdes serdes;
    
    public JMSProducer(Serdes serdes, T jmsProducer)
    {
        super(jmsProducer);
    }
    
    public Message onSend(Destination destination, Message message) {
        if (message instanceof javax.jms.ObjectMessage) {
            if (message instanceof SerdesObjectMessage) {
                try {
                    return ((SerdesObjectMessage) message).onSend(destination);
                } catch (JMSException e) {
                    throw JmsExceptionUtils.convertToRuntimeException(e);
                }
            } else {
                throw new javax.jms.IllegalStateRuntimeException("ObjectMessage not of expected type=" + SerdesObjectMessage.class + ", was=" + message.getClass());
            }
        }
        return message;
    }

    public javax.jms.JMSProducer send(Destination destination, Message message) {
        return delegate().send(destination, onSend(destination, message));
    }

    public javax.jms.JMSProducer send(Destination destination, String body) {
        return delegate().send(destination, body);
    }

    public javax.jms.JMSProducer send(Destination destination, Map<String, Object> body) {
        return delegate().send(destination, body);
    }

    public javax.jms.JMSProducer send(Destination destination, byte[] body) {
        return delegate().send(destination, body);
    }

    public javax.jms.JMSProducer send(Destination destination, Serializable body)
    {
        try {
            String address = destination == null ? null : destination instanceof Topic ? ((Topic) destination).getTopicName() : ((Queue) destination).getQueueName();
            send(destination, serdes.serialize(address, body));
        } catch (JMSException e) {
            throw JmsExceptionUtils.convertToRuntimeException(e);
        }
        return this;
    }

    public javax.jms.JMSProducer setDisableMessageID(boolean b)
    {
        return delegate().setDisableMessageID(b);
    }

    public boolean getDisableMessageID()
    {
        return delegate().getDisableMessageID();
    }

    public javax.jms.JMSProducer setDisableMessageTimestamp(boolean b)
    {
        return delegate().setDisableMessageTimestamp(b);
    }

    public boolean getDisableMessageTimestamp()
    {
        return delegate().getDisableMessageTimestamp();
    }

    public javax.jms.JMSProducer setDeliveryMode(int i)
    {
        return delegate().setDeliveryMode(i);
    }

    public int getDeliveryMode()
    {
        return delegate().getDeliveryMode();
    }

    public javax.jms.JMSProducer setPriority(int i)
    {
        return delegate().setPriority(i);
    }

    public int getPriority()
    {
        return delegate().getPriority();
    }

    public javax.jms.JMSProducer setTimeToLive(long l)
    {
        return delegate().setTimeToLive(l);
    }

    public long getTimeToLive()
    {
        return delegate().getTimeToLive();
    }

    public javax.jms.JMSProducer setDeliveryDelay(long l)
    {
        return delegate().setDeliveryDelay(l);
    }

    public long getDeliveryDelay()
    {
        return delegate().getDeliveryDelay();
    }

    public javax.jms.JMSProducer setAsync(CompletionListener completionListener)
    {
        return delegate().setAsync(completionListener);
    }

    public CompletionListener getAsync()
    {
        return delegate().getAsync();
    }

    public javax.jms.JMSProducer setProperty(String s, boolean b)
    {
        return delegate().setProperty(s, b);
    }

    public javax.jms.JMSProducer setProperty(String s, byte b)
    {
        return delegate().setProperty(s, b);
    }

    public javax.jms.JMSProducer setProperty(String s, short i)
    {
        return delegate().setProperty(s, i);
    }

    public javax.jms.JMSProducer setProperty(String s, int i)
    {
        return delegate().setProperty(s, i);
    }

    public javax.jms.JMSProducer setProperty(String s, long l)
    {
        return delegate().setProperty(s, l);
    }

    public javax.jms.JMSProducer setProperty(String s, float v)
    {
        return delegate().setProperty(s, v);
    }

    public javax.jms.JMSProducer setProperty(String s, double v)
    {
        return delegate().setProperty(s, v);
    }

    public javax.jms.JMSProducer setProperty(String s, String s1)
    {
        return delegate().setProperty(s, s1);
    }

    public javax.jms.JMSProducer setProperty(String s, Object o)
    {
        return delegate().setProperty(s, o);
    }

    public javax.jms.JMSProducer clearProperties()
    {
        return delegate().clearProperties();
    }

    public boolean propertyExists(String s)
    {
        return delegate().propertyExists(s);
    }

    public boolean getBooleanProperty(String s)
    {
        return delegate().getBooleanProperty(s);
    }

    public byte getByteProperty(String s)
    {
        return delegate().getByteProperty(s);
    }

    public short getShortProperty(String s)
    {
        return delegate().getShortProperty(s);
    }

    public int getIntProperty(String s)
    {
        return delegate().getIntProperty(s);
    }

    public long getLongProperty(String s)
    {
        return delegate().getLongProperty(s);
    }

    public float getFloatProperty(String s)
    {
        return delegate().getFloatProperty(s);
    }

    public double getDoubleProperty(String s)
    {
        return delegate().getDoubleProperty(s);
    }

    public String getStringProperty(String s)
    {
        return delegate().getStringProperty(s);
    }

    public Object getObjectProperty(String s)
    {
        return delegate().getObjectProperty(s);
    }

    public Set<String> getPropertyNames()
    {
        return delegate().getPropertyNames();
    }

    public javax.jms.JMSProducer setJMSCorrelationIDAsBytes(byte[] bytes)
    {
        return delegate().setJMSCorrelationIDAsBytes(bytes);
    }

    public byte[] getJMSCorrelationIDAsBytes()
    {
        return delegate().getJMSCorrelationIDAsBytes();
    }

    public javax.jms.JMSProducer setJMSCorrelationID(String s)
    {
        return delegate().setJMSCorrelationID(s);
    }

    public String getJMSCorrelationID()
    {
        return delegate().getJMSCorrelationID();
    }

    public javax.jms.JMSProducer setJMSType(String s)
    {
        return delegate().setJMSType(s);
    }

    public String getJMSType()
    {
        return delegate().getJMSType();
    }

    public javax.jms.JMSProducer setJMSReplyTo(Destination destination)
    {
        return delegate().setJMSReplyTo(destination);
    }

    public Destination getJMSReplyTo()
    {
        return delegate().getJMSReplyTo();
    }

}
