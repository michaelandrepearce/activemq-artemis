package org.apache.activemq.artemis.jms.client.serdes.internal;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.apache.activemq.artemis.jms.client.serdes.Serdes;


public class JMSConsumer<T extends javax.jms.JMSConsumer> extends ForwardingObject<T> implements javax.jms.JMSConsumer
{

    private Serdes serdes;

    public JMSConsumer(Serdes serdes, T jmsConsumer)
    {
        super(jmsConsumer);
        this.serdes = serdes;
    }

    public Message onReceive(Message message) {
        return message instanceof BytesMessage ? new SerdesObjectMessage(serdes, (BytesMessage) message) : message;
    }
    
    public String getMessageSelector()
    {
        return delegate().getMessageSelector();
    }

    public MessageListener getMessageListener() throws JMSRuntimeException
    {
        return delegate().getMessageListener();
    }

    public void setMessageListener(MessageListener messageListener) throws JMSRuntimeException
    {
        delegate().setMessageListener(new org.apache.activemq.artemis.jms.client.serdes.internal.MessageListener(serdes, messageListener));
    }

    public Message receive()
    {
        return onReceive(delegate().receive());
    }

    public Message receive(long l)
    {
        return onReceive(delegate().receive(l));
    }

    public Message receiveNoWait()
    {
        return onReceive(delegate().receiveNoWait());
    }

    public void close()
    {
        delegate().close();
    }

    public <T> T receiveBody(Class<T> aClass)
    {
        try
        {
            Message message = onReceive(delegate().receive());
            return message == null ? null : message.getBody(aClass);
        } catch (JMSException e){
            throw JmsExceptionUtils.convertToRuntimeException(e);
        }
    }

    public <T> T receiveBody(Class<T> aClass, long l)
    {
        try
        {
            Message message = onReceive(delegate().receive(l));
            return message == null ? null : message.getBody(aClass);
        } catch (JMSException e){
            throw JmsExceptionUtils.convertToRuntimeException(e);
        }
    }

    public <T> T receiveBodyNoWait(Class<T> aClass)
    {
        try
        {
            Message message = onReceive(delegate().receiveNoWait());
            return message == null ? null : message.getBody(aClass);
        } catch (JMSException e){
            throw JmsExceptionUtils.convertToRuntimeException(e);
        }
    }

}
