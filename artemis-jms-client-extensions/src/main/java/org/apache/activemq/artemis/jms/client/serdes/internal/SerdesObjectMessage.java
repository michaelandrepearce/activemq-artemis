package org.apache.activemq.artemis.jms.client.serdes.internal;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.Queue;
import javax.jms.Topic;
import java.io.Serializable;
import java.util.Enumeration;

import org.apache.activemq.artemis.jms.client.serdes.Serdes;

public class SerdesObjectMessage implements javax.jms.ObjectMessage {
    
    private BytesMessage bytesMessage;
    private Serializable object;
    private byte[] bytes; 
    private Serdes serdes;

    public SerdesObjectMessage(Serdes serdes, BytesMessage bytesMessage) {
        this.serdes = serdes;
        this.bytesMessage = bytesMessage;
    }

    @Override
    public void setObject(Serializable object) {
        this.object = object;
    }

    @Override
    public Serializable getObject() throws JMSException {
        if (object == null) {
            Destination destination = bytesMessage.getJMSDestination();
            String address = destination == null ? null : destination instanceof Topic ? ((Topic) destination).getTopicName() : ((Queue) destination).getQueueName();
            if (bytes == null) {
                byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
                bytesMessage.readBytes(bytes);
                this.bytes = bytes;
            }
            object = serdes.deserialize(address, bytes);
        }
        return object;
    }

    BytesMessage onSend(Destination destination) throws JMSException {
        String address = destination == null ? null : destination instanceof Topic ? ((Topic) destination).getTopicName() : ((Queue) destination).getQueueName();
        if (bytes == null) {
            bytes = serdes.serialize(address, object);
            if (bytes != null) {
                bytesMessage.writeBytes(bytes);
            }
        }
        return bytesMessage;
    }

    @Override
    public void clearBody() throws JMSException {
        bytesMessage.clearBody();
        bytes = null;
        object = null;
    }

    @Override
    public <T> T getBody(Class<T> c) throws MessageFormatException {
        try {
            return (T) getObject();
        } catch (JMSException e) {
            throw new MessageFormatException("Deserialization error on SerdesObjectMessage");
        }
    }

    @Override
    public boolean isBodyAssignableTo(Class c) {
        if (bytes == null && object == null) // we have no body
            return true;
        try {
            return Serializable.class == c || Object.class == c || c.isInstance(getObject());
        } catch (JMSException e) {
            return false;
        }
    }

    //Delegate------->
    @Override
    public String getJMSMessageID() throws JMSException {
        return bytesMessage.getJMSMessageID();
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException {
        bytesMessage.setJMSMessageID(id);
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return bytesMessage.getJMSTimestamp();
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        bytesMessage.setJMSTimestamp(timestamp);
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return bytesMessage.getJMSCorrelationIDAsBytes();
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        bytesMessage.setJMSCorrelationIDAsBytes(correlationID);
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        bytesMessage.setJMSCorrelationID(correlationID);
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return bytesMessage.getJMSCorrelationID();
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return bytesMessage.getJMSReplyTo();
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        bytesMessage.setJMSReplyTo(replyTo);
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return bytesMessage.getJMSDestination();
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        bytesMessage.setJMSDestination(destination);
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return bytesMessage.getJMSDeliveryMode();
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        bytesMessage.setJMSDeliveryMode(deliveryMode);
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return bytesMessage.getJMSRedelivered();
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        bytesMessage.setJMSRedelivered(redelivered);
    }

    @Override
    public String getJMSType() throws JMSException {
        return bytesMessage.getJMSType();
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        bytesMessage.setJMSType(type);
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return bytesMessage.getJMSExpiration();
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        bytesMessage.setJMSExpiration(expiration);
    }

    @Override
    public long getJMSDeliveryTime() throws JMSException {
        return bytesMessage.getJMSDeliveryTime();
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) throws JMSException {
        bytesMessage.setJMSDeliveryTime(deliveryTime);
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return bytesMessage.getJMSPriority();
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        bytesMessage.setJMSPriority(priority);
    }

    @Override
    public void clearProperties() throws JMSException {
        bytesMessage.clearProperties();
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return bytesMessage.propertyExists(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return bytesMessage.getBooleanProperty(name);
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        return bytesMessage.getByteProperty(name);
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        return bytesMessage.getShortProperty(name);
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        return bytesMessage.getIntProperty(name);
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        return bytesMessage.getLongProperty(name);
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        return bytesMessage.getFloatProperty(name);
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return bytesMessage.getDoubleProperty(name);
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        return bytesMessage.getStringProperty(name);
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        return bytesMessage.getObjectProperty(name);
    }

    @Override
    public Enumeration getPropertyNames() throws JMSException {
        return bytesMessage.getPropertyNames();
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        bytesMessage.setBooleanProperty(name, value);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        bytesMessage.setByteProperty(name, value);
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        bytesMessage.setShortProperty(name, value);
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        bytesMessage.setIntProperty(name, value);
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        bytesMessage.setLongProperty(name, value);
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        bytesMessage.setFloatProperty(name, value);
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        bytesMessage.setDoubleProperty(name, value);
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        bytesMessage.setStringProperty(name, value);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        bytesMessage.setObjectProperty(name, value);
    }

    @Override
    public void acknowledge() throws JMSException {
        bytesMessage.acknowledge();
    }
}
