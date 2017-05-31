package org.apache.activemq.artemis.api.jms;

import javax.jms.JMSException;
import java.io.Serializable;

/**
 * Created by pearcem on 31/05/2017.
 */
public interface ObjectMessageSerdes<T extends Serializable> {
 
    byte[] serialize(String address, T o) throws JMSException;

    T deserialize(String address, byte[] bytes) throws JMSException;

}
