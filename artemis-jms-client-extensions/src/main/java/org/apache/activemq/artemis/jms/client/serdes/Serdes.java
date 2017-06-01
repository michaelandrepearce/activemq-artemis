package org.apache.activemq.artemis.jms.client.serdes;

import java.io.Serializable;

public interface Serdes {
    
    byte[] serialize(String address, Serializable object);

    Serializable deserialize(String address, byte[] bytes);

}
