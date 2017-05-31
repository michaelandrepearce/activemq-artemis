package org.apache.activemq.artemis.jms.client;

import javax.jms.JMSException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.activemq.artemis.api.jms.ObjectMessageSerdes;
import org.apache.activemq.artemis.utils.ObjectInputStreamWithClassLoader;

public class ActiveMQJavaSerialisationSerdes implements ObjectMessageSerdes {

    private ConnectionFactoryOptions options;
    
    ActiveMQJavaSerialisationSerdes(ConnectionFactoryOptions options) {
        this.options = options;
    }
    
    @Override
    public byte[] serialize(String address, Serializable object) throws JMSException {
        byte[] bytes = null;
        try {
            if (object != null) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

                ObjectOutputStream oos = new ObjectOutputStream(baos);

                oos.writeObject(object);
                oos.flush();

                int size = baos.size();

                ByteArrayOutputStream outputStream = new ByteArrayOutputStream(size + 4);
                outputStream.write((byte)(size >>> 24));
                outputStream.write((byte)(size >>> 16));
                outputStream.write((byte)(size >>> 8));
                outputStream.write((byte)(size));
                baos.writeTo(outputStream);

                outputStream.flush();

                bytes = outputStream.toByteArray();
            }
        } catch (Exception e) {
            JMSException je = new JMSException("Failed to serialize object");
            je.setLinkedException(e);
            je.initCause(e);
            throw je;
        }
        return bytes;
    }

    @Override
    public Serializable deserialize(String address, byte[] bytes) throws JMSException {
        if (bytes == null || bytes.length == 0) return null;
        try (ObjectInputStreamWithClassLoader ois = new ObjectInputStreamWithClassLoader(new ByteArrayInputStream(bytes, 4, bytes.length))) {
            String blackList = getDeserializationBlackList();
            if (blackList != null) {
                ois.setBlackList(blackList);
            }
            String whiteList = getDeserializationWhiteList();
            if (whiteList != null) {
                ois.setWhiteList(whiteList);
            }
            Serializable object = (Serializable) ois.readObject();
            return  object;
        } catch (Exception e) {
            JMSException je = new JMSException(e.getMessage());
            je.setStackTrace(e.getStackTrace());
            throw je;
        }
    }

    private String getDeserializationBlackList() {
        if (options == null) {
            return null;
        } else {
            return options.getDeserializationBlackList();
        }
    }

    private String getDeserializationWhiteList() {
        if (options == null) {
            return null;
        } else {
            return options.getDeserializationWhiteList();
        }
    }
}
