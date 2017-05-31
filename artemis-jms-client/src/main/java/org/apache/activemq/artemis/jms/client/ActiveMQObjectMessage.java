/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jms.client;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.jms.ObjectMessageSerdes;
import org.apache.activemq.artemis.utils.ObjectInputStreamWithClassLoader;

/**
 * ActiveMQ Artemis implementation of a JMS ObjectMessage.
 * <br>
 * Don't used ObjectMessage if you want good performance!
 * <p>
 * Serialization is slooooow!
 */
public class ActiveMQObjectMessage extends ActiveMQMessage implements ObjectMessage {
   // Constants -----------------------------------------------------

   public static final byte TYPE = Message.OBJECT_TYPE;

   // Attributes ----------------------------------------------------

   // keep a snapshot of the Serializable Object as a byte[] to provide Object isolation
   private byte[] data;
   
   private Serializable object;
   
   private final ObjectMessageSerdes objectMessageSerdes;

   private final ObjectMessageSerdes defaultObjectMessageSerdes;


   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   protected ActiveMQObjectMessage(final ObjectMessageSerdes objectMessageSerdes,
                                   final ClientSession session, ConnectionFactoryOptions options) {
      super(ActiveMQObjectMessage.TYPE, session);
      this.objectMessageSerdes = objectMessageSerdes;
      this.defaultObjectMessageSerdes = new ActiveMQJavaSerialisationSerdes(options);
   }

   protected ActiveMQObjectMessage(final ObjectMessageSerdes objectMessageSerdes,
                                   final ClientMessage message,
                                   final ClientSession session,
                                   ConnectionFactoryOptions options) {
      super(message, session);
      this.objectMessageSerdes = objectMessageSerdes;
      this.defaultObjectMessageSerdes = new ActiveMQJavaSerialisationSerdes(options);
   }

   /**
    * A copy constructor for foreign JMS ObjectMessages.
    */
   public ActiveMQObjectMessage(final ObjectMessageSerdes objectMessageSerdes,
                                final ObjectMessage foreign,
                                final ClientSession session,
                                ConnectionFactoryOptions options) throws JMSException {
      super(foreign, ActiveMQObjectMessage.TYPE, session);

      setObject(foreign.getObject());
      this.objectMessageSerdes = objectMessageSerdes;
      this.defaultObjectMessageSerdes = new ActiveMQJavaSerialisationSerdes(options);
   }
   
   

   // Public --------------------------------------------------------

   @Override
   public byte getType() {
      return ActiveMQObjectMessage.TYPE;
   }

   @Override
   public void doBeforeSend() throws Exception {
      if (data == null) {
         if (objectMessageSerdes == null) {
            data = defaultObjectMessageSerdes.serialize(message.getAddress(), object);
         } else {
            data = objectMessageSerdes.serialize(message.getAddress(), object);
            message.setType(ActiveMQBytesMessage.TYPE);
         }
      }
      message.getBodyBuffer().clear();
      if (data != null) {
         message.getBodyBuffer().writeBytes(data);
      }

      super.doBeforeSend();
   }

   @Override
   public void doBeforeReceive() throws ActiveMQException {
      super.doBeforeReceive();
      try {
         int len = message.getBodySize();
         data = new byte[len];
         message.getBodyBuffer().readBytes(data);
      } catch (Exception e) {
         data = null;
      }

   }

   // ObjectMessage implementation ----------------------------------

   @Override
   public void setObject(final Serializable object) throws JMSException {
      checkWrite();
      this.object = object;
   }

   // lazy deserialize the Object the first time the client requests it
   @Override
   public Serializable getObject() throws JMSException {
      if (this.object != null) {
         return this.object;
      }
      if (objectMessageSerdes != null) {
         this.object = objectMessageSerdes.deserialize(message.getAddress(), data);
      } else {
         this.object = defaultObjectMessageSerdes.deserialize(message.getAddress(), data);
      }
      return this.object;
   }

   @Override
   public void clearBody() throws JMSException {
      super.clearBody();

      data = null;
   }

   @Override
   protected <T> T getBodyInternal(Class<T> c) throws MessageFormatException {
      try {
         return (T) getObject();
      } catch (JMSException e) {
         throw new MessageFormatException("Deserialization error on ActiveMQObjectMessage");
      }
   }

   @Override
   public boolean isBodyAssignableTo(Class c) {
      if (data == null) // we have no body
         return true;
      try {
         return Serializable.class == c || Object.class == c || c.isInstance(getObject());
      } catch (JMSException e) {
         return false;
      }
   }

   
}
