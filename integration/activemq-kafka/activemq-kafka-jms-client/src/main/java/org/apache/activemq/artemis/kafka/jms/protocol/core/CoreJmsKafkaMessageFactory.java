/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.kafka.jms.protocol.core;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import java.io.Serializable;

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQBytesMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQMapMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQObjectMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQStreamMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQTextMessage;
import org.apache.activemq.artemis.kafka.jms.message.KafkaJmsMessageFactory;

public class CoreJmsKafkaMessageFactory implements KafkaJmsMessageFactory {

   @Override
   public Message createMessage() throws JMSException {
      return createMessage(ActiveMQMessage.TYPE);
   }

   @Override
   public TextMessage createTextMessage(String payload) throws JMSException {
      TextMessage textMessage = (TextMessage) createMessage(ActiveMQTextMessage.TYPE);
      textMessage.setText(payload);
      return textMessage;
   }

   @Override
   public TextMessage createTextMessage() throws JMSException {
      return (TextMessage) createMessage(ActiveMQTextMessage.TYPE);
   }

   @Override
   public BytesMessage createBytesMessage() throws JMSException {
      return (BytesMessage) createMessage(ActiveMQBytesMessage.TYPE);
   }

   @Override
   public MapMessage createMapMessage() throws JMSException {
      return (MapMessage) createMessage(ActiveMQMapMessage.TYPE);
   }

   @Override
   public StreamMessage createStreamMessage() throws JMSException {
      return (StreamMessage) createMessage(ActiveMQStreamMessage.TYPE);
   }

   @Override
   public ObjectMessage createObjectMessage(Serializable payload) throws JMSException {
      ObjectMessage objectMessage = (ObjectMessage) createMessage(ActiveMQObjectMessage.TYPE);
      objectMessage.setObject(payload);
      return objectMessage;
   }

   @Override
   public ObjectMessage createObjectMessage() throws JMSException {
      return (ObjectMessage) createMessage(ActiveMQObjectMessage.TYPE);
   }

   public static Message createMessage(byte type) throws JMSException {
      ActiveMQMessage activeMQMessage = ActiveMQMessage.createMessage(createClientMessage(type), null);
      activeMQMessage.clearBody();
      activeMQMessage.clearProperties();
      return activeMQMessage;
   }

   public static ClientMessageImpl createClientMessage(byte type) {
      return new ClientMessageImpl(type, ActiveMQMessage.DEFAULT_DELIVERY_MODE == DeliveryMode.PERSISTENT, 0, 0,
                                   (byte) ActiveMQMessage.DEFAULT_PRIORITY, ActiveMQClient.DEFAULT_INITIAL_MESSAGE_PACKET_SIZE);
   }
}
