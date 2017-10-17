/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.kafka.jms.message;


import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import java.io.Serializable;

import org.apache.activemq.artemis.kafka.jms.message.simple.KafkaBytesMessage;
import org.apache.activemq.artemis.kafka.jms.message.simple.KafkaMapMessage;
import org.apache.activemq.artemis.kafka.jms.message.simple.KafkaMessage;
import org.apache.activemq.artemis.kafka.jms.message.simple.KafkaObjectMessage;
import org.apache.activemq.artemis.kafka.jms.message.simple.KafkaStreamMessage;
import org.apache.activemq.artemis.kafka.jms.message.simple.KafkaTextMessage;

/**
 * Ideally this should be agnostic and we should have our own message objects,
 * but for ease and to avoid bloat, for now we just extent AmqpMessageFactory.
 */
public class DefaultKafkaJmsMessageFactory implements KafkaJmsMessageFactory {
   @Override
   public Message createMessage() throws JMSException {
      return new KafkaMessage();
   }

   @Override
   public TextMessage createTextMessage(String payload) throws JMSException {
      TextMessage textMessage = new KafkaTextMessage();
      textMessage.setText(payload);
      return textMessage;
   }

   @Override
   public TextMessage createTextMessage() throws JMSException {
      return new KafkaTextMessage();
   }

   @Override
   public BytesMessage createBytesMessage() throws JMSException {
      return new KafkaBytesMessage();
   }

   @Override
   public MapMessage createMapMessage() throws JMSException {
      return new KafkaMapMessage();
   }

   @Override
   public StreamMessage createStreamMessage() throws JMSException {
      return new KafkaStreamMessage();
   }

   @Override
   public ObjectMessage createObjectMessage(Serializable payload) throws JMSException {
      ObjectMessage objectMessage = new KafkaObjectMessage();
      objectMessage.setObject(payload);
      return objectMessage;
   }

   @Override
   public ObjectMessage createObjectMessage() throws JMSException {
      return new KafkaObjectMessage();
   }
}
