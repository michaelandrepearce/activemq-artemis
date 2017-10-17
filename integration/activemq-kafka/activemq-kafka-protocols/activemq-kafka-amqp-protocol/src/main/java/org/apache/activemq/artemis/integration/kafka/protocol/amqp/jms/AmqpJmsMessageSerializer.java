/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.integration.kafka.protocol.amqp.jms;

import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import java.util.Map;

import org.apache.activemq.artemis.integration.kafka.protocol.amqp.jms.internal.JmsProtonMessageConverter;
import org.apache.activemq.artemis.integration.kafka.protocol.amqp.proton.ProtonMessageSerializer;
import org.apache.kafka.common.serialization.Serializer;

public class AmqpJmsMessageSerializer implements Serializer<Message> {

   private final ProtonMessageSerializer messageSerializer = new ProtonMessageSerializer();

   @Override
   public void configure(Map<String, ?> map, boolean b) {
      messageSerializer.configure(map, b);
   }

   @Override
   public byte[] serialize(String s, Message message) {
      try {
         org.apache.qpid.proton.message.Message protonMessage = JmsProtonMessageConverter.toProtonMessage(message);
         return messageSerializer.serialize(s, protonMessage);
      } catch (JMSException e) {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public void close() {
      messageSerializer.close();
   }
}