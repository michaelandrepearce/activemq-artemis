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
package org.apache.activemq.artemis.kafka.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.JMSSecurityException;
import javax.jms.JMSSecurityRuntimeException;
import javax.jms.Message;
import java.util.Properties;

import org.apache.activemq.artemis.kafka.jms.consumer.ConsumerFactory;
import org.apache.activemq.artemis.kafka.jms.consumer.DefaultConsumerFactory;
import org.apache.activemq.artemis.kafka.jms.exception.JmsExceptionSupport;
import org.apache.activemq.artemis.kafka.jms.message.DefaultKafkaJmsMessageFactory;
import org.apache.activemq.artemis.kafka.jms.message.KafkaJmsMessageFactory;
import org.apache.activemq.artemis.kafka.jms.producer.DefaultProducerFactory;
import org.apache.activemq.artemis.kafka.jms.producer.ProducerFactory;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConnectionFactory implements ConnectionFactory {

   public static final String USERNAME = "username";
   public static final String PASSWORD = "password";

   private static final Logger log = LoggerFactory.getLogger(KafkaConnectionFactory.class);
   private final Properties properties = new Properties();
   private final ProducerFactory producerFactory;
   private final ConsumerFactory consumerFactory;
   private final KafkaJmsMessageFactory messageFactory;

   public KafkaConnectionFactory(String uri) throws JMSException {
      this(uri, new DefaultProducerFactory(), new DefaultConsumerFactory());
   }

   public KafkaConnectionFactory(String uri, Serde<Message> messageSerde) throws JMSException {
      this(uri, new DefaultProducerFactory(messageSerde.serializer()), new DefaultConsumerFactory(messageSerde.deserializer()));
   }

   public KafkaConnectionFactory(String uri, ProducerFactory producerFactory, ConsumerFactory consumerFactory) throws JMSException {
      this(uri, producerFactory, consumerFactory, new DefaultKafkaJmsMessageFactory());
   }

   public KafkaConnectionFactory(String uri, ProducerFactory producerFactory, ConsumerFactory consumerFactory,
                                 KafkaJmsMessageFactory messageFactory)
      throws JMSException {
      try {
         KafkaURISchema.KafkaURIFactory kafkaURISchema = new KafkaURISchema.KafkaURIFactory();
         this.properties.putAll(kafkaURISchema.newObject(kafkaURISchema.parse(uri), null));
      } catch (Exception e) {
         JmsExceptionSupport.toJMSException(e);
      }
      this.producerFactory = producerFactory;
      this.consumerFactory = consumerFactory;
      this.messageFactory = messageFactory;
   }

   @Override
   public Connection createConnection() throws JMSException {
      return createConnection(properties.getProperty(USERNAME), properties.getProperty(PASSWORD));
   }

   @Override
   public Connection createConnection(String username, String password) throws JMSException {
      return this.createConnectionInternal(username, password);
   }

   @Override
   public JMSContext createContext() {
      return createContext(properties.getProperty(USERNAME), properties.getProperty(PASSWORD));
   }

   @Override
   public JMSContext createContext(final int sessionMode) {
      return createContext(properties.getProperty(USERNAME), properties.getProperty(PASSWORD), sessionMode);
   }

   @Override
   public JMSContext createContext(final String userName, final String password) {
      return createContext(userName, password, JMSContext.AUTO_ACKNOWLEDGE);
   }

   @Override
   public JMSContext createContext(String userName, String password, int sessionMode) {
      validateSessionMode(sessionMode);
      try {
         KafkaConnection kafkaConnection = createConnectionInternal(userName, password);
         return kafkaConnection.createContext(sessionMode);
      } catch (JMSSecurityException e) {
         throw new JMSSecurityRuntimeException(e.getMessage(), e.getErrorCode(), e);
      } catch (JMSException e) {
         throw JmsExceptionSupport.convertToRuntimeException(e);
      }
   }

   public void setProperty(String key, String value) {
      this.properties.setProperty(key, value);
   }

   public Properties getProperties() {
      return properties;
   }

   private KafkaConnection createConnectionInternal(String username, String password) throws JMSException {
      return new KafkaConnection(this.properties, producerFactory, consumerFactory, messageFactory);
   }

   static void validateSessionMode(int mode) {
      switch (mode) {
         case JMSContext.AUTO_ACKNOWLEDGE:
         case JMSContext.CLIENT_ACKNOWLEDGE:
         case JMSContext.DUPS_OK_ACKNOWLEDGE:
         case JMSContext.SESSION_TRANSACTED: {
            return;
         }
         default:
            throw new JMSRuntimeException("Invalid Session Mode: " + mode);
      }
   }
}
