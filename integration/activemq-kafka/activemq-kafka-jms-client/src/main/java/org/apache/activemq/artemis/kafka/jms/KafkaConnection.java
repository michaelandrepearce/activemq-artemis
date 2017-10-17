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
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.kafka.jms.common.ConnectionAwareSession;
import org.apache.activemq.artemis.kafka.jms.consumer.ConsumerFactory;
import org.apache.activemq.artemis.kafka.jms.exception.UnsupportedOperationException;
import org.apache.activemq.artemis.kafka.jms.message.KafkaJmsMessageFactory;
import org.apache.activemq.artemis.kafka.jms.producer.ProducerFactory;
import org.apache.activemq.artemis.kafka.jms.util.Unsupported;
import org.apache.kafka.clients.CommonClientConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConnection extends KafkaConnectionForContextImpl implements Connection {
   private static final Logger log = LoggerFactory.getLogger(KafkaConnection.class);
   private final Properties config;
   private final ProducerFactory producerFactory;
   private final ConsumerFactory consumerFactory;
   private final KafkaJmsMessageFactory messageFactory;
   private ExceptionListener exceptionListener;
   private List<ConnectionAwareSession> sessions = new ArrayList<>();
   private AtomicBoolean isStarted = new AtomicBoolean(false);

   KafkaConnection(Properties config, ProducerFactory producerFactory, ConsumerFactory consumerFactory,
                   KafkaJmsMessageFactory messageFactory) {
      this.config = new Properties();
      this.config.putAll(config);
      this.producerFactory = producerFactory;
      this.consumerFactory = consumerFactory;
      this.messageFactory = messageFactory;
   }

   Properties getConfig() {
      return config;
   }

   @Override
   public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
      ConnectionAwareSession kafkaSession =
         new KafkaSession(this, transacted, acknowledgeMode, producerFactory, consumerFactory, messageFactory);
      sessions.add(kafkaSession);
      return kafkaSession;
   }

   @Override
   public Session createSession(int acknowledgeMode) throws JMSException {
      return createSession(false, acknowledgeMode);
   }

   @Override
   public Session createSession() throws JMSException {
      return createSession(Session.AUTO_ACKNOWLEDGE);
   }

   void removeSession(Session session) {
      sessions.remove(session);
   }

   @Override
   public String getClientID() throws JMSException {
      return getConfig().getProperty(CommonClientConfigs.CLIENT_ID_CONFIG);
   }

   @Override
   public void setClientID(String clientID) throws JMSException {
      getConfig().setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, clientID);
   }

   @Override
   public ConnectionMetaData getMetaData() throws JMSException {
      return new KafkaConnectionMetaData();
   }

   @Override
   @Unsupported("ExceptionListeners are not supported")
   public ExceptionListener getExceptionListener() throws JMSException {
      return this.exceptionListener;
   }

   @Override
   @Unsupported("ExceptionListeners are not supported.")
   public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException {
      this.exceptionListener = exceptionListener;
   }

   @Override
   public void start() throws JMSException {
      if (isStarted.compareAndSet(false, true)) {
         sessions.forEach(ConnectionAwareSession::run);
      }
   }

   @Override
   public void stop() throws JMSException {
      isStarted.compareAndSet(true, false);
   }

   @Override
   public void close() throws JMSException {
      this.stop();
      //We do this to avoid concurrent modification as when a session closes it removes itself
      for (ConnectionAwareSession session : new ArrayList<>(sessions)) {
         session.close();
      }
   }

   @Override
   public ConnectionConsumer createConnectionConsumer(Destination destination, String s, ServerSessionPool serverSessionPool, int i)
      throws JMSException {
      throw new UnsupportedOperationException("connection.createConnectionConsumer is unsupported");
   }

   @Override
   public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String s, String s1, ServerSessionPool serverSessionPool, int i)
      throws JMSException {
      throw new UnsupportedOperationException("connection.createDurableConnectionConsumer is unsupported");
   }

   @Override
   public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String s, String s1, ServerSessionPool serverSessionPool,
                                                                   int i) throws JMSException {
      throw new UnsupportedOperationException("connection.createSharedDurableConnectionConsumer is unsupported");
   }

   @Override
   public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String s, String s1, ServerSessionPool serverSessionPool, int i)
      throws JMSException {
      throw new UnsupportedOperationException("connection.createSharedConnectionConsumer is unsupported");
   }

   public boolean isStarted() {
      return isStarted.get();
   }
}
