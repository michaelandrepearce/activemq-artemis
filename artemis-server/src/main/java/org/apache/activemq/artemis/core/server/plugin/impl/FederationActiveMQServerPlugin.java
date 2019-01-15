/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.plugin.impl;

import org.apache.activemq.artemis.api.core.*;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryImpl;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.server.*;
import org.apache.activemq.artemis.core.server.cluster.Bridge;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerConsumerPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.remoting.ConnectorFactory;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.critical.CriticalComponent;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * plugin to log various events within the broker, configured with the following booleans
 *
 * LOG_CONNECTION_EVENTS - connections creation/destroy
 * LOG_SESSION_EVENTS - sessions creation/close
 * LOG_CONSUMER_EVENTS - consumers creation/close
 * LOG_DELIVERING_EVENTS - messages delivered to consumer, acked by consumer
 * LOG_SENDING_EVENTS -  messaged is sent, message is routed
 * LOG_INTERNAL_EVENTS - critical failures, bridge deployments, queue creation/destroyed, message expired
 */

public class FederationActiveMQServerPlugin implements ActiveMQServerConsumerPlugin, Serializable {

   private ActiveMQServer server;
   private Link link;
   private Map<String, String> config;
   private final Map<Key, ClientConsumer> remoteQueueConsumers = new HashMap<>();
   /**
    * used to pass configured properties to Plugin
    *
    * @param properties
    */
   @Override
   public void init(Map<String, String> properties) {
      this.config = config;
      


   }

   /**
    * The plugin has been registered with the server
    *
    * @param server The ActiveMQServer the plugin has been registered to
    */
   @Override
   public void registered(ActiveMQServer server) {
      link = server.getLinkManager().get(config.get("link"));

   }

   /**
    * The plugin has been unregistered with the server
    *
    * @param server The ActiveMQServer the plugin has been unregistered to
    */
   @Override
   public void unregistered(ActiveMQServer server) {
   }

   /**
    * After a consumer has been created
    *
    * @param consumer the created consumer
    * @throws ActiveMQException
    */
   public void afterCreateConsumer(ServerConsumer consumer) throws ActiveMQException {
      try {
         Queue queue = consumer.getQueue();
         Filter filter = queue.getFilter();
         SimpleString filterString = filter == null ? null : filter.getFilterString();
         SimpleString queueName = queue.getName();
         byte[] queueID = ByteUtil.longToBytes(queue.getID());

         ClientSession session = link.clientSessionFactory().createSession(true, true);
         ClientConsumer consumer2 = session.createConsumer(queueName, filterString, -1, false);
      } catch (ActiveMQException e) {
         throw e;
      } catch (Exception e) {
         throw new ActiveMQException(ActiveMQExceptionType.GENERIC_EXCEPTION, e.getMessage(), e);
      }

   }

   /**
    * Before a consumer is closed
    *
    * @param consumer
    * @param failed
    * @throws ActiveMQException
    */
   public void beforeCloseConsumer(ServerConsumer consumer, boolean failed) throws ActiveMQException {
      try {
         Queue queue = consumer.getQueue();
         if (queue.getConsumerCount() <= 1) {
            ClientConsumer remoteQueueConsumer = remoteQueueConsumers.get(queue.getName());
            if (remoteQueueConsumer != null) {
               remoteQueueConsumer.close();
            }
         }
      } catch (ActiveMQException e) {
         throw e;
      } catch (Exception e) {
         throw new ActiveMQException(ActiveMQExceptionType.GENERIC_EXCEPTION, e.getMessage(), e);
      }
   }

   private class RemoteQueueConsumer implements MessageHandler{

      private final SimpleString queueName;
      private final SimpleString filterString;
      private final byte[] localQueueId;
      private final ClientSession clientSession;
      private final ClientConsumer clientConsumer;

      public RemoteQueueConsumer(SimpleString queueName, SimpleString filterString, byte[] localQueueId, ClientSession clientSession) throws ActiveMQException {
         this.queueName = queueName;
         this.filterString = filterString;
         this.localQueueId = localQueueId;
         this.clientSession = clientSession;
         this.clientSession.start();
         this.clientConsumer = clientSession.createConsumer(queueName, filterString, -1, false);
         this.clientConsumer.setMessageHandler(this);
      }

      public void close() throws ActiveMQException {
         clientConsumer.close();
         clientSession.close();
      }

      @Override
      public void onMessage(ClientMessage message) throws Exception {
         try {
            message.putExtraBytesProperty(Message.HDR_ROUTE_TO_IDS, localQueueId);
            server.getPostOffice().route(message, true);
            message.acknowledge();
         } catch (Exception e) {
            clientSession.rollback();
            throw e;
         }
      }
   }

   private class Key {
      private final SimpleString queueName;
      private final SimpleString filterString;

      public Key(SimpleString queueName, SimpleString filterString) {
         this.queueName = queueName;
         this.filterString = filterString;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         Key key = (Key) o;
         return Objects.equals(queueName, key.queueName) &&
                 Objects.equals(filterString, key.filterString);
      }

      @Override
      public int hashCode() {
         return Objects.hash(queueName, filterString);
      }
   }

}
