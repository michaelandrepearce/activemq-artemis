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

package org.apache.activemq.artemis.core.server.federation;

import java.util.HashMap;
import java.util.Map;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.transformer.Transformer;

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

public class FederatedAbstract {

   protected ActiveMQServer server;
   protected FederationConnection connection;
   protected RemoteQueueManager remoteQueueManager;
   protected final Map<RemoteConsumerKey, RemoteQueueConsumer> remoteQueueConsumers = new HashMap<>();

   public FederatedAbstract(ActiveMQServer server, FederationConnection federationConnection) {
      this.server = server;
      this.connection = federationConnection;
      this.remoteQueueManager = new RemoteQueueManager(server, connection);
   }

   /**
    * The plugin has been unregistered with the server
    */
   public void stop() {
      for(RemoteQueueConsumer remoteQueueConsumer : remoteQueueConsumers.values()) {
         remoteQueueConsumer.close();
      }
      remoteQueueConsumers.clear();
   }


   public synchronized void createConsumer(RemoteConsumerKey key, Transformer transformer) {
      RemoteQueueConsumer remoteQueueConsumer = remoteQueueConsumers.get(key);
      if (remoteQueueConsumer == null) {
         remoteQueueConsumer = new RemoteQueueConsumer(server, transformer, key, connection);
         remoteQueueConsumer.start();
         remoteQueueConsumers.put(key, remoteQueueConsumer);
      }
      remoteQueueConsumer.incrementCount();
   }


   public synchronized void removeConsumer(RemoteConsumerKey key) {
      RemoteQueueConsumer remoteQueueConsumer = remoteQueueConsumers.get(key);
      if (remoteQueueConsumer != null) {
         if (remoteQueueConsumer.decrementCount() <= 0) {
            remoteQueueConsumer.close();
            remoteQueueConsumers.remove(key);
         }
      }
   }

}
