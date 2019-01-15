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

package org.apache.activemq.artemis.core.server.federation.queue;

import java.io.Serializable;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.federation.FederatedAbstract;
import org.apache.activemq.artemis.core.server.federation.FederationConnection;
import org.apache.activemq.artemis.core.server.federation.RemoteConsumerKey;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerConsumerPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerQueuePlugin;

public class FederatedQueue extends FederatedAbstract implements ActiveMQServerConsumerPlugin, Serializable {

   private final FederatedQueueConfig federatedQueueConfig;

   public FederatedQueue(FederatedQueueConfig federatedQueueConfig, ActiveMQServer server, FederationConnection federationConnection) {
      super(server, federationConnection);
      this.federatedQueueConfig = federatedQueueConfig;
   }

   /**
    * After a consumer has been created
    *
    * @param consumer the created consumer
    */
   public synchronized void afterCreateConsumer(ServerConsumer consumer) {
      if (consumer.getQueue().getName().toString().equals(federatedQueueConfig.getQueueName())) {
         RemoteConsumerKey key = getKey(consumer);
         createConsumer(key, message -> message.setAddress(key.getFqqn()));
      }
   }

   /**
    * Before a consumer is closed
    *
    * @param consumer
    * @param failed
    */
   public synchronized void beforeCloseConsumer(ServerConsumer consumer, boolean failed) {
      RemoteConsumerKey key = getKey(consumer);
      removeConsumer(key);

   }

   private RemoteConsumerKey getKey(ServerConsumer consumer) {
      Queue queue = consumer.getQueue();
      return new FederatedQueueConsumerKey(queue.getAddress(), queue.getRoutingType(), queue.getName(), Filter.toFilterString(queue.getFilter()), Filter.toFilterString(consumer.getFilter()));
   }

}
