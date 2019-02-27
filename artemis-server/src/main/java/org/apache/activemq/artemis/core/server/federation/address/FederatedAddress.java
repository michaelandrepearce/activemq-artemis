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

package org.apache.activemq.artemis.core.server.federation.address;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.activemq.artemis.api.core.*;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.federation.FederatedAbstract;
import org.apache.activemq.artemis.core.server.federation.FederationUpstream;
import org.apache.activemq.artemis.core.server.federation.FederatedConsumerKey;
import org.apache.activemq.artemis.core.server.federation.FederationManager;
import org.apache.activemq.artemis.core.config.federation.FederationAddressPolicyConfiguration;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerQueuePlugin;
import org.apache.activemq.artemis.core.settings.impl.Match;
import org.apache.activemq.artemis.utils.ByteUtil;

/**
 * Federated Address, replicate messages from the remote brokers address to itself.
 *
 * Only when a queue exists on the local broker do we replicate, this is to avoid
 *
 * All messages are replicated, this is on purpose so should a number of lo
 *
 *
 */
public class FederatedAddress extends FederatedAbstract implements ActiveMQServerQueuePlugin, Serializable {

   public static final SimpleString HDR_HOPS = new SimpleString("_AMQ_Hops");
   private final SimpleString queueNameFormat;
   private final SimpleString filterString;
   private final Set<Predicate<String>> includes;
   private final Set<Predicate<String>> excludes;

   private final FederationAddressPolicyConfiguration config;

   public FederatedAddress(FederationManager federationManager, FederationAddressPolicyConfiguration config, ActiveMQServer server, FederationUpstream upstream) {
      super(federationManager, server, upstream);
      this.config = config;
      this.filterString =  HDR_HOPS.concat(" IS NULL OR ").concat(HDR_HOPS).concat("<").concat(Integer.toString(config.getMaxHops()));
      this.queueNameFormat = SimpleString.toSimpleString("federated.${connection}.${address}.${routeType}");
      if (config.getIncludes().isEmpty()) {
         includes = Collections.emptySet();
      } else {
         includes = new HashSet<>(config.getIncludes().size());
         for (String include : config.getIncludes()) {
            includes.add(new Match<>(include, null, wildcardConfiguration).getPattern().asPredicate());
         }
      }

      if (config.getExcludes().isEmpty()) {
         excludes = Collections.emptySet();
      } else {
         excludes = new HashSet<>(config.getExcludes().size());
         for (String exclude : config.getExcludes()) {
            excludes.add(new Match<>(exclude, null, wildcardConfiguration).getPattern().asPredicate());
         }
      }
   }

   @Override
   public void start() {
      super.start();
      server.getPostOffice()
            .getAllBindings()
            .values()
            .stream()
            .filter(b -> b instanceof QueueBinding)
            .map(b -> ((QueueBinding) b).getQueue())
            .forEach(this::createRemoteConsumer);
   }

   /**
    * After a queue has been created
    *
    * @param queue The newly created queue
    */
   public synchronized void afterCreateQueue(Queue queue) {
      createRemoteConsumer(queue);
   }

   public FederationAddressPolicyConfiguration getConfig() {
      return config;
   }

   private void createRemoteConsumer(Queue queue) {
      if (match(queue)) {
         FederatedConsumerKey key = getKey(queue);
         createRemoteConsumer(key, FederatedAddress::addHop, clientSession -> createRemoteQueue(clientSession, key));
      }
   }

   private void createRemoteQueue(ClientSession clientSession, FederatedConsumerKey key) throws ActiveMQException {
      if (!clientSession.queueQuery(key.getQueueName()).isExists()) {
         if (key.isTemporary()) {
            clientSession.createTemporaryQueue(key.getAddress(), key.getRoutingType(), key.getQueueName(), key.getQueueFilterString());
         } else {
            QueueAttributes queueAttributes = new QueueAttributes()
                  .setRoutingType(key.getRoutingType())
                  .setFilterString(key.getQueueFilterString())
                  .setDurable(true)
                  .setAutoDelete(true)
                  .setAutoDeleteDelay(TimeUnit.MINUTES.toMillis(5))
                  .setAutoDeleteMessageCount(-1L)
                  .setMaxConsumers(-1)
                  .setPurgeOnNoConsumers(false);
            clientSession.createQueue(key.getAddress(), key.getQueueName(), false, queueAttributes);
         }
      }
   }

   private boolean match(Queue queue) {
      return RoutingType.MULTICAST.equals(queue.getRoutingType()) && match(queue.getAddress());
   }

   private boolean match(SimpleString address) {
      return match(address.toString());
   }

   private boolean match(String address) {
      for(Predicate<String> exclude : excludes) {
         if (exclude.test(address)) {
            return false;
         }
      }
      if (includes.isEmpty()) {
         return true;
      } else {
         for(Predicate<String> include : includes) {
            if (include.test(address)) {
               return true;
            }
         }
         return false;
      }
   }

   private static Message addHop(Message message) {
      int hops = toInt(message.getExtraBytesProperty(HDR_HOPS));
      hops++;
      message.putExtraBytesProperty(HDR_HOPS, ByteUtil.intToBytes(hops));
      return message;
   }

   private static int toInt(byte[] bytes) {
      if (bytes != null && bytes.length == 4) {
         return ByteUtil.bytesToInt(bytes);
      } else {
         return 0;
      }
   }

   /**
    * Before an address is removed
    *
    * @param queue The queue that will be removed
    */
   @Override
   public synchronized void beforeDestroyQueue(Queue queue, final SecurityAuth session, boolean checkConsumerCount,
      boolean removeConsumers, boolean autoDeleteAddress) {
      FederatedConsumerKey key = getKey(queue);
      removeRemoteConsumer(key);
   }

   private FederatedConsumerKey getKey(Queue queue) {
      return new FederatedAddressConsumerKey(upstream.getName(), queue.getAddress(), queue.getRoutingType(), queueNameFormat, filterString, config.isTemporary());
   }


}
