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
import java.util.function.Predicate;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.federation.FederatedAbstract;
import org.apache.activemq.artemis.core.server.federation.FederationConnection;
import org.apache.activemq.artemis.core.server.federation.FederatedConsumerKey;
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

   private final FederatedAddressConfig federatedAddressConfig;

   public FederatedAddress(FederatedAddressConfig federatedAddressConfig, ActiveMQServer server, FederationConnection federationConnection) {
      super(server, federationConnection);
      this.federatedAddressConfig = federatedAddressConfig;
      this.filterString =  HDR_HOPS.concat(" IS NULL OR ").concat(HDR_HOPS).concat("<").concat(Integer.toString(federatedAddressConfig.getMaxHops()));
      this.queueNameFormat = SimpleString.toSimpleString("federated.${connection}.${address}.${routeType}");
      if (federatedAddressConfig.getIncludes().isEmpty()) {
         includes = Collections.emptySet();
      } else {
         includes = new HashSet<>(federatedAddressConfig.getIncludes().size());
         for (String include : federatedAddressConfig.getIncludes()) {
            includes.add(new Match<>(include, null, wildcardConfiguration).getPattern().asPredicate());
         }
      }

      if (federatedAddressConfig.getExcludes().isEmpty()) {
         excludes = Collections.emptySet();
      } else {
         excludes = new HashSet<>(federatedAddressConfig.getExcludes().size());
         for (String exclude : federatedAddressConfig.getExcludes()) {
            excludes.add(new Match<>(exclude, null, wildcardConfiguration).getPattern().asPredicate());
         }
      }
   }

   @Override
   public void start() {
      server.getPostOffice()
            .getAllBindings()
            .values()
            .stream()
            .filter(b -> b instanceof QueueBinding)
            .map(b -> ((QueueBinding) b).getQueue())
            .forEach(this::createRemoteQueue);
   }

   /**
    * After a queue has been created
    *
    * @param queue The newly created queue
    */
   public synchronized void afterCreateQueue(Queue queue) {
      createRemoteQueue(queue);
   }

   private void createRemoteQueue(Queue queue) {
      if (RoutingType.MULTICAST.equals(queue.getRoutingType())) {
         if (match(queue.getAddress().toString())) {
            FederatedConsumerKey key = getKey(queue);
            remoteQueueManager.createQueue(key);
            createRemoteConsumer(key, FederatedAddress::addHop);
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
      return new FederatedAddressConsumerKey(connection.getName(), queue.getAddress(), queue.getRoutingType(), queueNameFormat, filterString, federatedAddressConfig.isTemporary());
   }


}
