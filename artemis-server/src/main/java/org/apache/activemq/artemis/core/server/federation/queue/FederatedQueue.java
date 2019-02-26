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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.federation.FederatedAbstract;
import org.apache.activemq.artemis.core.server.federation.FederationConnection;
import org.apache.activemq.artemis.core.server.federation.FederatedConsumerKey;
import org.apache.activemq.artemis.core.server.federation.FederatedQueueConsumer;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerConsumerPlugin;
import org.apache.activemq.artemis.core.settings.impl.Match;

public class FederatedQueue extends FederatedAbstract implements ActiveMQServerConsumerPlugin, Serializable {

   private final Set<Predicate<String>> includes;
   private final Set<Predicate<String>> excludes;
   private final Filter metaDataFilter;

   public FederatedQueue(FederatedQueueConfig federatedQueueConfig, ActiveMQServer server, FederationConnection federationConnection) throws ActiveMQException {
      super(server, federationConnection);
      String metaDataFilterString = federatedQueueConfig.isIncludeFederated() ? null : FederatedQueueConsumer.FEDERATED_CONNECTION_NAME_PROPERTY +  " IS NULL";
      metaDataFilter = FilterImpl.createFilter(metaDataFilterString);
      if (federatedQueueConfig.getIncludes().isEmpty()) {
         includes = Collections.emptySet();
      } else {
         includes = new HashSet<>(federatedQueueConfig.getIncludes().size());
         for (String include : federatedQueueConfig.getIncludes()) {
            includes.add(new Match<>(include, null, wildcardConfiguration).getPattern().asPredicate());
         }
      }

      if (federatedQueueConfig.getExcludes().isEmpty()) {
         excludes = Collections.emptySet();
      } else {
         excludes = new HashSet<>(federatedQueueConfig.getExcludes().size());
         for (String exclude : federatedQueueConfig.getExcludes()) {
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
            .map(b -> (QueueBinding) b)
            .forEach(b -> createRemoteConsumer(b.getQueue()));
   }

   /**
    * After a consumer has been created
    *
    * @param consumer the created consumer
    */
   public synchronized void afterCreateConsumer(ServerConsumer consumer) {
      createRemoteConsumer(consumer);
   }

   private void createRemoteConsumer(Queue queue) {
      queue.getConsumers()
            .stream()
            .filter(consumer -> consumer instanceof ServerConsumer)
            .map(c -> (ServerConsumer) c).forEach(this::createRemoteConsumer);
   }

   private void createRemoteConsumer(ServerConsumer consumer) {
      if (metaDataFilter != null) {
         metaDataFilter.match(server.getSessionByID(consumer.getSessionID()).getMetaData());
      }
      if (match(consumer.getQueueName())) {
         FederatedConsumerKey key = getKey(consumer);
         createRemoteConsumer(key, message -> message.setAddress(key.getFqqn()));
      }
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

   /**
    * Before a consumer is closed
    *
    * @param consumer
    * @param failed
    */
   public synchronized void beforeCloseConsumer(ServerConsumer consumer, boolean failed) {
      FederatedConsumerKey key = getKey(consumer);
      removeRemoteConsumer(key);

   }

   private FederatedConsumerKey getKey(ServerConsumer consumer) {
      Queue queue = consumer.getQueue();
      return new FederatedQueueConsumerKey(queue.getAddress(), queue.getRoutingType(), queue.getName(), Filter.toFilterString(queue.getFilter()), Filter.toFilterString(consumer.getFilter()));
   }
}
