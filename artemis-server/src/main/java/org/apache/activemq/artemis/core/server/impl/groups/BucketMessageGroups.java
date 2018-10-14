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
package org.apache.activemq.artemis.core.server.impl.groups;

import java.util.List;
import java.util.function.BiConsumer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.MessageReference;

public class BucketMessageGroups implements MessageGroups {

   private final Consumer[] bucket;
   private int count;

   public BucketMessageGroups(int noBuckets) {
      bucket = new Consumer[noBuckets];
   }

   private MessageReference lastSeenMessageReference;
   private int lastSeenBucket;

   private int getBucketId(MessageReference ref) {
      if (lastSeenMessageReference != ref) {
         SimpleString groupdId = extractGroupId(ref);
         lastSeenBucket = getBucketForGroupId(groupdId);
         lastSeenMessageReference = ref;
      }
      return lastSeenBucket;
   }

   private int getBucketForGroupId(SimpleString groupId) {
      return groupId == null ? -1 : groupId.hashCode() % bucket.length;
   }

   private static SimpleString extractGroupId(MessageReference ref) {
      try {
         return ref.getMessage().getGroupID();
      } catch (Throwable e) {
         ActiveMQServerLogger.LOGGER.unableToExtractGroupID(e);
         return null;
      }
   }

   @Override
   public void register(MessageReference messageReference, Consumer consumer) {
      int bucketId = getBucketId(messageReference);
      if (bucketId != -1 && bucket[bucketId] == null) {
         bucket[bucketId] = consumer;
         count++;
      }
   }

   @Override
   public Consumer consumer(List<Consumer> consumers, MessageReference messageReference) {
      int bucketId = getBucketId(messageReference);
      if (bucketId != -1) {
         return bucket[bucketId];
      } else {
         return null;
      }
   }

   @Override
   public void removeConsumer(Consumer consumer) {
      for (int i = 0; i < bucket.length; i++) {
         if (bucket[i] == consumer) {
            bucket[i] = null;
            count--;
         }
      }
   }

   @Override
   public Consumer resetMessageGroupId(SimpleString groupId) {
      return reset(getBucketForGroupId(groupId));
   }

   @Override
   public Consumer reset(SimpleString id) {
      try {
         int bucketId = Integer.parseInt(id.toString());
         if (bucketId >= bucket.length) {
            throw new IllegalArgumentException("id must be a group bucket");
         }
         return reset(bucketId);
      } catch (NumberFormatException nfe) {
         throw new IllegalArgumentException("id must be a group bucket");
      }
   }

   public Consumer reset(int bucketId) {
      Consumer consumer = bucket[bucketId];
      bucket[bucketId] = null;
      return consumer;
   }

   @Override
   public void resetAll() {
      for (int i = 0; i < bucket.length; i++) {
         bucket[i] = null;
      }
      count = 0;
   }

   @Override
   public int count() {
      return count;
   }

   @Override
   public void forEach(BiConsumer<Object, Consumer> action) {
      for (int i = 0; i < bucket.length; i++) {
         if (bucket[i] != null) {
            action.accept(i, bucket[i]);
         }
      }
   }
}
