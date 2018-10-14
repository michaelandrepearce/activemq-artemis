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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.MessageReference;

public class SimpleMessageGroups implements MessageGroups {

   private final Map<SimpleString, Consumer> groups = new HashMap<>();

   private MessageReference lastSeenMessageReference;
   private SimpleString lastSeenGroup;

   private SimpleString getGroupId(MessageReference ref) {
      if (lastSeenMessageReference != ref) {
         lastSeenGroup = MessageGroups.extractGroupId(ref);
         lastSeenMessageReference = ref;
      }
      return lastSeenGroup;
   }



   @Override
   public void register(MessageReference messageReference, Consumer consumer) {
      SimpleString groupId = getGroupId(messageReference);
      if (groupId != null) {
         groups.put(groupId, consumer);
      }
   }

   @Override
   public Consumer consumer(List<Consumer> consumers, MessageReference messageReference) {
      SimpleString groupId = getGroupId(messageReference);
      if (groupId != null) {
         return groups.get(groupId);
      } else {
         return null;
      }
   }

   @Override
   public void removeConsumer(Consumer consumer) {
      groups.entrySet().removeIf(entry -> consumer == entry.getValue());
   }

   @Override
   public Consumer resetMessageGroupId(SimpleString groupId) {
      return reset(groupId);
   }

   @Override
   public Consumer reset(SimpleString groupId) {
      return groups.remove(groupId);
   }

   @Override
   public void resetAll() {
      groups.clear();
   }

   @Override
   public int count() {
      return groups.size();
   }

   @Override
   public void forEach(BiConsumer<Object, Consumer> action) {
      groups.forEach(action);
   }
}
