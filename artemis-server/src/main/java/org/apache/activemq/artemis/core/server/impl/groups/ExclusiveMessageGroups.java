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
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.MessageReference;

public class ExclusiveMessageGroups implements MessageGroups {

   private static final SimpleString EXCLUSIVE = SimpleString.toSimpleString("exclusive");
   private Consumer consumer;

   private MessageReference lastSeenMessageReference;
   private SimpleString lastSeenGroup;

   @Override
   public void register(MessageReference messageReference, Consumer consumer) {
      this.consumer = consumer;
   }

   @Override
   public Consumer consumer(List<Consumer> consumers, MessageReference messageReference) {
      return this.consumer;
   }

   @Override
   public void removeConsumer(Consumer consumer) {
      if (this.consumer == consumer) {
         this.consumer = null;
      }
   }

   @Override
   public Consumer resetMessageGroupId(SimpleString groupId) {
      return reset(groupId);
   }

   @Override
   public Consumer reset(SimpleString groupIdAsSimpleString) {
      Consumer consumer = this.consumer;
      this.consumer = null;
      return consumer;
   }

   @Override
   public void resetAll() {
      this.consumer = null;
   }

   @Override
   public int count() {
      return this.consumer == null ? 0 : 1;
   }

   @Override
   public void forEach(BiConsumer<Object, Consumer> action) {
      if (this.consumer != null) {
         action.accept(EXCLUSIVE, this.consumer);
      }
   }
}
