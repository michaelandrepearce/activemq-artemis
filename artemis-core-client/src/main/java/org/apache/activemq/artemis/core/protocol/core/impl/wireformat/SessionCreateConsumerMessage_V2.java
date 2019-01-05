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
package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;

public class SessionCreateConsumerMessage_V2 extends SessionCreateConsumerMessage {

   private int priority;

   public SessionCreateConsumerMessage_V2(final long id,
                                          final SimpleString queueName,
                                          final SimpleString filterString,
                                          final int priority,
                                          final boolean browseOnly,
                                          final boolean requiresResponse) {
      super(SESS_CREATECONSUMER_V2, id, queueName, filterString, browseOnly, requiresResponse);
      this.priority = priority;
   }

   public SessionCreateConsumerMessage_V2() {
      super(SESS_CREATECONSUMER);
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      appendToString(buff);
      buff.append(", priority=" + priority);
      buff.append("]");
      return buff.toString();
   }

   public int getPriority() {
      return priority;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      super.encodeRest(buffer);
      buffer.writeInt(priority);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      super.decodeRest(buffer);
      priority = buffer.readInt();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + priority;
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionCreateConsumerMessage_V2))
         return false;
      SessionCreateConsumerMessage_V2 other = (SessionCreateConsumerMessage_V2) obj;
      if (priority != other.priority)
         return false;
      return true;
   }
}
