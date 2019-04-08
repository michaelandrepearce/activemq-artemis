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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.collections.ConcurrentLongHashMap;

public class InVmPacket extends PacketImpl {

   private static AtomicLong idGenerator = new AtomicLong();
   private static ConcurrentLongHashMap<Packet> map = new ConcurrentLongHashMap<>();

   private Packet packet;

   public InVmPacket() {
      super(IN_VM);
   }

   public InVmPacket(Packet messagePacket) {
      super(messagePacket.getType());
   }

   @Override
   public void encodeRest(ActiveMQBuffer buffer) {
      long id = idGenerator.incrementAndGet();
      buffer.writeLong(id);
      map.put(id, packet);

   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      long id = buffer.readLong();
      packet = map.remove(id);
   }

   @Override
   public int expectedEncodeSize() {
      return PACKET_HEADERS_SIZE +  DataConstants.SIZE_LONG;
   }

   public Packet getPacket() {
      return packet;
   }

}
