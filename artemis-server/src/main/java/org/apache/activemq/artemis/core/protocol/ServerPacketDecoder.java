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
package org.apache.activemq.artemis.core.protocol;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.*;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.*;

public class ServerPacketDecoder extends ClientPacketDecoder {

   private static final long serialVersionUID = 3348673114388400766L;

   private SessionSendMessage decodeSessionSendMessage(final ActiveMQBuffer in, CoreRemotingConnection connection) {
      final SessionSendMessage sendMessage;

      if (connection.isVersionBeforeAddressChange()) {
         sendMessage = new SessionSendMessage_1X(new CoreMessage(this.coreMessageObjectPools));
      } else if (connection.isVersionBeforeAsyncResponseChange()) {
         sendMessage = new SessionSendMessage(new CoreMessage(this.coreMessageObjectPools));
      } else {
         sendMessage = new SessionSendMessage_V2(new CoreMessage(this.coreMessageObjectPools));
      }

      sendMessage.decode(in);
      return sendMessage;
   }

   private static SessionAcknowledgeMessage decodeSessionAcknowledgeMessage(final ActiveMQBuffer in, CoreRemotingConnection connection) {
      final SessionAcknowledgeMessage acknowledgeMessage = new SessionAcknowledgeMessage();
      acknowledgeMessage.decode(in);
      return acknowledgeMessage;
   }

   private static SessionRequestProducerCreditsMessage decodeRequestProducerCreditsMessage(final ActiveMQBuffer in, CoreRemotingConnection connection) {
      final SessionRequestProducerCreditsMessage requestProducerCreditsMessage = new SessionRequestProducerCreditsMessage();
      requestProducerCreditsMessage.decode(in);
      return requestProducerCreditsMessage;
   }

   private static SessionConsumerFlowCreditMessage decodeSessionConsumerFlowCreditMessage(final ActiveMQBuffer in, CoreRemotingConnection connection) {
      final SessionConsumerFlowCreditMessage sessionConsumerFlowCreditMessage = new SessionConsumerFlowCreditMessage();
      sessionConsumerFlowCreditMessage.decode(in);
      return sessionConsumerFlowCreditMessage;
   }

   @Override
   public Packet decode(final ActiveMQBuffer in, CoreRemotingConnection connection) {
      final byte packetType = in.readByte();
      //optimized for the most common cases: hottest and commons methods will be inlined and this::decode too due to the byte code size
      switch (packetType) {
         case IN_VM:
            return decodeInVMPacket(in);
         case SESS_SEND:
            return decodeSessionSendMessage(in, connection);
         case SESS_ACKNOWLEDGE:
            return decodeSessionAcknowledgeMessage(in, connection);
         case SESS_PRODUCER_REQUEST_CREDITS:
            return decodeRequestProducerCreditsMessage(in, connection);
         case SESS_FLOWTOKEN:
            return decodeSessionConsumerFlowCreditMessage(in, connection);
         default:
            return slowPathDecode(in, packetType, connection);
      }
   }

   private Packet decodeInVMPacket(ActiveMQBuffer in) {
      InVmPacket inVmPacket = new InVmPacket();
      inVmPacket.decode(in);
      return inVmPacket.getPacket();
   }


   // separating for performance reasons
   private Packet slowPathDecode(ActiveMQBuffer in, byte packetType, CoreRemotingConnection connection) {
      Packet packet;

      switch (packetType) {

         case SESS_SEND_LARGE: {
            packet = new SessionSendLargeMessage(new CoreMessage());
            break;
         }
         case REPLICATION_APPEND: {
            packet = new ReplicationAddMessage();
            break;
         }
         case REPLICATION_APPEND_TX: {
            packet = new ReplicationAddTXMessage();
            break;
         }
         case REPLICATION_DELETE: {
            packet = new ReplicationDeleteMessage();
            break;
         }
         case REPLICATION_DELETE_TX: {
            packet = new ReplicationDeleteTXMessage();
            break;
         }
         case REPLICATION_PREPARE: {
            packet = new ReplicationPrepareMessage();
            break;
         }
         case REPLICATION_COMMIT_ROLLBACK: {
            packet = new ReplicationCommitMessage();
            break;
         }
         case REPLICATION_RESPONSE: {
            packet = new ReplicationResponseMessage();
            break;
         }
         case REPLICATION_RESPONSE_V2: {
            packet = new ReplicationResponseMessageV2();
            break;
         }
         case REPLICATION_PAGE_WRITE: {
            packet = new ReplicationPageWriteMessage();
            break;
         }
         case REPLICATION_PAGE_EVENT: {
            packet = new ReplicationPageEventMessage();
            break;
         }
         case REPLICATION_LARGE_MESSAGE_BEGIN: {
            packet = new ReplicationLargeMessageBeginMessage();
            break;
         }
         case REPLICATION_LARGE_MESSAGE_END: {
            packet = new ReplicationLargeMessageEndMessage();
            break;
         }
         case REPLICATION_LARGE_MESSAGE_WRITE: {
            packet = new ReplicationLargeMessageWriteMessage();
            break;
         }
         case PacketImpl.BACKUP_REGISTRATION: {
            packet = new BackupRegistrationMessage();
            break;
         }
         case PacketImpl.BACKUP_REGISTRATION_FAILED: {
            packet = new BackupReplicationStartFailedMessage();
            break;
         }
         case PacketImpl.REPLICATION_START_FINISH_SYNC: {
            packet = new ReplicationStartSyncMessage();
            break;
         }
         case PacketImpl.REPLICATION_SYNC_FILE: {
            packet = new ReplicationSyncFileMessage();
            break;
         }
         case PacketImpl.REPLICATION_SCHEDULED_FAILOVER: {
            packet = new ReplicationLiveIsStoppingMessage();
            break;
         }
         case CLUSTER_CONNECT: {
            packet = new ClusterConnectMessage();
            break;
         }
         case CLUSTER_CONNECT_REPLY: {
            packet = new ClusterConnectReplyMessage();
            break;
         }
         case NODE_ANNOUNCE: {
            packet = new NodeAnnounceMessage();
            break;
         }
         case BACKUP_REQUEST: {
            packet = new BackupRequestMessage();
            break;
         }
         case BACKUP_REQUEST_RESPONSE: {
            packet = new BackupResponseMessage();
            break;
         }
         case QUORUM_VOTE: {
            packet = new QuorumVoteMessage();
            break;
         }
         case QUORUM_VOTE_REPLY: {
            packet = new QuorumVoteReplyMessage();
            break;
         }
         case SCALEDOWN_ANNOUNCEMENT: {
            packet = new ScaleDownAnnounceMessage();
            break;
         }
         default: {
            packet = super.decode(packetType, connection);
         }
      }

      packet.decode(in);

      return packet;
   }
}
