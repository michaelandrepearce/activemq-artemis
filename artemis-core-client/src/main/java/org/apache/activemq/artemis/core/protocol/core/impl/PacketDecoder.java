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
package org.apache.activemq.artemis.core.protocol.core.impl;

import java.io.Serializable;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.client.ActiveMQClientMessageBundle;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.*;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.*;

public abstract class PacketDecoder implements Serializable {

   public abstract Packet decode(ActiveMQBuffer in, CoreRemotingConnection connection);

   public Packet decode(byte packetType, CoreRemotingConnection connection) {
      Packet packet;

      switch (packetType) {
         case IN_VM: {
            packet = new InVmPacket();
            break;
         }
         case PING: {
            packet = new Ping();
            break;
         }
         case DISCONNECT: {
            packet = new DisconnectMessage();
            break;
         }
         case DISCONNECT_V2: {
            packet = new DisconnectMessage_V2();
            break;
         }
         case DISCONNECT_CONSUMER: {
            packet = new DisconnectConsumerMessage();
            break;
         }
         case EXCEPTION: {
            if (connection.isVersionBeforeAsyncResponseChange()) {
               packet = new ActiveMQExceptionMessage();
            } else {
               packet = new ActiveMQExceptionMessage_V2();
            }
            break;
         }
         case PACKETS_CONFIRMED: {
            packet = new PacketsConfirmedMessage();
            break;
         }
         case NULL_RESPONSE: {
            if (connection.isVersionBeforeAsyncResponseChange()) {
               packet = new NullResponseMessage();
            } else {
               packet = new NullResponseMessage_V2();
            }
            break;
         }
         case CREATESESSION: {
            packet = new CreateSessionMessage();
            break;
         }
         case CHECK_FOR_FAILOVER: {
            packet = new CheckFailoverMessage();
            break;
         }
         case CREATESESSION_RESP: {
            packet = new CreateSessionResponseMessage();
            break;
         }
         case REATTACH_SESSION: {
            packet = new ReattachSessionMessage();
            break;
         }
         case REATTACH_SESSION_RESP: {
            packet = new ReattachSessionResponseMessage();
            break;
         }
         case SESS_CLOSE: {
            packet = new SessionCloseMessage();
            break;
         }
         case SESS_CREATECONSUMER: {
            packet = new SessionCreateConsumerMessage();
            break;
         }
         case SESS_ACKNOWLEDGE: {
            packet = new SessionAcknowledgeMessage();
            break;
         }
         case SESS_EXPIRED: {
            packet = new SessionExpireMessage();
            break;
         }
         case SESS_COMMIT: {
            packet = new SessionCommitMessage();
            break;
         }
         case SESS_ROLLBACK: {
            packet = new RollbackMessage();
            break;
         }
         case SESS_QUEUEQUERY: {
            packet = new SessionQueueQueryMessage();
            break;
         }
         case SESS_QUEUEQUERY_RESP: {
            packet = new SessionQueueQueryResponseMessage();
            break;
         }
         case SESS_QUEUEQUERY_RESP_V2: {
            packet = new SessionQueueQueryResponseMessage_V2();
            break;
         }
         case SESS_QUEUEQUERY_RESP_V3: {
            packet = new SessionQueueQueryResponseMessage_V3();
            break;
         }
         case CREATE_ADDRESS: {
            packet = new CreateAddressMessage();
            break;
         }
         case CREATE_QUEUE: {
            packet = new CreateQueueMessage();
            break;
         }
         case CREATE_QUEUE_V2: {
            packet = new CreateQueueMessage_V2();
            break;
         }
         case CREATE_SHARED_QUEUE: {
            packet = new CreateSharedQueueMessage();
            break;
         }
         case CREATE_SHARED_QUEUE_V2: {
            packet = new CreateSharedQueueMessage_V2();
            break;
         }
         case DELETE_QUEUE: {
            packet = new SessionDeleteQueueMessage();
            break;
         }
         case SESS_BINDINGQUERY: {
            packet = new SessionBindingQueryMessage();
            break;
         }
         case SESS_BINDINGQUERY_RESP: {
            packet = new SessionBindingQueryResponseMessage();
            break;
         }
         case SESS_BINDINGQUERY_RESP_V2: {
            packet = new SessionBindingQueryResponseMessage_V2();
            break;
         }
         case SESS_BINDINGQUERY_RESP_V3: {
            packet = new SessionBindingQueryResponseMessage_V3();
            break;
         }
         case SESS_BINDINGQUERY_RESP_V4: {
            packet = new SessionBindingQueryResponseMessage_V4();
            break;
         }
         case SESS_XA_START: {
            packet = new SessionXAStartMessage();
            break;
         }
         case SESS_XA_FAILED: {
            packet = new SessionXAAfterFailedMessage();
            break;
         }
         case SESS_XA_END: {
            packet = new SessionXAEndMessage();
            break;
         }
         case SESS_XA_COMMIT: {
            packet = new SessionXACommitMessage();
            break;
         }
         case SESS_XA_PREPARE: {
            packet = new SessionXAPrepareMessage();
            break;
         }
         case SESS_XA_RESP: {
            if (connection.isVersionBeforeAsyncResponseChange()) {
               packet = new SessionXAResponseMessage();
            } else {
               packet = new SessionXAResponseMessage_V2();
            }
            break;
         }
         case SESS_XA_ROLLBACK: {
            packet = new SessionXARollbackMessage();
            break;
         }
         case SESS_XA_JOIN: {
            packet = new SessionXAJoinMessage();
            break;
         }
         case SESS_XA_SUSPEND: {
            packet = new PacketImpl(PacketImpl.SESS_XA_SUSPEND);
            break;
         }
         case SESS_XA_RESUME: {
            packet = new SessionXAResumeMessage();
            break;
         }
         case SESS_XA_FORGET: {
            packet = new SessionXAForgetMessage();
            break;
         }
         case SESS_XA_INDOUBT_XIDS: {
            packet = new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS);
            break;
         }
         case SESS_XA_INDOUBT_XIDS_RESP: {
            packet = new SessionXAGetInDoubtXidsResponseMessage();
            break;
         }
         case SESS_XA_SET_TIMEOUT: {
            packet = new SessionXASetTimeoutMessage();
            break;
         }
         case SESS_XA_SET_TIMEOUT_RESP: {
            packet = new SessionXASetTimeoutResponseMessage();
            break;
         }
         case SESS_XA_GET_TIMEOUT: {
            packet = new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT);
            break;
         }
         case SESS_XA_GET_TIMEOUT_RESP: {
            packet = new SessionXAGetTimeoutResponseMessage();
            break;
         }
         case SESS_START: {
            packet = new PacketImpl(PacketImpl.SESS_START);
            break;
         }
         case SESS_STOP: {
            packet = new PacketImpl(PacketImpl.SESS_STOP);
            break;
         }
         case SESS_FLOWTOKEN: {
            packet = new SessionConsumerFlowCreditMessage();
            break;
         }
         case SESS_CONSUMER_CLOSE: {
            packet = new SessionConsumerCloseMessage();
            break;
         }
         case SESS_INDIVIDUAL_ACKNOWLEDGE: {
            packet = new SessionIndividualAcknowledgeMessage();
            break;
         }
         case SESS_RECEIVE_CONTINUATION: {
            packet = new SessionReceiveContinuationMessage();
            break;
         }
         case SESS_SEND_CONTINUATION: {
            if (connection.isVersionBeforeAsyncResponseChange()) {
               packet = new SessionSendContinuationMessage();
            } else {
               packet = new SessionSendContinuationMessage_V2();
            }
            break;
         }
         case SESS_PRODUCER_REQUEST_CREDITS: {
            packet = new SessionRequestProducerCreditsMessage();
            break;
         }
         case SESS_PRODUCER_CREDITS: {
            packet = new SessionProducerCreditsMessage();
            break;
         }
         case SESS_PRODUCER_FAIL_CREDITS: {
            packet = new SessionProducerCreditsFailMessage();
            break;
         }
         case SESS_FORCE_CONSUMER_DELIVERY: {
            packet = new SessionForceConsumerDelivery();
            break;
         }
         case CLUSTER_TOPOLOGY: {
            packet = new ClusterTopologyChangeMessage();
            break;
         }
         case CLUSTER_TOPOLOGY_V2: {
            packet = new ClusterTopologyChangeMessage_V2();
            break;
         }
         case CLUSTER_TOPOLOGY_V3: {
            packet = new ClusterTopologyChangeMessage_V3();
            break;
         }
         case SUBSCRIBE_TOPOLOGY: {
            packet = new SubscribeClusterTopologyUpdatesMessage();
            break;
         }
         case SUBSCRIBE_TOPOLOGY_V2: {
            packet = new SubscribeClusterTopologyUpdatesMessageV2();
            break;
         }
         case SESS_ADD_METADATA: {
            packet = new SessionAddMetaDataMessage();
            break;
         }
         case SESS_ADD_METADATA2: {
            packet = new SessionAddMetaDataMessageV2();
            break;
         }
         case SESS_UNIQUE_ADD_METADATA: {
            packet = new SessionUniqueAddMetaDataMessage();
            break;
         }
         case PacketImpl.CHECK_FOR_FAILOVER_REPLY: {
            packet = new CheckFailoverReplyMessage();
            break;
         }
         case PacketImpl.DISCONNECT_CONSUMER_KILL: {
            packet = new DisconnectConsumerWithKillMessage();
            break;
         }
         default: {
            throw ActiveMQClientMessageBundle.BUNDLE.invalidType(packetType);
         }
      }

      return packet;
   }

}
