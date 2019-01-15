package org.apache.activemq.artemis.core.server.federation;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.transformer.Transformer;

public class RemoteQueueManager {

   private final FederationConnection federationConnection;
   private final ScheduledExecutorService scheduledExecutorService;
   private final int intialConnectDelayMultiplier = 2;
   private final int intialConnectDelayMax = 100;

   public RemoteQueueManager(ActiveMQServer server, FederationConnection federationConnection) {
      this.federationConnection = federationConnection;
      this.scheduledExecutorService = server.getScheduledPool();
   }

   public void createQueue(RemoteConsumerKey key) {
      scheduleCreateQueue(key, 0);
   }

   private void scheduleCreateQueue(RemoteConsumerKey key, int delay) {
      scheduledExecutorService.schedule(() -> {
         try {
            internalCreateQueue(key);
         } catch (Exception e) {
            scheduleCreateQueue(key, RemoteQueueConsumer.getNextDelay(delay, intialConnectDelayMultiplier, intialConnectDelayMax));
         }
      }, delay, TimeUnit.SECONDS);
   }

   private void internalCreateQueue(RemoteConsumerKey key) throws Exception {
      synchronized (this) {
         try (ClientSession clientSession = federationConnection.clientSessionFactory().createSession(true, true)) {
            if (!clientSession.queueQuery(key.getQueueName()).isExists()) {
               if (key.isTemporary()) {
                  clientSession.createTemporaryQueue(key.getAddress(), key.getRoutingType(), key.getQueueName(), key.getQueueFilterString());
               } else {
                  clientSession.createQueue(key.getAddress(), key.getRoutingType(), key.getQueueName(), key.getQueueFilterString(), true);
               }
            }
         }
      }
   }

}