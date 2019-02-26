package org.apache.activemq.artemis.core.server.federation;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.artemis.api.core.QueueAttributes;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.server.ActiveMQServer;

public class FederatedQueueManager {

   private final FederationConnection federationConnection;
   private final ScheduledExecutorService scheduledExecutorService;
   private final int intialConnectDelayMultiplier = 2;
   private final int intialConnectDelayMax = 100;

   public FederatedQueueManager(ActiveMQServer server, FederationConnection federationConnection) {
      this.federationConnection = federationConnection;
      this.scheduledExecutorService = server.getScheduledPool();
   }

   public void createQueue(FederatedConsumerKey key) {
      scheduleCreateQueue(key, 0);
   }

   private void scheduleCreateQueue(FederatedConsumerKey key, int delay) {
      scheduledExecutorService.schedule(() -> {
         try {
            internalCreateQueue(key);
         } catch (Exception e) {
            scheduleCreateQueue(key, FederatedQueueConsumer.getNextDelay(delay, intialConnectDelayMultiplier, intialConnectDelayMax));
         }
      }, delay, TimeUnit.SECONDS);
   }

   private void internalCreateQueue(FederatedConsumerKey key) throws Exception {
      synchronized (this) {
         try (ClientSession clientSession = federationConnection.clientSessionFactory().createSession(true, true)) {
            if (!clientSession.queueQuery(key.getQueueName()).isExists()) {
               if (key.isTemporary()) {
                  clientSession.createTemporaryQueue(key.getAddress(), key.getRoutingType(), key.getQueueName(), key.getQueueFilterString());
               } else {
                  QueueAttributes queueAttributes = new QueueAttributes()
                        .setRoutingType(key.getRoutingType())
                        .setFilterString(key.getQueueFilterString())
                        .setDurable(true)
                        .setAutoDelete(true)
                        .setAutoDeleteDelay(120000L)
                        .setAutoDeleteMessageCount(-1L);
                  clientSession.createQueue(key.getAddress(), key.getQueueName(), false, queueAttributes);
               }
            }
         }
      }
   }

}