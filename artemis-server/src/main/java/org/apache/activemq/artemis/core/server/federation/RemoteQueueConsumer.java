package org.apache.activemq.artemis.core.server.federation;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.transformer.Transformer;

public class RemoteQueueConsumer implements MessageHandler {

   private final ActiveMQServer server;
   private final RemoteConsumerKey key;
   private final Transformer transformer;
   private final FederationConnection federationConnection;
   private final AtomicInteger count = new AtomicInteger();
   private final ScheduledExecutorService scheduledExecutorService;
   private final int intialConnectDelayMultiplier = 2;
   private final int intialConnectDelayMax = 100;

   private ClientSession clientSession;
   private ClientConsumer clientConsumer;

   public RemoteQueueConsumer(ActiveMQServer server, Transformer transformer, RemoteConsumerKey key, FederationConnection federationConnection) {
      this.server = server;
      this.key = key;
      this.transformer = transformer;
      this.federationConnection = federationConnection;
      this.scheduledExecutorService = server.getScheduledPool();
   }

   public int incrementCount() {
      return count.incrementAndGet();
   }

   public int decrementCount() {
      return count.decrementAndGet();
   }

   public void start() {
      scheduleConnect(0);
   }

   private void scheduleConnect(int delay) {
      scheduledExecutorService.schedule(() -> {
         try {
            connect();
         } catch (Exception e) {
            scheduleConnect(getNextDelay(delay, intialConnectDelayMultiplier, intialConnectDelayMax));
         }
      }, delay, TimeUnit.SECONDS);
   }

   static int getNextDelay(int delay, int delayMultiplier, int delayMax) {
      int nextDelay;
      if (delay == 0) {
         nextDelay = 1;
      } else {
         nextDelay = delay * delayMultiplier;
         if (nextDelay > delayMax) {
            nextDelay = delayMax;
         }
      }
      return nextDelay;
   }

   private void connect() throws Exception {
      try {
         if (clientConsumer == null) {
            synchronized (this) {
               this.clientSession = federationConnection.clientSessionFactory().createSession(true, true);
               this.clientSession.start();
               if (clientSession.queueQuery(key.getQueueName()).isExists()) {
                  this.clientConsumer = clientSession.createConsumer(key.getQueueName(), key.getFilterString(), -1, false);
                  this.clientConsumer.setMessageHandler(this);
               } else {
                  throw new ActiveMQNonExistentQueueException("Queue " + key.getQueueName() + " does not exist on remote");
               }
            }
         }
      } catch (Exception e) {
         try {
            disconnect();
         } catch (ActiveMQException ignored) {
         }
         throw e;
      }
   }

   public void close() {
      scheduleDisconnect(0);
   }

   private void scheduleDisconnect(int delay) {
      scheduledExecutorService.schedule(() -> {
         try {
            disconnect();
         } catch (Exception ignored) {
         }
      }, delay, TimeUnit.SECONDS);
   }

   private void disconnect() throws ActiveMQException {
      if (clientConsumer != null) {
         clientConsumer.close();
      }
      if (clientSession != null) {
         clientSession.close();
      }
      clientConsumer = null;
      clientSession = null;
   }

   @Override
   public void onMessage(ClientMessage clientMessage) {
      try {
         Message message = transformer == null ? clientMessage : transformer.transform(clientMessage);
         server.getPostOffice().route(message, true);
         clientMessage.acknowledge();
      } catch (Exception e) {
         try {
            clientSession.rollback();
         } catch (ActiveMQException e1) {
         }
      }
   }
}