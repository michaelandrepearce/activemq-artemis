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

public class FederatedQueueConsumer implements MessageHandler, SessionFailureListener {

   public static final String FEDERATED_CONNECTION_NAME_PROPERTY = "federated-connection-name";
   private final ActiveMQServer server;
   private final FederationManager federationManager;
   private final FederatedConsumerKey key;
   private final Transformer transformer;
   private final FederationUpstream upstream;
   private final AtomicInteger count = new AtomicInteger();
   private final ScheduledExecutorService scheduledExecutorService;
   private final int intialConnectDelayMultiplier = 2;
   private final int intialConnectDelayMax = 30;
   private final ClientSessionCallback clientSessionCallback;

   private ClientSessionFactory clientSessionFactory;
   private ClientSession clientSession;
   private ClientConsumer clientConsumer;

   public FederatedQueueConsumer(FederationManager federationManager, ActiveMQServer server, Transformer transformer, FederatedConsumerKey key, FederationUpstream upstream, ClientSessionCallback clientSessionCallback) {
      this.federationManager = federationManager;
      this.server = server;
      this.key = key;
      this.transformer = transformer;
      this.upstream = upstream;
      this.scheduledExecutorService = server.getScheduledPool();
      this.clientSessionCallback = clientSessionCallback;
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
               this.clientSessionFactory = upstream.getConnection().clientSessionFactory();
               this.clientSession = clientSessionFactory.createSession(federationManager.getFederationUser(), federationManager.getFederationPassword(), false, true, true, clientSessionFactory.getServerLocator().isPreAcknowledge(), clientSessionFactory.getServerLocator().getAckBatchSize());
               this.clientSession.addFailureListener(this);
               this.clientSession.addMetaData(FEDERATED_CONNECTION_NAME_PROPERTY, upstream.getName().toString());
               this.clientSession.start();
               if (clientSessionCallback != null) {
                  clientSessionCallback.callback(clientSession);
               }
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
            clientSessionFactory.cleanup();
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
      if (clientSessionFactory != null) {
         clientSessionFactory.close();
      }
      clientConsumer = null;
      clientSession = null;
      clientSessionFactory = null;

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

   @Override
   public void connectionFailed(ActiveMQException exception, boolean failedOver) {
      connectionFailed(exception, failedOver, null);
   }

   @Override
   public void connectionFailed(ActiveMQException exception, boolean failedOver, String scaleDownTargetNodeID) {
      try {
         clientSessionFactory.cleanup();
         clientSessionFactory.close();
         clientConsumer = null;
         clientSession = null;
         clientSessionFactory = null;
      } catch (Throwable dontCare) {
      }
      start();
   }

   @Override
   public void beforeReconnect(ActiveMQException exception) {
   }

   public interface ClientSessionCallback {
      void callback(ClientSession clientSession) throws ActiveMQException;
   }
}