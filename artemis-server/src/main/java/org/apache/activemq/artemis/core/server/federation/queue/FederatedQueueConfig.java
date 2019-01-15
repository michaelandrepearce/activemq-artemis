package org.apache.activemq.artemis.core.server.federation.queue;

import org.apache.activemq.artemis.api.core.SimpleString;

public class FederatedQueueConfig {

   private String name;
   private String connectionName;
   private String queueName;

   public FederatedQueueConfig(String name, String connectionName, String queueName) {
      this.name = name;
      this.connectionName = connectionName;
      this.queueName = queueName;
   }

   public String getQueueName() {
      return queueName;
   }

   public String getName() {
      return name;
   }

   public String getConnectionName() {
      return connectionName;
   }
}
