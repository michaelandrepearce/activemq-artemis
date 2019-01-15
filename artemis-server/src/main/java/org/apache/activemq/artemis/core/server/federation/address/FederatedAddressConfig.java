package org.apache.activemq.artemis.core.server.federation.address;

public class FederatedAddressConfig {

   private String name;
   private String connectionName;
   private String address;
   private boolean temporary;
   private int maxHops;

   public FederatedAddressConfig(String name, String connectionName, String address, int maxHops, boolean temporary) {
      this.name = name;
      this.connectionName = connectionName;
      this.address = address;
      this.maxHops = maxHops;
      this.temporary = temporary;
   }

   public String getAddress() {
      return address;
   }

   public String getName() {
      return name;
   }

   public String getConnectionName() {
      return connectionName;
   }

   public int getMaxHops() {
      return maxHops;
   }

   public boolean isTemporary() {
      return temporary;
   }
}
