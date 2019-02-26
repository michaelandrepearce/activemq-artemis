package org.apache.activemq.artemis.core.server.federation.address;

import java.util.*;

public class FederatedAddressConfig {

   private Set<String> includes;
   private Set<String> excludes;
   private boolean temporary;
   private int maxHops;

   public FederatedAddressConfig(int maxHops, boolean temporary) {
      this(null, null, maxHops, temporary);
   }

      public FederatedAddressConfig(Set<String> includes, Set<String> excludes, int maxHops, boolean temporary) {
      this.includes = includes == null ? new HashSet<>() : includes;
      this.excludes = excludes == null ? new HashSet<>() : excludes;
      this.maxHops = maxHops;
      this.temporary = temporary;
   }

   public Set<String> getIncludes() {
      return includes;
   }

   public Set<String> getExcludes() {
      return excludes;
   }

   public int getMaxHops() {
      return maxHops;
   }

   public boolean isTemporary() {
      return temporary;
   }
}
