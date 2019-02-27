package org.apache.activemq.artemis.core.config.federation;

import java.util.HashSet;
import java.util.Set;

public class FederationAddressPolicyConfiguration implements FederationPolicy<FederationAddressPolicyConfiguration> {

   private String name;
   private Set<String> includes = new HashSet<>();
   private Set<String> excludes = new HashSet<>();
   private boolean temporary;
   private int maxHops;

   public String getName() {
      return name;
   }

   public FederationAddressPolicyConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public Set<String> getIncludes() {
      return includes;
   }

   public Set<String> getExcludes() {
      return excludes;
   }

   public FederationAddressPolicyConfiguration addInclude(String include) {
      includes.add(include);
      return this;
   }

   public FederationAddressPolicyConfiguration addExclude(String exclude) {
      excludes.add(exclude);
      return this;
   }

   public int getMaxHops() {
      return maxHops;
   }

   public FederationAddressPolicyConfiguration setMaxHops(int maxHops) {
      this.maxHops = maxHops;
      return this;
   }

   public boolean isTemporary() {
      return temporary;
   }
}
