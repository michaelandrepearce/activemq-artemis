package org.apache.activemq.artemis.core.config.federation;

import java.util.HashSet;
import java.util.Set;

public class FederationQueuePolicyConfiguration implements FederationPolicy<FederationQueuePolicyConfiguration> {

   private String name;
   private boolean includeFederated;
   private Set<String> includes = new HashSet<>();
   private Set<String> excludes = new HashSet<>();

   public String getName() {
      return name;
   }

   public FederationQueuePolicyConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public Set<String> getIncludes() {
      return includes;
   }

   public Set<String> getExcludes() {
      return excludes;
   }

   public FederationQueuePolicyConfiguration addInclude(String include) {
      includes.add(include);
      return this;
   }

   public FederationQueuePolicyConfiguration addExclude(String exclude) {
      excludes.add(exclude);
      return this;
   }

   public boolean isIncludeFederated() {
      return includeFederated;
   }

   public FederationQueuePolicyConfiguration setIncludeFederated(boolean includeFederated) {
      this.includeFederated = includeFederated;
      return this;
   }


}
