package org.apache.activemq.artemis.core.server.federation.queue;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.activemq.artemis.api.core.SimpleString;

public class FederatedQueueConfig {

   private boolean includeFederated;
   private Set<String> includes;
   private Set<String> excludes;

   public FederatedQueueConfig() {
      this(null, null);
   }

   public FederatedQueueConfig(Set<String> includes, Set<String> excludes) {
      this.includes = includes == null ? new HashSet<>() : includes;
      this.excludes = excludes == null ? new HashSet<>() : excludes;
   }

   public Set<String> getIncludes() {
      return includes;
   }

   public Set<String> getExcludes() {
      return excludes;
   }

   public boolean isIncludeFederated() {
      return includeFederated;
   }

   public void setIncludeFederated(boolean includeFederated) {
      this.includeFederated = includeFederated;
   }
}
