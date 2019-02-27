package org.apache.activemq.artemis.core.config.federation;

import java.util.HashSet;
import java.util.Set;

public class FederationPolicySet implements FederationPolicy<FederationPolicySet> {

   private String name;
   private Set<String> policyRefs = new HashSet<>();

   public String getName() {
      return name;
   }

   public FederationPolicySet setName(String name) {
      this.name = name;
      return this;
   }

   public Set<String> getPolicyRefs() {
      return policyRefs;
   }

   public FederationPolicySet addPolicyRef(String name) {
      policyRefs.add(name);
      return this;
   }

}
