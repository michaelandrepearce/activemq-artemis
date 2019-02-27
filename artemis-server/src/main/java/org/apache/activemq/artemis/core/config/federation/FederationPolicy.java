package org.apache.activemq.artemis.core.config.federation;

public interface FederationPolicy<T> {

   String getName();

   T setName(String name);
}
