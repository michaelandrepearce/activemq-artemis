package org.apache.activemq.artemis.core.server.federation;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;

public interface FederatedConsumerKey {
   SimpleString getQueueName();

   SimpleString getAddress();

   SimpleString getFqqn();

   RoutingType getRoutingType();

   SimpleString getFilterString();

   SimpleString getQueueFilterString();

   boolean isTemporary();

}
