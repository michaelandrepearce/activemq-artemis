package org.apache.activemq.artemis.core.server.federation.queue;

import java.util.Objects;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.federation.FederatedConsumerKey;
import org.apache.activemq.artemis.utils.CompositeAddress;

public class FederatedQueueConsumerKey implements FederatedConsumerKey {
   private final SimpleString address;
   private final SimpleString queueName;
   private final RoutingType routingType;
   private final SimpleString queueFilterString;
   private final SimpleString filterString;
   private final SimpleString fqqn;

   FederatedQueueConsumerKey(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString queueFilterString, SimpleString filterString) {
      this.address = address;
      this.routingType = routingType;
      this.queueName = queueName;
      this.fqqn  = CompositeAddress.toFullyQualified(address, queueName);
      this.filterString = filterString;
      this.queueFilterString = queueFilterString;
   }

   @Override
   public SimpleString getQueueName() {
      return queueName;
   }

   @Override
   public SimpleString getQueueFilterString() {
      return queueFilterString;
   }

   @Override
   public SimpleString getAddress() {
      return address;
   }

   @Override
   public SimpleString getFqqn() {
      return fqqn;
   }

   @Override
   public RoutingType getRoutingType() {
      return routingType;
   }

   @Override
   public SimpleString getFilterString() {
      return filterString;
   }

   @Override
   public boolean isTemporary() {
      return false;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof FederatedQueueConsumerKey)) return false;
      FederatedQueueConsumerKey that = (FederatedQueueConsumerKey) o;
      return Objects.equals(address, that.address) &&
            Objects.equals(queueName, that.queueName) &&
            routingType == that.routingType &&
            Objects.equals(queueFilterString, that.queueFilterString) &&
            Objects.equals(filterString, that.filterString) &&
            Objects.equals(fqqn, that.fqqn);
   }

   @Override
   public int hashCode() {
      return Objects.hash(address, queueName, routingType, queueFilterString, filterString, fqqn);
   }
}