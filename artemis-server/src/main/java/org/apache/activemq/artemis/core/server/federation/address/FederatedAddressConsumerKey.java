package org.apache.activemq.artemis.core.server.federation.address;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.federation.RemoteConsumerKey;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.apache.commons.lang3.text.StrSubstitutor;

public class FederatedAddressConsumerKey implements RemoteConsumerKey {

   private final SimpleString connectionName;
   private final SimpleString address;
   private final SimpleString queueNameFormat;
   private final RoutingType routingType;
   private final SimpleString queueFilterString;
   private final boolean temporary;


   private SimpleString fqqn;
   private SimpleString queueName;

   FederatedAddressConsumerKey(SimpleString connectionName, SimpleString address, RoutingType routingType, SimpleString queueNameFormat, SimpleString queueFilterString, boolean temporary) {
      this.connectionName = connectionName;
      this.address = address;
      this.routingType = routingType;
      this.queueNameFormat = queueNameFormat;
      this.queueFilterString = queueFilterString;
      this.temporary = temporary;
   }

   @Override
   public SimpleString getQueueName() {
      if (queueName == null) {
         Map<String, String> data = new HashMap<>();
         data.put("address", address.toString());
         data.put("routeType", routingType.name().toLowerCase());
         data.put("connection", connectionName.toString());
         queueName = SimpleString.toSimpleString(StrSubstitutor.replace(queueNameFormat, data));
      }
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
      if (fqqn == null) {
         fqqn = CompositeAddress.toFullyQualified(getAddress(), getQueueName());
      }
      return fqqn;
   }

   @Override
   public RoutingType getRoutingType() {
      return routingType;
   }

   @Override
   public SimpleString getFilterString() {
      return null;
   }

   @Override
   public boolean isTemporary() {
      return temporary;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof FederatedAddressConsumerKey)) return false;
      FederatedAddressConsumerKey that = (FederatedAddressConsumerKey) o;
      return Objects.equals(address, that.address) &&
            Objects.equals(queueNameFormat, that.queueNameFormat) &&
            routingType == that.routingType &&
            Objects.equals(queueFilterString, that.queueFilterString);
   }

   @Override
   public int hashCode() {
      return Objects.hash(address, queueNameFormat, routingType, queueFilterString);
   }
}