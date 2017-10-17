/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.provider.amqp.message;

import java.nio.charset.Charset;

import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;

/**
 * This class exists as the methods needing access are package level access.
 *
 * If/when Apache Qpid makes these methods public they can be removed from this class.
 */
public class AmqpJmsMessageFacadeSupport {

   public static Header getHeader(AmqpJmsMessageFacade amqpJmsMessageFacade) {
      return amqpJmsMessageFacade.getHeader();
   }

   public static void setHeader(AmqpJmsMessageFacade amqpJmsMessageFacade, Header header) {
      amqpJmsMessageFacade.setHeader(header);
   }

   public static DeliveryAnnotations getDeliveryAnnotations(AmqpJmsMessageFacade amqpJmsMessageFacade) {
      return amqpJmsMessageFacade.getDeliveryAnnotations();
   }

   public static void setDeliveryAnnotations(AmqpJmsMessageFacade amqpJmsMessageFacade, DeliveryAnnotations deliveryAnnotations) {
      amqpJmsMessageFacade.setDeliveryAnnotations(deliveryAnnotations);
   }

   public static MessageAnnotations getMessageAnnotations(AmqpJmsMessageFacade amqpJmsMessageFacade) {
      return amqpJmsMessageFacade.getMessageAnnotations();
   }

   public static void setMessageAnnotations(AmqpJmsMessageFacade amqpJmsMessageFacade, MessageAnnotations messageAnnotations) {
      amqpJmsMessageFacade.setMessageAnnotations(messageAnnotations);
   }

   public static ApplicationProperties getApplicationProperties(AmqpJmsMessageFacade amqpJmsMessageFacade) {
      return amqpJmsMessageFacade.getApplicationProperties();
   }

   public static void setApplicationProperties(AmqpJmsMessageFacade amqpJmsMessageFacade, ApplicationProperties applicationProperties) {
      amqpJmsMessageFacade.setApplicationProperties(applicationProperties);
   }

   public static Properties getProperties(AmqpJmsMessageFacade amqpJmsMessageFacade) {
      return amqpJmsMessageFacade.getProperties();
   }

   public static void setProperties(AmqpJmsMessageFacade amqpJmsMessageFacade, Properties properties) {
      amqpJmsMessageFacade.setProperties(properties);
   }

   public static Section getBody(AmqpJmsMessageFacade amqpJmsMessageFacade) {
      return amqpJmsMessageFacade.getBody();
   }

   public static void setBody(AmqpJmsMessageFacade amqpJmsMessageFacade, Section section) {
      amqpJmsMessageFacade.setBody(section);
   }

   public static Footer getFooter(AmqpJmsMessageFacade amqpJmsMessageFacade) {
      return amqpJmsMessageFacade.getFooter();
   }

   public static void setFooter(AmqpJmsMessageFacade amqpJmsMessageFacade, Footer footer) {
      amqpJmsMessageFacade.setFooter(footer);
   }

   public static AmqpJmsTextMessageFacade amqpJmsTextMessageFacade(Charset charset) {
      return new AmqpJmsTextMessageFacade(charset);
   }
}
