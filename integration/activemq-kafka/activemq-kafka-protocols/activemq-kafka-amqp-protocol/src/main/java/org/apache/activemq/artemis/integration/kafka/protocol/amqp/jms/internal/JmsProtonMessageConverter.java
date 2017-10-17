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
package org.apache.activemq.artemis.integration.kafka.protocol.amqp.jms.internal;

import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.getBody;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.getDeliveryAnnotations;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.getFooter;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.getHeader;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.getMessageAnnotations;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.getProperties;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.setApplicationProperties;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.setBody;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.setDeliveryAnnotations;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.setFooter;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.setHeader;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.setMessageAnnotations;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.setProperties;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_BYTES_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MAP_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_OBJECT_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_STREAM_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_TEXT_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.isContentType;

import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacadeSupport.getApplicationProperties;

import javax.jms.JMSException;
import java.nio.charset.Charset;

import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacade;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.util.ContentTypeSupport;
import org.apache.qpid.jms.util.InvalidContentTypeException;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

/**
 * AMQP Codec class used to hide the details of encode / decode
 */
public final class JmsProtonMessageConverter {

   private static JmsMessageFactory factory = new JmsMessageFactory();


   /**
    * Given a Message instance, encode the Message to the wire level representation
    * of that Message.
    *
    * @param message
    *     the Message that is converted to AMQP proton equivalent
    *
    * @return a buffer containing the wire level representation of the input Message.
    */
   public static Message toProtonMessage(javax.jms.Message message) throws JMSException {

      JmsMessage jmsMessage;
      if (JmsMessage.class.isAssignableFrom(message.getClass())) {
         jmsMessage = (JmsMessage) message;
      } else {
         jmsMessage = JmsMessageTransformation.transformMessage(message);
      }
      if (jmsMessage instanceof JmsBytesMessage) {
         ((JmsBytesMessage) jmsMessage).reset();
      }

      JmsMessageFacade jmsMessageFacade = jmsMessage.getFacade();
      if (AmqpJmsMessageFacade.class.isAssignableFrom(jmsMessageFacade.getClass())) {
         AmqpJmsMessageFacade amqpJmsMessageFacade = (AmqpJmsMessageFacade) jmsMessageFacade;
         Message protonMessage = Message.Factory.create();
         protonMessage.setHeader(getHeader(amqpJmsMessageFacade));
         protonMessage.setDeliveryAnnotations(getDeliveryAnnotations(amqpJmsMessageFacade));
         protonMessage.setMessageAnnotations(getMessageAnnotations(amqpJmsMessageFacade));
         protonMessage.setProperties(getProperties(amqpJmsMessageFacade));
         protonMessage.setApplicationProperties(getApplicationProperties(amqpJmsMessageFacade));
         protonMessage.setBody(getBody(amqpJmsMessageFacade));
         protonMessage.setFooter(getFooter(amqpJmsMessageFacade));
         if (amqpJmsMessageFacade.getDestination() != null)
            protonMessage.setAddress(amqpJmsMessageFacade.getDestination().getAddress());
         return protonMessage;
      }
      throw new JMSException("Could not create a JMS message from incoming message");
   }

   /**
    * Create a new JmsMessage and underlying JmsMessageFacade that represents the proper
    * message type for the incoming AMQP proton message.
    *
    * @param protonMessage
    *     the Message that is converted to JMS equivalent
    *
    * @return a JmsMessage instance decoded from the message bytes.
    *
    * @throws JMSException if an error occurs while creating the message objects.
    */
   public static javax.jms.Message toJmsMessage(Message protonMessage) throws JMSException {

      // First we try the easy way, if the annotation is there we don't have to work hard.
      JmsMessage jmsMessage = createFromMsgAnnotation(protonMessage.getMessageAnnotations());
      if (jmsMessage == null) {
         // Next, match specific section structures and content types
         jmsMessage = createWithoutAnnotation(protonMessage.getBody(), protonMessage.getProperties());
      }

      if (jmsMessage != null) {
         AmqpJmsMessageFacade result = (AmqpJmsMessageFacade) jmsMessage.getFacade();
         setHeader(result, protonMessage.getHeader());
         setDeliveryAnnotations(result, protonMessage.getDeliveryAnnotations());
         setMessageAnnotations(result, protonMessage.getMessageAnnotations());
         setProperties(result, protonMessage.getProperties());
         setApplicationProperties(result, protonMessage.getApplicationProperties());
         setBody(result, protonMessage.getBody());
         setFooter(result, protonMessage.getFooter());
         result.setDestination(new JmsTopic(protonMessage.getAddress()));
         return result.asJmsMessage();
      }
      throw new JMSException("Could not create a JMS message from incoming message");
   }

   private static JmsMessage createFromMsgAnnotation(MessageAnnotations messageAnnotations) throws JMSException {
      Object annotation = AmqpMessageSupport.getMessageAnnotation(JMS_MSG_TYPE, messageAnnotations);
      if (annotation != null) {
         switch ((byte) annotation) {
            case JMS_MESSAGE:
               return factory.createMessage();
            case JMS_BYTES_MESSAGE:
               return factory.createBytesMessage();
            case JMS_TEXT_MESSAGE:
               return factory.createTextMessage();
            case JMS_MAP_MESSAGE:
               return factory.createMapMessage();
            case JMS_STREAM_MESSAGE:
               return factory.createStreamMessage();
            case JMS_OBJECT_MESSAGE:
               return factory.createObjectMessage();
            default:
               throw new JMSException("Invalid JMS Message Type annotation value found in message: " + annotation);
         }
      }
      return null;
   }

   private static JmsMessage createWithoutAnnotation(Section body, Properties properties) throws JMSException {
      Symbol messageContentType = properties != null ? properties.getContentType() : null;

      if (body == null) {
         if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, messageContentType)) {
         } else if (isContentType(OCTET_STREAM_CONTENT_TYPE, messageContentType) || isContentType(null, messageContentType)) {
            return factory.createBytesMessage();
         } else {
            Charset charset = getCharsetForTextualContent(messageContentType);
            if (charset != null) {
               return factory.createTextMessage(charset);
            } else {
               return factory.createMessage();
            }
         }
      } else if (body instanceof Data) {
         if (isContentType(OCTET_STREAM_CONTENT_TYPE, messageContentType) || isContentType(null, messageContentType)) {
            return factory.createBytesMessage();
         } else if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, messageContentType)) {
            return factory.createObjectMessage();
         } else {
            Charset charset = getCharsetForTextualContent(messageContentType);
            if (charset != null) {
               return factory.createTextMessage(charset);
            } else {
               return factory.createBytesMessage();
            }
         }
      } else if (body instanceof AmqpValue) {
         Object value = ((AmqpValue) body).getValue();

         if (value == null || value instanceof String) {
            return factory.createTextMessage();
         } else if (value instanceof Binary) {
            return factory.createBytesMessage();
         } else {
            return factory.createObjectMessage();
         }
      } else if (body instanceof AmqpSequence) {
         return factory.createObjectMessage();
      }
      return null;
   }

   private static Charset getCharsetForTextualContent(Symbol messageContentType) {
      if (messageContentType != null) {
         try {
            return ContentTypeSupport.parseContentTypeForTextualCharset(messageContentType.toString());
         } catch (InvalidContentTypeException e) {
         }
      }
      return null;
   }
}
