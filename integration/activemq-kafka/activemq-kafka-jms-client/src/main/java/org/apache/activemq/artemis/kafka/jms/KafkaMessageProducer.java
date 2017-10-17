/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.kafka.jms;

import static org.apache.activemq.artemis.kafka.jms.KafkaSession.toJMSMessageID;

import javax.jms.CompletionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.activemq.artemis.kafka.jms.exception.JmsExceptionSupport;
import org.apache.activemq.artemis.kafka.jms.message.KafkaJmsMessageFactory;
import org.apache.activemq.artemis.kafka.jms.util.Preconditions;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMessageProducer implements MessageProducer {
   private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);
   private boolean disableMessageID;
   private boolean disableMessageTimestamp;
   private int deliveryMode = 2;
   private long deliveryDelay = 0;
   private int priority = 4;
   private long timeToLive = 0L;
   private Destination destination;
   private final Producer<String, Message> producer;
   private final KafkaJmsMessageFactory messageFactory;

   KafkaMessageProducer(KafkaSession session, javax.jms.Destination destination) throws JMSException {
      this.destination = Preconditions.ensureKafkaDestination(destination);
      this.messageFactory = session.getMessageFactory();
      this.producer = session.getProducerFactory().createProducer();
   }

   @Override
   public boolean getDisableMessageID() throws JMSException {
      return this.disableMessageID;
   }

   @Override
   public void setDisableMessageID(boolean b) throws JMSException {
      this.disableMessageID = b;
   }

   @Override
   public boolean getDisableMessageTimestamp() throws JMSException {
      return this.disableMessageTimestamp;
   }

   @Override
   public void setDisableMessageTimestamp(boolean b) throws JMSException {
      this.disableMessageTimestamp = b;
   }

   @Override
   public int getDeliveryMode() throws JMSException {
      return this.deliveryMode;
   }

   @Override
   public void setDeliveryMode(int i) throws JMSException {
      this.deliveryMode = i;
   }

   @Override
   public int getPriority() throws JMSException {
      return this.priority;
   }

   @Override
   public void setPriority(int i) throws JMSException {
      this.priority = i;
   }

   @Override
   public long getTimeToLive() throws JMSException {
      return this.timeToLive;
   }

   @Override
   public void setTimeToLive(long timeToLive) throws JMSException {
      if (timeToLive < 0L) {
         throw new JMSException("timeToLive must be greater than 0.");
      } else {
         this.timeToLive = timeToLive;
      }
   }

   @Override
   public javax.jms.Destination getDestination() throws JMSException {
      return this.destination;
   }

   @Override
   public void close() throws JMSException {
      this.producer.close();
   }

   @Override
   public void send(Message message) throws JMSException {
      send(this.destination, message, this.deliveryMode, this.priority, this.timeToLive);
   }

   @Override
   public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
      send(this.destination, message, deliveryMode, priority, timeToLive);
   }

   @Override
   public void send(javax.jms.Destination destination, Message message) throws JMSException {
      send(destination, message, this.deliveryMode, this.priority, this.timeToLive);
   }

   @Override
   public void send(Message message, CompletionListener completionListener) throws JMSException {
      send(destination, message, completionListener);
   }

   @Override
   public void send(javax.jms.Destination destination, Message message, int deliveryMode, int priority, long timeToLive)
      throws JMSException {
      send(destination, message, deliveryMode, priority, timeToLive, null);
   }

   @Override
   public void send(javax.jms.Destination destination, Message message, CompletionListener completionListener) throws JMSException {
      send(destination, message, deliveryMode, priority, timeToLive, completionListener);
   }

   @Override
   public void send(Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener)
      throws JMSException {
      send(destination, message, deliveryMode, priority, timeToLive, completionListener);
   }

   @Override
   public void send(javax.jms.Destination destination, Message message, int deliveryMode, int priority, long timeToLive,
                    CompletionListener completionListener) throws JMSException {
      Destination kafkaDestination = Preconditions.checkDestination(destination);
      Message jmsMessage = Preconditions.checkMessage(message);
      if (timeToLive < 0L) {
         throw new JMSException("timeToLive must be greater than or equal to 0.");
      } else {
         long time = System.currentTimeMillis();
         if (jmsMessage.getJMSTimestamp() <= 0L) {
            jmsMessage.setJMSTimestamp(time);
         }
         jmsMessage.setJMSDeliveryMode(deliveryMode);
         jmsMessage.setJMSDeliveryTime(message.getJMSTimestamp() + deliveryDelay);
         jmsMessage.setJMSPriority(priority);
         if (timeToLive == 0L) {
            jmsMessage.setJMSExpiration(0L);
         } else {
            jmsMessage.setJMSExpiration(message.getJMSTimestamp() + timeToLive);
         }

         jmsMessage.setJMSDestination(kafkaDestination);
         String key = messageFactory.getCompactionProperty(jmsMessage);
         Integer partition = null;
         List<PartitionInfo> partitions = producer.partitionsFor(kafkaDestination.getName());
         String groupId = messageFactory.getGroupID(message);
         if (groupId != null) {
            partition = Utils.toPositive(Utils.murmur2(groupId.getBytes(Charset.forName("UTF-8")))) % partitions.size();
         }

         ProducerRecord producerRecord = new ProducerRecord(kafkaDestination.getName(), partition, jmsMessage.getJMSTimestamp(), key, jmsMessage);

         if (completionListener == null) {
            Future<RecordMetadata> result = this.producer.send(producerRecord);
            try {
               RecordMetadata recordMetadata = result.get();
               TopicPartition topicPartition = new TopicPartition(recordMetadata.topic(), recordMetadata.partition());

               jmsMessage.setJMSMessageID(toJMSMessageID(topicPartition.topic(), topicPartition.partition(), recordMetadata.offset()));
               if (log.isDebugEnabled()) {
                  log.debug("Message accepted to partition {} offset {}", recordMetadata.partition(), recordMetadata.offset());
               }
            } catch (ExecutionException | InterruptedException e) {
               if (log.isErrorEnabled()) {
                  log.error("Exception thrown while sending message.", e);
               }
               throw JmsExceptionSupport.toJMSException(e);
            }
         } else {
            producer.send(producerRecord, (recordMetadata, e) -> {
               if (e == null) {
                  TopicPartition topicPartition = new TopicPartition(recordMetadata.topic(), recordMetadata.partition());

                  try {
                     jmsMessage
                        .setJMSMessageID(toJMSMessageID(topicPartition.topic(), topicPartition.partition(), recordMetadata.offset()));
                  } catch (JMSException e1) {
                  }
                  if (log.isDebugEnabled()) {
                     log.debug("Message accepted to partition {} offset {}", recordMetadata.partition(), recordMetadata.offset());
                  }
                  completionListener.onCompletion(message);
               } else {
                  if (e instanceof ExecutionException || e instanceof InterruptedException) {
                     if (log.isErrorEnabled()) {
                        log.error("Exception thrown while sending message.", e);
                     }
                     e = JmsExceptionSupport.toJMSException(e);
                  }
                  completionListener.onException(message, e);
               }
            });
         }
      }
   }

   @Override
   public long getDeliveryDelay() throws JMSException {
      return deliveryDelay;
   }

   @Override
   public void setDeliveryDelay(long deliveryDelay) throws JMSException {
      this.deliveryDelay = deliveryDelay;
   }


}
