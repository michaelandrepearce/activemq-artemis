/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.kafka.jms.protocol.amqp;

import javax.jms.JMSException;

import org.apache.activemq.artemis.integration.kafka.protocol.amqp.jms.AmqpJmsMessageDeserializer;
import org.apache.activemq.artemis.integration.kafka.protocol.amqp.jms.AmqpJmsMessageSerializer;
import org.apache.activemq.artemis.kafka.jms.KafkaConnectionFactory;
import org.apache.activemq.artemis.kafka.jms.consumer.DefaultConsumerFactory;
import org.apache.activemq.artemis.kafka.jms.producer.DefaultProducerFactory;

public class AmqpJmsKafkaConnectionFactory extends KafkaConnectionFactory {

   public AmqpJmsKafkaConnectionFactory(String uri) throws JMSException {
      super(uri, new DefaultProducerFactory(new AmqpJmsMessageSerializer()), new DefaultConsumerFactory(new AmqpJmsMessageDeserializer()),
            new AmqpJmsKafkaMessageFactory());
   }
}
