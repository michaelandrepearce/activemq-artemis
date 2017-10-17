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
package org.apache.activemq.artemis.integration.kafka.bridge;

import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.integration.kafka.protocol.amqp.proton.ProtonMessageSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaConstants {

   public static final String TOPIC_NAME = "kafka-topic";
   public static final String QUEUE_NAME = "queue-name";
   public static final String FILTER_STRING = "filter-string";

   public static final String RETRY_INTERVAL_NAME = "retry-interval";
   public static final String RETRY_MAX_INTERVAL_NAME = "retry-max-interval";
   public static final String RETRY_ATTEMPTS_NAME = "retry-attempts";
   public static final String RETRY_MULTIPLIER_NAME = "retry-multiplier";

   public static final String SERIALIZER_STRING_ENCODING = "serializer.encoding";

   public static final Set<String> ALLOWABLE_OUTGOING_CONNECTOR_KEYS;
   public static final Set<String> REQUIRED_OUTGOING_CONNECTOR_KEYS;

   static {
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS = new HashSet<>();
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.addAll(ProducerConfig.configNames());
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(TOPIC_NAME);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(QUEUE_NAME);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(FILTER_STRING);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(RETRY_INTERVAL_NAME);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(RETRY_MAX_INTERVAL_NAME);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(RETRY_ATTEMPTS_NAME);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(RETRY_MULTIPLIER_NAME);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(SERIALIZER_STRING_ENCODING);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(ProtonMessageSerializer.SERIALIZER_BUFFER_SIZE);

      REQUIRED_OUTGOING_CONNECTOR_KEYS = new HashSet<>();
      REQUIRED_OUTGOING_CONNECTOR_KEYS.add(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
      REQUIRED_OUTGOING_CONNECTOR_KEYS.add(TOPIC_NAME);
      REQUIRED_OUTGOING_CONNECTOR_KEYS.add(QUEUE_NAME);

   }
}
