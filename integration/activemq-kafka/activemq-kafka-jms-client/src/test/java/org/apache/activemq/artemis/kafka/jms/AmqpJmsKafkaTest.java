package org.apache.activemq.artemis.kafka.jms;/*
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

import javax.jms.JMSException;
import java.io.IOException;
import java.util.Properties;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import org.apache.activemq.artemis.kafka.jms.protocol.amqp.AmqpJmsKafkaConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;


public class AmqpJmsKafkaTest {

   Properties properties = new Properties();

   {
      properties.put("group.initial.rebalance.delay.ms", 0);
   }

   @Rule
   public KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create(-1, -1, properties)).waitForStartup();

   KafkaConnectionFactory connectionFactory;

   @Before
   public void before() throws IOException, InterruptedException, JMSException {
      kafkaRule.waitForStartup();
      connectionFactory = new AmqpJmsKafkaConnectionFactory("kafka://localhost:" + kafkaRule.helper().kafkaPort());
   }

   @After
   public void after() throws InterruptedException {

   }

}
