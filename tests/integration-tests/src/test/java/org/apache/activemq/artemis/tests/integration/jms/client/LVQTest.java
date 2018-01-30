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
package org.apache.activemq.artemis.tests.integration.jms.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Before;
import org.junit.Test;

/**
 * LVQ Test
 */
public class LVQTest extends JMSTestBase {

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }


   protected ConnectionFactory getCF() throws Exception {
      return cf;
   }

   @Test
   public void testLastValueQueueUsingAddressQueueParameters() throws Exception {
      ConnectionFactory fact = getCF();
      Connection connection = fact.createConnection();

      try {

         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         Queue queue = session.createQueue("random?last-value=true");
         assertEquals("random", queue.getQueueName());

         ActiveMQDestination a = (ActiveMQDestination) queue;
         assertTrue(a.getQueueParameters().getLastValue());
         
         MessageProducer producer = session.createProducer(queue);
         

         for (int j = 0; j < 100; j++) {
            TextMessage message = session.createTextMessage();

            message.setText("Message" + j);
            message.setStringProperty(Message.HDR_LAST_VALUE_NAME.toString(), "key");
            producer.send(message);
         }
         
         MessageConsumer consumer1 = session.createConsumer(queue);

         connection.start();

         //All msgs should go to the first consumer
         TextMessage tm = (TextMessage) consumer1.receive(10000);

         assertNotNull(tm);

         assertEquals("Message99", tm.getText());
         
      } finally {
         connection.close();
      }
   }

}
