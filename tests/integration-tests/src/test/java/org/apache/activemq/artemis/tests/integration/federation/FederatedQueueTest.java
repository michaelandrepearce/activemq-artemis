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
package org.apache.activemq.artemis.tests.integration.federation;

import java.util.Collections;
import javax.jms.*;
import org.apache.activemq.artemis.core.server.federation.queue.FederatedQueueConfig;
import org.apache.activemq.artemis.core.server.federation.FederationConnectionConfiguration;
import org.apache.activemq.artemis.core.server.federation.FederationManager;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Consumer Priority Test
 */
public class FederatedQueueTest extends FederatedTestBase {


   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }


   protected ConnectionFactory getCF(int i) throws Exception {
      return new ActiveMQConnectionFactory("vm://" + i);
   }



   @Test
   public void testMike() throws Exception {
      String queueName = "federatedQueue";

      FederationConnectionConfiguration federationConnectionConfiguration = new FederationConnectionConfiguration("nnnn");
      federationConnectionConfiguration.setStaticConnectors(Collections.singletonList("server1"));

      FederatedQueueConfig federatedQueueConfig = new FederatedQueueConfig();
      federatedQueueConfig.getIncludes().add(queueName);

      federationConnectionConfiguration.setQueueConfig(federatedQueueConfig);


      FederationManager fm = getServer(0).getFederationManager();
      fm.deploy(federationConnectionConfiguration);


      ConnectionFactory cf1 = getCF(1);
      Connection connection1 = cf1.createConnection();
      connection1.start();
      Session session1 = connection1.createSession();
      Queue queue1 =  session1.createQueue(queueName);
      MessageProducer producer = session1.createProducer(queue1);
      producer.send(session1.createTextMessage("hello"));

      ConnectionFactory cf0 = getCF(0);
      Connection connection0 = cf0.createConnection();
      connection0.start();
      Session session0 = connection0.createSession();
      Queue queue0 =  session0.createQueue(queueName);
      MessageConsumer consumer0 = session0.createConsumer(queue0);

      assertNotNull(consumer0.receive(10000));


      producer.send(session1.createTextMessage("hello"));

      assertNotNull(consumer0.receive(10000));

      MessageConsumer consumer1 = session1.createConsumer(queue1);

      producer.send(session1.createTextMessage("hello"));

      assertNotNull(consumer1.receive(10000));
      assertNull(consumer0.receive(10));
      consumer1.close();

      //Groups
      producer.send(session1.createTextMessage("hello"));
      assertNotNull(consumer0.receive(10000));

      producer.send(createTextMessage(session1, "groupA"));

      assertNotNull(consumer0.receive(10000));
      consumer1 = session1.createConsumer(queue1);

      producer.send(createTextMessage(session1, "groupA"));
      assertNull(consumer1.receive(10));
      assertNotNull(consumer0.receive(10000));

   }

   private Message createTextMessage(Session session1, String group) throws JMSException {
      Message message = session1.createTextMessage("hello");
      message.setStringProperty("JMSXGroupID", group);
      return message;
   }


}
