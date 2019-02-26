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
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.federation.FederationConnectionConfiguration;
import org.apache.activemq.artemis.core.server.federation.FederationManager;
import org.apache.activemq.artemis.core.server.federation.address.FederatedAddressConfig;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Before;
import org.junit.Test;

/**
 * Consumer Priority Test
 */
public class FederatedAddressTest extends FederatedTestBase {


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
      String address = "federatedAddress";


      FederationConnectionConfiguration federationConnectionConfiguration = new FederationConnectionConfiguration("connectionA");
      federationConnectionConfiguration.setStaticConnectors(Collections.singletonList("server1"));

      FederatedAddressConfig federatedAddressConfig = new FederatedAddressConfig(1, true);
      federatedAddressConfig.getIncludes().add(address);
      federationConnectionConfiguration.setAddressConfig(federatedAddressConfig);

      FederationManager fm = getServer(0).getFederationManager();



      fm.deploy(federationConnectionConfiguration);

      ConnectionFactory cf1 = getCF(1);
      ConnectionFactory cf0 = getCF(0);
      try (Connection connection1 = cf1.createConnection(); Connection connection0 = cf0.createConnection()) {
         connection1.start();
         connection0.start();

         Session session1 = connection1.createSession();
         Topic topic1 = session1.createTopic(address);
         MessageProducer producer = session1.createProducer(topic1);
         producer.send(session1.createTextMessage("hello"));


         Session session0 = connection0.createSession();
         Topic topic0 = session0.createTopic(address);
         MessageConsumer consumer0 = session0.createConsumer(topic0);

         Wait.waitFor(() -> getServer(1).getPostOffice().getBindingsForAddress(SimpleString.toSimpleString(address)).getBindings().size() == 1);

         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer0.receive(10000));


         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer0.receive(10000));

         MessageConsumer consumer1 = session1.createConsumer(topic1);

         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer1.receive(10000));
         assertNotNull(consumer0.receive(10000));
         consumer1.close();

         //Groups
         producer.send(session1.createTextMessage("hello"));
         assertNotNull(consumer0.receive(10000));

         producer.send(createTextMessage(session1, "groupA"));

         assertNotNull(consumer0.receive(10000));
         consumer1 = session1.createConsumer(topic1);

         producer.send(createTextMessage(session1, "groupA"));
         assertNotNull(consumer1.receive(10000));
         assertNotNull(consumer0.receive(10000));

      }

   }







   @Test
   public void testMike2() throws Exception {
      String address = "federatedAddress";



      ConnectionFactory cf1 = getCF(1);
      ConnectionFactory cf0 = getCF(0);
      try (Connection connection1 = cf1.createConnection(); Connection connection0 = cf0.createConnection()) {
         connection1.start();
         connection0.start();

         Session session1 = connection1.createSession();
         Topic topic1 = session1.createTopic(address);
         MessageProducer producer = session1.createProducer(topic1);
         producer.send(session1.createTextMessage("hello"));


         Session session0 = connection0.createSession();
         Topic topic0 = session0.createTopic(address);
         MessageConsumer consumer0 = session0.createConsumer(topic0);


         producer.send(session1.createTextMessage("hello"));

         assertNull(consumer0.receive(100));



         FederationConnectionConfiguration federationConnectionConfiguration = new FederationConnectionConfiguration("connectionA");
         federationConnectionConfiguration.setStaticConnectors(Collections.singletonList("server1"));

         FederatedAddressConfig federatedAddressConfig = new FederatedAddressConfig(1, true);
         federatedAddressConfig.getIncludes().add(address);
         federationConnectionConfiguration.setAddressConfig(federatedAddressConfig);

         FederationManager fm = getServer(0).getFederationManager();



         fm.deploy(federationConnectionConfiguration);

         Wait.waitFor(() -> getServer(1).getPostOffice().getBindingsForAddress(SimpleString.toSimpleString(address)).getBindings().size() == 1);


         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer0.receive(10000));

         MessageConsumer consumer1 = session1.createConsumer(topic1);

         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer1.receive(10000));
         assertNotNull(consumer0.receive(10000));
         consumer1.close();

         //Groups
         producer.send(session1.createTextMessage("hello"));
         assertNotNull(consumer0.receive(10000));

         producer.send(createTextMessage(session1, "groupA"));

         assertNotNull(consumer0.receive(10000));
         consumer1 = session1.createConsumer(topic1);

         producer.send(createTextMessage(session1, "groupA"));
         assertNotNull(consumer1.receive(10000));
         assertNotNull(consumer0.receive(10000));

      }

   }

   private Message createTextMessage(Session session1, String group) throws JMSException {
      Message message = session1.createTextMessage("hello");
      message.setStringProperty("JMSXGroupID", group);
      return message;
   }


}
