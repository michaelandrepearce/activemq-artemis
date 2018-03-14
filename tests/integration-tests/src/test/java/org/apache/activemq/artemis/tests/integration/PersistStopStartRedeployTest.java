/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PersistStopStartRedeployTest extends ActiveMQTestBase {


   /**
    * Simulates Stop and Start that occurs when network health checker stops the server when network is detected unhealthy
    * and re-starts the broker once detected that it is healthy again.
    *
    * @throws Exception for anything un-expected, test will fail.
    */
   @Test
   public void testRedeployStopAndRestart() throws Exception {
      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");
      URL url1 = PersistStopStartRedeployTest.class.getClassLoader().getResource("reload-original.xml");
      URL url2 = PersistStopStartRedeployTest.class.getClassLoader().getResource("reload-changed.xml");
      Files.copy(url1.openStream(), brokerXML);

      EmbeddedJMS embeddedJMS = new EmbeddedJMS();
      embeddedJMS.setConfigResourcePath(brokerXML.toUri().toString());
      embeddedJMS.start();

      final ReusableLatch latch = new ReusableLatch(1);

      Runnable tick = new Runnable() {
         @Override
         public void run() {
            latch.countDown();
         }
      };

      embeddedJMS.getActiveMQServer().getReloadManager().setTick(tick);

      try {
         latch.await(10, TimeUnit.SECONDS);

         Assert.assertEquals(getSecurityRoles(embeddedJMS, "security_address").size(), 1);
         Assert.assertEquals(getSecurityRoles(embeddedJMS, "security_address").iterator().next().getName(), "b");

         Assert.assertEquals(getAddressSettings(embeddedJMS, "address_settings_address").getDeadLetterAddress(), SimpleString.toSimpleString("OriginalDLQ"));
         Assert.assertEquals(getAddressSettings(embeddedJMS, "address_settings_address").getExpiryAddress(), SimpleString.toSimpleString("OriginalExpiryQueue"));

         Assert.assertNotNull(getAddressInfo(embeddedJMS, "config_test_address_removal_no_queue"));
         Assert.assertNotNull(getAddressInfo(embeddedJMS, "config_test_address_removal"));
         Assert.assertNotNull(getAddressInfo(embeddedJMS, "config_test_queue_removal"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedJMS, "config_test_queue_removal").contains("config_test_queue_removal_queue_1"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedJMS, "config_test_queue_removal").contains("config_test_queue_removal_queue_2"));

         Assert.assertNotNull(getAddressInfo(embeddedJMS, "permanent_test_address_removal"));
         Assert.assertNotNull(getAddressInfo(embeddedJMS, "permanent_test_queue_removal"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedJMS, "permanent_test_queue_removal").contains("permanent_test_queue_removal_queue_1"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedJMS, "permanent_test_queue_removal").contains("permanent_test_queue_removal_queue_2"));

         Assert.assertNotNull(getAddressInfo(embeddedJMS, "config_test_queue_change"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedJMS, "config_test_queue_change").contains("config_test_queue_change_queue"));
         Assert.assertEquals(10, getQueue(embeddedJMS, "config_test_queue_change_queue").getMaxConsumers());
         Assert.assertEquals(false, getQueue(embeddedJMS, "config_test_queue_change_queue").isPurgeOnNoConsumers());

         Files.copy(url2.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);
         brokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);
         latch.setCount(1);
         embeddedJMS.getActiveMQServer().getReloadManager().setTick(tick);
         latch.await(10, TimeUnit.SECONDS);

         //Assert that the security settings change applied
         Assert.assertEquals(getSecurityRoles(embeddedJMS, "security_address").size(), 1);
         Assert.assertEquals(getSecurityRoles(embeddedJMS, "security_address").iterator().next().getName(), "c");

         //Assert that the address settings change applied
         Assert.assertEquals(getAddressSettings(embeddedJMS, "address_settings_address").getDeadLetterAddress(), SimpleString.toSimpleString("NewDLQ"));
         Assert.assertEquals(getAddressSettings(embeddedJMS, "address_settings_address").getExpiryAddress(), SimpleString.toSimpleString("NewExpiryQueue"));

         //Assert the address and queue changes applied
         Assert.assertNull(getAddressInfo(embeddedJMS, "config_test_address_removal_no_queue"));
         Assert.assertNull(getAddressInfo(embeddedJMS, "config_test_address_removal"));
         Assert.assertNotNull(getAddressInfo(embeddedJMS, "config_test_queue_removal"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedJMS, "config_test_queue_removal").contains("config_test_queue_removal_queue_1"));
         Assert.assertFalse(listQueuesNamesForAddress(embeddedJMS, "config_test_queue_removal").contains("config_test_queue_removal_queue_2"));

         Assert.assertNotNull(getAddressInfo(embeddedJMS, "permanent_test_address_removal"));
         Assert.assertNotNull(getAddressInfo(embeddedJMS, "permanent_test_queue_removal"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedJMS, "permanent_test_queue_removal").contains("permanent_test_queue_removal_queue_1"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedJMS, "permanent_test_queue_removal").contains("permanent_test_queue_removal_queue_2"));

         Assert.assertNotNull(getAddressInfo(embeddedJMS, "config_test_queue_change"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedJMS, "config_test_queue_change").contains("config_test_queue_change_queue"));
         Assert.assertEquals(1, getQueue(embeddedJMS, "config_test_queue_change_queue").getMaxConsumers());
         Assert.assertEquals(true, getQueue(embeddedJMS, "config_test_queue_change_queue").isPurgeOnNoConsumers());
      } finally {
         embeddedJMS.stop();
      }


      try {
         embeddedJMS.start();

         //Assert that the security settings changes persist a stop and start server (e.g. like what occurs if network health check stops the node), but JVM remains up.
         Assert.assertEquals(getSecurityRoles(embeddedJMS, "security_address").size(), 1);
         Assert.assertEquals(getSecurityRoles(embeddedJMS, "security_address").iterator().next().getName(), "c");

         //Assert that the address settings changes persist a stop and start server (e.g. like what occurs if network health check stops the node), but JVM remains up.
         Assert.assertEquals(getAddressSettings(embeddedJMS, "address_settings_address").getDeadLetterAddress(), SimpleString.toSimpleString("NewDLQ"));
         Assert.assertEquals(getAddressSettings(embeddedJMS, "address_settings_address").getExpiryAddress(), SimpleString.toSimpleString("NewExpiryQueue"));

         //Assert that the address and queue changes persist a stop and start server (e.g. like what occurs if network health check stops the node), but JVM remains up.
         Assert.assertNull(getAddressInfo(embeddedJMS, "config_test_address_removal_no_queue"));
         Assert.assertNull(getAddressInfo(embeddedJMS, "config_test_address_removal"));
         Assert.assertNotNull(getAddressInfo(embeddedJMS, "config_test_queue_removal"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedJMS, "config_test_queue_removal").contains("config_test_queue_removal_queue_1"));
         Assert.assertFalse(listQueuesNamesForAddress(embeddedJMS, "config_test_queue_removal").contains("config_test_queue_removal_queue_2"));

         Assert.assertNotNull(getAddressInfo(embeddedJMS, "permanent_test_address_removal"));
         Assert.assertNotNull(getAddressInfo(embeddedJMS, "permanent_test_queue_removal"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedJMS, "permanent_test_queue_removal").contains("permanent_test_queue_removal_queue_1"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedJMS, "permanent_test_queue_removal").contains("permanent_test_queue_removal_queue_2"));

         Assert.assertNotNull(getAddressInfo(embeddedJMS, "config_test_queue_change"));
         Assert.assertTrue(listQueuesNamesForAddress(embeddedJMS, "config_test_queue_change").contains("config_test_queue_change_queue"));
         Assert.assertEquals(1, getQueue(embeddedJMS, "config_test_queue_change_queue").getMaxConsumers());
         Assert.assertEquals(true, getQueue(embeddedJMS, "config_test_queue_change_queue").isPurgeOnNoConsumers());

      } finally {
         embeddedJMS.stop();
      }
   }

   private AddressInfo getAddressInfo(EmbeddedJMS embeddedJMS, String address) {
      return embeddedJMS.getActiveMQServer().getPostOffice().getAddressInfo(SimpleString.toSimpleString(address));
   }

   private AddressSettings getAddressSettings(EmbeddedJMS embeddedJMS, String address) {
      return embeddedJMS.getActiveMQServer().getAddressSettingsRepository().getMatch(address);
   }

   private Set<Role> getSecurityRoles(EmbeddedJMS embeddedJMS, String address) {
      return embeddedJMS.getActiveMQServer().getSecurityRepository().getMatch(address);
   }

   private org.apache.activemq.artemis.core.server.Queue getQueue(EmbeddedJMS embeddedJMS, String queueName) throws Exception {
      QueueBinding queueBinding = (QueueBinding) embeddedJMS.getActiveMQServer().getPostOffice().getBinding(SimpleString.toSimpleString(queueName));
      return queueBinding == null ? null : queueBinding.getQueue();
   }

   private List<String> listQueuesNamesForAddress(EmbeddedJMS embeddedJMS, String address) throws Exception {
      return embeddedJMS.getActiveMQServer().getPostOffice().listQueuesForAddress(SimpleString.toSimpleString(address)).stream().map(
          org.apache.activemq.artemis.core.server.Queue::getName).map(SimpleString::toString).collect(Collectors.toList());
   }

}
