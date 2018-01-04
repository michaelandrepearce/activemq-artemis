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

package org.apache.activemq.artemis.tests.compatibility;

import org.apache.activemq.artemis.utils.FileUtil;
import org.junit.After;
import org.junit.Before;

public class ServerBaseTest extends VersionedBaseTest {

   public ServerBaseTest(String server, String sender, String receiver) throws Exception {
      super(server, sender, receiver);
   }

   @Before
   public void setUp() throws Throwable {
      FileUtil.deleteDirectory(serverFolder.getRoot());
      setVariable(serverClassloader, "persistent", Boolean.FALSE);
      startServer(serverFolder.getRoot(), serverClassloader, "live");
   }

   @After
   public void tearDown() throws Throwable {
      stopServer(serverClassloader);
   }
}