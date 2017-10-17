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

import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;
import java.util.Enumeration;

import org.apache.kafka.common.utils.AppInfoParser;

public class KafkaConnectionMetaData implements ConnectionMetaData {

   KafkaConnectionMetaData() {
   }

   @Override
   public String getJMSVersion() throws JMSException {
      return "2.0";
   }

   @Override
   public int getJMSMajorVersion() throws JMSException {
      return 2;
   }

   @Override
   public int getJMSMinorVersion() throws JMSException {
      return 0;
   }

   @Override
   public String getJMSProviderName() throws JMSException {
      return "Apache Kafka";
   }

   @Override
   public String getProviderVersion() throws JMSException {
      return AppInfoParser.getVersion();
   }

   @Override
   public int getProviderMajorVersion() throws JMSException {
      return Integer.valueOf(AppInfoParser.getVersion().split("\\.")[0]);
   }

   @Override
   public int getProviderMinorVersion() throws JMSException {
      return Integer.valueOf(AppInfoParser.getVersion().split("\\.")[1]);
   }

   @Override
   public Enumeration getJMSXPropertyNames() throws JMSException {
      return null;
   }
}
