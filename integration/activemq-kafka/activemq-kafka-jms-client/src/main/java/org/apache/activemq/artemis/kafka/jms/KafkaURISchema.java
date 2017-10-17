/*
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
package org.apache.activemq.artemis.kafka.jms;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;

import org.apache.activemq.artemis.utils.uri.URIFactory;
import org.apache.activemq.artemis.utils.uri.URISchema;
import org.apache.kafka.clients.CommonClientConfigs;

public class KafkaURISchema extends URISchema<Properties, String> {

   public static final String SCHEMA_NAME = "kafka";
   public static final String URI_PREFIX = SCHEMA_NAME + "://";

   @Override
   public String getSchemaName() {
      return SCHEMA_NAME;
   }

   @Override
   protected Properties internalNewObject(URI uri, Map<String, String> query, String param) throws Exception {

      String connectors = uri.getFragment();

      StringBuilder bootstap = new StringBuilder();
      bootstap.append(uri.getHost());
      if (uri.getPort() != -1) {
         bootstap.append(":").append(uri.getPort());
      }

      if (connectors != null && !connectors.trim().isEmpty()) {
         String[] split = connectors.split(",");
         for (String s : split) {
            URI extraUri = new URI(s);
            bootstap.append(",");
            bootstap.append(uri.getHost());
            if (uri.getPort() != -1) {
               bootstap.append(":").append(uri.getPort());
            }
         }
      }

      Properties properties = new Properties();
      properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstap.toString());
      query.forEach(properties::setProperty);
      if (uri.getUserInfo() != null) {
         if (uri.getUserInfo().contains(":")) {
            String[] userinfo = uri.getUserInfo().split(":");
            properties.setProperty(KafkaConnectionFactory.USERNAME, userinfo[0]);
            properties.setProperty(KafkaConnectionFactory.PASSWORD, userinfo[1]);
         } else {
            properties.setProperty(KafkaConnectionFactory.USERNAME, uri.getUserInfo());
         }
      }
      return properties;
   }

   public static void main(String... args) throws Exception {
      String a = "kafka://michael@bruatkaf001:9092,bruatkaf002:9092,buratkaf004?blah=true&client.id=ddd";
      String b = "kafka://bruatkaf001:9092";

      String prefix = "kafka://";

      KafkaURIFactory k = new KafkaURIFactory();

      Properties ks = k.newObject(k.parse(a), null);
      System.out.println(ks);
   }

   static class KafkaURIFactory extends URIFactory<Properties, String> {

      KafkaURIFactory() {
         this.registerSchema(new KafkaURISchema());
      }


      public URI parse(String connectionString) throws URISyntaxException {
         int idx = connectionString.indexOf("://");
         if (idx == -1) {
            throw new IllegalArgumentException("The connection string is invalid. "
                                               + "Connection strings must start with a [prefix]://");
         }

         String prefix = connectionString.substring(0, idx + 3);

         String unprocessedConnectionString = connectionString.substring(prefix.length());

         // Split out the user and host information
         String userAndHostInformation = null;
         idx = unprocessedConnectionString.lastIndexOf("/");
         if (idx == -1) {
            idx = unprocessedConnectionString.lastIndexOf("?");
         }
         if (idx == -1) {
            userAndHostInformation = unprocessedConnectionString;
            unprocessedConnectionString = "";
         } else {
            userAndHostInformation = unprocessedConnectionString.substring(0, idx);
            unprocessedConnectionString = unprocessedConnectionString.substring(idx);
         }

         // Split the user and host information
         String userInfo = null;
         String hostIdentifier = null;
         idx = userAndHostInformation.lastIndexOf("@");
         if (idx > 0) {
            userInfo = userAndHostInformation.substring(0, idx);
            hostIdentifier = userAndHostInformation.substring(idx + 1);
            int colonCount = countOccurrences(userInfo, ":");
            if (userInfo.contains("@") || colonCount > 1) {
               throw new IllegalArgumentException("The connection string contains invalid user information. "
                                                  +
                                                  "If the username or password contains a colon (:) or an at-sign (@) then it must be urlencoded");
            }
         } else {
            hostIdentifier = userAndHostInformation;
         }

         // Validate the hosts
         String[] hosts = hostIdentifier.split(",");


         StringBuilder sb = new StringBuilder(prefix);
         if (userInfo != null) {
            sb.append(userInfo);
            sb.append("@");
         }
         sb.append(hosts[0]);

         sb.append(unprocessedConnectionString);

         if (hosts.length > 1) {
            sb.append("#");
            for (int i = 1; i < hosts.length; i++) {
               if (i > 1) {
                  sb.append(",");
               }
               sb.append(hosts[i]);
            }
         }

         return new URI(sb.toString());
      }

      private int countOccurrences(final String haystack, final String needle) {
         return haystack.length() - haystack.replace(needle, "").length();
      }
   }

}
