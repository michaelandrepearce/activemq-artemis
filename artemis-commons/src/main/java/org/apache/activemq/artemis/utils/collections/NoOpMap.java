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
package org.apache.activemq.artemis.utils.collections;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class NoOpMap<K,V> extends AbstractMap<K,V> {

   public V put(K key, V value) {
      return null;
   }

   public int size() {
      return 0;
   }

   public boolean isEmpty() {
      return true;
   }

   public boolean containsKey(Object key) {
      return false;
   }

   public boolean containsValue(Object value) {
      return false;
   }

   public V get(Object key) {
      return null;
   }

   public Set<K> keySet() {
      return Collections.emptySet();
   }

   public Collection<V> values() {
      return Collections.emptySet();
   }

   public Set<Entry<K,V>> entrySet() {
      return Collections.emptySet();
   }

   public boolean equals(Object o) {
      return (o instanceof Map) && ((Map)o).size() == 0;
   }

   public int hashCode() {
      return 0;
   }
}