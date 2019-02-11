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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class BucketMap<K, V> implements Map<K, V> {

    private final Map<Integer, V> buckets = new IntHashMap<>();
    private final int bucketCount;

    public BucketMap(int bucketCount) {
        this.bucketCount = bucketCount;
    }

    @Override
    public int size() {
        return buckets.size();
    }

    @Override
    public boolean isEmpty() {
        return buckets.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return buckets.containsKey(getBucket(key));
    }

    private int getBucket(Object key) {
        return (key.hashCode() & Integer.MAX_VALUE) % bucketCount;
    }

    @Override
    public boolean containsValue(Object value) {
        return buckets.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return buckets.get(getBucket(key));
    }

    @Override
    public V put(K key, V value) {
        return buckets.put(getBucket(key), value);
    }

    @Override
    public V remove(Object key) {
        return buckets.remove(getBucket(key));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        m.forEach(this::put);
    }

    @Override
    public void clear() {
        buckets.clear();
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<V> values() {
        return buckets.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        return buckets.equals(o);
    }

    @Override
    public int hashCode() {
        return buckets.hashCode();
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        return buckets.getOrDefault(getBucket(key), defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return buckets.putIfAbsent(getBucket(key), value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return buckets.remove(getBucket(key), value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return buckets.replace(getBucket(key), oldValue, newValue);
    }

    @Override
    public V replace(K key, V value) {
        return buckets.replace(getBucket(key), value);
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        return buckets.computeIfAbsent(getBucket(key), (k) -> mappingFunction.apply(key));
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return buckets.computeIfPresent(getBucket(key), (k, v) -> remappingFunction.apply(key, v));
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return buckets.compute(getBucket(key), (k, v) -> remappingFunction.apply(key, v));
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return buckets.merge(getBucket(key), value, remappingFunction);
    }
}
