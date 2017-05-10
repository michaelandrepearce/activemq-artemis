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
package org.apache.activemq.artemis.utils;

import org.jctools.maps.NonBlockingHashMapLong;
import org.jctools.maps.NonBlockingHashMapLong.IteratorLong;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;

/**
 * A simple wrapper around {@link NonBlockingHashMapLong} making it implement the
 * {@link java.util.Set} interface.  All operations are Non-Blocking and multi-thread safe.
 */
public class NonBlockingHashSetLong extends AbstractSet<Long> implements Serializable {

   private final NonBlockingHashMapLong<Boolean> _map;

   /**
    * Make a new empty {@link NonBlockingHashSetLong}.
    */
   public NonBlockingHashSetLong() {
      super();
      _map = new NonBlockingHashMapLong<>();
   }

   @Override
   public boolean addAll(Collection<? extends Long> c) {
      if (!NonBlockingHashSetLong.class.equals(c.getClass())) {
         return super.addAll(c);
      }
      boolean modified = false;
      for (final NonBlockingHashMapLong.IteratorLong it = ((NonBlockingHashSetLong) c).longIterator(); it.hasNext(); ) {
         modified |= add(it.nextLong());
      }
      return modified;
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      if (!NonBlockingHashSetLong.class.equals(c.getClass())) {
         return super.removeAll(c);
      }
      boolean modified = false;
      for (final IteratorLong it = ((NonBlockingHashSetLong) c).longIterator(); it.hasNext(); ) {
         modified |= remove(it.nextLong());
      }
      return modified;
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      if (!NonBlockingHashSetLong.class.equals(c.getClass())) {
         return super.containsAll(c);
      }
      for (final IteratorLong it = ((NonBlockingHashSetLong) c).longIterator(); it.hasNext(); ) {
         if (!contains(it.nextLong())) {
            return false;
         }
      }
      return true;
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      if (!NonBlockingHashSetLong.class.equals(c.getClass())) {
         return super.retainAll(c);
      }
      boolean modified = false;
      final NonBlockingHashSetLong nonBlockingHashSetLong = (NonBlockingHashSetLong) c;
      for (final IteratorLong it = longIterator(); it.hasNext(); ) {
         if (!nonBlockingHashSetLong.contains(it.nextLong())) {
            it.remove();
            modified = true;
         }
      }
      return modified;
   }

   @Override
   public int hashCode() {
      int hashCode = 0;
      for (final IteratorLong it = longIterator(); it.hasNext(); ) {
         final long value = it.nextLong();
         hashCode += (int) (value ^ (value >>> 32));
      }
      return hashCode;
   }

   /**
    * Add {@code o} to the set.
    *
    * @return <tt>true</tt> if {@code o} was added to the set, <tt>false</tt>
    * if {@code o} was already in the set.
    */
   public boolean add(final long o) {
      return _map.putIfAbsent(o, Boolean.TRUE) != Boolean.TRUE;
   }

   /**
    * To support AbstractCollection.addAll
    */
   @Override
   public boolean add(final Long o) {
      return _map.putIfAbsent(o.longValue(), Boolean.TRUE) != Boolean.TRUE;
   }

   /**
    * @return <tt>true</tt> if {@code o} is in the set.
    */
   public boolean contains(final long o) {
      return _map.containsKey(o);
   }

   @Override
   public boolean contains(Object o) {
      return o instanceof Long && contains(((Long) o).longValue());
   }

   /**
    * Remove {@code o} from the set.
    *
    * @return <tt>true</tt> if {@code o} was removed to the set, <tt>false</tt>
    * if {@code o} was not in the set.
    */
   public boolean remove(final long o) {
      return _map.remove(o) == Boolean.TRUE;
   }

   @Override
   public boolean remove(final Object o) {
      return o instanceof Long && remove(((Long) o).longValue());
   }

   /**
    * Current count of elements in the set.  Due to concurrent racing updates,
    * the size is only ever approximate.  Updates due to the calling thread are
    * immediately visible to calling thread.
    *
    * @return count of elements.
    */
   @Override
   public int size() {
      return _map.size();
   }

   /**
    * Empty the set.
    */
   @Override
   public void clear() {
      _map.clear();
   }

   @Override
   public String toString() {
      final IteratorLong it = longIterator();
      if (!it.hasNext()) {
         return "[]";
      }
      final StringBuilder sb = new StringBuilder().append('[');
      for (; ; ) {
         sb.append(it.next());
         if (!it.hasNext()) {
            return sb.append(']').toString();
         }
         sb.append(", ");
      }
   }

   @Override
   public Iterator<Long> iterator() {
      return _map.keySet().iterator();
   }

   public IteratorLong longIterator() {
      return (IteratorLong) _map.keySet().iterator();
   }
}