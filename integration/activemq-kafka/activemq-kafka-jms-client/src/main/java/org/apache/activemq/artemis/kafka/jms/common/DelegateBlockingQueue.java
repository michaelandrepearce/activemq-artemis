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
package org.apache.activemq.artemis.kafka.jms.common;

import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class DelegateBlockingQueue<E> implements BlockingQueue<E> {

   public abstract BlockingQueue<E> delegate();

   @Override
   public boolean add(E o) {
      return delegate().add(o);
   }

   @Override
   public boolean offer(E o) {
      return delegate().offer(o);
   }

   @Override
   public void put(E o) throws InterruptedException {
      delegate().put(o);
   }

   @Override
   public boolean offer(E o, long timeout, TimeUnit unit) throws InterruptedException {
      return delegate().offer(o, timeout, unit);
   }

   @Override
   public E take() throws InterruptedException {
      return delegate().take();
   }

   @Override
   public E poll(long timeout, TimeUnit unit) throws InterruptedException {
      return delegate().poll(timeout, unit);
   }

   @Override
   public int remainingCapacity() {
      return delegate().remainingCapacity();
   }

   @Override
   public boolean remove(Object o) {
      return delegate().remove(o);
   }

   @Override
   public boolean contains(Object o) {
      return delegate().contains(o);
   }

   @Override
   public int drainTo(Collection<? super E> c) {
      return delegate().drainTo(c);
   }

   @Override
   public int drainTo(Collection<? super E> c, int maxElements) {
      return delegate().drainTo(c, maxElements);
   }

   @Override
   public E remove() {
      return delegate().remove();
   }

   @Override
   public E poll() {
      return delegate().poll();
   }

   @Override
   public E element() {
      return delegate().element();
   }

   @Override
   public E peek() {
      return delegate().peek();
   }

   @Override
   public int size() {
      return delegate().size();
   }

   @Override
   public boolean isEmpty() {
      return delegate().isEmpty();
   }

   @Override
   public Iterator<E> iterator() {
      return delegate().iterator();
   }

   @Override
   public Object[] toArray() {
      return delegate().toArray();
   }

   @Override
   public Object[] toArray(Object[] a) {
      return delegate().toArray(a);
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      return delegate().containsAll(c);
   }

   @Override
   public boolean addAll(Collection<? extends E> c) {
      return delegate().addAll(c);
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      return delegate().removeAll(c);
   }

   @Override
   public boolean removeIf(Predicate<? super E> filter) {
      return delegate().removeIf(filter);
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      return delegate().retainAll(c);
   }

   @Override
   public void clear() {
      delegate().clear();
   }

   @Override
   public boolean equals(Object o) {
      return delegate().equals(o);
   }

   @Override
   public int hashCode() {
      return delegate().hashCode();
   }

   @Override
   public Spliterator<E> spliterator() {
      return delegate().spliterator();
   }

   @Override
   public Stream<E> stream() {
      return delegate().stream();
   }

   @Override
   public Stream<E> parallelStream() {
      return delegate().parallelStream();
   }

   @Override
   public void forEach(Consumer<? super E> action) {
      delegate().forEach(action);
   }

}
