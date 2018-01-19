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
package org.apache.activemq.artemis.core.message.impl;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.utils.collections.TypedProperties;

public class CoreMessageObjectPools {

   private Supplier<SimpleString.ByteBufSimpleStringPool> groupIdDecoderPool = Suppliers.memoize(SimpleString.ByteBufSimpleStringPool::new);
   private Supplier<SimpleString.ByteBufSimpleStringPool> addressDecoderPool = Suppliers.memoize(SimpleString.ByteBufSimpleStringPool::new);
   private Supplier<SimpleString.ByteBufSimpleStringPool> validatedUserIDDecoderPool = Suppliers.memoize(SimpleString.ByteBufSimpleStringPool::new);
   private Supplier<SimpleString.ByteBufSimpleStringPool> lastKeyValueDecoderPool = Suppliers.memoize(SimpleString.ByteBufSimpleStringPool::new);
   private Supplier<TypedProperties.TypedPropertiesDecoderPools> propertiesDecoderPools = Suppliers.memoize(TypedProperties.TypedPropertiesDecoderPools::new);

   private Supplier<SimpleString.StringSimpleStringPool> groupIdStringSimpleStringPool = Suppliers.memoize(SimpleString.StringSimpleStringPool::new);
   private Supplier<SimpleString.StringSimpleStringPool> addressStringSimpleStringPool = Suppliers.memoize(SimpleString.StringSimpleStringPool::new);
   private Supplier<SimpleString.StringSimpleStringPool> validatedUserIDStringSimpleStringPool = Suppliers.memoize(SimpleString.StringSimpleStringPool::new);
   private Supplier<TypedProperties.TypedPropertiesStringSimpleStringPools> propertiesStringSimpleStringPools = Suppliers.memoize(TypedProperties.TypedPropertiesStringSimpleStringPools::new);

   public CoreMessageObjectPools() {
   }

   public SimpleString.ByteBufSimpleStringPool getGroupIdDecoderPool() {
      return groupIdDecoderPool.get();
   }

   public SimpleString.ByteBufSimpleStringPool getAddressDecoderPool() {
      return addressDecoderPool.get();
   }

   public SimpleString.ByteBufSimpleStringPool getValidatedUserIDDecoderPool() {
      return validatedUserIDDecoderPool.get();
   }

   public SimpleString.ByteBufSimpleStringPool getLastKeyValueDecoderPool() {
      return lastKeyValueDecoderPool.get();
   }


   public SimpleString.StringSimpleStringPool getAddressStringSimpleStringPool() {
      return addressStringSimpleStringPool.get();
   }

   public SimpleString.StringSimpleStringPool getGroupIdStringSimpleStringPool() {
      return groupIdStringSimpleStringPool.get();
   }

   public SimpleString.StringSimpleStringPool getValidatedUserIDStringSimpleStringPool() {
      return validatedUserIDStringSimpleStringPool.get();
   }

   public TypedProperties.TypedPropertiesDecoderPools getPropertiesDecoderPools() {
      return propertiesDecoderPools.get();
   }

   public TypedProperties.TypedPropertiesStringSimpleStringPools getPropertiesStringSimpleStringPools() {
      return propertiesStringSimpleStringPools.get();
   }
}
