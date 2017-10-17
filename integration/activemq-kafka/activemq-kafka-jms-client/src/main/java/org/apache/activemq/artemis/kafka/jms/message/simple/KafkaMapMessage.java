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
package org.apache.activemq.artemis.kafka.jms.message.simple;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class KafkaMapMessage extends AbstractKafkaMessage<Map<String, Serializable>> implements MapMessage {

   @Override
   public boolean getBoolean(String name) throws JMSException {
      return get(name, Boolean.class);
   }

   @Override
   public byte getByte(String name) throws JMSException {
      return get(name, Byte.class);
   }

   @Override
   public short getShort(String name) throws JMSException {
      return get(name, Short.class);
   }

   @Override
   public char getChar(String name) throws JMSException {
      return get(name, Character.class);
   }

   @Override
   public int getInt(String name) throws JMSException {
      return get(name, Integer.class);
   }

   @Override
   public long getLong(String name) throws JMSException {
      return get(name, Long.class);
   }

   @Override
   public float getFloat(String name) throws JMSException {
      return get(name, Float.class);
   }

   @Override
   public double getDouble(String name) throws JMSException {
      return get(name, Double.class);
   }

   @Override
   public String getString(String name) throws JMSException {
      return get(name, String.class);
   }

   @Override
   public byte[] getBytes(String name) throws JMSException {
      return get(name, byte[].class);
   }

   @Override
   public Object getObject(String name) throws JMSException {
      return get(name, Object.class);
   }

   @Override
   public Enumeration getMapNames() throws JMSException {
      return Collections.enumeration(body.keySet());
   }

   @Override
   public void setBoolean(String name, boolean b) throws JMSException {
      set(name, b);
   }

   @Override
   public void setByte(String name, byte b) throws JMSException {
      set(name, b);
   }

   @Override
   public void setShort(String name, short i) throws JMSException {
      set(name, i);
   }

   @Override
   public void setChar(String name, char c) throws JMSException {
      set(name, c);
   }

   @Override
   public void setInt(String name, int i) throws JMSException {
      set(name, i);
   }

   @Override
   public void setLong(String name, long l) throws JMSException {
      set(name, l);
   }

   @Override
   public void setFloat(String name, float v) throws JMSException {
      set(name, v);

   }

   @Override
   public void setDouble(String name, double v) throws JMSException {
      set(name, v);

   }

   @Override
   public void setString(String name, String s) throws JMSException {
      set(name, s);
   }

   @Override
   public void setBytes(String name, byte[] value) throws JMSException {
      setBytes(name, value, 0, value.length);
   }

   @Override
   public void setBytes(String name, byte[] value, int offset, int length) throws JMSException {
      byte[] copy = new byte[length];
      System.arraycopy(value, offset, copy, 0, length);
      body.put(name, copy);
   }

   @Override
   public void setObject(String name, Object o) throws JMSException {
      set(name, o);
   }

   void set(String name, Object value) throws JMSException {
      if (name == null) {
         throw new IllegalArgumentException("The name of the property cannot be null.");
      }
      if (name.length() == 0) {
         throw new IllegalArgumentException("The name of the property cannot be an empty string.");
      }
      if (body == null) {
         body = new HashMap<>();
      }
      if (value == null) {
         body.put(name, null);
      } else if (value instanceof String) {
         body.put(name, (String) value);
      } else if (value instanceof Character) {
         body.put(name, (Character) value);
      } else if (value instanceof Boolean) {
         body.put(name, (Boolean) value);
      } else if (value instanceof Byte) {
         body.put(name, (Byte) value);
      } else if (value instanceof Short) {
         body.put(name, (Short) value);
      } else if (value instanceof Integer) {
         body.put(name, (Integer) value);
      } else if (value instanceof Long) {
         body.put(name, (Long) value);
      } else if (value instanceof Float) {
         body.put(name, (Float) value);
      } else if (value instanceof Double) {
         body.put(name, (Double) value);
      } else if (value instanceof byte[]) {
         setBytes(name, (byte[]) value);
      } else {
         throw new MessageFormatException("Unsupported Object type: " + value.getClass().getSimpleName());
      }
   }

   @Override
   public boolean itemExists(String name) throws JMSException {
      return body.containsKey(name);
   }

   <T> T get(String name, Class<T> clazz) throws JMSException {
      if (name == null) {
         throw new IllegalArgumentException("The name of the property cannot be null.");
      }
      if (name.length() == 0) {
         throw new IllegalArgumentException("The name of the property cannot be an empty string.");
      }
      Object result;
      T typedResult;
      try {
         Object value = body.get(name);
         if (value == null) {
            result = null;
         } else if (value instanceof String) {
            result = value;
         } else if (value instanceof Float) {
            result = value;
         } else if (value instanceof Double) {
            result = value;
         } else if (value instanceof Long) {
            result = value;
         } else if (value instanceof Integer) {
            result = value;
         } else if (value instanceof Short) {
            result = value;
         } else if (value instanceof Byte) {
            result = value;
         } else if (value instanceof Boolean) {
            result = value;
         } else if (value instanceof Character) {
            result = value;
         } else if (value instanceof byte[]) {
            byte[] original = (byte[]) value;
            result = new byte[original.length];
            System.arraycopy(original, 0, result, 0, original.length);
         } else {
            throw new MessageFormatException("Unknown type found in stream");
         }

         typedResult = clazz.cast(result);
      } catch (ClassCastException cce) {
         throw new MessageFormatException(cce.getMessage());
      }
      return typedResult;

   }

   @Override
   public void clearBody() throws JMSException {
      super.clearBody();
   }
}
