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
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.StreamMessage;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class KafkaStreamMessage extends AbstractKafkaMessage<List<Serializable>> implements StreamMessage {

   private int index = 0;
   private byte[] bytes;
   private int remainingBytes = -1;

   @Override
   public boolean readBoolean() throws JMSException {
      return read(Boolean.class);
   }

   @Override
   public byte readByte() throws JMSException {
      return read(Byte.class);
   }

   @Override
   public short readShort() throws JMSException {
      return read(Short.class);
   }

   @Override
   public char readChar() throws JMSException {
      return read(Character.class);
   }

   @Override
   public int readInt() throws JMSException {
      return read(Integer.class);
   }

   @Override
   public long readLong() throws JMSException {
      return read(Long.class);
   }

   @Override
   public float readFloat() throws JMSException {
      return read(Float.class);
   }

   @Override
   public double readDouble() throws JMSException {
      return read(Double.class);
   }

   @Override
   public String readString() throws JMSException {
      return read(String.class);
   }

   @Override
   public int readBytes(byte[] target) throws JMSException {
      if (target == null) {
         throw new NullPointerException("target byte array was null");
      } else {
         if (this.remainingBytes == -1) {
            this.bytes = read(byte[].class);
            this.remainingBytes = this.bytes.length;
         } else if (this.remainingBytes == 0) {
            this.remainingBytes = -1;
            this.bytes = null;
            return -1;
         }

         int previouslyRead1 = this.bytes.length - this.remainingBytes;
         int lengthToCopy = Math.min(target.length, this.remainingBytes);
         if (lengthToCopy > 0) {
            System.arraycopy(this.bytes, previouslyRead1, target, 0, lengthToCopy);
         }

         this.remainingBytes -= lengthToCopy;
         if (this.remainingBytes == 0 && lengthToCopy < target.length) {
            this.remainingBytes = -1;
            this.bytes = null;
         }
         return lengthToCopy;
      }
   }

   @Override
   public Object readObject() throws JMSException {
      return read(Object.class);
   }

   @Override
   public void writeBoolean(boolean b) throws JMSException {
      write(b);
   }

   @Override
   public void writeByte(byte b) throws JMSException {
      write(b);
   }

   @Override
   public void writeShort(short i) throws JMSException {
      write(i);
   }

   @Override
   public void writeChar(char c) throws JMSException {
      write(c);
   }

   @Override
   public void writeInt(int i) throws JMSException {
      write(i);
   }

   @Override
   public void writeLong(long l) throws JMSException {
      write(l);
   }

   @Override
   public void writeFloat(float v) throws JMSException {
      write(v);

   }

   @Override
   public void writeDouble(double v) throws JMSException {
      write(v);

   }

   @Override
   public void writeString(String s) throws JMSException {
      write(s);
   }

   @Override
   public void writeBytes(byte[] value) throws JMSException {
      writeBytes(value, 0, value.length);
   }

   @Override
   public void writeBytes(byte[] value, int offset, int length) throws JMSException {
      byte[] copy = new byte[length];
      System.arraycopy(value, offset, copy, 0, length);
      body.add(copy);
   }

   @Override
   public void writeObject(Object o) throws JMSException {
      write(o);
   }

   void write(Object value) throws JMSException {
      if (body == null) {
         body = new ArrayList<>();
      }
      if (value == null) {
         body.add(null);
      } else if (value instanceof String) {
         body.add((String) value);
      } else if (value instanceof Character) {
         body.add((Character) value);
      } else if (value instanceof Boolean) {
         body.add((Boolean) value);
      } else if (value instanceof Byte) {
         body.add((Byte) value);
      } else if (value instanceof Short) {
         body.add((Short) value);
      } else if (value instanceof Integer) {
         body.add((Integer) value);
      } else if (value instanceof Long) {
         body.add((Long) value);
      } else if (value instanceof Float) {
         body.add((Float) value);
      } else if (value instanceof Double) {
         body.add((Double) value);
      } else if (value instanceof byte[]) {
         writeBytes((byte[]) value);
      } else {
         throw new MessageFormatException("Unsupported Object type: " + value.getClass().getSimpleName());
      }
   }

   <T> T read(Class<T> clazz) throws JMSException {
      if (body == null || index >= body.size()) {
         throw new MessageEOFException("Attempt to read past end of stream");
      }
      checkBytesInFlight();
      Object result;
      T typedResult;
      try {
         Object value = body.get(index);
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
      index++;
      return typedResult;

   }

   @Override
   public void clearBody() throws JMSException {
      super.clearBody();
      this.index = 0;
      this.bytes = null;
      this.remainingBytes = -1;
   }


   @Override
   public void reset() throws JMSException {
      this.index = 0;
      this.bytes = null;
      this.remainingBytes = -1;
   }

   private void checkBytesInFlight() throws MessageFormatException {
      if (this.remainingBytes != -1) {
         throw new MessageFormatException("Partially read byte[] entry still being retrieved using readBytes(byte[] dest)");
      }
   }
}
