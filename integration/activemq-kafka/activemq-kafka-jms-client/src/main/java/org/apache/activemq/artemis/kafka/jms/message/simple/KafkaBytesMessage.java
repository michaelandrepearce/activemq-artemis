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

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.activemq.artemis.kafka.jms.exception.JmsExceptionSupport;

public class KafkaBytesMessage extends AbstractKafkaMessage<byte[]> implements BytesMessage {

   private ByteArrayInputStream byteArrayInputStream;
   private ByteArrayOutputStream byteArrayOutputStream;
   private DataInputStream inputStream;
   private DataOutputStream outputStream;
   private long bodyLength;

   void setReadBody() {
      if (null != byteArrayOutputStream) {
         body = byteArrayOutputStream.toByteArray();
         byteArrayOutputStream = null;
         outputStream = null;
      }
      if (body == null) {
         byteArrayInputStream = new ByteArrayInputStream(new byte[0]);
         bodyLength = 0L;
      } else {
         byteArrayInputStream = new ByteArrayInputStream(body);
         bodyLength = body.length;
      }
      inputStream = new DataInputStream(byteArrayInputStream);
   }

   void setWriteBody() throws JMSException {
      if (null != outputStream) {
         if (body == null) {
            byteArrayOutputStream = new ByteArrayOutputStream();
         } else {
            byteArrayOutputStream = new ByteArrayOutputStream(body.length);
            try {
               byteArrayOutputStream.write(body);
            } catch (IOException ioe) {
               throw JmsExceptionSupport.toJMSException(ioe);
            }
         }
         outputStream = new DataOutputStream(byteArrayOutputStream);
      }
   }

   @Override
   public long getBodyLength() throws JMSException {
      setReadBody();
      return bodyLength;
   }

   @Override
   public boolean readBoolean() throws JMSException {
      setReadBody();

      try {
         return inputStream.readBoolean();
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public byte readByte() throws JMSException {
      setReadBody();

      try {
         return inputStream.readByte();
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public int readUnsignedByte() throws JMSException {
      setReadBody();

      try {
         return inputStream.readUnsignedByte();
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public short readShort() throws JMSException {
      setReadBody();

      try {
         return inputStream.readShort();
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public int readUnsignedShort() throws JMSException {
      setReadBody();

      try {
         return inputStream.readUnsignedShort();
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public char readChar() throws JMSException {
      setReadBody();

      try {
         return inputStream.readChar();
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public int readInt() throws JMSException {
      setReadBody();

      try {
         return inputStream.readInt();
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public long readLong() throws JMSException {
      setReadBody();

      try {
         return inputStream.readLong();
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public float readFloat() throws JMSException {
      setReadBody();

      try {
         return inputStream.readFloat();
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public double readDouble() throws JMSException {
      setReadBody();

      try {
         return inputStream.readDouble();
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public String readUTF() throws JMSException {
      setReadBody();

      try {
         return inputStream.readUTF();
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public int readBytes(byte[] bytes) throws JMSException {
      setReadBody();

      try {
         return inputStream.read(bytes);
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public int readBytes(byte[] bytes, int i) throws JMSException {
      setReadBody();

      try {
         return inputStream.read(bytes, 0, i);
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public void writeBoolean(boolean b) throws JMSException {
      setWriteBody();

      try {
         outputStream.writeBoolean(b);
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public void writeByte(byte b) throws JMSException {
      setWriteBody();

      try {
         outputStream.writeByte(b);
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public void writeShort(short i) throws JMSException {
      setWriteBody();

      try {
         outputStream.writeShort(i);
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public void writeChar(char c) throws JMSException {
      setWriteBody();

      try {
         outputStream.writeChar(c);
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public void writeInt(int i) throws JMSException {
      setWriteBody();

      try {
         outputStream.writeInt(i);
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public void writeLong(long l) throws JMSException {
      setWriteBody();

      try {
         outputStream.writeLong(l);
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public void writeFloat(float v) throws JMSException {
      setWriteBody();

      try {
         outputStream.writeFloat(v);
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public void writeDouble(double v) throws JMSException {
      setWriteBody();

      try {
         outputStream.writeDouble(v);
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public void writeUTF(String s) throws JMSException {
      setWriteBody();

      try {
         outputStream.writeUTF(s);
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public void writeBytes(byte[] bytes) throws JMSException {
      setWriteBody();

      try {
         outputStream.write(bytes);
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public void writeBytes(byte[] bytes, int i, int i1) throws JMSException {
      setWriteBody();

      try {
         outputStream.write(bytes, i, i1);
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }

   @Override
   public void writeObject(Object value) throws JMSException {
      if (value == null) {
         throw new NullPointerException();
      }
      setWriteBody();
      if (value instanceof Boolean) {
         writeBoolean((Boolean) value);
      } else if (value instanceof Character) {
         writeChar((Character) value);
      } else if (value instanceof Byte) {
         writeByte((Byte) value);
      } else if (value instanceof Short) {
         writeShort((Short) value);
      } else if (value instanceof Integer) {
         writeInt((Integer) value);
      } else if (value instanceof Long) {
         writeLong((Long) value);
      } else if (value instanceof Float) {
         writeFloat((Float) value);
      } else if (value instanceof Double) {
         writeDouble((Double) value);
      } else if (value instanceof String) {
         writeUTF(value.toString());
      } else if (value instanceof byte[]) {
         writeBytes((byte[]) value);
      } else {
         throw new MessageFormatException("Cannot write non-primitive type:" + value.getClass());
      }
   }

   @Override
   public void clearBody() throws JMSException {
      super.clearBody();
      byteArrayInputStream = null;
      byteArrayOutputStream = null;
      inputStream = null;
      outputStream = null;
      bodyLength = 0L;
   }


   @Override
   public void reset() throws JMSException {
      setReadBody();
      try {
         inputStream.reset();
      } catch (IOException ioe) {
         throw JmsExceptionSupport.toJMSException(ioe);
      }
   }
}
