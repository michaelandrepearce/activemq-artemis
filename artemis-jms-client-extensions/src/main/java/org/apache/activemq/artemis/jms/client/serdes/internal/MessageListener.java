package org.apache.activemq.artemis.jms.client.serdes.internal;

import javax.jms.BytesMessage;
import javax.jms.Message;

import org.apache.activemq.artemis.jms.client.serdes.Serdes;

/**
 * Created by pearcem on 13/12/2016.
 */
public class MessageListener<T extends javax.jms.MessageListener> extends ForwardingObject<T> implements javax.jms.MessageListener
{
   private Serdes serdes;
   
   public MessageListener(Serdes serdes, T messageListener){
      super(messageListener);
      this.serdes = serdes;
   }

   public void onMessage(Message message)
   {
      delegate().onMessage(intercept(message));
   }
   
   public Message intercept(Message message)
   {
      return message instanceof BytesMessage ? new SerdesObjectMessage(serdes, (BytesMessage) message) : message;
   }
}
