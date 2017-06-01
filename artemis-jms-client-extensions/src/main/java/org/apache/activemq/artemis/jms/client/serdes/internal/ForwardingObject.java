package org.apache.activemq.artemis.jms.client.serdes.internal;

public abstract class ForwardingObject<T> {

    private T delegate;

    public ForwardingObject(T delegate){
        this.delegate = delegate;
    }

    protected T delegate(){
        return delegate;
    }

    @Override
    public String toString() {
        return delegate().toString();
    }
}