package org.apache.activemq.artemis.tests.integration;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class EncodingDecodingCombinations {

    private SimpleString simpleString = SimpleString.toSimpleString("hello");


    @Test
    public void testHappyCase_NullableEncode_NullableDecode() {

        UnpooledByteBufAllocator unpooledByteBufAllocator = new UnpooledByteBufAllocator(false);

        ActiveMQBuffer activeMQBuffer = new ChannelBufferWrapper(unpooledByteBufAllocator.buffer());

        activeMQBuffer.writeNullableSimpleString(simpleString);

        SimpleString read = activeMQBuffer.duplicate().readNullableSimpleString();

        assertEquals(simpleString, read);
    }

    @Test
    public void testHappyCase_NonNullableEncode_NonNullableDecode() {

        UnpooledByteBufAllocator unpooledByteBufAllocator = new UnpooledByteBufAllocator(false);

        ActiveMQBuffer activeMQBuffer = new ChannelBufferWrapper(unpooledByteBufAllocator.buffer());

        activeMQBuffer.writeSimpleString(simpleString);

        SimpleString read = activeMQBuffer.duplicate().readSimpleString();

        assertEquals(simpleString, read);
    }


    @Test
    public void testUnHappyCase_NonNullableEncode_NullableDecode() {

        UnpooledByteBufAllocator unpooledByteBufAllocator = new UnpooledByteBufAllocator(false);

        ActiveMQBuffer activeMQBuffer = new ChannelBufferWrapper(unpooledByteBufAllocator.buffer());

        activeMQBuffer.writeSimpleString(simpleString);

        SimpleString read = activeMQBuffer.duplicate().readNullableSimpleString();

        assertEquals(simpleString, read);
    }


    @Test
    public void testUnHappyCase_NullableEncode_NonNullableDecode() {

        UnpooledByteBufAllocator unpooledByteBufAllocator = new UnpooledByteBufAllocator(false);

        ActiveMQBuffer activeMQBuffer = new ChannelBufferWrapper(unpooledByteBufAllocator.buffer());

        activeMQBuffer.writeNullableSimpleString(simpleString);

        SimpleString read = activeMQBuffer.duplicate().readSimpleString();

        assertEquals(simpleString, read);
    }

}
