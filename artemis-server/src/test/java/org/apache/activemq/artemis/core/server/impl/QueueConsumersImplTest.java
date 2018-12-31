package org.apache.activemq.artemis.core.server.impl;

import org.apache.activemq.artemis.core.server.PriorityAware;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class QueueConsumersImplTest {

    private QueueConsumers<TestPriority> queueConsumers;

    @Before
    public void setUp() {
        queueConsumers = new QueueConsumersImpl<>();
    }

    @Test
    public void addTest() {
        TestPriority testPriority = new TestPriority("hello", 0);
        assertFalse(queueConsumers.hasNext());

        queueConsumers.add(testPriority);
        queueConsumers.reset();
        assertTrue(queueConsumers.hasNext());

        assertEquals(testPriority, queueConsumers.next());
    }

    @Test
    public void removeTest() {
        TestPriority testPriority = new TestPriority("hello", 0);
        assertFalse(queueConsumers.hasNext());

        queueConsumers.add(testPriority);
        queueConsumers.reset();
        assertTrue(queueConsumers.hasNext());

        queueConsumers.remove(testPriority);
        queueConsumers.reset();
        assertFalse(queueConsumers.hasNext());

        assertEquals(0, queueConsumers.getPriorites().size());
        queueConsumers.remove(testPriority);
        queueConsumers.remove(testPriority);

    }



    @Test
    public void roundRobinTest() {
        queueConsumers.add(new TestPriority("A", 127));
        queueConsumers.add(new TestPriority("B", 127));
        queueConsumers.add(new TestPriority("E", 0));
        queueConsumers.add(new TestPriority("D", 20));
        queueConsumers.add(new TestPriority("C", 127));
        queueConsumers.reset();
        assertTrue(queueConsumers.hasNext());

        assertEquals("A", queueConsumers.next().getName());

        //Reset iterator should mark start as current position
        queueConsumers.reset();
        assertTrue(queueConsumers.hasNext());
        assertEquals("B", queueConsumers.next().getName());

        assertTrue(queueConsumers.hasNext());
        assertEquals("C", queueConsumers.next().getName());

        //Expect another A as after reset, we started at B so after A we then expect the next level
        assertTrue(queueConsumers.hasNext());
        assertEquals("A", queueConsumers.next().getName());

        assertTrue(queueConsumers.hasNext());
        assertEquals("D", queueConsumers.next().getName());

        assertTrue(queueConsumers.hasNext());
        assertEquals("E", queueConsumers.next().getName());

        //We have iterated all.
        assertFalse(queueConsumers.hasNext());

        //Reset to iterate again.
        queueConsumers.reset();

        //We expect the iteration to round robin from last returned at the level.
        assertTrue(queueConsumers.hasNext());
        assertEquals("B", queueConsumers.next().getName());


    }





    private class TestPriority implements PriorityAware {

        private final int priority;
        private final String name;

        private TestPriority(String name, int priority) {
            this.priority = priority;
            this.name = name;
        }

        @Override
        public int getPriority() {
            return priority;
        }

        public String getName() {
            return name;
        }
    }

}