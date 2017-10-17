# Kafka Bridges

The function of a bridge is to consume messages from a source queue, and
forward them to a target topic, on a remote Apache Kafka
server.

The source and target servers are remote making bridging suitable
for reliably sending messages from one artemis cluster to kafka, 
for instance across a WAN, to the cloud, or internet and where the connection may be unreliable.

The bridge has built in resilience to failure so if the target server
connection is lost, e.g. due to network failure, the bridge will retry
connecting to the target until it comes back online. When it comes back
online it will resume operation as normal.

In summary, bridges are a way to reliably connect separate Apache ActiveMQ Artemis
server with Apache Kafka together.

![ActiveMQ Artemis Kafka Bridge Diagram](images/artemis-kafka-bridge.jpg)

## Configuring Kakfa Bridges

Bridges are configured in `broker.xml`.  
Let's kick off
with an example (this is actually from the kafka bridge test example):


    <connector-services>
         <connector-service name="my-kafka-bridge">
            <factory-class>org.apache.activemq.artemis.integration.kafka.bridge.KafkaProducerBridgeFactory</factory-class>
            <param key="bootstrap.servers" value="kafka-1.domain.local:9092,kafka-2.domain.local:9092,kafka-3.domain.local:9092" />
            <param key="queue-name" value="my.artemis.queue" />
            <param key="kafka-topic" value="my_kafka_topic" />
         </connector-service>
    </connector-services>

In the above example we have shown the required parameters to
configure for a kakfa bridge. See below for a complete list of available configuration options. 

### Serialization
By default the CoreMessageSerializer is used.

#### CoreMessageSerializer
Default but can be explicitly set using
           
    <param key="value.serializer" value="org.apache.activemq.artemis.integration.kafka.protocol.core.CoreMessageSerializer" />

This maps the Message properties to Record headers.
And then maps the payload binary as the Record value, encoding TextMessage

This makes it easy to consume from Kafka using default deserializers
TextMessage using StringDeserializer
ByteMessage using BytesDeserializer.

Also supplied are some deserializers so if you wish to consume from Kafka 
and get a more familiar CoreMessage or JMSMessage, your consumers can use:

`org.apache.activemq.artemis.integration.kafka.protocol.core.jms.CoreJmsMessageDeserializer`
`org.apache.activemq.artemis.integration.kafka.protocol.core.CoreMessageDeserializer`


#### AMQPMessageSerializer
Can be set by using:
    
    <param key="value.serializer" value="org.apache.activemq.artemis.integration.kafka.protocol.amqp.AMQPMessageSerializer" />


This encodes the whole message into amqp binary protocol into the Record value.

This is useful if you have something like

   https://github.com/EnMasseProject/amqp-kafka-bridge

As it allows messages to pump into kafka in amqp binary, to then consumed from kafka by amqp clients.

Also we provide an Apache Qpid Proton deserializers allowing you to consume from Kafka and get
a ProtonMessage, or a JMSMessage

`org.apache.activemq.artemis.integration.kafka.protocol.amqp.proton.ProtonMessageDeserializer`
`org.apache.activemq.artemis.integration.kafka.protocol.amqp.jms.CoreJmsMessageDeserializer`

#### Custom Serializer

You can actually provide your own custom serializer if the provided don't fit your needs

By implement Kafka Serializer that take type of `org.apache.activemq.artemis.api.core.Message`

And then simply add you jar to the classpath and then set:

    <param key="value.serializer" value="org.foo.bar.MySerializer" />



### Full Configuration

In practice you might use many of the defaults
so it won't be necessary to specify them all but there addtiona.

All kafka producer configs can be set using the same keys defined here:
https://kafka.apache.org/documentation/#producerconfigs

Now Let's take a look at all kafka bride specific parameters in turn:

-   `name` attribute. All bridges must have a unique name in the server.

-   `queue-name`. This is the unique name of the local queue that the
    bridge consumes from, it's a mandatory parameter.

    The queue must already exist by the time the bridge is instantiated
    at start-up.

-   `kafka-topic`. This is the topic on the target kafka server that
    the message will be forwarded to.

-   `filter-string`. An optional filter string can be supplied. If
    specified then only messages which match the filter expression
    specified in the filter string will be forwarded. The filter string
    follows the ActiveMQ Artemis filter expression syntax described in [Filter Expressions](filter-expressions.md).

-   `retry-interval`. This optional parameter determines the period in
    milliseconds between subsequent reconnection attempts, if the
    connection to the target server has failed. The default value is
    `2000`milliseconds.
    
-   `retry-max-interval`. This optional parameter determines the maximum 
    period in milliseconds between subsequent reconnection attempts, if the
    connection to the target server has failed. The default value is
    `30000`milliseconds.

-   `retry-interval-multiplier`. This optional parameter determines
    determines a multiplier to apply to the time since the last retry to
    compute the time to the next retry.

    This allows you to implement an *exponential backoff* between retry
    attempts.

    Let's take an example:

    If we set `retry-interval`to `1000` ms and we set
    `retry-interval-multiplier` to `2.0`, then, if the first reconnect
    attempt fails, we will wait `1000` ms then `2000` ms then `4000` ms
    between subsequent reconnection attempts.

    The default value is `1.0` meaning each reconnect attempt is spaced
    at equal intervals.


-   `retry-attempts`. This optional parameter determines the total
    number of retry attempts the kafka bridge will make before giving up
    and shutting down. A value of `-1` signifies an unlimited number of
    attempts. The default value is `-1`.
