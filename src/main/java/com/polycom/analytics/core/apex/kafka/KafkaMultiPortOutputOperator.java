package com.polycom.analytics.core.apex.kafka;

import org.apache.apex.malhar.kafka.AbstractKafkaOutputOperator;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.datatorrent.api.DefaultInputPort;

public class KafkaMultiPortOutputOperator<K, V> extends AbstractKafkaOutputOperator
{
    public final transient DefaultInputPort<V> inputPort1 = new DefaultInputPort<V>()
    {
        @Override
        public void process(V tuple)
        {
            getProducer().send(new ProducerRecord<K, V>(getTopic(), tuple));
        }
    };

    public final transient DefaultInputPort<V> inputPort2 = new DefaultInputPort<V>()
    {
        @Override
        public void process(V tuple)
        {
            getProducer().send(new ProducerRecord<K, V>(getTopic(), tuple));
        }
    };
}
