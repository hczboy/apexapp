package com.polycom.analytic;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyValPair;

public class KafkaSinglePortStringInputOperator extends AbstractKafkaInputOperator
{

    public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<>();
    public final transient DefaultOutputPort<KeyValPair<String, byte[]>> hdfsOut = new DefaultOutputPort<>();

    @Override
    protected void emitTuple(String cluster, ConsumerRecord<byte[], byte[]> message)
    {
        String msg = new String(message.value());
        outputPort.emit(msg);
        String topic = message.topic();
        hdfsOut.emit(new KeyValPair<>(topic, message.value()));
    }
}
