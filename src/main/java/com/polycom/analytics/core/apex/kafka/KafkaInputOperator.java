package com.polycom.analytics.core.apex.kafka;

import java.util.Map;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.alibaba.fastjson.JSON;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyValPair;

public class KafkaInputOperator extends AbstractKafkaInputOperator
{

    public final transient DefaultOutputPort<Map<String, Object>> output1 = new DefaultOutputPort<>();
    public final transient DefaultOutputPort<KeyValPair<String, byte[]>> hdfsOut = new DefaultOutputPort<>();
    public final transient DefaultOutputPort<Map<String, Object>> output2 = new DefaultOutputPort<>();

    @Override
    protected void emitTuple(String cluster, ConsumerRecord<byte[], byte[]> message)
    {
        String msg = new String(message.value());
        Map<String, Object> msgMap = JSON.parseObject(msg);
        output1.emit(msgMap);
        output2.emit(msgMap);
        String topic = message.topic();
        hdfsOut.emit(new KeyValPair<>(topic, message.value()));
    }
}
