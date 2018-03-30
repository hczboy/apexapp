package com.polycom.analytics.core.apex.kafka;

import java.util.Map;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyValPair;

public class KafkaInputOperator extends AbstractKafkaInputOperator
{
    private static final Logger log = LoggerFactory.getLogger(KafkaInputOperator.class);

    public final transient DefaultOutputPort<Map<String, Object>> output1 = new DefaultOutputPort<>();
    public final transient DefaultOutputPort<KeyValPair<String, byte[]>> hdfsOut = new DefaultOutputPort<>();
    public final transient DefaultOutputPort<Map<String, Object>> output2 = new DefaultOutputPort<>();

    @Override
    protected void emitTuple(String cluster, ConsumerRecord<byte[], byte[]> message)
    {
        String msg = new String(message.value());
        Map<String, Object> msgMap = null;
        try
        {
            msgMap = JSON.parseObject(msg);
        }
        catch (Exception e)
        {
            log.error("Failed to parse kafka message:{} for topic: {}", msg, message.topic());
            return;
        }
        output1.emit(msgMap);
        output2.emit(msgMap);
        String topic = message.topic();
        hdfsOut.emit(new KeyValPair<>(topic, message.value()));
    }
}
