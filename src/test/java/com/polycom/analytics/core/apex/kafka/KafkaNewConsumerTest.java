package com.polycom.analytics.core.apex.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaNewConsumerTest
{
    private KafkaConsumer<byte[], byte[]> kc;

    private static final Logger log = LoggerFactory.getLogger(KafkaNewConsumerTest.class);

    private Map<Integer, Integer> partToMsgNb = new HashMap<>();

    private List<TopicPartition> assignments;

    @Before
    public void setUp() throws Exception
    {
        Properties prop = new Properties();
        // prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "hanalytics-ambari-master-scus-1:6667,hanalytics-ambari-master-scus-2:6667,hanalytics-ambari-master-scus-3:6667");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        // never auto commit the offsets
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "deviceInfo_Consumer");
        kc = new KafkaConsumer<>(prop);
        assignments = new LinkedList<>();
        for (int i = 0; i < 12; i++)
        {
            // assignments.add(new TopicPartition("DeviceInfo.networkInfo", i));
            assignments.add(new TopicPartition("DeviceInfo.primaryDeviceInfo", i));

            partToMsgNb.put(Integer.valueOf(i), Integer.valueOf(0));
        }
        kc.assign(assignments);

    }

    @After
    public void tearDown() throws Exception
    {
        kc.close();
    }

    @Test
    public void test()
    {
        while (true)
        {
            try
            {
                log.info("before poll");
                ConsumerRecords<byte[], byte[]> records = kc.poll(2000L);
                log.info("end poll");
                for (ConsumerRecord<byte[], byte[]> record : records)
                {
                    //String key = new String(record.key());
                    String key = null;
                    String msg = new String(record.value());
                    // System.out.println(key + ": " + msg);
                    log.info(record.partition() + ":" + record.offset() + ": " + msg);
                    partToMsgNb.put(Integer.valueOf(record.partition()), partToMsgNb.get(record.partition()) + 1);
                    log.info(partToMsgNb.toString());
                }
            }
            catch (NoOffsetForPartitionException e)
            {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testPosition()
    {

        for (TopicPartition tp : assignments)
        {
            kc.seekToBeginning(tp);
            log.info(tp.toString() + ":" + kc.position(tp));
            log.info("+++++++++");

        }

    }

    @Test
    public void testCommit()
    {
        for (TopicPartition tp : assignments)
        {
            kc.seekToBeginning(tp);
            log.info(tp.toString() + ":" + kc.position(tp));
            log.info("+++++++++");
            kc.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(kc.position(tp))));
        }

        /* kc.commitSync(Collections.singletonMap(new TopicPartition("DeviceInfo.primaryDeviceInfo", 2),
                new OffsetAndMetadata(3956)));
        kc.commitSync(Collections.singletonMap(new TopicPartition("DeviceInfo.primaryDeviceInfo", 5),
                new OffsetAndMetadata(3963)));*/
    }

}
