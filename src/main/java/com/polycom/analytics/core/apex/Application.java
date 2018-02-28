/**
 * Put your copyright and license info here.
 */
package com.polycom.analytics.core.apex;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.polycom.analytics.core.apex.common.EnhancedMapEnricher;
import com.polycom.analytics.core.apex.data.mongo.BasicMongoLoader;
import com.polycom.analytics.core.apex.event.brancher.DeviceEventBrancher;
import com.polycom.analytics.core.apex.kafka.KafkaInputOperator;

@ApplicationAnnotation(name = "deviceEvent")
public class Application implements StreamingApplication
{

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
        // Sample DAG with 2 operators
        // Replace this code with the DAG you want to build

        /*      RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
        randomGenerator.setNumTuples(500);*/

        //ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());

        //dag.addStream("kafkaToConsole", kafkaInput.outputPort, cons.input).setLocality(Locality.CONTAINER_LOCAL);

        /*  KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("kafkaInput",
                KafkaSinglePortStringInputOperator.class);
        
        HdfsFileOutputOperator hdfsOut = dag.addOperator("hdfs", new HdfsFileOutputOperator());
        TranquilityOutputOperator tranquilityOut = dag.addOperator("tranquility", new TranquilityOutputOperator());
        
        dag.addStream("kafkaToHdfs", kafkaInput.hdfsOut, hdfsOut.input).setLocality(Locality.CONTAINER_LOCAL);
        dag.addStream("kafkaToTranquility", kafkaInput.outputPort, tranquilityOut.input)
                .setLocality(Locality.CONTAINER_LOCAL);*/

        //================================poc========================
        /*    KafkaInputOperator kafkaInput = dag.addOperator("kafkaInput", KafkaInputOperator.class);
        
        HdfsFileOutputOperator hdfsOut = dag.addOperator("hdfs", new HdfsFileOutputOperator());
        
        RuleEnricherOperator ruleEnricher = dag.addOperator("ruleEnricher", new RuleEnricherOperator());
        MongoLoader store = new MongoLoader();
        ruleEnricher.setStore(store);
        ruleEnricher.setIncludeFields(Collections.EMPTY_LIST);
        ruleEnricher.setLookupFields(Collections.EMPTY_LIST);
        ruleEnricher.setCacheExpirationInterval(600000);// cache expiration 10 mins
        dag.addStream("kafkaToRuleEnricher", kafkaInput.output2, ruleEnricher.input)
                .setLocality(Locality.CONTAINER_LOCAL);
        MvelRuleEvalService ruleEvalService = new MvelRuleEvalService();
        RuleCheckOperator ruleCheckOut = dag.addOperator("ruleCheck", new RuleCheckOperator());
        
        ruleCheckOut.setRuleEvalService(ruleEvalService);
        
        dag.addStream("ruleEnricherToRuleCheck", ruleEnricher.output, ruleCheckOut.input)
                .setLocality(Locality.CONTAINER_LOCAL);
        dag.addStream("kafkaToHdfs", kafkaInput.hdfsOut, hdfsOut.input).setLocality(Locality.CONTAINER_LOCAL);
        
        TranquilityOutputOperator tranquilityOut = dag.addOperator("tranquility", new TranquilityOutputOperator());
        dag.addStream("kafkaToTranquility", kafkaInput.druidOut, tranquilityOut.input)
                .setLocality(Locality.CONTAINER_LOCAL);
        KafkaSinglePortOutputOperator<String, String> out = dag.addOperator("kafkaOutput",
                new KafkaSinglePortOutputOperator<String, String>());
        
        dag.addStream("ruleCheckToKafka", ruleCheckOut.kafkaOut, out.inputPort)
                .setLocality(Locality.CONTAINER_LOCAL);*/

        //========================== end poc
        //================================deviceInfo=========
        /*  KafkaInputOperator deviceInfoInput = dag.addOperator("deviceInfoInput", KafkaInputOperator.class);
        HdfsFileOutputOperator hdfsOut = dag.addOperator("deviceInfoHdfs", new HdfsFileOutputOperator());
        dag.addStream("deviceInfoToHdfs", deviceInfoInput.hdfsOut, hdfsOut.input)
                .setLocality(Locality.CONTAINER_LOCAL);
        FingerprintsOutputOperator fingerprintOut = dag.addOperator("fingerprintOut",
                FingerprintsOutputOperator.class);
        dag.addStream("deviceInfoToFingerprint", deviceInfoInput.output1, fingerprintOut.inputPort)
                .setLocality(Locality.CONTAINER_LOCAL);*/
        //======================end deviceInfo==========================
        KafkaInputOperator deviceEventInput = dag.addOperator("deviceEventInput", KafkaInputOperator.class);
        HdfsFileOutputOperator hdfsOut = dag.addOperator("deviceEventHdfs", new HdfsFileOutputOperator());
        dag.addStream("deviceEventToHdfs", deviceEventInput.hdfsOut, hdfsOut.input)
                .setLocality(Locality.CONTAINER_LOCAL);
        DeviceEventBrancher deviceEventBrancher = dag.addOperator("deviceEventBrancher",
                DeviceEventBrancher.class);
        dag.addStream("deviceEventToBrancher", deviceEventInput.output1, deviceEventBrancher.input)
                .setLocality(Locality.THREAD_LOCAL);
        EnhancedMapEnricher deviceAttachmentEnricher = dag.addOperator("deviceAttachmentEnricher",
                EnhancedMapEnricher.class);
        BasicMongoLoader loader = new BasicMongoLoader();
        deviceAttachmentEnricher.setStore(loader);
        dag.addStream("deviceBrancherToDeviceAttEnricher", deviceEventBrancher.deviceAttachmentOutput,
                deviceAttachmentEnricher.input).setLocality(Locality.CONTAINER_LOCAL);
        ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());
        dag.addStream("dconsole", deviceAttachmentEnricher.output, cons.input);

    }
}
