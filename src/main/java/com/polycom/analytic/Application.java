/**
 * Put your copyright and license info here.
 */
package com.polycom.analytic;

import org.apache.apex.malhar.kafka.KafkaSinglePortOutputOperator;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.polycom.analytic.data.MongoLoader;
import com.polycom.analytic.event.rule.MvelRuleEvalService;
import com.polycom.analytic.event.rule.RuleCheckOperator;
import com.polycom.analytic.kafka.KafkaInputOperator;

@ApplicationAnnotation(name = "kafkademo")
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

        KafkaInputOperator kafkaInput = dag.addOperator("kafkaInput", KafkaInputOperator.class);

        HdfsFileOutputOperator hdfsOut = dag.addOperator("hdfs", new HdfsFileOutputOperator());

        MongoLoader store = new MongoLoader();
        MvelRuleEvalService ruleEvalService = new MvelRuleEvalService();
        RuleCheckOperator ruleCheckOut = dag.addOperator("ruleCheck", new RuleCheckOperator());
        ruleCheckOut.setStore(store);
        ruleCheckOut.setRuleEvalService(ruleEvalService);
        dag.addStream("kafkaToRuleCheck", kafkaInput.ruleCheckOut, ruleCheckOut.input)
                .setLocality(Locality.CONTAINER_LOCAL);
        dag.addStream("kafkaToHdfs", kafkaInput.hdfsOut, hdfsOut.input).setLocality(Locality.CONTAINER_LOCAL);

        /*TranquilityOutputOperator tranquilityOut = dag.addOperator("tranquility", new TranquilityOutputOperator());
        dag.addStream("kafkaToTranquility", kafkaInput.druidOut, tranquilityOut.input)
                .setLocality(Locality.CONTAINER_LOCAL);*/
        KafkaSinglePortOutputOperator<String, String> out = dag.addOperator("kafkaOutput",
                new KafkaSinglePortOutputOperator<String, String>());

        dag.addStream("ruleCheckToKafka", ruleCheckOut.kafkaOut, out.inputPort)
                .setLocality(Locality.CONTAINER_LOCAL);

    }
}
