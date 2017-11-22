/**
 * Put your copyright and license info here.
 */
package com.polycom.analytic;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "kafkademo")
public class Application implements StreamingApplication
{

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
        // Sample DAG with 2 operators
        // Replace this code with the DAG you want to build

        /*      RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
        randomGenerator.setNumTuples(500);*/

        KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("kafkaInput",
                KafkaSinglePortStringInputOperator.class);
        ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());
        HdfsFileOutputOperator hdfsOut = dag.addOperator("hdfs", new HdfsFileOutputOperator());

        dag.addStream("kafkaToConsole", kafkaInput.outputPort, cons.input).setLocality(Locality.NODE_LOCAL);
        dag.addStream("kafkaToHdfs", kafkaInput.hdfsOut, hdfsOut.input).setLocality(Locality.NODE_LOCAL);
    }
}
