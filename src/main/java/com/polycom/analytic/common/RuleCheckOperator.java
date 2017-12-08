package com.polycom.analytic.common;

import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

public class RuleCheckOperator extends BaseOperator
{

    private static final String SEVERITY_FIELD = "severity";
    private static final String EVENTTIME_FIELD = "eventTime";
    private static final String EVENTTYPE_FIELD = "eventType";
    private static final String DEVICEID_FIELD = "deviceID";
    private static final String TENANTID_FIELD = "tenantID";
    private static final String MESSAGE_FIELD = "message";

    @OutputPortFieldAnnotation(optional = true)
    public final transient DefaultOutputPort<String> kafkaOut = new DefaultOutputPort<>();
    public final transient DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>()
    {

        @Override
        public void process(Map<String, Object> tuple)
        {
            processTuple(tuple);

        }
    };

    private void processTuple(Map<String, Object> tuple)
    {

        if ("CRITICAL".equals(tuple.get(SEVERITY_FIELD)))
        {

            kafkaOut.emit(generateMsg(tuple));
        }
    }

    private String generateMsg(Map<String, Object> tuple)
    {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put(TENANTID_FIELD, tuple.get(TENANTID_FIELD));
        jsonObj.put(DEVICEID_FIELD, tuple.get(DEVICEID_FIELD));
        jsonObj.put(EVENTTYPE_FIELD, tuple.get(EVENTTYPE_FIELD));
        jsonObj.put(EVENTTIME_FIELD, tuple.get(EVENTTIME_FIELD));

        JSONObject msgObj = JSON.parseObject(
                "{\"attr\":\"DeviceAnalyticsCommand\", \"version\": \"0.0.1\", \"value\":{\"commandType\":\"startUploadLog\",\"globalLogLevel\":\"DEBUG\", \"logFileSize\":\"512\" ,\"resourceName\":\"anIDForTheLogSentBackInTheUpload\"}}");
        jsonObj.put(MESSAGE_FIELD, msgObj);
        return jsonObj.toJSONString();

    }

    /* public static void main(String[] args)
    {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put(TENANTID_FIELD, "t1");
        jsonObj.put(DEVICEID_FIELD, "d1");
        jsonObj.put(EVENTTYPE_FIELD, "devicetyype");
        jsonObj.put(EVENTTIME_FIELD, "1982");
    
        System.out.println(jsonObj.toJSONString());
    }*/

}
