package com.polycom.analytic.event.rule;

import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.polycom.analytic.event.rule.IRuleEvalService.Action;

@Stateless
public class RuleCheckOperator extends BaseOperator implements Operator.ActivationListener<Context.OperatorContext>
{

    private static final Logger log = LoggerFactory.getLogger(RuleCheckOperator.class);
    private static final String SEVERITY_FIELD = "severity";
    private static final String EVENTTIME_FIELD = "eventTime";
    private static final String EVENTTYPE_FIELD = "eventType";
    private static final String DEVICEID_FIELD = "deviceID";
    private static final String TENANTID_FIELD = "tenantID";
    private static final String MESSAGE_FIELD = "message";
    private static final String CUSTOMERID_FIELD = "customerID";

    private static final String TABLE_PREFIX = "customer_";
    private static final String PER_TENANT_QUERY_TPL = "{tenantID:#}";
    private static final String RULES_FIELD = "rules";

    @NotNull
    private IRuleEvalService ruleEvalService;

    public IRuleEvalService getRuleEvalService()
    {
        return ruleEvalService;
    }

    public void setRuleEvalService(IRuleEvalService ruleEvalService)
    {
        this.ruleEvalService = ruleEvalService;
    }

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

    @SuppressWarnings("null")
    private void processTuple(final Map<String, Object> tuple)
    {

        log.debug("tuple: {}", tuple);
        JSONArray rules = (JSONArray) tuple.get(RuleEnricherOperator.ENRICHER_RULES);

        if (null == rules)
        {
            log.info("No rules found for customerID: {}, tenantId: {}", tuple.get(CUSTOMERID_FIELD),
                    tuple.get(TENANTID_FIELD));
        }
        else
        {
            int ruleSize = rules.size();

            for (int i = 0; i < ruleSize; i++)
            {
                final JSONObject ruleJobj = rules.getJSONObject(i);

                String ruleStr = ruleJobj.getString("def");
                log.debug("ruledef: {}", ruleStr);
                ruleEvalService.evaluateAndDoAction(ruleStr, tuple, new Action()
                {

                    @Override
                    public void perform()
                    {
                        kafkaOut.emit(generateMsg(tuple, ruleJobj.getJSONArray("commands")));

                    }

                });

            }
        }

    }

    /*  private JSONArray getRules(Map<String, Object> tuple)
    {
        String cusId = (String) tuple.get(CUSTOMERID_FIELD);
        String tenantId = (String) tuple.get(TENANTID_FIELD);
        Criteria c = new Criteria();
        c.setTableName(new StringBuilder(TABLE_PREFIX).append(cusId).toString());
        c.setParas(new Object[] { tenantId });
        c.setReturnType(JSONObject.class);
        c.setCondition(PER_TENANT_QUERY_TPL);
    
        JSONObject doc = (JSONObject) cacheManager.get(c);
    
        if (null == doc)
        {
            return null;
        }
        return doc.getJSONArray(RULES_FIELD);
    }*/

    /*  private String generateMsg(Map<String, Object> tuple)
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
    
    }*/

    private String generateMsg(Map<String, Object> tuple, JSONArray cmds)
    {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put(TENANTID_FIELD, tuple.get(TENANTID_FIELD));
        jsonObj.put(DEVICEID_FIELD, tuple.get(DEVICEID_FIELD));
        jsonObj.put(EVENTTYPE_FIELD, tuple.get(EVENTTYPE_FIELD));
        jsonObj.put(EVENTTIME_FIELD, tuple.get(EVENTTIME_FIELD));

        jsonObj.put(MESSAGE_FIELD, cmds);
        return jsonObj.toJSONString();

    }

    @Override
    public void activate(OperatorContext context)
    {
        try
        {
            ruleEvalService.activate(context);

        }
        catch (Exception e)
        {
            throw new IllegalStateException("failed to activate", e);

        }

    }

    @Override
    public void deactivate()
    {
        try
        {
            ruleEvalService.deactivate();

        }
        catch (Exception e)
        {
            throw new IllegalStateException("failed to deactivate", e);

        }
    }

}
