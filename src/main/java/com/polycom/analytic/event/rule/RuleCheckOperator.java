package com.polycom.analytic.event.rule;

import java.io.IOException;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
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
import com.datatorrent.lib.db.cache.CacheStore;
import com.polycom.analytic.data.Criteria;
import com.polycom.analytic.data.IBackendLoader;
import com.polycom.analytic.event.rule.IRuleEvalService.Action;
import com.polycom.analytic.util.BasicCacheManager;

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
    private IBackendLoader store;

    @NotNull
    private IRuleEvalService ruleEvalService;

    private transient BasicCacheManager cacheManager;

    /*
     * those cache fields NOT exposed 
     * */
    private int cacheExpirationInterval = 1 * 60 * 60 * 1000; // 1 hour
    private int cacheCleanupInterval = 1 * 60 * 60 * 1000; // 1 hour
    private int cacheSize = 1024; // 1024 records

    public IBackendLoader getStore()
    {
        return store;
    }

    public void setStore(IBackendLoader store)
    {

        this.store = store;
    }

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

    @Override
    public void setup(OperatorContext context)
    {
        super.setup(context);

        cacheManager = new BasicCacheManager();
        CacheStore primaryCache = new CacheStore();

        // set expiration to one day.
        primaryCache.setEntryExpiryDurationInMillis(cacheExpirationInterval);
        primaryCache.setCacheCleanupInMillis(cacheCleanupInterval);
        primaryCache.setEntryExpiryStrategy(CacheStore.ExpiryType.EXPIRE_AFTER_WRITE);
        primaryCache.setMaxCacheSize(cacheSize);

        cacheManager.setPrimary(primaryCache);
        cacheManager.setBackup(store);
    }

    @SuppressWarnings("null")
    private void processTuple(final Map<String, Object> tuple)
    {

        JSONArray rules = getRules(tuple);

        if (null == rules)
        {
            //TODO apply default rule
            if ("CRITICAL".equals(tuple.get(SEVERITY_FIELD)))
            {

                kafkaOut.emit(generateMsg(tuple));
            }
        }
        else
        {
            int ruleSize = rules.size();

            for (int i = 0; i < ruleSize; i++)
            {
                final JSONObject ruleJobj = rules.getJSONObject(i);

                String ruleStr = ruleJobj.getString("def");
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

    private JSONArray getRules(Map<String, Object> tuple)
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
            cacheManager.initialize();
        }
        catch (IOException e)
        {
            throw new IllegalStateException("failed to connect", e);

        }
        catch (Exception ue)
        {
            throw new IllegalStateException("failed to connect", ue);

        }

    }

    @Override
    public void deactivate()
    {
        try
        {
            ruleEvalService.deactivate();
            cacheManager.close();
        }
        catch (IOException e)
        {
            throw new IllegalStateException("failed to close", e);

        }
    }

}
