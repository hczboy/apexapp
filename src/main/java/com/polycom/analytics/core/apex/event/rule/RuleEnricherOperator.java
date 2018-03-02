package com.polycom.analytics.core.apex.event.rule;

import static com.polycom.analytics.core.apex.common.Constants.CUSTOMERID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.contrib.enrich.MapEnricher;
import com.datatorrent.lib.db.cache.CacheStore;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.FieldInfo.SupportType;
import com.polycom.analytics.core.apex.data.mongo.Criteria;
import com.polycom.analytics.core.apex.util.BasicCacheManager;

public class RuleEnricherOperator extends MapEnricher
{
    private static final Logger log = LoggerFactory.getLogger(RuleEnricherOperator.class);

    private static final String TABLE_PREFIX = "customer_";
    private static final String PER_TENANT_QUERY_TPL = "{tenantID:#}";
    private static final String RULES_FIELD = "rules";
    public static final String ENRICHER_RULES = "enricher_rules";

    //that's an intended overwrite default cacheManager, as it's has a bug 
    //NOT use default cacheManager
    private transient BasicCacheManager cacheManager;

    @Override
    public void setup(OperatorContext context)
    {
        //avoid call super.setup(), as it would setup default cacheManager 
        cacheManager = new BasicCacheManager();
        CacheStore primaryCache = new CacheStore();

        primaryCache.setEntryExpiryDurationInMillis(getCacheExpirationInterval());
        primaryCache.setCacheCleanupInMillis(getCacheCleanupInterval());
        primaryCache.setEntryExpiryStrategy(CacheStore.ExpiryType.EXPIRE_AFTER_WRITE);
        primaryCache.setMaxCacheSize(getCacheSize());

        cacheManager.setPrimary(primaryCache);
        cacheManager.setBackup(getStore());

    }

    @Override
    protected void enrichTuple(Map<String, Object> tuple)
    {
        Object key = getKey(tuple);
        if (key != null)
        {
            Object result = cacheManager.get(key);
            Map<String, Object> out = convert(tuple, result);
            if (out != null)
            {
                emitEnrichedTuple(out);
            }
        }

    }

    /*
     * 
     * */
    @Override
    protected Object getKey(Map<String, Object> tuple)
    {
        String cusId = (String) tuple.get(CUSTOMERID_FIELD);
        String tenantId = (String) tuple.get(TENANTID_FIELD);
        Criteria c = new Criteria();
        c.setTableName(new StringBuilder(TABLE_PREFIX).append(cusId).toString());
        c.setParas(new Object[] { tenantId });
        c.setReturnType(JSONObject.class);
        c.setCondition(PER_TENANT_QUERY_TPL);
        if (c.isValid())
        {
            return c;
        }
        return null; //return null, will ignore following process 

    }

    @Override
    protected Map<String, Object> convert(Map<String, Object> in, Object ruleObj)
    {
        if (ruleObj == null)
        {
            log.info("No rule found for customerID: {}, tenantId: {}", in.get(CUSTOMERID_FIELD),
                    in.get(TENANTID_FIELD));
            return null; //if ruleObj is null, no need to emit the tuple to downstream operator
        }
        JSONObject ruleDoc = (JSONObject) ruleObj;
        JSONArray rules = ruleDoc.getJSONArray(RULES_FIELD);
        if (rules == null)
        {
            log.info("No value for field '{}' in ruleDoc[customerID: {}, tenantID: {}]", RULES_FIELD,
                    in.get(CUSTOMERID_FIELD), in.get(TENANTID_FIELD));
            return null; // No rules found, no need to emit the tuple to downstream operator
        }
        in.put(ENRICHER_RULES, rules);
        return in;
    }

    /*
     * overwrite super activate(), provides same implementation
     * but with new cacheManager initialize
     * */
    @Override
    public void activate(Context context)
    {
        for (String s : lookupFields)
        {
            lookupFieldInfo.add(new FieldInfo(s, s, SupportType.getFromJavaType(getLookupFieldType(s))));
        }

        if (includeFields != null)
        {
            for (String s : includeFields)
            {
                includeFieldInfo.add(new FieldInfo(s, s, SupportType.getFromJavaType(getIncludeFieldType(s))));
            }
        }

        getStore().setFieldInfo(lookupFieldInfo, includeFieldInfo);

        try
        {
            cacheManager.initialize();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to initialize primary cache", e);
        }

    }

    @Override
    public void deactivate()
    {
        try
        {
            cacheManager.close();
        }
        catch (IOException e)
        {
            throw new IllegalStateException("failed to close", e);

        }

    }

}
