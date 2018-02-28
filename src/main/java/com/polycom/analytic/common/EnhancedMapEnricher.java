package com.polycom.analytic.common;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.contrib.enrich.MapEnricher;
import com.datatorrent.lib.db.cache.CacheManager.Primary;
import com.datatorrent.lib.db.cache.CacheStore;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.FieldInfo.SupportType;
import com.google.common.collect.Lists;
import com.polycom.analytic.util.BasicCacheManager;

/*
 * compared to MapEnricher, this EnhancedMapEnricher provides following capabilities:
 *   1. enable/disable cache 
 *   2. overwrite default cacheManager, as it's has a bug on close()
 * */
public class EnhancedMapEnricher extends MapEnricher
{

    public void setLookupFieldsItem(int index, String val)
    {
        if (null == lookupFields)
        {
            lookupFields = Lists.newArrayList();
        }
        final int need = index - lookupFields.size() + 1;
        for (int i = 0; i < need; i++)
        {
            lookupFields.add(null);
        }
        lookupFields.set(index, val);
    }

    public void setIncludeFieldsItem(int index, String val)
    {
        if (null == includeFields)
        {
            includeFields = Lists.newArrayList();
        }
        final int need = index - includeFields.size() + 1;
        for (int i = 0; i < need; i++)
        {
            includeFields.add(null);
        }
        includeFields.set(index, val);
    }

    @NotNull
    protected boolean isUsingCache = true;
    /*@NotNull
    private String loaderlClazz;
    
    
    public String getLoaderlClazz()
    {
        return loaderlClazz;
    }
    
    public void setLoaderlClazz(String loaderlClazz)
    {
        this.loaderlClazz = loaderlClazz;
    }*/

    public boolean getIsUsingCache()
    {
        return isUsingCache;
    }

    public void setIsUsingCache(boolean isUsingCache)
    {
        this.isUsingCache = isUsingCache;
    }

    private static final Logger log = LoggerFactory.getLogger(EnhancedMapEnricher.class);

    //that's an intended overwrite default cacheManager, as it's has a bug 
    //NOT use default cacheManager
    private transient BasicCacheManager cacheManager;

    private static class NullPrimary implements Primary
    {

        @Override
        public Object get(Object key)
        {
            return null;

        }

        @Override
        public List<Object> getAll(List<Object> keys)
        {
            return Collections.EMPTY_LIST;

        }

        @Override
        public void put(Object key, Object value)
        {

        }

        @Override
        public void putAll(Map<Object, Object> m)
        {

        }

        @Override
        public void remove(Object key)
        {

        }

        @Override
        public void connect() throws IOException
        {

        }

        @Override
        public void disconnect() throws IOException
        {

        }

        @Override
        public boolean isConnected()
        {
            // TODO Implement isConnected
            throw new UnsupportedOperationException("isConnected Not Implemented");

        }

        @Override
        public Set<Object> getKeys()
        {
            return Collections.EMPTY_SET;
        }

    }

    @Override
    public void setup(OperatorContext context)
    {
        //avoid call super.setup(), as it would setup default cacheManager 
        cacheManager = new BasicCacheManager();

        if (isUsingCache)
        {
            CacheStore primaryCache = new CacheStore();

            primaryCache.setEntryExpiryDurationInMillis(getCacheExpirationInterval());
            primaryCache.setCacheCleanupInMillis(getCacheCleanupInterval());
            primaryCache.setEntryExpiryStrategy(CacheStore.ExpiryType.EXPIRE_AFTER_WRITE);
            primaryCache.setMaxCacheSize(getCacheSize());
            cacheManager.setPrimary(primaryCache);
        }
        else
        {
            Primary nullPrimary = new NullPrimary();
            cacheManager.setPrimary(nullPrimary);
        }

        cacheManager.setBackup(getStore());
        log.info("setup+, isUsingCache: {}", isUsingCache);
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
