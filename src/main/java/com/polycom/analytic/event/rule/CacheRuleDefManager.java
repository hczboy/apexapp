package com.polycom.analytic.event.rule;

import java.util.concurrent.ExecutionException;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public abstract class CacheRuleDefManager<T>
        implements IRuleDefManager<T>, Operator.ActivationListener<Context.OperatorContext>
{

    @NotNull
    protected transient LoadingCache<String, T> cache;

    private static final Logger log = LoggerFactory.getLogger(CacheRuleDefManager.class);

    private static final int DEFAULT_CACHE_SIZE = 1024;

    @Min(0)
    protected int cacheSize = 0;

    public CacheRuleDefManager()
    {

    }

    @Override
    public void activate(OperatorContext context)
    {
        cacheSize = cacheSize <= 0 ? DEFAULT_CACHE_SIZE : cacheSize;
        cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(new CacheLoader<String, T>()
        {

            @Override
            public T load(String key) throws Exception
            {

                return customizeLoad(key);

            }

        });

        log.info("CacheRuleDefManager.activate+, cacheSize: {}", cacheSize);

    }

    @Override
    public void deactivate()
    {

    }

    public int getCacheSize()
    {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize)
    {
        this.cacheSize = cacheSize;
    }

    protected abstract T customizeLoad(String key);

    @Override
    public T getRuleDef(String ruleStr)
    {
        try
        {
            return cache.get(ruleStr);
        }
        catch (ExecutionException e)
        {
            // TODO implement catch ExecutionException
            log.error("got rule: {} failed", ruleStr, e);
            return customizeLoad(ruleStr);

        }

    }

}
