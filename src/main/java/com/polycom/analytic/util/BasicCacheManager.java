package com.polycom.analytic.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.cache.CacheManager.Backup;
import com.datatorrent.lib.db.cache.CacheManager.Primary;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * @author czhhu
 * 
 * This BasicCacheManager provides same implementation as com.datatorrent.lib.db.cache.CacheManager
 * except changing the behavior of method close() to fix 
 * issue of NullPointerException when call close()
 *
 */
public class BasicCacheManager implements Closeable
{
    @NotNull
    protected Primary primary;
    @NotNull
    protected Backup backup;
    protected String refreshTime;
    private transient Timer refresher;

    public BasicCacheManager()
    {

    }

    public void initialize() throws IOException
    {
        primary.connect();
        backup.connect();
        Map<Object, Object> initialEntries = backup.loadInitialData();
        if (initialEntries != null)
        {
            primary.putAll(initialEntries);
        }

        if (!Strings.isNullOrEmpty(refreshTime))
        {

            String[] parts = refreshTime.split("[:\\s]");

            Calendar timeToRefresh = Calendar.getInstance();
            timeToRefresh.set(Calendar.HOUR_OF_DAY, Integer.parseInt(parts[0]));
            if (parts.length >= 2)
            {
                timeToRefresh.set(Calendar.MINUTE, Integer.parseInt(parts[1]));
            }
            if (parts.length >= 3)
            {
                timeToRefresh.set(Calendar.SECOND, Integer.parseInt(parts[2]));
            }
            long initialDelay = timeToRefresh.getTimeInMillis() - Calendar.getInstance().getTimeInMillis();

            TimerTask task = new TimerTask()
            {
                @Override
                public void run()
                {
                    List<Object> keysToRefresh = Lists.newArrayList(primary.getKeys());
                    if (keysToRefresh.size() > 0)
                    {
                        List<Object> refreshedValues = backup.getAll(keysToRefresh);
                        if (refreshedValues != null)
                        {
                            for (int i = 0; i < keysToRefresh.size(); i++)
                            {
                                primary.put(keysToRefresh.get(i), refreshedValues.get(i));
                            }
                        }
                    }
                }
            };

            refresher = new Timer();
            if (initialDelay < 0)
            {
                refresher.schedule(task, 0);
                timeToRefresh.add(Calendar.DAY_OF_MONTH, 1);
                initialDelay = timeToRefresh.getTimeInMillis();
            }
            refresher.scheduleAtFixedRate(task, initialDelay, 86400000);
        }
    }

    @Nullable
    public Object get(@Nonnull Object key)
    {
        Object primaryVal = primary.get(key);
        if (primaryVal != null)
        {
            log.info("return from cache: {}->{}", key, primaryVal);
            return primaryVal;
        }

        Object backupVal = backup.get(key);
        if (backupVal != null)
        {
            primary.put(key, backupVal);
        }
        log.info("return from backup: {}->{}", key, backupVal);
        return backupVal;
    }

    public void put(@Nonnull Object key, @Nonnull Object value)
    {
        primary.put(key, value);
        backup.put(key, value);
    }

    @Override
    public void close() throws IOException
    {
        if (refresher != null)
        {
            refresher.cancel();
        }
        primary.disconnect();
        backup.disconnect();
    }

    public void setPrimary(Primary primary)
    {
        this.primary = primary;
    }

    public Primary getPrimary()
    {
        return primary;
    }

    public void setBackup(Backup backup)
    {
        this.backup = backup;
    }

    public Backup getBackup()
    {
        return backup;
    }

    /**
     * The cache store can be refreshed every day at a specific time. This sets
     * the time. If the time is not set, cache is not refreshed.
     *
     * @param time time at which cache is refreshed everyday. Format is HH:mm:ss Z.
     */
    public void setRefreshTime(String time)
    {
        refreshTime = time;
    }

    public String getRefreshTime()
    {
        return refreshTime;
    }

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(BasicCacheManager.class);
}
