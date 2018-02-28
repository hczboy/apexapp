package com.polycom.analytics.core.apex.event.brancher;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.Maps;

abstract class AbstractDeviceEventRouter implements IDeviceEventRouter
{
    protected DeviceEventBrancher deviceEventBrancher;

    protected AbstractDeviceEventRouter(DeviceEventBrancher deviceEventBrancher)
    {
        super();
        this.deviceEventBrancher = deviceEventBrancher;
    }

    protected Map<String, Object> extractFields(Map<String, Object> tuple)
    {
        List<String> passThroughFields = getPassThroughFields();
        if (CollectionUtils.isEmpty(passThroughFields))
        {
            return tuple;
        }
        Map<String, Object> result = Maps.newHashMapWithExpectedSize(passThroughFields.size());
        for (String f : passThroughFields)
        {
            result.put(f, tuple.get(f));
        }
        return result;
    }

    @Override
    public abstract boolean isHandle(String eventType);

    @Override
    public abstract void routeEvent(Map<String, Object> tuple);

    protected List<String> getPassThroughFields()
    {
        return Collections.EMPTY_LIST;
    }

}
