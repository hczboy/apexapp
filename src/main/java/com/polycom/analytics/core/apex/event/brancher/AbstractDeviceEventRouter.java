package com.polycom.analytics.core.apex.event.brancher;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.Maps;

abstract class AbstractDeviceEventRouter implements IDeviceEventRouter
{
    @NotNull
    protected DeviceEventBrancher deviceEventBrancher;

    protected List<String> path_through_fields = Collections.EMPTY_LIST;

    @NotNull
    protected String eventTypeName;

    protected AbstractDeviceEventRouter(DeviceEventBrancher deviceEventBrancher, String eventName)
    {
        super();
        this.deviceEventBrancher = deviceEventBrancher;
        this.eventTypeName = eventName;
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
    public boolean isHandle(String eventType)
    {
        if (eventTypeName.equals(eventType))
        {
            return true;
        }

        return false;
    }

    @Override
    public void routeEvent(Map<String, Object> tuple)
    {
        deviceEventBrancher.fingerprintEnricherOutput.emit(extractFields(tuple));
    }

    protected List<String> getPassThroughFields()
    {
        return path_through_fields;
    }

}
