package com.polycom.analytics.core.apex.event.brancher;

import java.util.Map;

import javax.validation.constraints.NotNull;

class BasicDeviceEventRouter extends AbstractEventRouter implements IDeviceEventRouter
{
    @NotNull
    protected DeviceEventBrancher deviceEventBrancher;

    /* protected List<String> path_through_fields = Collections.EMPTY_LIST;
    
    @NotNull
    protected String eventTypeName;*/

    protected BasicDeviceEventRouter(DeviceEventBrancher deviceEventBrancher, String eventName)
    {
        super(eventName);
        this.deviceEventBrancher = deviceEventBrancher;

    }

    /* protected Map<String, Object> extractFields(Map<String, Object> tuple)
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
    
     protected List<String> getPassThroughFields()
    {
        return path_through_fields;
    }
    
    */

    @Override
    public void routeEvent(Map<String, Object> tuple)
    {
        deviceEventBrancher.fingerprintEnricherOutput.emit(extractFields(tuple));
    }

}
