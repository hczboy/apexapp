package com.polycom.analytic.event.brancher;

import java.util.Map;

abstract class AbstractDeviceEventRouter implements IDeviceEventRouter
{
    protected DeviceEventBrancher deviceEventBrancher;

    public AbstractDeviceEventRouter(DeviceEventBrancher deviceEventBrancher)
    {
        super();
        this.deviceEventBrancher = deviceEventBrancher;
    }

    @Override
    public abstract boolean isHandle(String eventType);

    @Override
    public abstract void routeEvent(Map<String, Object> tuple);

}
