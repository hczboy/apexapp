package com.polycom.analytics.core.apex.event.brancher;

import javax.validation.constraints.NotNull;

abstract class AbstractDeviceCallEventRouter extends AbstractEventRouter implements IDeviceCallEventRouter
{

    AbstractDeviceCallEventRouter(DeviceCallEventBrancher deviceCallEventBrancher, String eventName)
    {
        super(eventName);
        this.deviceCallEventBrancher = deviceCallEventBrancher;

    }

    @NotNull
    protected DeviceCallEventBrancher deviceCallEventBrancher;

}
