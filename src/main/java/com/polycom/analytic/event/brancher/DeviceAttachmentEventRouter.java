package com.polycom.analytic.event.brancher;

import static com.polycom.analytic.common.Constants.EVENTTYPE_DEVICEATTACHMENT;

import java.util.Map;

class DeviceAttachmentEventRouter extends AbstractDeviceEventRouter
{
    public DeviceAttachmentEventRouter(DeviceEventBrancher deviceEventBrancher)
    {
        super(deviceEventBrancher);
    }

    @Override
    public boolean isHandle(String eventType)
    {
        if (EVENTTYPE_DEVICEATTACHMENT.equals(eventType))
        {
            return true;
        }

        return false;
    }

    @Override
    public void routeEvent(Map<String, Object> tuple)
    {
        deviceEventBrancher.deviceAttachmentOutput.emit(tuple);

    }

}
