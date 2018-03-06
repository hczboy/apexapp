package com.polycom.analytics.core.apex.event.brancher;

import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_REBOOT;
import static com.polycom.analytics.core.apex.common.Constants.FINGERPRINTS_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.REBOOTTYPE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SERIALNUMBER_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

class DeviceRebootEventRouter extends AbstractDeviceEventRouter
{
    private static final List<String> PASS_THROUGH_FIELDS = Arrays.asList(DEVICEID_FIELD, TENANTID_FIELD,
            SERIALNUMBER_FIELD, EVENTTYPE_FIELD, FINGERPRINTS_FIELD, REBOOTTYPE_FIELD);

    DeviceRebootEventRouter(DeviceEventBrancher deviceEventBrancher)
    {
        super(deviceEventBrancher);

    }

    @Override
    public boolean isHandle(String eventType)
    {

        if (EVENTTYPE_REBOOT.equals(eventType))
        {
            return true;
        }

        return false;
    }

    @Override
    protected List<String> getPassThroughFields()
    {
        return PASS_THROUGH_FIELDS;

    }

    @Override
    public void routeEvent(Map<String, Object> tuple)
    {
        deviceEventBrancher.fingerprintEnricherOutput.emit(extractFields(tuple));

    }

}
