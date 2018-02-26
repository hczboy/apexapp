package com.polycom.analytic.event.brancher;

import static com.polycom.analytic.common.Constants.ATTACHEDSERIALNUMBER_FIELD;
import static com.polycom.analytic.common.Constants.ATTACHMENTSTATE_FIELD;
import static com.polycom.analytic.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytic.common.Constants.EVENTTYPE_DEVICEATTACHMENT;
import static com.polycom.analytic.common.Constants.EVENTTYPE_FIELD;
import static com.polycom.analytic.common.Constants.FINGERPRINTS_FIELD;
import static com.polycom.analytic.common.Constants.SERIALNUMBER_FIELD;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DeviceAttachmentEventRouter extends AbstractDeviceEventRouter
{
    private static final List<String> PASS_THROUGH_FIELDS = Arrays.asList(DEVICEID_FIELD, SERIALNUMBER_FIELD,
            EVENTTYPE_FIELD, FINGERPRINTS_FIELD, ATTACHEDSERIALNUMBER_FIELD, ATTACHMENTSTATE_FIELD);

    private static final Logger log = LoggerFactory.getLogger(DeviceAttachmentEventRouter.class);

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
    protected List<String> getPassThroughFields()
    {
        return PASS_THROUGH_FIELDS;

    }

    @Override
    public void routeEvent(Map<String, Object> tuple)
    {
        Integer attachmentState = (Integer) tuple.get(ATTACHMENTSTATE_FIELD);
        if (null == attachmentState)
        {
            log.error("field: {} is null", ATTACHMENTSTATE_FIELD);
            return;
        }
        if (attachmentState.intValue() != 0)
        {
            deviceEventBrancher.deviceAttachmentOutput.emit(extractFields(tuple));
        }

    }

}
