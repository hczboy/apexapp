package com.polycom.analytics.core.apex.event.brancher;

import static com.polycom.analytics.core.apex.common.Constants.ATTACHEDSERIALNUMBER_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.ATTACHMENTSTATE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_DEVICEATTACHMENT;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.FINGERPRINTS_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SERIALNUMBER_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DeviceAttachmentEventRouter extends AbstractDeviceEventRouter
{
    private static final List<String> PASS_THROUGH_FIELDS = Arrays.asList(DEVICEID_FIELD, TENANTID_FIELD,
            SERIALNUMBER_FIELD, EVENTTYPE_FIELD, FINGERPRINTS_FIELD, ATTACHEDSERIALNUMBER_FIELD,
            ATTACHMENTSTATE_FIELD);

    private static final Logger log = LoggerFactory.getLogger(DeviceAttachmentEventRouter.class);

    DeviceAttachmentEventRouter(DeviceEventBrancher deviceEventBrancher)
    {
        super(deviceEventBrancher, EVENTTYPE_DEVICEATTACHMENT);
        path_through_fields = PASS_THROUGH_FIELDS;
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
            deviceEventBrancher.fingerprintEnricherOutput.emit(extractFields(tuple));
        }

    }

}
