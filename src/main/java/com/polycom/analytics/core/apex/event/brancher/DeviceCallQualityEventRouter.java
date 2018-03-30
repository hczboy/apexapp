package com.polycom.analytics.core.apex.event.brancher;

import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.DISCARDRATE_RTCPXR_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.DRUIDDS_CALLQUALITY;
import static com.polycom.analytics.core.apex.common.Constants.DRUIDDS_INTER_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.DRUIDDS_OUTOFBOUNDCALLQUALITY;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_CALLQUALITY;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.INGESTIONTIME_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.LOSSRATE_RTCPXR_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.ORGANIZATIONID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.RFACTOR_RTCPXR_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SERIALNUMBER_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.ListUtils;

import com.google.common.collect.Maps;

class DeviceCallQualityEventRouter extends AbstractDeviceCallEventRouter
{

    private static final List<String> RTCP_XR_FIELDS = Arrays.asList(LOSSRATE_RTCPXR_FIELD,
            DISCARDRATE_RTCPXR_FIELD, RFACTOR_RTCPXR_FIELD);
    private static final List<String> PASS_THROUGH_FIELDS = ListUtils.union(RTCP_XR_FIELDS,
            Arrays.asList(DEVICEID_FIELD, TENANTID_FIELD, SERIALNUMBER_FIELD, EVENTTYPE_FIELD,
                    ORGANIZATIONID_FIELD, INGESTIONTIME_FIELD));

    DeviceCallQualityEventRouter(DeviceCallEventBrancher deviceCallEventBrancher)
    {
        super(deviceCallEventBrancher, EVENTTYPE_CALLQUALITY);
        path_through_fields = PASS_THROUGH_FIELDS;
    }

    @Override
    public void routeEvent(Map<String, Object> tuple)
    {
        Map<String, Object> next = extractFields(tuple);
        Map<String, Object> next1 = Maps.newHashMap(next);
        next.put(DRUIDDS_INTER_FIELD, DRUIDDS_CALLQUALITY);
        deviceCallEventBrancher.druidOutput.emit(next);

        next1.put(DRUIDDS_INTER_FIELD, DRUIDDS_OUTOFBOUNDCALLQUALITY);
        deviceCallEventBrancher.druidOutput.emit(next1);

    }

}
