package com.polycom.analytics.core.apex.event.brancher;

import static com.polycom.analytics.core.apex.common.Constants.CALLISSUE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_INCALLERROR;
import static com.polycom.analytics.core.apex.common.Constants.ORGANIZATIONID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SERIALNUMBER_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.polycom.analytics.core.apex.command.SendCurrentLogsCommandObj;
import com.polycom.analytics.core.apex.data.mongo.MongoUtil;
import com.polycom.analytics.core.apex.data.mongo.MongoUtil.ObjectIdInfo;

class DeviceCallErrorEventRouter extends AbstractDeviceCallEventRouter
{

    private static final List<String> PASS_THROUGH_FIELDS = Arrays.asList(DEVICEID_FIELD, TENANTID_FIELD,
            SERIALNUMBER_FIELD, EVENTTYPE_FIELD, ORGANIZATIONID_FIELD, CALLISSUE_FIELD);

    DeviceCallErrorEventRouter(DeviceCallEventBrancher deviceCallEventBrancher)
    {
        super(deviceCallEventBrancher, EVENTTYPE_INCALLERROR);
        path_through_fields = PASS_THROUGH_FIELDS;
    }

    @Override
    public void routeEvent(Map<String, Object> tuple)
    {
        SendCurrentLogsCommandObj sendCurrentLogsCmdObj = SendCurrentLogsCommandObj.fromTuple(tuple);
        sendCurrentLogsCmdObj.setFileID(generateFileID());
        String sendCurrentLogsStr = sendCurrentLogsCmdObj.toCmdString();
        if (null != sendCurrentLogsStr)
        {
            deviceCallEventBrancher.cmdOutput.emit(sendCurrentLogsStr);
        }
    }

    private String generateFileID()
    {
        return MongoUtil.generateObjectIdStr(new ObjectIdInfo(deviceCallEventBrancher.getWindowId(),
                deviceCallEventBrancher.getOperatorId(), deviceCallEventBrancher.getTupleId()));
    }

}
