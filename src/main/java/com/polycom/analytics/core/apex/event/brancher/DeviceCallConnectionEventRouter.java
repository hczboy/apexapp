package com.polycom.analytics.core.apex.event.brancher;

import static com.polycom.analytics.core.apex.common.Constants.CALLID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.CONNECTIONSTATUS_CALLENDED;
import static com.polycom.analytics.core.apex.common.Constants.CONNECTIONSTATUS_CALLSTARTED;
import static com.polycom.analytics.core.apex.common.Constants.CONNECTIONSTATUS_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.DESCRIPTION_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_CALLCONNECTION;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.ORGANIZATIONID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SERIALNUMBER_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.polycom.analytics.core.apex.command.SendCurrentLogsCommandObj;
import com.polycom.analytics.core.apex.command.StartCallEventsCommandObj;
import com.polycom.analytics.core.apex.command.StopCallEventsCommandObj;
import com.polycom.analytics.core.apex.data.mongo.MongoUtil;
import com.polycom.analytics.core.apex.data.mongo.MongoUtil.ObjectIdInfo;

class DeviceCallConnectionEventRouter extends AbstractDeviceCallEventRouter
{
    private static final Logger log = LoggerFactory.getLogger(DeviceCallConnectionEventRouter.class);
    private static final List<String> PASS_THROUGH_FIELDS = Arrays.asList(DEVICEID_FIELD, TENANTID_FIELD,
            SERIALNUMBER_FIELD, EVENTTYPE_FIELD, ORGANIZATIONID_FIELD, CONNECTIONSTATUS_FIELD, DESCRIPTION_FIELD);

    DeviceCallConnectionEventRouter(DeviceCallEventBrancher deviceCallEventBrancher)
    {
        super(deviceCallEventBrancher, EVENTTYPE_CALLCONNECTION);
        path_through_fields = PASS_THROUGH_FIELDS;

    }

    @Override
    public void routeEvent(Map<String, Object> tuple)
    {
        handleConnectionStatus(tuple);
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

    private void handleConnectionStatus(Map<String, Object> tuple)
    {
        String connectionStatus = (String) tuple.get(CONNECTIONSTATUS_FIELD);
        String cmd;
        if (CONNECTIONSTATUS_CALLSTARTED.equals(connectionStatus))
        {
            StartCallEventsCommandObj startCallEventsCmdObj = StartCallEventsCommandObj.fromTuple(tuple);
            startCallEventsCmdObj.setCallID((String) tuple.get(CALLID_FIELD));
            cmd = startCallEventsCmdObj.toCmdString();

        }
        else if (CONNECTIONSTATUS_CALLENDED.equals(connectionStatus))
        {
            StopCallEventsCommandObj stopCallEventsCmdObj = StopCallEventsCommandObj.fromTuple(tuple);
            stopCallEventsCmdObj.setCallID((String) tuple.get(CALLID_FIELD));
            cmd = stopCallEventsCmdObj.toCmdString();
        }
        else
        {
            log.error("unexpected value:{} for connectionStatus", connectionStatus);
            return;
        }
        if (null != cmd)
        {
            deviceCallEventBrancher.cmdOutput.emit(cmd);
        }
    }

}
