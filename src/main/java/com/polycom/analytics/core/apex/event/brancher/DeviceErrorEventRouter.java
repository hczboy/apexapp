package com.polycom.analytics.core.apex.event.brancher;

import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_DEVICEERROR;
import static com.polycom.analytics.core.apex.common.Constants.INTOTYPE_DEVICEHEALTHINFO;
import static com.polycom.analytics.core.apex.common.Constants.SERIALNUMBER_FIELD;

import java.util.Map;

import com.polycom.analytics.core.apex.command.SendInfoCommandObj;

class DeviceErrorEventRouter extends BasicDeviceEventRouter
{

    DeviceErrorEventRouter(DeviceEventBrancher deviceEventBrancher)
    {
        super(deviceEventBrancher, EVENTTYPE_DEVICEERROR);

    }

    @Override
    public void routeEvent(Map<String, Object> tuple)
    {

        SendInfoCommandObj sendInfoCmdObj = SendInfoCommandObj.fromTuple(tuple);
        sendInfoCmdObj.setInfoType(INTOTYPE_DEVICEHEALTHINFO);
        sendInfoCmdObj.setSerialNumber((String) tuple.get(SERIALNUMBER_FIELD));
        String sendInfoCmd = sendInfoCmdObj.toCmdString();
        if (null != sendInfoCmd)
        {
            deviceEventBrancher.cmdOutput.emit(sendInfoCmd);
        }

    }
}
