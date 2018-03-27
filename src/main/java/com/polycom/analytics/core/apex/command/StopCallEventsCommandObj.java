package com.polycom.analytics.core.apex.command;

import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.util.Map;

import javax.validation.constraints.NotNull;

import com.polycom.analytics.core.apex.command.CommandTemplateUtil.CommandType;

public class StopCallEventsCommandObj extends AbstractCommandObj
{
    @NotNull
    private String callID;

    public void setCallID(String callID)
    {
        this.callID = callID;
    }

    public static StopCallEventsCommandObj fromTuple(Map<String, Object> incomingTuple)
    {
        StopCallEventsCommandObj stopCallEventsCmdObj = new StopCallEventsCommandObj();
        stopCallEventsCmdObj.setDeviceId((String) incomingTuple.get(DEVICEID_FIELD));
        stopCallEventsCmdObj.setTenantId((String) incomingTuple.get(TENANTID_FIELD));
        return stopCallEventsCmdObj;
    }

    @Override
    protected String getCmdString()
    {
        return String.format(CommandTemplateUtil.getCmdTpl(CommandType.stopCallEvents), callID, deviceId,
                tenantId);
    }

}
