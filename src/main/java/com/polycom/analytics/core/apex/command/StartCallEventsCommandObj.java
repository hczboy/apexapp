package com.polycom.analytics.core.apex.command;

import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.util.Map;

import javax.validation.constraints.NotNull;

import com.polycom.analytics.core.apex.command.CommandTemplateUtil.CommandType;

public class StartCallEventsCommandObj extends AbstractCommandObj
{

    @NotNull
    private String callID;
    @NotNull
    private String url;

    public static StartCallEventsCommandObj fromTuple(Map<String, Object> incomingTuple)
    {
        StartCallEventsCommandObj startCallEventsCmdObj = new StartCallEventsCommandObj();
        startCallEventsCmdObj.setDeviceId((String) incomingTuple.get(DEVICEID_FIELD));
        startCallEventsCmdObj.setTenantId((String) incomingTuple.get(TENANTID_FIELD));
        startCallEventsCmdObj.url = "ws://websocket.com";
        return startCallEventsCmdObj;
    }

    public void setCallID(String callID)
    {
        this.callID = callID;
    }

    @Override
    protected String getCmdString()
    {
        return String.format(CommandTemplateUtil.getCmdTpl(CommandType.startCallEvents), callID, url, deviceId,
                tenantId);

    }

}
