package com.polycom.analytics.core.apex.command;

import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.util.Map;

import javax.validation.constraints.NotNull;

import com.polycom.analytics.core.apex.command.CommandTemplateUtil.CommandType;

public class SendInfoCommandObj extends AbstractCommandObj
{
    @NotNull
    private String trigger;
    @NotNull
    private String infoType;
    @NotNull
    private String serialNumber;

    public String getTrigger()
    {
        return trigger;
    }

    public void setTrigger(String trigger)
    {
        this.trigger = trigger;
    }

    public String getInfoType()
    {
        return infoType;
    }

    public void setInfoType(String infoType)
    {
        this.infoType = infoType;
    }

    public String getSerialNumber()
    {
        return serialNumber;
    }

    public void setSerialNumber(String serialNumber)
    {
        this.serialNumber = serialNumber;
    }

    public static SendInfoCommandObj fromTuple(Map<String, Object> incomingTuple)
    {
        SendInfoCommandObj sendInfoCmdObj = new SendInfoCommandObj();
        sendInfoCmdObj.setDeviceId((String) incomingTuple.get(DEVICEID_FIELD));
        sendInfoCmdObj.setTenantId((String) incomingTuple.get(TENANTID_FIELD));
        String eventType = (String) incomingTuple.get(EVENTTYPE_FIELD);
        String trigger = null;
        if (null != eventType)
        {
            trigger = eventType + "Event";
        }
        sendInfoCmdObj.setTrigger(trigger);
        return sendInfoCmdObj;
    }

    @Override
    protected String getCmdString()
    {
        return String.format(CommandTemplateUtil.getCmdTpl(CommandType.sendInfo), infoType, trigger, serialNumber,
                deviceId, tenantId);
    }

}
