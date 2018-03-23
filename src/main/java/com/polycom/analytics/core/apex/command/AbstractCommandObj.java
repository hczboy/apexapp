package com.polycom.analytics.core.apex.command;

import javax.validation.constraints.NotNull;

import com.polycom.analytics.core.apex.common.SimpleValidator;

public abstract class AbstractCommandObj extends SimpleValidator implements ICommandObj
{
    @NotNull
    protected String deviceId;
    @NotNull
    protected String tenantId;

    public String getDeviceId()
    {
        return deviceId;
    }

    public void setDeviceId(String deviceId)
    {
        this.deviceId = deviceId;
    }

    public String getTenantId()
    {
        return tenantId;
    }

    public void setTenantId(String tenantId)
    {
        this.tenantId = tenantId;
    }

    protected abstract String getCmdString();

    @Override
    public String toCmdString()
    {
        if (isValid())
        {
            return getCmdString();
        }
        return null;
    }

}
