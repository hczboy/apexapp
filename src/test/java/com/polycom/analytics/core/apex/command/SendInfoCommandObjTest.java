package com.polycom.analytics.core.apex.command;

import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SECONDARYDEVICEINFO_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

public class SendInfoCommandObjTest
{

    @Test
    public void test()
    {
        Map<String, Object> incomingTuple = Maps.newHashMapWithExpectedSize(5);
        incomingTuple.put(DEVICEID_FIELD, "d1");
        incomingTuple.put(TENANTID_FIELD, "t1");
        incomingTuple.put(EVENTTYPE_FIELD, "reboot");

        SendInfoCommandObj sendInfoCmdObj = SendInfoCommandObj.fromTuple(incomingTuple);
        sendInfoCmdObj.setInfoType(SECONDARYDEVICEINFO_FIELD);
        sendInfoCmdObj.setSerialNumber("64167f093255");
        String sendInfoCmd = sendInfoCmdObj.toCmdString();
        String expectCmd = "{\"category\":\"C2DP\",\"attr\":\"DeviceAnalyticsCommandPOC\",\"version\":\"0.0.1\",\"value\":{\"commandType\":\"sendInfo\",\"infoType\":\"secondaryDeviceInfo\",\"trigger\":\"rebootEvent\",\"serialNumber\":\"64167f093255\",\"deviceID\":\"d1\",\"tenantID\":\"t1\"}}";
        Assert.assertEquals(expectCmd, sendInfoCmd);

        sendInfoCmdObj.setInfoType(null);
        sendInfoCmdObj.setTenantId(null);
        sendInfoCmd = sendInfoCmdObj.toCmdString();
        Assert.assertNull(sendInfoCmd);
    }

}
