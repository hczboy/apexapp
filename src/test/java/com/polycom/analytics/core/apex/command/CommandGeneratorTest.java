package com.polycom.analytics.core.apex.command;

import static com.polycom.analytics.core.apex.command.CommandGenerator.INFOTYPE_CMD;
import static com.polycom.analytics.core.apex.command.CommandGenerator.SERIALNUMBER_CMD;
import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.polycom.analytics.core.apex.command.CommandGenerator.CommandType;

public class CommandGeneratorTest
{

    @Test
    public void test()
    {
        /*Object[] paras = new Object[] { "secondaryDeviceInfo", "rebootEvent", "64167f093255", "d1", "t1" };
        String s = CommandGenerator.generateSendInfoCmd(paras);
        String expectCmd = "{\"categody\":\"C2DP\",\"attr\":\"DeviceAnalyticsCommandPOC\",\"version\":\"0.0.1\",\"value\":{\"commandType\":\"sendInfo\",\"infoType\":\"secondaryDeviceInfo\",\"trigger\":\"rebootEvent\",\"id\":\"64167f093255\",\"deviceID\":\"d1\",\"tenantID\":\"t1\"}}";
        Assert.assertEquals(expectCmd, s);*/
        Map<String, Object> incomingTuple = Maps.newHashMapWithExpectedSize(5);
        incomingTuple.put(DEVICEID_FIELD, "d1");
        incomingTuple.put(TENANTID_FIELD, "t1");
        incomingTuple.put(EVENTTYPE_FIELD, "reboot");

        Map<String, Object> sendInfoTuple = CommandGenerator.constructCmdTupleFromIncomingTuple(incomingTuple,
                CommandType.sendInfo);
        sendInfoTuple.put(INFOTYPE_CMD, "secondaryDeviceInfo");
        sendInfoTuple.put(SERIALNUMBER_CMD, "64167f093255");
        String s = CommandGenerator.generateCmd(sendInfoTuple, CommandType.sendInfo);
        String expectCmd = "{\"category\":\"C2DP\",\"attr\":\"DeviceAnalyticsCommandPOC\",\"version\":\"0.0.1\",\"value\":{\"commandType\":\"sendInfo\",\"infoType\":\"secondaryDeviceInfo\",\"trigger\":\"rebootEvent\",\"serialNumber\":\"64167f093255\",\"deviceID\":\"d1\",\"tenantID\":\"t1\"}}";
        Assert.assertEquals(expectCmd, s);
    }

}
