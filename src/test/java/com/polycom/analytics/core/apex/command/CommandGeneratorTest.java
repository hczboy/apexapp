package com.polycom.analytics.core.apex.command;

import org.junit.Assert;
import org.junit.Test;

public class CommandGeneratorTest
{

    @Test
    public void test()
    {
        Object[] paras = new Object[] { "secondaryDeviceInfo", "rebootEvent", "64167f093255", "d1", "t1" };
        String s = CommandGenerator.generateSendInfoCmd(paras);
        String expectCmd = "{\"categody\":\"C2DP\",\"attr\":\"DeviceAnalyticsCommandPOC\",\"version\":\"0.0.1\",\"value\":{\"commandType\":\"sendInfo\",\"infoType\":\"secondaryDeviceInfo\",\"trigger\":\"rebootEvent\",\"id\":\"64167f093255\",\"deviceID\":\"d1\",\"tenantID\":\"t1\"}}";
        Assert.assertEquals(expectCmd, s);
    }

}
