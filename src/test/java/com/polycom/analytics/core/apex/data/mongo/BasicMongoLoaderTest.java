package com.polycom.analytics.core.apex.data.mongo;

import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.NETWORKINFO_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SECONDARYDEVICEINFO_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SERIALNUMBER_FIELD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datatorrent.lib.util.FieldInfo;

public class BasicMongoLoaderTest
{
    private BasicMongoLoader loader;

    @Before
    public void setUp()
    {
        loader = new BasicMongoLoader();
        loader.setHostName("172.21.120.143:27017");
        loader.setDataBase("device_fingerprints");
        List<FieldInfo> lookupFields = Arrays.asList(new FieldInfo(DEVICEID_FIELD, null, null),
                new FieldInfo(SERIALNUMBER_FIELD, null, null));
        List<FieldInfo> includeFields = Arrays.asList(new FieldInfo(SECONDARYDEVICEINFO_FIELD, null, null),
                new FieldInfo(NETWORKINFO_FIELD, null, null));
        loader.setFieldInfo(lookupFields, includeFields);
        loader.setCollectionName("fingerprints");
        loader.connect();
    }

    @Test
    public void test()
    {
        ArrayList<Object> r = (ArrayList<Object>) loader
                .get(new ArrayList<>(Arrays.asList("65f32291-89f8-4140-ac17-3217e9629188", "0004F27B7F9F")));
        for (Object obj : r)
        {
            System.out.println(obj);
            System.out.println(obj.getClass());
        }
    }

    @After
    public void tearDown()
    {
        loader.disconnect();
    }

}
