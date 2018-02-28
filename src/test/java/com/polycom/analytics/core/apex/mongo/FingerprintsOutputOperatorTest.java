package com.polycom.analytics.core.apex.mongo;

import static com.polycom.analytics.core.apex.common.Constants.ATTACHEDDEVICE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.FINGERPRINT_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.INFOTYPE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SERIALNUMBER_FIELD;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datatorrent.stram.engine.OperatorContext;
import com.google.common.collect.Maps;
import com.polycom.analytics.core.apex.mongo.FingerprintsOutputOperator;

public class FingerprintsOutputOperatorTest
{

    private FingerprintsOutputOperator fingerprintsOutputOperator;

    @Before
    public void setUp() throws Exception
    {
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader) cl).getURLs();

        for (URL url : urls)
        {
            System.out.println(url.getFile());
        }
        /*System.setProperty("DEBUG.MONGO", "true");
        
        // Enable DB operation tracing  
        System.setProperty("DB.TRACE", "true");*/
        fingerprintsOutputOperator = new FingerprintsOutputOperator();
        fingerprintsOutputOperator.setCollectionName("fingerprints");
        fingerprintsOutputOperator.setDataBase("device_fingerprints");
        fingerprintsOutputOperator.setHostName("172.21.120.143:27017");
        OperatorContext context = new OperatorContext(1, "fingerprintUpdater", null, null);
        fingerprintsOutputOperator.setup(context);
    }

    @After
    public void tearDown() throws Exception
    {

        fingerprintsOutputOperator.teardown();
    }

    @Test
    public void test()
    {
        Map<String, Object> tuple = Maps.newHashMap();
        // String secDevices = "{\"connectionType\":\"USB\",\"serialNumber\":\"2014F27B7F99\",\"macAddress\":\"00:04:F2:7A:7F:9F\",\"peripheralType\":\"VVX Camera\",\"displayName\":\"Camera\",\"wifiAddress\":\"00:04:F2:6B:7F:9F\",\"bluetoothAddress\":\"000666422152\",\"powerSource\":\"PoE\",\"deviceSignature\":\"Dev\",\"attachmentState\":0,\"fingerprint\":\"283ee18a0684f19f7ededb9229e4a5af\"}, {\"serialNumber\":\"2018F27B7AAA\", \"fingerprint\":\"283ee18a0684f19f7ededb9229e66666\"}";
        String secDevices = "{\"serialNumber\":\"2018F27B7AAA\", \"fingerprint\":\"283ee18a0684f19f7ededb9229e66666\"}";
        tuple.put(DEVICEID_FIELD, "65f32291-89f8-4140-ac17-3217e9629188");
        tuple.put(SERIALNUMBER_FIELD, "0004F27B7F9F");
        //tuple.put(INFOTYPE_FIELD, "primaryDeviceInfo");
        /*tuple.put(INFOTYPE_FIELD, "networkInfo");
        tuple.put(FINGERPRINT_FIELD, "fb5e76d565d76d7031f8d5863fbf12cc");*/
        tuple.put(INFOTYPE_FIELD, "secondaryDeviceInfo");
        tuple.put(ATTACHEDDEVICE_FIELD, secDevices);
        fingerprintsOutputOperator.beginWindow(4);
        fingerprintsOutputOperator.processTuple(tuple);
        tuple.put(DEVICEID_FIELD, "65f32291-89f8-4140-ac17-3217e9629188");
        tuple.put(INFOTYPE_FIELD, "networkInfo");
        tuple.put(FINGERPRINT_FIELD, "fb5e76d565d76d7031f8d5863fbf12ff");
        fingerprintsOutputOperator.processTuple(tuple);
        fingerprintsOutputOperator.endWindow();

    }

}
