package com.polycom.analytic.tranquility;

import java.io.InputStream;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.FutureEventListener;

import scala.runtime.BoxedUnit;

public class TranquiliityOutputOperator
{
    private static final Logger log = LoggerFactory.getLogger(TranquiliityOutputOperator.class);

    public static void main(String[] args)
    {
        final InputStream configStream = TranquiliityOutputOperator.class.getClassLoader()
                .getResourceAsStream("server.json");
        final TranquilityConfig<PropertiesBasedConfig> config = TranquilityConfig.read(configStream);
        final DataSourceConfig<PropertiesBasedConfig> deviceEventConfig = config.getDataSource("deviceEvent");
        final Tranquilizer<Map<String, Object>> sender = DruidBeams.fromConfig(deviceEventConfig)
                .buildTranquilizer(deviceEventConfig.tranquilizerBuilder());
        sender.start();
        try
        {

            final Map<String, Object> obj = new ImmutableMap.Builder<String, Object>()
                    .put("uploadTime", new DateTime().toString())
                    .put("siteID", "7ff32290-7ff8-4140-ac17-3217e96291c2")
                    .put("roomID", "77f79221-3a8d-4984-a8e9-efccc7e84ea8")
                    .put("tenantID", "9384b5bf-52a1-40f0-8faa-83f9d82c49fd")
                    .put("customerID", "4254d035-f1f0-45b4-9a9c-013f9099235a")
                    .put("deviceID", "65f32291-89f8-4140-ac17-3217e9629178").put("macAddress", "00:04:F2:7B:7F:9F")
                    .put("serialNumber", "0004F27B7F9F").put("arrivalTime", "2017-11-15T11:49:22.659Z")
                    .put("realIP", "140.242.214.5").put("range", new String[] { "2364724736", "2364724991" })
                    .put("country", "AP").put("region", "").put("city", "").put("ll", new String[] { "35", "105" })
                    .put("metro", Integer.valueOf(0)).put("zip", Integer.valueOf(0))
                    .put("eventType", "serviceRegistrationStatus").put("eventTime", "2017-11-15T09:44:21.0003")
                    .put("serviceName", "Zoom").put("serviceID", Integer.valueOf(1))
                    .put("serverAddress", "zoom.us").put("status", Integer.valueOf(0))
                    .put("statusDescription", "up").put("username", "john.smith").build();

            sender.send(obj).addEventListener(new FutureEventListener<BoxedUnit>()
            {
                @Override
                public void onSuccess(BoxedUnit value)
                {
                    log.info("Sent message: {}", obj);
                }

                @Override
                public void onFailure(Throwable e)
                {
                    if (e instanceof MessageDroppedException)
                    {
                        log.warn("Dropped message: {}", obj, e);
                    }
                    else
                    {
                        log.error("Failed to send message: {}", obj, e);
                    }
                }
            });
            //  Await.result(future);
        }
        finally

        {
            sender.flush();
            sender.stop();
        }
    }

}
