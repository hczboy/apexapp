package com.polycom.analytic.tranquility;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.PropertiesBasedConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.metamx.tranquility.typeclass.JsonWriter;
import com.metamx.tranquility.typeclass.Timestamper;
import com.twitter.util.FutureEventListener;

import scala.runtime.BoxedUnit;

public class TranquiliityOutputOperator
{
    private static final Logger log = LoggerFactory.getLogger(TranquiliityOutputOperator.class);

    private static class TextObjectWriter extends JsonWriter<Map<String, Object>>
    {

        /**
         * 
         */
        private static final long serialVersionUID = 3268729852613502583L;

        private static ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public String contentType()
        {
            return MediaType.APPLICATION_JSON;
        }

        @Override
        public void viaJsonGenerator(Map<String, Object> event, JsonGenerator jg)
        {
            try
            {
                objectMapper.writeValue(jg, event);
            }
            catch (JsonGenerationException e)
            {
                // TODO implement catch JsonGenerationException
                log.error("Unexpected Exception", e);
                throw new UnsupportedOperationException("Unexpected Exception", e);

            }
            catch (JsonMappingException e)
            {
                // TODO implement catch JsonMappingException
                log.error("Unexpected Exception", e);
                throw new UnsupportedOperationException("Unexpected Exception", e);

            }
            catch (IOException e)
            {
                // TODO implement catch IOException
                log.error("Unexpected Exception", e);
                throw new UnsupportedOperationException("Unexpected Exception", e);

            }

        }
    }

    public static class MyTimestamper implements Timestamper<Map<String, Object>>
    {

        /**
         * 
         */
        private static final long serialVersionUID = 6340132443874849759L;

        @Override
        public DateTime timestamp(Map<String, Object> event)
        {
            return DateTime.parse((String) event.get("uploadTime"));

        }

    }

    public static void main(String[] args)
    {
        final InputStream configStream = TranquiliityOutputOperator.class.getClassLoader()
                .getResourceAsStream("server.json");
        final TranquilityConfig<PropertiesBasedConfig> config = TranquilityConfig.read(configStream);
        final DataSourceConfig<PropertiesBasedConfig> deviceEventConfig = config.getDataSource("deviceEvent");
        /* final Tranquilizer<Map<String, Object>> sender = DruidBeams.fromConfig(deviceEventConfig)
                .buildTranquilizer(deviceEventConfig.tranquilizerBuilder());*/

        final Tranquilizer<Map<String, Object>> sender = DruidBeams
                .fromConfig(deviceEventConfig, new MyTimestamper(), new TextObjectWriter())
                .buildTranquilizer(deviceEventConfig.tranquilizerBuilder());
        sender.start();

        try
        {

            final Map<String, Object> obj1 = new ImmutableMap.Builder<String, Object>()
                    .put("uploadTime", new DateTime().toString())
                    .put("siteID", "7ff32290-7ff8-4140-ac17-3217e96291c2")
                    .put("roomID", "77f79221-3a8d-4984-a8e9-efccc7e84ea8")
                    .put("tenantID", "9384b5bf-52a1-40f0-8faa-83f9d82c49fd")
                    .put("customerID", "4254d035-f1f0-45b4-9a9c-013f9099235a")
                    .put("deviceID", "65f32291-89f8-4140-ac17-3217e9629178").put("macAddress", "00:04:F2:7B:7F:9F")
                    .put("serialNumber", "0004F27B7F9F").put("arrivalTime", "2017-11-15T11:49:22.659Z")
                    .put("realIP", "140.242.214.5").put("range", Arrays.asList("2364724736", "2364724991"))
                    .put("country", "AP").put("region", "").put("city", "").put("ll", Arrays.asList("40", "110"))
                    .put("metro", Integer.valueOf(1)).put("zip", Integer.valueOf(0))
                    .put("eventType", "serviceRegistrationStatus").put("eventTime", "2017-11-15T09:44:21.0003")
                    .put("serviceName", "Zoom").put("serviceID", Integer.valueOf(2))
                    .put("serverAddress", "zoom.us").put("status", Integer.valueOf(0))
                    .put("statusDescription", "up").put("username", "john.smith").build();

            final Map<String, Object> obj2 = new ImmutableMap.Builder<String, Object>()
                    .put("uploadTime", new DateTime().toString()).put("siteID", "88887ff8-4140-ac17-3217e96291c2")
                    .put("roomID", "88f89221-3a8d-4984-a8e9-efccc7e84ea8")
                    .put("tenantID", "9384b5bf-52a1-40f0-8faa-83f9d82c49fd")
                    .put("customerID", "4254d035-f1f0-45b4-9a9c-013f9099235a")
                    .put("deviceID", "65f32291-89f8-4140-ac17-3217e9629178").put("macAddress", "00:04:F2:7B:7F:9F")
                    .put("serialNumber", "0004F27B7F9F").put("arrivalTime", "2017-11-15T12:00:22.659Z")
                    .put("realIP", "127.0.0.1").put("range", Arrays.asList("2364724736", "2364724991"))
                    .put("country", "AP").put("region", "").put("city", "").put("ll", Arrays.asList("40", "110"))
                    .put("metro", Integer.valueOf(1)).put("zip", Integer.valueOf(0))
                    .put("eventType", "deviceError").put("eventTime", "2017-11-15T09:44:21.0003")
                    .put("message", "Power insufficient").put("severity", "CRITICAL").build();

            for (final Map<String, Object> obj : Arrays.asList(obj1, obj2))
            {
                sender.send(obj).addEventListener(new FutureEventListener<BoxedUnit>()
                {
                    @Override
                    public void onSuccess(BoxedUnit value)
                    {
                        log.info("Sent message: {}", obj);
                        System.out.println(obj);
                    }

                    @Override
                    public void onFailure(Throwable e)
                    {
                        if (e instanceof MessageDroppedException)
                        {
                            log.warn("Dropped message: {}", obj, e);
                            System.out.println("drop m: " + obj);
                            e.printStackTrace();
                        }
                        else
                        {
                            log.error("Failed to send message: {}", obj, e);
                            System.out.println("failed m: " + obj);
                            e.printStackTrace();
                        }
                    }
                });
                //  Await.result(future);
            }
        }
        finally

        {
            sender.flush();
            sender.stop();
        }
    }

}
