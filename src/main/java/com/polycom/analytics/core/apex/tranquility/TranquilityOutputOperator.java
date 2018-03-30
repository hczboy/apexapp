package com.polycom.analytics.core.apex.tranquility;

import static com.polycom.analytics.core.apex.common.Constants.DRUIDDS_INTER_FIELD;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import javax.validation.constraints.Min;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.CollectionUtils;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.metamx.tranquility.typeclass.JsonWriter;
import com.metamx.tranquility.typeclass.Timestamper;
import com.polycom.analytics.core.apex.util.JsonUtil;

public class TranquilityOutputOperator extends BaseOperator
        implements Operator.ActivationListener<Context.OperatorContext>
{
    private static final Logger log = LoggerFactory.getLogger(TranquilityOutputOperator.class);

    //rivate transient TranquilitySender tranquilitySender;

    private static final String DATASOURCE = "dataSource";

    public static final String CONFIG_FILE_NAME = "server.json";

    private transient Map<String, TranquilitySender> dsToTranquilitySenderMap;

    private Map<String, BlockingQueue<Map<String, Object>>> dsToPendingEventQueueMap;

    // private ArrayBlockingQueue<Map<String, Object>> pendingEventQueue;

    public Map<String, BlockingQueue<Map<String, Object>>> getDsToPendingEventQueueMap()
    {
        return dsToPendingEventQueueMap;
    }

    @Min(1)
    private int pendingEventQueueSize = 1024;

    @Min(1)
    private int senderThreadCount = 2;

    private List<String> dataSources;

    /*public void setDataSources(String dataSources)
    {
        this.dataSources = Iterables.toArray(Splitter.on(',').trimResults().omitEmptyStrings().split(dataSources),
                String.class);
    }*/

    public int getSenderThreadCount()
    {
        return senderThreadCount;
    }

    public void setSenderThreadCount(int senderThreadCount)
    {
        this.senderThreadCount = senderThreadCount;
    }

    private transient int operatorId;

    private transient String appName;

    /* ArrayBlockingQueue<Map<String, Object>> getPendingEventQueue()
    {
        return pendingEventQueue;
    }
    
    void setPendingEventQueue(ArrayBlockingQueue<Map<String, Object>> pendingEventQueue)
    {
        this.pendingEventQueue = pendingEventQueue;
    }*/

    public int getPendingEventQueueSize()
    {
        return pendingEventQueueSize;
    }

    public void setPendingEventQueueSize(int pendingEventQueueSize)
    {
        this.pendingEventQueueSize = pendingEventQueueSize;
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    public String getAppName()
    {
        return appName;
    }

    @Override
    public void setup(OperatorContext context)
    {
        appName = context.getValue(Context.DAGContext.APPLICATION_NAME);
        operatorId = context.getId();
        dataSources = JsonUtil.findValuesFromFile(CONFIG_FILE_NAME, DATASOURCE);
        if (CollectionUtils.isNullOrEmpty(dataSources))
        {
            String msg = String.format("[FATAL]NOT found druid dataSources in application[%s].operator[%s]",
                    appName, operatorId);
            log.error(msg);
            throw new IllegalStateException(msg);
        }

        if (null == dsToPendingEventQueueMap)
        {
            dsToPendingEventQueueMap = Maps.newHashMapWithExpectedSize(dataSources.size());
        }
        dsToTranquilitySenderMap = Maps.newHashMapWithExpectedSize(dataSources.size());
        TranquilitySender tranquilitySender;
        for (String dataSource : dataSources)
        {

            tranquilitySender = new TranquilitySender(dataSource, this);
            dsToTranquilitySenderMap.put(dataSource, tranquilitySender);
        }

        log.info("TranquilityOutputOperator is setup");
    }

    public final transient DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>()
    {

        @Override
        public void process(Map<String, Object> tuple)
        {
            processTuple(tuple);

        }
    };

    private void processTuple(Map<String, Object> tuple)
    {
        log.info("8888:{}", tuple);
        String druidDataSource = (String) tuple.remove(DRUIDDS_INTER_FIELD);

        if (StringUtils.isNotEmpty(druidDataSource))
        {

            TranquilitySender sender = dsToTranquilitySenderMap.get(druidDataSource);
            if (null != sender)
            {
                sender.putEvent(tuple);
            }
            else
            {
                log.error("NOT found TranquilitySender for dataSource:{}", druidDataSource);
                return;
            }

        }
        else
        {
            log.error("NO druid dataSource found, dataSource:{}", druidDataSource);
            return;
        }
    }

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

    /* public static void main(String[] args)
    {
        final InputStream configStream = TranquilityOutputOperator.class.getClassLoader()
                .getResourceAsStream("server.json");
        final TranquilityConfig<PropertiesBasedConfig> config = TranquilityConfig.read(configStream);
        final DataSourceConfig<PropertiesBasedConfig> deviceEventConfig = config
                .getDataSource("outOfBoundCallQuality");
        final Tranquilizer<Map<String, Object>> sender = DruidBeams.fromConfig(deviceEventConfig)
                .buildTranquilizer(deviceEventConfig.tranquilizerBuilder());
    
        final Tranquilizer<Map<String, Object>> sender = DruidBeams
                .fromConfig(deviceEventConfig, new MyTimestamper(), new TextObjectWriter())
                .buildTranquilizer(deviceEventConfig.tranquilizerBuilder());
        sender.start();
    
        try
        {
    
            String srJson = "{\"ingestionTime\":\"" + new DateTime().toString()
                    + "\",\"organizationID\":\"4254d035-f1f0-45b4-9a9c-013f9099235a\",\"deviceID\":\"65f32291-89f8-4140-ac17-3217e9629178\",\"serialNumber\":\"0004F27B7F9F\",\"eventType\":\"callQuality\",\"rFactor\":88,\"lossRate\":20,\"discardRate\":10}";
            Map<String, Object> obj1 = JSON.parseObject(srJson);
    
             String errJson = "{\"uploadTime\":\"" + new DateTime().toString()
                    + "\",\"siteID\":\"7ff3-7ff8-4140-ac17-3217e96291c2\",\"roomID\":\"77f79221-3a8d-4984-a8e9-efccc7e84ea8\",\"tenantID\":\"9384b5bf-52a1-40f0-8faa-83f9d82c49fd\",\"customerID\":\"4254d035-f1f0-45b4-9a9c-013f9099235a\",\"deviceID\":\"65f32291-89f8-4140-ac17-3217e9629178\",\"macAddress\":\"00:04:F2:7B:7F:9F\",\"serialNumber\":\"0004F27B7F9F\",\"arrivalTime\":\"2017-11-15T11:49:22.659Z\",\"realIP\":\"140.242.214.5\",\"range\":[2364724736,2364724991],\"country\":\"AP\",\"region\":\"\",\"city\":\"\",\"ll\":[35,105],\"metro\":0,\"zip\":0,\"eventType\":\"deviceError\",\"eventTime\":\"2017-11-01T08:44:55.0003\",\"message\":\"Power insufficient\",\"severity\":\"CRITICAL\"}";
            Map<String, Object> obj2 = JSON.parseObject(errJson);
    
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
    
            for (final Map<String, Object> obj : Arrays.asList(obj1))
            {
                System.out.println("sending thread: " + Thread.currentThread().getName());
                sender.send(obj).addEventListener(new FutureEventListener<BoxedUnit>()
                {
                    @Override
                    public void onSuccess(BoxedUnit value)
                    {
                        log.info("Sent message: {}", obj);
                        System.out.println("succ: " + obj);
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
    }*/

    @Override
    public void activate(OperatorContext context)
    {
        for (String dataSource : dsToTranquilitySenderMap.keySet())
        {
            dsToTranquilitySenderMap.get(dataSource).start();
        }
        /*tranquilitySender.start();*/

    }

    @Override
    public void deactivate()
    {
        for (String dataSource : dsToTranquilitySenderMap.keySet())
        {
            dsToTranquilitySenderMap.get(dataSource).stop();
        }
        /* tranquilitySender.stop();*/

    }

}
