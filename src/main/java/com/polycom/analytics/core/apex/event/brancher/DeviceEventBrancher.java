package com.polycom.analytics.core.apex.event.brancher;

import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_FIELD;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

@Stateless
public class DeviceEventBrancher extends BaseOperator
{
    private static final Logger log = LoggerFactory.getLogger(DeviceEventBrancher.class);
    private transient List<IDeviceEventRouter> routerList;

    public final transient DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>()
    {
        @Override
        public void process(Map<String, Object> obj)
        {
            processTuple(obj);
        }
    };

    public final transient DefaultOutputPort<Map<String, Object>> fingerprintEnricherOutput = new DefaultOutputPort<>();

    @Override
    public void setup(OperatorContext context)
    {
        IDeviceEventRouter deviceAttachmentEventRouter = new DeviceAttachmentEventRouter(this);
        IDeviceEventRouter rebootEventRouter = new DeviceRebootEventRouter(this);
        routerList = Arrays.asList(deviceAttachmentEventRouter, rebootEventRouter);
    }

    protected void processTuple(Map<String, Object> tuple)
    {
        String eventType = (String) tuple.get(EVENTTYPE_FIELD);
        if (StringUtils.isEmpty(eventType))
        {
            log.error("field eventType is null or empty");
            return;
        }
        boolean isFound = false;
        for (IDeviceEventRouter deRouter : routerList)
        {
            if (deRouter.isHandle(eventType))
            {
                isFound = true;
                deRouter.routeEvent(tuple);
            }
        }
        if (!isFound)
        {
            log.error("NO suitable IDeviceEventRouter Found for eventType: {}", eventType);
        }
    }

}
