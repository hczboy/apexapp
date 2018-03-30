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
public class DeviceCallEventBrancher extends BaseOperator
{
    private static final Logger log = LoggerFactory.getLogger(DeviceCallEventBrancher.class);
    private transient List<IDeviceCallEventRouter> routerList;

    protected transient long windowId;

    protected transient int operatorId;
    protected transient int tupleId;

    public final transient DefaultOutputPort<Map<String, Object>> druidOutput = new DefaultOutputPort<>();

    public final transient DefaultOutputPort<String> cmdOutput = new DefaultOutputPort<>();
    public final transient DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>()
    {
        @Override
        public void process(Map<String, Object> obj)
        {
            processTuple(obj);
        }

    };

    @Override
    public void setup(OperatorContext context)
    {
        this.operatorId = context.getId();
        IDeviceCallEventRouter deviceCallErrorEventRouter = new DeviceCallErrorEventRouter(this);
        IDeviceCallEventRouter deviceCallConnectionEventRouter = new DeviceCallConnectionEventRouter(this);
        IDeviceCallEventRouter deviceCallQulityEventRouter = new DeviceCallQualityEventRouter(this);
        routerList = Arrays.asList(deviceCallErrorEventRouter, deviceCallConnectionEventRouter,
                deviceCallQulityEventRouter);
    }

    @Override
    public void beginWindow(@SuppressWarnings("hiding") long windowId)
    {
        this.windowId = windowId;
        this.tupleId = 1;

    }

    public int getOperatorId()
    {
        return operatorId;
    }

    public long getWindowId()
    {
        return windowId;
    }

    public int getTupleId()
    {
        return tupleId;
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
        for (IDeviceCallEventRouter dceRouter : routerList)
        {
            if (dceRouter.isHandle(eventType))
            {
                isFound = true;
                dceRouter.routeEvent(tuple);
                tupleId++;
            }
        }
        if (!isFound)
        {
            log.error("NO suitable IDeviceCallEventRouter Found for eventType: {}", eventType);
        }

    }
}
