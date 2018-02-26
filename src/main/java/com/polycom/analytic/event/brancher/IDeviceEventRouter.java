package com.polycom.analytic.event.brancher;

import java.util.Map;

public interface IDeviceEventRouter
{
    boolean isHandle(String eventType);

    void routeEvent(Map<String, Object> obj);
}
