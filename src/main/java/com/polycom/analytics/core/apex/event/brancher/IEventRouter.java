package com.polycom.analytics.core.apex.event.brancher;

import java.util.Map;

public interface IEventRouter
{
    boolean isHandle(String eventType);

    void routeEvent(Map<String, Object> obj);
}
