package com.polycom.analytic.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

public final class JsonUtil
{
    private JsonUtil()
    {

    }

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static JsonNodeFactory getJsonNodeFactory()
    {
        return JsonNodeFactory.instance;
    }

    public static ObjectMapper getObjectMapper()
    {
        return objectMapper;
    }

}
