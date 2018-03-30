package com.polycom.analytics.core.apex.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

public final class JsonUtil
{

    private static final Logger log = LoggerFactory.getLogger(JsonUtil.class);

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

    public static List<String> findValuesFromFile(String fileName, String elementName)
    {
        final InputStream inputStream = JsonUtil.class.getClassLoader().getResourceAsStream(fileName);
        JsonNode jsonNode;
        try
        {
            jsonNode = objectMapper.readValue(inputStream, JsonNode.class);
        }
        catch (IOException e)
        {
            // TODO implement catch Exception
            log.error("Unexpected Exception", e);
            return Collections.EMPTY_LIST;

        }

        return jsonNode.findValuesAsText(elementName);

    }

    /*public static void main(String[] args)
    {
        System.out.println(findValuesFromFile("server.json", "dataSource"));
    }
    */
}
