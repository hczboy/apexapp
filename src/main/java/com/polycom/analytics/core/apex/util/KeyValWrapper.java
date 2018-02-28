package com.polycom.analytics.core.apex.util;

import com.fasterxml.jackson.annotation.JsonAnySetter;

public class KeyValWrapper<T>
{
    private String key;
    private T value;

    @JsonAnySetter
    public void set(String key, Object value)
    {
        this.key = key;
        this.value = (T) value;
    }

    @Override
    public String toString()
    {
        return new StringBuilder().append(key).append(":").append(value).toString();
    }

}
