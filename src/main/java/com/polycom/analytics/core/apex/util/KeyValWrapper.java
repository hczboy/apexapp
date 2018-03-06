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

    public KeyValWrapper()
    {

    }

    public KeyValWrapper(String key, T value)
    {
        this.key = key;
        this.value = (T) value;
    }

    public String getKey()
    {
        return key;
    }

    public T getValue()
    {
        return value;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        KeyValWrapper other = (KeyValWrapper) obj;
        if (key == null)
        {
            if (other.key != null)
                return false;
        }
        else if (!key.equals(other.key))
            return false;
        if (value == null)
        {
            if (other.value != null)
                return false;
        }
        else if (!value.equals(other.value))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return new StringBuilder().append(key).append(":").append(value).toString();
    }

}
