package com.polycom.analytic.data;

import java.util.Arrays;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.ArrayUtils;

public class Criteria
{
    @NotNull
    private String tableName;
    @NotNull
    private String condition;
    private Object[] paras;
    @NotNull
    private Class< ? > returnType;

    public Criteria()
    {
        super();

    }

    public Criteria(String tableName, String condition, Object[] paras, Class< ? > returnType)
    {
        super();
        this.tableName = tableName;
        this.condition = condition;
        this.paras = paras;
        this.returnType = returnType;
    }

    public Object[] getParas()
    {
        return paras;
    }

    public void setParas(Object[] paras)
    {
        this.paras = paras;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public String getCondition()
    {
        return condition;
    }

    public void setCondition(String condition)
    {
        this.condition = condition;
    }

    public Class< ? > getReturnType()
    {
        return returnType;
    }

    public void setReturnType(Class< ? > returnType)
    {
        this.returnType = returnType;
    }

    public boolean isConditionTemplate()
    {
        return ArrayUtils.isNotEmpty(paras);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((condition == null) ? 0 : condition.hashCode());
        result = prime * result + Arrays.hashCode(paras);
        result = prime * result + ((returnType == null) ? 0 : returnType.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
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
        Criteria other = (Criteria) obj;
        if (condition == null)
        {
            if (other.condition != null)
                return false;
        }
        else if (!condition.equals(other.condition))
            return false;
        if (!Arrays.equals(paras, other.paras))
            return false;
        if (returnType == null)
        {
            if (other.returnType != null)
                return false;
        }
        else if (!returnType.equals(other.returnType))
            return false;
        if (tableName == null)
        {
            if (other.tableName != null)
                return false;
        }
        else if (!tableName.equals(other.tableName))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "Criteria [tableName=" + tableName + ", condition=" + condition + ", paras="
                + Arrays.toString(paras) + ", returnType=" + returnType + "]";
    }

}
