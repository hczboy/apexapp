package com.polycom.analytics.core.apex.event.rule;

public interface IRuleDefManager<T>
{
    T getRuleDef(String ruleStr);
}
