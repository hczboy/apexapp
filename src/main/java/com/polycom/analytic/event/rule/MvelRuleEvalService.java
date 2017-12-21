package com.polycom.analytic.event.rule;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.mvel2.MVEL;

public class MvelRuleEvalService implements IRuleEvalService
{

    private IRuleDefManager<Serializable> ruleDefManager;

    private static final String EVENT_PLACEHOLDER = "event";

    public MvelRuleEvalService()
    {

    }

    @Override
    public boolean evaluate(String rule, Map<String, Object> target)
    {
        Map<String, Object> vars = new HashMap<>(3);
        vars.put(EVENT_PLACEHOLDER, target);
        return (Boolean) MVEL.executeExpression(ruleDefManager.getRuleDef(rule), vars);
    }

    @Override
    public void evaluateAndDoAction(String rule, Map<String, Object> target, Action action)
    {
        if (evaluate(rule, target))
        {
            action.perform();
        }

    }

    @Override
    public void init()
    {
        ruleDefManager = new MvelCacheRuleDefManager();
        ((MvelCacheRuleDefManager) ruleDefManager).setCacheSize(500);
        ((MvelCacheRuleDefManager) ruleDefManager).init();
    }

}
