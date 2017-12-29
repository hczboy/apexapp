package com.polycom.analytic.event.rule;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.mvel2.MVEL;

import com.datatorrent.api.Context.OperatorContext;

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
    public void activate(OperatorContext context)
    {
        MvelCacheRuleDefManager mvelRuleDefManager = new MvelCacheRuleDefManager();
        mvelRuleDefManager.setCacheSize(1024);
        mvelRuleDefManager.activate(context);
        ruleDefManager = mvelRuleDefManager;

    }

    @Override
    public void deactivate()
    {
        ((MvelCacheRuleDefManager) ruleDefManager).deactivate();

    }

}
