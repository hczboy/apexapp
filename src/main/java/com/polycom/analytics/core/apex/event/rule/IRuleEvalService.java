package com.polycom.analytics.core.apex.event.rule;

import java.util.Map;

import com.datatorrent.api.Context;
import com.datatorrent.api.Operator.ActivationListener;

public interface IRuleEvalService extends ActivationListener<Context.OperatorContext>
{
    boolean evaluate(String rule, Map<String, Object> target);

    static interface Action
    {
        void perform();
    }

    void evaluateAndDoAction(String rule, Map<String, Object> target, Action action);
}
