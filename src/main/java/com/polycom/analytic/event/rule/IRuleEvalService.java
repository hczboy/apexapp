package com.polycom.analytic.event.rule;

import java.util.Map;

import com.polycom.analytic.common.Initializable;

public interface IRuleEvalService extends Initializable
{
    boolean evaluate(String rule, Map<String, Object> target);

    static interface Action
    {
        void perform();
    }

    void evaluateAndDoAction(String rule, Map<String, Object> target, Action action);
}
