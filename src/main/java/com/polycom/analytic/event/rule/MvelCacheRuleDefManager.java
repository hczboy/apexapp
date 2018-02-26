package com.polycom.analytic.event.rule;

import java.io.Serializable;

import org.mvel2.MVEL;

public class MvelCacheRuleDefManager extends CacheRuleDefManager<Serializable>
{

    @Override
    protected Serializable customizeLoad(String ruleStr)
    {
        return MVEL.compileExpression(ruleStr);
    }

}
