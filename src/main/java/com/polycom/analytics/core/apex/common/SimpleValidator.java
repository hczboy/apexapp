package com.polycom.analytics.core.apex.common;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.polycom.analytics.core.apex.util.ValidationUtil;

public class SimpleValidator implements IValidable
{
    private static final Logger log = LoggerFactory.getLogger(SimpleValidator.class);

    @Override
    public boolean isValid()
    {
        Set< ? > violationSet = ValidationUtil.getValidator().validate(this);
        if (!violationSet.isEmpty())
        {
            log.error(violationSet.toString());

        }
        return violationSet.isEmpty();
    }

}
