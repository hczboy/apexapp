package com.polycom.analytics.core.apex.util;

import java.util.Collections;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.executable.ExecutableValidator;
import javax.validation.metadata.BeanDescriptor;

public class ValidationUtil
{
    private ValidationUtil()
    {

    }

    private static class EmptyValidatior implements Validator
    {

        @Override
        public <T> Set<ConstraintViolation<T>> validate(T object, Class< ? >... groups)
        {
            return Collections.EMPTY_SET;

        }

        @Override
        public <T> Set<ConstraintViolation<T>> validateProperty(T object, String propertyName,
                Class< ? >... groups)
        {
            return Collections.EMPTY_SET;

        }

        @Override
        public <T> Set<ConstraintViolation<T>> validateValue(Class<T> beanType, String propertyName, Object value,
                Class< ? >... groups)
        {
            return Collections.EMPTY_SET;

        }

        @Override
        public BeanDescriptor getConstraintsForClass(Class< ? > clazz)
        {
            return null;

        }

        @Override
        public <T> T unwrap(Class<T> type)
        {
            return null;

        }

        @Override
        public ExecutableValidator forExecutables()
        {
            return null;
        }

    }

    private static ValidatorFactory validatorFactory;
    private static Validator validator;
    static
    {
        validatorFactory = Validation.buildDefaultValidatorFactory();
        validator = validatorFactory.getValidator();
    }
    private static Validator emptyValidator = new EmptyValidatior();

    public static Validator getValidator()
    {
        if (null != validator)
        {
            return validator;
        }
        return emptyValidator;

    }
}
