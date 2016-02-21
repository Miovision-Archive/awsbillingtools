/*
 * The MIT License (MIT)
 *
 * Copyright (c)  2016 the original author or authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.slf4j.impl;

import com.miovision.oss.awsbillingtools.lambda.logging.LambdaLoggerFactory;
import com.miovision.oss.awsbillingtools.lambda.logging.LambdaLoggerProxy;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.slf4j.ILoggerFactory;
import org.slf4j.spi.LoggerFactoryBinder;

/**
 * The StaticLoggerBinder for Lambda logging.
 */
public class StaticLoggerBinder implements LoggerFactoryBinder {
    private static final StaticLoggerBinder SINGLETON = new StaticLoggerBinder();
    private final LambdaLoggerProxy lambdaLoggerProxy = new LambdaLoggerProxy();
    private final ILoggerFactory loggerFactory;

    private StaticLoggerBinder() {
        loggerFactory = new LambdaLoggerFactory(lambdaLoggerProxy);
    }

    public LambdaLogger getLambdaLogger() {
        return lambdaLoggerProxy.getImpl();
    }

    public void setLambdaLogger(LambdaLogger lambdaLogger) {
        lambdaLoggerProxy.setImpl(lambdaLogger);
    }

    @Override
    public ILoggerFactory getLoggerFactory() {
        return loggerFactory;
    }

    @Override
    public String getLoggerFactoryClassStr() {
        return LambdaLoggerFactory.class.getName();
    }

    public static final StaticLoggerBinder getSingleton() {
        return SINGLETON;
    }

}
