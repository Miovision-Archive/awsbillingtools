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

package com.miovision.oss.awsbillingtools.lambda.logging;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * An implementation of an slf4j Logger based on the 'Simple' logger.
 */
public class LambdaLoggerAdapter extends SimpleLogger {
    private final LambdaLogger lambdaLogger;

    LambdaLoggerAdapter(String name, LambdaLogger lambdaLogger) {
        super(name);
        this.lambdaLogger = lambdaLogger;
    }

    @Override
    void write(StringBuilder buf, Throwable t) {
        lambdaLogger.log(buf.toString());
        if(t != null) {
            StringWriter stringWriter = new StringWriter(buf.length());
            try(PrintWriter writer = new PrintWriter(stringWriter)) {
                t.printStackTrace(writer);
                lambdaLogger.log(stringWriter.toString());
            }
        }
    }

}
