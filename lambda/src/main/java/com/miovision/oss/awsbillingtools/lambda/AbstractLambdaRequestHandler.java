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

package com.miovision.oss.awsbillingtools.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.util.stream.Stream;

/**
 * An abstract Lambda RequestHandler instance that delegates to S3BillingRecordLoader.
 *
 * @param <InputT> The Lambda RequestHandler's input type.
 * @param <RecordTypeT> The S3 billing record type.
 * @param <OutputT> The Lambda RequestHandler's output type.
 */
public abstract class AbstractLambdaRequestHandler<InputT, RecordTypeT, OutputT>
        implements RequestHandler<InputT, OutputT> {

    @Override
    public OutputT handleRequest(InputT input, Context context) {
        try(Stream<RecordTypeT> stream = loadStream(input)) {
            return handleStream(stream, context);
        }
        catch (UnableToLoadStreamException e) {
            LambdaUtils.logThrowable(context, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Load a stream of billing records.
     *
     * @param input The input object.
     *
     * @return The stream of billing records.
     *
     * @throws UnableToLoadStreamException When unable to load the stream.
     */
    protected abstract Stream<RecordTypeT> loadStream(InputT input) throws UnableToLoadStreamException;

    /**
     * Handle (process) a stream of billing records.
     *
     * @param stream The stream of billing records.
     * @param context The lambda context.
     *
     * @return The output object.
     */
    protected abstract OutputT handleStream(Stream<RecordTypeT> stream, Context context);

}
