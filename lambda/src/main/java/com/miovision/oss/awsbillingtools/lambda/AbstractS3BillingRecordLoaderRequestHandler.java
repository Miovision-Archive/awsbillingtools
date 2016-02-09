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

import com.miovision.oss.awsbillingtools.s3.loader.S3BillingRecordLoader;
import java.util.stream.Stream;

/**
 * An abstract implementation of AbstractLambdaRequestHandler that delegates to an S3BillingRecordLoader to load
 * billing records.
 *
 * @param <RecordTypeT> The billing record type.
 * @param <OutputT> The lambda request handler output type.
 */
public abstract class AbstractS3BillingRecordLoaderRequestHandler<RecordTypeT, OutputT>
        extends AbstractLambdaRequestHandler<
            AbstractS3BillingRecordLoaderRequestHandler.Request, RecordTypeT, OutputT> {

    private final S3BillingRecordLoader<RecordTypeT> loader;

    public AbstractS3BillingRecordLoaderRequestHandler(S3BillingRecordLoader<RecordTypeT> loader) {
        this.loader = loader;
    }

    @Override
    protected Stream<RecordTypeT> loadStream(Request input) throws UnableToLoadStreamException {
        try {
            return loader.load(input.getYear(), input.getMonth());
        }
        catch (Exception e) {
            throw new UnableToLoadStreamException();
        }
    }

    /**
     * The Lambda RequestHandler request class.
     */
    public static class Request {
        private int year;
        private int month;

        public Request(int year, int month) {
            this.year = year;
            this.month = month;
        }

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }

        public int getMonth() {
            return month;
        }

        public void setMonth(int month) {
            this.month = month;
        }
    }
}
