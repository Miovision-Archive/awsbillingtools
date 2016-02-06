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
import com.miovision.oss.awsbillingtools.s3.scanner.S3BillingRecordFile;
import com.amazonaws.services.lambda.runtime.Context;
import java.io.IOException;
import java.util.stream.Stream;

/**
 * An abstract implementation of S3BillingRecordFileProcessor that uses a S3BillingRecordLoader to retrieve a stream
 * of billing records.
 *
 * @param <RecordTypeT> The record type.
 */
public abstract class AbstractS3BillingRecordFileProcessor<RecordTypeT> implements S3BillingRecordFileProcessor {
    private final S3BillingRecordLoader<RecordTypeT> s3BillingRecordLoader;
    private final Context context;

    public AbstractS3BillingRecordFileProcessor(S3BillingRecordLoader<RecordTypeT> s3BillingRecordLoader,
                                                Context context) {
        this.s3BillingRecordLoader = s3BillingRecordLoader;
        this.context = context;
    }

    @Override
    public void begin() {

    }

    @Override
    public void apply(S3BillingRecordFile s3BillingRecordFile) {
        if(!s3BillingRecordLoader.canLoad(s3BillingRecordFile)) {
            log(String.format(
                    "%s/%s of type %s is not a supported billing file and will be ignored",
                    s3BillingRecordFile.getBucketName(),
                    s3BillingRecordFile.getKey(),
                    s3BillingRecordFile));
            return;
        }

        try {
            try(Stream<RecordTypeT> stream = s3BillingRecordLoader.load(s3BillingRecordFile)) {
                apply(stream);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void apply(Stream<RecordTypeT> stream);

    @Override
    public void completeSuccess() {

    }

    @Override
    public void completeError(Throwable throwable) {

    }

    protected void log(String message) {
        context.getLogger().log(message);
    }
}
