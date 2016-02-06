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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.miovision.oss.awsbillingtools.FileType;
import com.miovision.oss.awsbillingtools.s3.loader.S3BillingRecordLoader;
import com.miovision.oss.awsbillingtools.s3.scanner.S3BillingRecordFile;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Unit tests for the AbstractS3BillingRecordFileProcessor class.
 */
@RunWith(MockitoJUnitRunner.class)
public class AbstractS3BillingRecordFileProcessorTest {
    @Mock
    private S3BillingRecordLoader<String> loader;

    @Mock
    private Context context;

    private Stream<String> appliedStream;
    private AbstractS3BillingRecordFileProcessor<String> fileProcessor;

    @Before
    public void setUp() {
        fileProcessor = new AbstractS3BillingRecordFileProcessor<String>(loader, context) {
            @Override
            protected void apply(Stream<String> stream) {
                appliedStream = stream;
            }
        };

        when(context.getLogger()).thenReturn(mock(LambdaLogger.class));
    }

    @Test
    public void testApply_WhenFileTypeNotSupported() {
        // Setup
        S3BillingRecordFile s3BillingRecordFile =
                new S3BillingRecordFile("bucketName", "key", "111111111111", FileType.CSV, 2015, 6, false);
        when(loader.canLoad(s3BillingRecordFile)).thenReturn(false);

        // Execute
        fileProcessor.apply(s3BillingRecordFile);

        // Verify
        assertNull(appliedStream);
    }

    @Test
    public void testApply_WhenFileTypeIsSupported() throws IOException {
        // Setup
        S3BillingRecordFile s3BillingRecordFile =
                new S3BillingRecordFile("bucketName", "key", "111111111111", FileType.CSV, 2015, 6, false);
        when(loader.canLoad(s3BillingRecordFile)).thenReturn(true);
        Stream<String> stream = Arrays.asList("1", "2").stream();
        when(loader.load(s3BillingRecordFile)).thenReturn(stream);

        // Execute
        fileProcessor.apply(s3BillingRecordFile);

        // Verify
        assertSame(stream, appliedStream);
    }
}