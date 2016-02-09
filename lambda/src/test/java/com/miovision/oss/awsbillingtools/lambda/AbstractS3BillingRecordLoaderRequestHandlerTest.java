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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.miovision.oss.awsbillingtools.FileType;
import com.miovision.oss.awsbillingtools.s3.loader.S3BillingRecordLoader;
import com.miovision.oss.awsbillingtools.s3.scanner.BillingRecordFileNotFoundException;
import com.amazonaws.services.lambda.runtime.Context;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Unit tests for the AbstractS3BillingRecordLoaderRequestHandler class.
 */
@RunWith(MockitoJUnitRunner.class)
public class AbstractS3BillingRecordLoaderRequestHandlerTest {
    public static final AbstractS3BillingRecordLoaderRequestHandler.Request TEST_REQUEST =
            new AbstractS3BillingRecordLoaderRequestHandler.Request(2015, 6);

    @Mock
    private S3BillingRecordLoader<String> loader;
    private AbstractS3BillingRecordLoaderRequestHandlerTss requestHandler;

    @Before
    public void setUp() throws Exception {
        requestHandler = new AbstractS3BillingRecordLoaderRequestHandlerTss(loader);
    }

    @Test
    public void testHandleRequest_Success() throws BillingRecordFileNotFoundException, IOException {
        // Setup
        when(loader.load(2015, 6)).thenReturn(Arrays.asList("Foo", "Bar").stream());

        // Exercise
        final Integer result = requestHandler.handleRequest(TEST_REQUEST, mock(Context.class));

        // Verify
        verify(loader).load(2015, 6);
        assertEquals(5, result.intValue());
    }

    @Test(expected = RuntimeException.class)
    public void testHandleRequest_Exception() throws BillingRecordFileNotFoundException, IOException {
        // Setup
        when(loader.load(2015, 6)).thenThrow(new BillingRecordFileNotFoundException(FileType.COST_ALLOCATION, 2016, 6));

        // Exercise
        requestHandler.handleRequest(TEST_REQUEST, mock(Context.class));
    }

    private static class AbstractS3BillingRecordLoaderRequestHandlerTss
            extends AbstractS3BillingRecordLoaderRequestHandler<String, Integer> {
        public AbstractS3BillingRecordLoaderRequestHandlerTss(S3BillingRecordLoader<String> loader) {
            super(loader);
        }

        @Override
        protected Integer handleStream(Stream<String> stream, Context context) {
            return 5;
        }
    }
}