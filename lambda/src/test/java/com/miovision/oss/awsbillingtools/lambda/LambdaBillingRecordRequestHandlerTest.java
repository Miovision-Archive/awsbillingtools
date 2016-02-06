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
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.miovision.oss.awsbillingtools.s3.scanner.S3BillingRecordFile;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Unit tests for the LambdaBillingRecordRequestHandler class.
 */
@RunWith(MockitoJUnitRunner.class)
public class LambdaBillingRecordRequestHandlerTest {
    @Captor
    private ArgumentCaptor<S3BillingRecordFile> s3BillingRecordFileArgumentCaptor;

    @Mock
    private Context context;

    @Mock
    private S3BillingRecordFileProcessor processor;

    private S3BillingRecordFileProcessorFactory processorFactory;

    private LambdaBillingRecordRequestHandler requestHandler;

    @Before
    public void setUp() throws Exception {
        processorFactory = new S3BillingRecordFileProcessorFactory() {
            private boolean firstRequest = true;
            @Override
            public S3BillingRecordFileProcessor create(Context context) {
                if(firstRequest) {
                    return processor;
                }
                else {
                    fail("Expected only one call to the processor factory");
                    return null;
                }
            }
        };
        requestHandler = new LambdaBillingRecordRequestHandler(processorFactory);

        when(context.getLogger()).thenReturn(mock(LambdaLogger.class));
    }

    @Test
    public void testSetDelimiter() throws Exception {
        // Execute
        requestHandler.setDelimiter("|");

        // Verify
        assertEquals("|", requestHandler.getDelimiter());
    }

    @Test
    public void testHandleRequest_WhenInputContainsZeroRecords() throws Exception {
        // Setup
        S3Event input = new S3Event(new ArrayList<>());

        // Execute
        requestHandler.handleRequest(input, context);

        // Verify
        InOrder inOrder = Mockito.inOrder(processor);
        inOrder.verify(processor).begin();
        inOrder.verify(processor).completeSuccess();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testHandleRequest_WhenInputContainsOneRecordThatIsNotABillingFile() throws Exception {
        // Setup
        String bucketName = "bucketName";
        String key = "key";
        S3EventNotification.S3EventNotificationRecord record = givenS3EventNotificationRecord(bucketName, key);
        S3Event input = new S3Event(Arrays.asList(record));

        // Execute
        requestHandler.handleRequest(input, context);

        // Verify
        InOrder inOrder = Mockito.inOrder(processor);
        inOrder.verify(processor).begin();
        inOrder.verify(processor).completeSuccess();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testHandleRequest_WhenInputContainsOneRecordThatIsABillingFile() throws Exception {
        // Setup
        String bucketName = "bucketName";
        String key = "111111111111-aws-billing-csv-2015-06.csv";
        S3EventNotification.S3EventNotificationRecord record = givenS3EventNotificationRecord(bucketName, key);
        S3Event input = new S3Event(Arrays.asList(record));

        // Execute
        requestHandler.handleRequest(input, context);

        // Verify
        InOrder inOrder = Mockito.inOrder(processor);
        inOrder.verify(processor).begin();
        inOrder.verify(processor).apply(s3BillingRecordFileArgumentCaptor.capture());
        inOrder.verify(processor).completeSuccess();
        inOrder.verifyNoMoreInteractions();
        assertEquals(bucketName, s3BillingRecordFileArgumentCaptor.getValue().getBucketName());
        assertEquals(key, s3BillingRecordFileArgumentCaptor.getValue().getKey());
    }

    @Test
    public void testHandleRequest_WhenInputContainsManyRecords() throws Exception {
        // Setup
        String bucketName = "bucketName";
        String key1 = "111111111111-aws-billing-csv-2015-06.csv";
        String key2 = "111111111111-aws-billing-csv-2015-07.csv";
        S3EventNotification.S3EventNotificationRecord record1 = givenS3EventNotificationRecord(bucketName, key1);
        S3EventNotification.S3EventNotificationRecord record2 = givenS3EventNotificationRecord(bucketName, key2);
        S3Event input = new S3Event(Arrays.asList(record1, record2));

        // Execute
        requestHandler.handleRequest(input, context);

        // Verify
        InOrder inOrder = Mockito.inOrder(processor);
        inOrder.verify(processor).begin();
        inOrder.verify(processor, times(2)).apply(s3BillingRecordFileArgumentCaptor.capture());
        inOrder.verify(processor).completeSuccess();
        inOrder.verifyNoMoreInteractions();
        assertEquals(bucketName, s3BillingRecordFileArgumentCaptor.getAllValues().get(0).getBucketName());
        assertEquals(key1, s3BillingRecordFileArgumentCaptor.getAllValues().get(0).getKey());
        assertEquals(bucketName, s3BillingRecordFileArgumentCaptor.getAllValues().get(1).getBucketName());
        assertEquals(key2, s3BillingRecordFileArgumentCaptor.getAllValues().get(1).getKey());
    }

    @Test
    public void testHandleRequest_WhenAnExceptionIsThrownInBegin() throws Exception {
        // Setup
        S3Event input = new S3Event(new ArrayList<>());
        RuntimeException toBeThrown = new RuntimeException("Oops!");
        doThrow(toBeThrown).when(processor).begin();

        // Execute
        requestHandler.handleRequest(input, context);

        // Verify
        InOrder inOrder = Mockito.inOrder(processor);
        inOrder.verify(processor).begin();
        inOrder.verify(processor).completeError(toBeThrown);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testHandleRequest_WhenAnExceptionIsThrownInApply() throws Exception {
        // Setup
        String bucketName = "bucketName";
        String key = "111111111111-aws-billing-csv-2015-06.csv";
        S3EventNotification.S3EventNotificationRecord record = givenS3EventNotificationRecord(bucketName, key);
        S3Event input = new S3Event(Arrays.asList(record));
        RuntimeException toBeThrown = new RuntimeException("Oops!");
        doThrow(toBeThrown).when(processor).apply(anyObject());

        // Execute
        requestHandler.handleRequest(input, context);

        // Verify
        InOrder inOrder = Mockito.inOrder(processor);
        inOrder.verify(processor).begin();
        inOrder.verify(processor).apply(anyObject());
        inOrder.verify(processor).completeError(toBeThrown);
        inOrder.verifyNoMoreInteractions();
    }

    private S3EventNotification.S3EventNotificationRecord givenS3EventNotificationRecord(
            String bucketName,
            String key) {
        return new S3EventNotification.S3EventNotificationRecord(
                "us-east-1",
                "Event!",
                "Source!",
                null,
                "1",
                null,
                null,
                new S3EventNotification.S3Entity(
                        "configId",
                        new S3EventNotification.S3BucketEntity(bucketName, null, ""),
                new S3EventNotification.S3ObjectEntity(key, 5L, "etag", "v"), "1"),
                null);
    }
}