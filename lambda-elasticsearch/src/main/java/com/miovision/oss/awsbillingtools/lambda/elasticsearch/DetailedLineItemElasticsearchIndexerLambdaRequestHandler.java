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

package com.miovision.oss.awsbillingtools.lambda.elasticsearch;

import com.miovision.oss.awsbillingtools.elasticsearch.DetailedLineItemRecordConverter;
import com.miovision.oss.awsbillingtools.elasticsearch.ElasticsearchBillingRecordConverter;
import com.miovision.oss.awsbillingtools.parser.BillingRecordParser;
import com.miovision.oss.awsbillingtools.parser.DetailedLineItem;
import com.miovision.oss.awsbillingtools.parser.DetailedLineItemParser;
import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/**
 * An implementation of AbstractElasticsearchIndexerLambdaRequestHandler for DetailedLineItem records.
 */
public class DetailedLineItemElasticsearchIndexerLambdaRequestHandler
        extends AbstractElasticsearchIndexerLambdaRequestHandler<DetailedLineItem> {
    public DetailedLineItemElasticsearchIndexerLambdaRequestHandler(
            AmazonS3 amazonS3,
            BillingRecordParser<DetailedLineItem> billingRecordParser) {
        super(amazonS3, billingRecordParser);
    }

    public DetailedLineItemElasticsearchIndexerLambdaRequestHandler(
            BillingRecordParser<DetailedLineItem> billingRecordParser) {
        super(billingRecordParser);
    }

    public DetailedLineItemElasticsearchIndexerLambdaRequestHandler() {
        this(new DetailedLineItemParser());
    }

    @Override
    protected ElasticsearchBillingRecordConverter<DetailedLineItem> createRecordConverter() {
        return new DetailedLineItemRecordConverter();
    }

    /**
     * A debug main method.
     *
     * <p>
     *     This method is a debug method that allows you to run the Lambda function locally (without lambda) for
     *     manual testing purposes.
     * </p>
     *
     * @param args The input arguments.
     *
     * @throws IOException Thrown when unable to read the request from the input stream.
     */
    public static void main(String[] args) throws IOException {
        final Request request = new ObjectMapper().reader(Request.class).readValue(System.in);
        final DetailedLineItemElasticsearchIndexerLambdaRequestHandler requestHandler =
                new DetailedLineItemElasticsearchIndexerLambdaRequestHandler();
        final Context context = new DebugContext();
        requestHandler.handleRequest(request, context);
    }

    private static class DebugContext implements Context {

        private DebugLambdaLogger debugLambdaLogger = new DebugLambdaLogger();

        @Override
        public String getAwsRequestId() {
            return null;
        }

        @Override
        public String getLogGroupName() {
            return null;
        }

        @Override
        public String getLogStreamName() {
            return null;
        }

        @Override
        public String getFunctionName() {
            return null;
        }

        @Override
        public String getFunctionVersion() {
            return null;
        }

        @Override
        public String getInvokedFunctionArn() {
            return null;
        }

        @Override
        public CognitoIdentity getIdentity() {
            return null;
        }

        @Override
        public ClientContext getClientContext() {
            return null;
        }

        @Override
        public int getRemainingTimeInMillis() {
            return 0;
        }

        @Override
        public int getMemoryLimitInMB() {
            return 0;
        }

        @Override
        public LambdaLogger getLogger() {
            return debugLambdaLogger;
        }
    }

    private static class DebugLambdaLogger implements LambdaLogger {
        @Override
        public void log(String string) {
            System.out.println(string);
        }
    }
}
