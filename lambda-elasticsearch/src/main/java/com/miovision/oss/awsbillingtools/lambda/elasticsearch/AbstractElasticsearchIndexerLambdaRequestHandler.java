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

import com.miovision.oss.awsbillingtools.elasticsearch.DefaultElasticsearchIndexer;
import com.miovision.oss.awsbillingtools.elasticsearch.ElasticsearchBillingRecordConverter;
import com.miovision.oss.awsbillingtools.elasticsearch.ElasticsearchIndexer;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchConnectionFactory;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.jest.JestElasticsearchConnectionFactory;
import com.miovision.oss.awsbillingtools.lambda.LambdaUtils;
import com.miovision.oss.awsbillingtools.parser.BillingRecordParser;
import com.miovision.oss.awsbillingtools.s3.loader.S3BillingRecordLoader;
import com.miovision.oss.awsbillingtools.s3.scanner.S3BillingRecordFileScanner;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import java.util.stream.Stream;

/**
 * An abstract AWS Lambda request handler for indexing billing records into Elasticsearch.
 */
public abstract class AbstractElasticsearchIndexerLambdaRequestHandler<RecordTypeT>
        implements RequestHandler<AbstractElasticsearchIndexerLambdaRequestHandler.Request, Void> {
    private final AmazonS3 amazonS3;
    private final BillingRecordParser<RecordTypeT> billingRecordParser;

    protected AbstractElasticsearchIndexerLambdaRequestHandler(AmazonS3 amazonS3,
                                                               BillingRecordParser<RecordTypeT> billingRecordParser) {
        this.amazonS3 = amazonS3;
        this.billingRecordParser = billingRecordParser;


    }

    protected AbstractElasticsearchIndexerLambdaRequestHandler(BillingRecordParser<RecordTypeT> billingRecordParser) {
        this(new AmazonS3Client(new DefaultAWSCredentialsProviderChain()), billingRecordParser);
    }

    @Override
    public Void handleRequest(Request input, Context context) {
        final S3BillingRecordFileScanner fileScanner = createS3BillingRecordFileScanner(input);
        final S3BillingRecordLoader<RecordTypeT> recordLoader = createS3BillingRecordLoader(fileScanner);
        final ElasticsearchBillingRecordConverter<RecordTypeT> recordConverter = createRecordConverter();
        try(Stream<RecordTypeT> stream = recordLoader.load(input.getYear(), input.getMonth())) {
            final ElasticsearchConnectionFactory connectionFactory = createElasticsearchConnectionFactory(input);
            final ElasticsearchIndexer<RecordTypeT> elasticsearchIndexer =
                    createElasticsearchIndexer(input, recordConverter, connectionFactory);
            elasticsearchIndexer.index(stream);
        }
        catch (Exception e) {
            LambdaUtils.logThrowable(context, e);
            throw new RuntimeException(e);
        }
        return null;
    }

    protected ElasticsearchIndexer<RecordTypeT> createElasticsearchIndexer(
            Request input,
            ElasticsearchBillingRecordConverter<RecordTypeT> recordConverter,
            ElasticsearchConnectionFactory connectionFactory) {
        return new DefaultElasticsearchIndexer<>(
                connectionFactory,
                recordConverter,
                input.getElasticsearchIndex(),
                input.getBatchSize());
    }

    protected ElasticsearchConnectionFactory createElasticsearchConnectionFactory(Request input) {
        return new JestElasticsearchConnectionFactory(input.getElasticsearchHost());
    }

    protected abstract ElasticsearchBillingRecordConverter<RecordTypeT> createRecordConverter();

    protected S3BillingRecordLoader<RecordTypeT> createS3BillingRecordLoader(
            S3BillingRecordFileScanner fileScanner) {
        return new S3BillingRecordLoader<>(amazonS3, fileScanner, billingRecordParser);
    }

    protected S3BillingRecordFileScanner createS3BillingRecordFileScanner(Request input) {
        return new S3BillingRecordFileScanner(
                amazonS3,
                input.getBucketName(),
                input.getPrefix(),
                input.getAwsAccountId());
    }

    /**
     * The request payload.
     */
    public static class Request {
        private String bucketName;
        private String prefix;
        private String awsAccountId;
        private int year;
        private int month;
        private String elasticsearchHost;
        private int elasticsearchPort;
        private String elasticsearchIndex;
        private int batchSize;

        public String getBucketName() {
            return bucketName;
        }

        public void setBucketName(String bucketName) {
            this.bucketName = bucketName;
        }

        public String getPrefix() {
            return prefix;
        }

        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }

        public String getAwsAccountId() {
            return awsAccountId;
        }

        public void setAwsAccountId(String awsAccountId) {
            this.awsAccountId = awsAccountId;
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

        public String getElasticsearchHost() {
            return elasticsearchHost;
        }

        public void setElasticsearchHost(String elasticsearchHost) {
            this.elasticsearchHost = elasticsearchHost;
        }

        public int getElasticsearchPort() {
            return elasticsearchPort;
        }

        public void setElasticsearchPort(int elasticsearchPort) {
            this.elasticsearchPort = elasticsearchPort;
        }

        public String getElasticsearchIndex() {
            return elasticsearchIndex;
        }

        public void setElasticsearchIndex(String elasticsearchIndex) {
            this.elasticsearchIndex = elasticsearchIndex;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }
    }
}
