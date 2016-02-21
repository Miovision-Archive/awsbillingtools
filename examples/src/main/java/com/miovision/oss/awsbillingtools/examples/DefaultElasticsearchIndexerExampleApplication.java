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

package com.miovision.oss.awsbillingtools.examples;

import com.miovision.oss.awsbillingtools.elasticsearch.DefaultElasticsearchIndexer;
import com.miovision.oss.awsbillingtools.elasticsearch.DetailedLineItemRecordConverter;
import com.miovision.oss.awsbillingtools.elasticsearch.ElasticsearchBillingRecordConverter;
import com.miovision.oss.awsbillingtools.elasticsearch.ElasticsearchIndexer;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.esclient.DefaultElasticsearchConnectionFactory;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchConnectionFactory;
import com.miovision.oss.awsbillingtools.parser.DetailedLineItem;
import com.miovision.oss.awsbillingtools.parser.DetailedLineItemParser;
import com.miovision.oss.awsbillingtools.s3.loader.S3BillingRecordLoader;
import com.miovision.oss.awsbillingtools.s3.scanner.S3BillingRecordFileScanner;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import java.net.InetAddress;
import java.util.stream.Stream;

/**
 * An example application for DefaultElasticsearchIndexer.
 */
public class DefaultElasticsearchIndexerExampleApplication {
    public static final String DEFAULT_INDEX_NAME = "billing-index";
    public static final int DEFAULT_BATCH_SIZE = 500;

    public static void main(String[] args) throws Exception {
        final String bucketName = args[0];
        final String awsAccountId = args[1];
        final int year = Integer.parseInt(args[2]);
        final int month = Integer.parseInt(args[3]);
        final String elasticsearchHostname = args[4];
        final int elasticsearchPort = Integer.parseInt(args[5]);

        // Create an Elasticsearch connection factory. The connection factory is used to create connections to
        // Elasticsearch. This code clearly assumes you have an Elasticsearch server running on 'localhost' on port
        // 9300.
        ElasticsearchConnectionFactory connectionFactory =
                new DefaultElasticsearchConnectionFactory(
                        elasticsearchPort, InetAddress.getByName(elasticsearchHostname));

        // Create a record converter. The record converter is used to convert AWS billing record objects into key/value
        // pairs.
        ElasticsearchBillingRecordConverter<DetailedLineItem> recordConverter = new DetailedLineItemRecordConverter();

        // Create an Elasticsearch indexer.
        ElasticsearchIndexer<DetailedLineItem> elasticsearchIndexer =
                new DefaultElasticsearchIndexer<>(
                        connectionFactory, recordConverter, DEFAULT_INDEX_NAME, DEFAULT_BATCH_SIZE);

        // Create a record loader. The details are really important here so we'll just squirrel 'em away in a helper
        // method.
        S3BillingRecordLoader<DetailedLineItem> recordLoader = createRecordLoader(bucketName, awsAccountId);

        // Here is the heart of the example. We use the record loader to retrieve a stream of billing records and then
        // we pass that stream to the indexer.
        try(Stream<DetailedLineItem> stream = recordLoader.load(year, month)) {
            elasticsearchIndexer.index(stream);
        }
    }

    private static S3BillingRecordLoader<DetailedLineItem> createRecordLoader(
            String bucketName,
            String awsAccountId) {
        AmazonS3 amazonS3 = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
        S3BillingRecordFileScanner fileScanner = new S3BillingRecordFileScanner(amazonS3, bucketName, "", awsAccountId);
        DetailedLineItemParser parser = new DetailedLineItemParser();
        return new S3BillingRecordLoader<>(amazonS3, fileScanner, parser);
    }
}
