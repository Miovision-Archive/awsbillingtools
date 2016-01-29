/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 the original author or authors.
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
 */

package com.miovision.oss.awsbillingtools.s3.loader;

import com.miovision.oss.awsbillingtools.parser.DetailedLineItem;
import com.miovision.oss.awsbillingtools.parser.DetailedLineItemParser;
import com.miovision.oss.awsbillingtools.s3.scanner.BillingRecordFileNotFoundException;
import com.miovision.oss.awsbillingtools.s3.scanner.S3BillingRecordFileScanner;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import java.io.IOException;
import java.util.stream.Stream;

/**
 * An example application for demonstrating how to use S3BillingRecordLoader.
 */
public class S3BillingRecordLoaderExampleApplication {
    public static void main(String[] args) throws BillingRecordFileNotFoundException, IOException {
        S3BillingRecordLoader<DetailedLineItem> billingRecordLoader = createBillingRecordLoader(args);

        // The S3BillingRecordLoader<T> is a helper class that wraps up a file scanner instance and a parser instance
        // so that if you want to read billing records from a specific file you can do so with a more convenient
        // interface.
        try(Stream<DetailedLineItem> billingRecords = billingRecordLoader.load(2016, 1)) {
            billingRecords.limit(10).forEach(billingRecord -> {
                System.out.println(String.format("%s\t\t$%f", billingRecord.getProductName(), billingRecord.getCost()));
            });
        }
    }

    protected static S3BillingRecordLoader<DetailedLineItem> createBillingRecordLoader(String[] args) {
        if(args.length < 2) {
            System.err.println("Invalid arguments: expected <bucketName> and <awsAccountId>");
            System.exit(-1);
        }

        // Construct an instance of AmazonS3 (the AWS S3 client) using the default credentials provider chain. This will
        // use all of the usual approaches to try and find AWS credentials. The easiest approach is to define the
        // environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
        //
        // For more information:
        // http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html
        AmazonS3 amazonS3 = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());

        String bucketName = args[0];

        // This is the AWS account ID of the billing files and NOT the AWS credentials. They may not be the same.
        String awsAccountId = args[1];

        // The S3 prefix for where your billing files live.
        String prefix = "";
        if(args.length > 2) {
            prefix = args[2];
        }

        S3BillingRecordFileScanner fileScanner =
                new S3BillingRecordFileScanner(amazonS3, bucketName, prefix, awsAccountId);

        DetailedLineItemParser billingRecordParser = new DetailedLineItemParser();

        return new S3BillingRecordLoader<>(amazonS3, fileScanner, billingRecordParser);
    }
}
