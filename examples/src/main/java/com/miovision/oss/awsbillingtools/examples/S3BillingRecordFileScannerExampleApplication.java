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

import com.miovision.oss.awsbillingtools.FileType;
import com.miovision.oss.awsbillingtools.s3.scanner.S3BillingRecordFile;
import com.miovision.oss.awsbillingtools.s3.scanner.S3BillingRecordFileScanner;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import java.io.PrintStream;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * An example application for demonstrating how to use S3BillingRecordFileScanner.
 */
public class S3BillingRecordFileScannerExampleApplication {
    public static void main(String[] args) {
        S3BillingRecordFileScanner fileScanner = createBillingRecordFileScanner(args);

        // The scan() method returns the full list of all billing files. This list could get rather large over time.
        // For this example we'll just print a few files to demonstrate the method.
        System.out.println("Printing a few billing files...");
        try(Stream<S3BillingRecordFile> billingRecordFileStream = fileScanner.scan()) {
            // For the purpose of this example we'll limit the output to 3 files. Java's streams make this trivial.
            billingRecordFileStream.limit(3).forEach(s3BillingRecordFile -> {
                printBillingRecoreFile(System.out, s3BillingRecordFile);
            });
        }
        System.out.println();

        // Scanning for all of the billing files has margin value as the contents of the various file types differs
        // dramatically. It is likely that you are only interested in files of a particular type. The scan() method
        // takes an argument that is the file type you are interested in.
        System.out.println("Printing a few detailed line item files...");
        try(Stream<S3BillingRecordFile> billingRecordFileStream = fileScanner.scan(FileType.DETAILED_LINE_ITEMS)) {
            // For the purpose of this example we'll limit the output to 3 files. Java's streams make this trivial.
            billingRecordFileStream.limit(3).forEach(s3BillingRecordFile -> {
                printBillingRecoreFile(System.out, s3BillingRecordFile);
            });
        }
        System.out.println();

        // If you are interested in a billing file for a specific month (ex. this month or last month) there
        // is an additional overload of the scan() method that accepts a month and year.
        System.out.println("Printing the detailed line item for Jan. 2016...");
        try(Stream<S3BillingRecordFile> billingRecordFileStream =
                    fileScanner.scan(FileType.DETAILED_LINE_ITEMS, 2016, 1)) {
            // Since we have specified the file type, year and month the scan() method above _should_ return a stream
            // with only zero or one items. We'll limit the results to 1 for sanity sake.
            billingRecordFileStream.limit(1).forEach(s3BillingRecordFile -> {
                printBillingRecoreFile(System.out, s3BillingRecordFile);
            });
        }
        System.out.println();

        // Finally, if you want to search the billing files using some other criteria you can pass in a predicate. Note
        // that this predicate version of scan() isn't magic: you can just pass the same predicate to the filter()
        // method of the resultant stream.
        System.out.println("Printing a few detailed line item files where the month is Jan or Feb...");
        Predicate<S3BillingRecordFile> searchPredicate = billingRecordFile -> billingRecordFile.getMonth() < 3;
        try(Stream<S3BillingRecordFile> billingRecordFileStream = fileScanner.scan(searchPredicate)) {
            billingRecordFileStream.limit(5).forEach(s3BillingRecordFile -> {
                printBillingRecoreFile(System.out, s3BillingRecordFile);
            });
        }
        System.out.println();
    }

    protected static S3BillingRecordFileScanner createBillingRecordFileScanner(String[] args) {
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

        return new S3BillingRecordFileScanner(amazonS3, bucketName, prefix, awsAccountId);
    }

    private static void printBillingRecoreFile(PrintStream outputStream, S3BillingRecordFile billingRecordFile) {
        outputStream.println(String.format(
                "%s %d-%d",
                billingRecordFile.getType(),
                billingRecordFile.getYear(),
                billingRecordFile.getMonth()));
    }
}
