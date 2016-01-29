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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import com.miovision.oss.awsbillingtools.FileType;
import com.miovision.oss.awsbillingtools.parser.BillingRecordParser;
import com.miovision.oss.awsbillingtools.parser.DetailedLineItem;
import com.miovision.oss.awsbillingtools.parser.DetailedLineItemParser;
import com.miovision.oss.awsbillingtools.s3.scanner.BillingRecordFileNotFoundException;
import com.miovision.oss.awsbillingtools.s3.scanner.S3BillingRecordFile;
import com.miovision.oss.awsbillingtools.s3.scanner.S3BillingRecordFileScanner;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringInputStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Unit tests for the S3BillingRecordLoader class.
 */
@RunWith(MockitoJUnitRunner.class)
public class S3BillingRecordLoaderTest {
    //CHECKSTYLE.OFF: LineLength
    private static final String HEADER = "\"InvoiceID\",\"PayerAccountId\",\"LinkedAccountId\",\"RecordType\",\"RecordId\",\"ProductName\",\"RateId\",\"SubscriptionId\",\"PricingPlanId\",\"UsageType\",\"Operation\",\"AvailabilityZone\",\"ReservedInstance\",\"ItemDescription\",\"UsageStartDate\",\"UsageEndDate\",\"UsageQuantity\",\"Rate\",\"Cost\",\"ResourceId\",\"user:foo\"\n";
    private static final String RECORD_1 = "\"12345678\",\"111111111111\",\"222222222222\",\"LineItem\",\"12345678901234567890123456\",\"Product Name Here\",\"9191919\",\"723456723\",\"515253\",\"Usage Type Here\",\"Hourly\",\"us-east-1\",\"Y\",\"$0.05 per hour for a thing\",\"2015-12-01 00:00:00\",\"2015-12-01 01:00:00\",\"1.02007083\",\"0.0500000000\",\"0.05100354\",\"Resource ID\",\"bar\"\n";
    //CHECKSTYLE.ON: LineLength

    @Mock
    private AmazonS3 amazonS3;

    @Mock
    private S3BillingRecordFileScanner fileScanner;

    private BillingRecordParser<DetailedLineItem> billingRecordParser;

    private S3BillingRecordLoader<DetailedLineItem> billingRecordLoader;

    @Before
    public void setUp() {
        billingRecordParser = new DetailedLineItemParser();

        billingRecordLoader = new S3BillingRecordLoader<>(amazonS3, fileScanner, billingRecordParser);
    }

    @Test
    public void testLoad_WithS3BillingFileRecord_NonZipFile() throws Exception {
        // Setup
        S3BillingRecordFile billingRecordFile = givenAS3BillingRecordFile(false);
        givenAmazonS3GetObjectReturnsAnS3Object(billingRecordFile, new StringInputStream(HEADER + RECORD_1));

        // Execute
        try(Stream<DetailedLineItem> stream = billingRecordLoader.load(billingRecordFile)) {
            // Verify
            List<DetailedLineItem> billingRecords = stream.collect(Collectors.toList());
            assertEquals(1, billingRecords.size());
            assertEquals("12345678", billingRecords.get(0).getInvoiceId());
        }
    }

    @Test
    public void testLoad_WithS3BillingFileRecord_ZipFile() throws Exception {
        // Setup
        S3BillingRecordFile billingRecordFile = givenAS3BillingRecordFile(true);
        givenAmazonS3GetObjectReturnsAnS3Object(billingRecordFile, createZipCompressedInputStream(HEADER + RECORD_1));

        // Execute
        try(Stream<DetailedLineItem> stream = billingRecordLoader.load(billingRecordFile)) {
            // Verify
            List<DetailedLineItem> billingRecords = stream.collect(Collectors.toList());
            assertEquals(1, billingRecords.size());
            assertEquals("12345678", billingRecords.get(0).getInvoiceId());
        }
    }

    @Test
    public void testLoad_WithYearAndMonth_BillingFileFound() throws IOException, BillingRecordFileNotFoundException {
        // Setup
        S3BillingRecordFile billingRecordFile = givenAS3BillingRecordFile(false);
        givenFileScannerReturnsBillingRecordFiles(billingRecordFile);

        givenAmazonS3GetObjectReturnsAnS3Object(billingRecordFile, new StringInputStream(HEADER + RECORD_1));

        // Exercise
        try(Stream<DetailedLineItem> stream = billingRecordLoader.load(2015, 6)) {
            // Verify
            List<DetailedLineItem> billingRecords = stream.collect(Collectors.toList());
            assertEquals(1, billingRecords.size());
            assertEquals("12345678", billingRecords.get(0).getInvoiceId());
        }
    }

    @Test(expected = BillingRecordFileNotFoundException.class)
    public void testLoad_WithYearAndMonth_BillingFileNotFound() throws IOException, BillingRecordFileNotFoundException {
        // Setup
        givenFileScannerReturnsBillingRecordFiles();

        // Exercise
        try(Stream<DetailedLineItem> ignored = billingRecordLoader.load(2015, 6)) {
            // Verify
            fail("Expected load() to throw a BillingFileNotFoundException.");
        }
    }

    protected void givenFileScannerReturnsBillingRecordFiles(S3BillingRecordFile... s3BillingRecordFiles) {
        when(fileScanner.scan(FileType.DETAILED_LINE_ITEMS, 2015, 6))
                .thenReturn(Arrays.asList(s3BillingRecordFiles).stream());
    }

    protected void givenAmazonS3GetObjectReturnsAnS3Object(
            S3BillingRecordFile billingRecordFile,
            InputStream inputStream)
            throws IOException {
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(inputStream);
        when(amazonS3.getObject(billingRecordFile.getBucketName(), billingRecordFile.getKey()))
                .thenReturn(s3Object);
    }

    private S3BillingRecordFile givenAS3BillingRecordFile(boolean zip) {
        return new S3BillingRecordFile("bucketName", "key", "111111111111", FileType.DETAILED_LINE_ITEMS, 2015, 6, zip);
    }

    protected InputStream createZipCompressedInputStream(String fileContents) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try(ZipOutputStream zipOutputStream = new ZipOutputStream(byteArrayOutputStream)) {
            zipOutputStream.putNextEntry(new ZipEntry("Entry1"));
            byte[] buffer = fileContents.getBytes(Charset.defaultCharset());
            zipOutputStream.write(buffer, 0, buffer.length);
            zipOutputStream.closeEntry();
        }
        return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    }
}