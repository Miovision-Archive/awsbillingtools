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

package com.miovision.oss.awsbillingtools.s3.scanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.miovision.oss.awsbillingtools.FileType;
import org.junit.Before;
import org.junit.Test;
import java.util.Optional;

/**
 * Unit tests for the S3BillingRecordFile class.
 */
public class S3BillingRecordFileTest {
    public static final String TEST_BUCKET_NAME = "bucketName";
    public static final String TEST_ACCOUNT_ID = "111111111111";
    private S3BillingRecordFile s3BillingRecordFile;

    @Before
    public void setUp() throws Exception {
        s3BillingRecordFile = new S3BillingRecordFile(
                "bucketName",
                "key",
                "accountId",
                FileType.DETAILED_LINE_ITEMS,
                2015,
                5,
                true);
    }

    @Test
    public void testGetAccountId() throws Exception {
        // Execute
        String accountId = s3BillingRecordFile.getAccountId();

        // Verify
        assertEquals("accountId", accountId);
    }

    @Test
    public void testGetType() throws Exception {
        // Execute
        FileType fileType = s3BillingRecordFile.getType();

        // Verify
        assertEquals(FileType.DETAILED_LINE_ITEMS, fileType);
    }

    @Test
    public void testGetYear() throws Exception {
        // Execute
        int year = s3BillingRecordFile.getYear();

        // Verify
        assertEquals(2015, year);
    }

    @Test
    public void testGetMonth() throws Exception {
        // Execute
        int month = s3BillingRecordFile.getMonth();

        // Verify
        assertEquals(5, month);
    }

    @Test
    public void testIsZip() throws Exception {
        // Execute
        boolean zip = s3BillingRecordFile.isZip();

        // Verify
        assertTrue(zip);
    }

    @Test
    public void testGetBucketName() throws Exception {
        // Execute
        String bucketName = s3BillingRecordFile.getBucketName();

        // Verify
        assertEquals("bucketName", bucketName);
    }

    @Test
    public void testGetKey() throws Exception {
        // Execute
        String key = s3BillingRecordFile.getKey();

        // Verify
        assertEquals("key", key);
    }

    @Test
    public void testParseS3Key_S3KeyIsNull() {
        // Execute
        Optional<S3BillingRecordFile> result = S3BillingRecordFile.parseS3Key("bucketName", null);

        // Verify
        assertFalse(result.isPresent());
    }

    @Test
    public void testParseS3Key_S3KeyIsEmptyString() {
        // Execute
        Optional<S3BillingRecordFile> result = S3BillingRecordFile.parseS3Key("bucketName", "");

        // Verify
        assertFalse(result.isPresent());
    }

    @Test
    public void testParseS3Key_S3KeyIsNotABillingRecordFile() {
        // Execute
        Optional<S3BillingRecordFile> result = S3BillingRecordFile.parseS3Key("bucketName", "");

        // Verify
        assertFalse(result.isPresent());
    }

    @Test
    public void testParseS3Key_S3KeyIsACsvFile() {
        // Setup
        String s3Key = "111111111111-aws-billing-csv-2015-06.csv";

        // Execute
        Optional<S3BillingRecordFile> result =
                S3BillingRecordFile.parseS3Key(TEST_BUCKET_NAME, s3Key);

        // Verify
        assertS3BillingRecordEquals(
                result,
                s3Key,
                FileType.CSV,
                2015,
                6,
                false);
    }

    @Test
    public void testParseS3Key_S3KeyIsALineItemsFile() {
        // Setup
        String s3Key = "111111111111-aws-billing-detailed-line-items-2015-06.csv.zip";

        // Execute
        Optional<S3BillingRecordFile> result =
                S3BillingRecordFile.parseS3Key(TEST_BUCKET_NAME, s3Key);

        // Verify
        assertS3BillingRecordEquals(
                result,
                s3Key,
                FileType.LINE_ITEMS,
                2015,
                6,
                true);
    }

    @Test
    public void testParseS3Key_S3KeyIsADetailedLineItemsFile() {
        // Setup
        String s3Key = "111111111111-aws-billing-detailed-line-items-with-resources-and-tags-2015-06.csv.zip";

        // Execute
        Optional<S3BillingRecordFile> result =
                S3BillingRecordFile.parseS3Key(TEST_BUCKET_NAME, s3Key);

        // Verify
        assertS3BillingRecordEquals(
                result,
                s3Key,
                FileType.DETAILED_LINE_ITEMS,
                2015,
                6,
                true);
    }

    @Test
    public void testParseS3Key_S3KeyIsACostAllocationFile() {
        // Setup
        String s3Key = "111111111111-aws-cost-allocation-2015-06.csv";

        // Execute
        Optional<S3BillingRecordFile> result =
                S3BillingRecordFile.parseS3Key(TEST_BUCKET_NAME, s3Key);

        // Verify
        assertS3BillingRecordEquals(
                result,
                s3Key,
                FileType.COST_ALLOCATION,
                2015,
                6,
                false);
    }

    @Test
    public void testParseS3Key_S3KeyHasASimplePrefix() {
        // Setup
        String s3Key = "prefix/111111111111-aws-billing-csv-2015-06.csv";

        // Execute
        Optional<S3BillingRecordFile> result =
                S3BillingRecordFile.parseS3Key(TEST_BUCKET_NAME, s3Key);

        // Verify
        assertS3BillingRecordEquals(
                result,
                s3Key,
                FileType.CSV,
                2015,
                6,
                false);
    }

    @Test
    public void testParseS3Key_S3KeyHasAComplexPrefix() {
        // Setup
        String s3Key = "/complex/prefix/111111111111-aws-billing-csv-2015-06.csv";

        // Execute
        Optional<S3BillingRecordFile> result =
                S3BillingRecordFile.parseS3Key(TEST_BUCKET_NAME, s3Key);

        // Verify
        assertS3BillingRecordEquals(
                result,
                s3Key,
                FileType.CSV,
                2015,
                6,
                false);
    }

    @Test
    public void testParseS3Key_AllowsNullBucketName() {
        // Setup
        String s3Key = "/complex/prefix/111111111111-aws-billing-csv-2015-06.csv";

        // Execute
        Optional<S3BillingRecordFile> result =
                S3BillingRecordFile.parseS3Key(null, s3Key);

        // Verify
        assertS3BillingRecordEquals(
                result,
                null,
                s3Key,
                FileType.CSV,
                2015,
                6,
                false);
    }

    @Test
    public void isS3BillingRecordFile_True() {
        // Execute
        boolean result = S3BillingRecordFile.isS3BillingRecordFile("111111111111-aws-billing-csv-2015-06.csv");

        // Verify
        assertTrue(result);
    }

    @Test
    public void isS3BillingRecordFile_False() {
        // Execute
        boolean result = S3BillingRecordFile.isS3BillingRecordFile("foo.txt");

        // Verify
        assertFalse(result);
    }

    private void assertS3BillingRecordEquals(Optional<S3BillingRecordFile> result,
                                             String expectedS3Key,
                                             FileType expectedFileType,
                                             int expectedYear,
                                             int expectedMonth,
                                             boolean expectedIsZip) {
        assertS3BillingRecordEquals(
                result,
                TEST_BUCKET_NAME,
                expectedS3Key,
                expectedFileType,
                expectedYear,
                expectedMonth,
                expectedIsZip);
    }

    private void assertS3BillingRecordEquals(Optional<S3BillingRecordFile> result,
                                             String expectedBucketName,
                                             String expectedS3Key,
                                             FileType expectedFileType,
                                             int expectedYear,
                                             int expectedMonth,
                                             boolean expectedIsZip) {
        assertTrue(result.isPresent());
        assertEquals(expectedBucketName, result.get().getBucketName());
        assertEquals(expectedS3Key, result.get().getKey());
        assertEquals(TEST_ACCOUNT_ID, result.get().getAccountId());
        assertEquals(expectedFileType, result.get().getType());
        assertEquals(expectedYear, result.get().getYear());
        assertEquals(expectedMonth, result.get().getMonth());
        assertEquals(expectedIsZip, result.get().isZip());
    }
}