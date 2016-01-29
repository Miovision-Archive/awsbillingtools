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

import static com.miovision.oss.awsbillingtools.s3.testutils.CustomAssertions.assertS3BillingRecordFile;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.miovision.oss.awsbillingtools.FileType;
import com.miovision.oss.awsbillingtools.s3.testutils.CustomAssertions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Unit tests for the S3BillingRecordFileScanner.
 */
@RunWith(MockitoJUnitRunner.class)
public class S3BillingRecordFileScannerTest {
    public static final String TEST_BUCKET_NAME = "bucketName";
    public static final String TEST_AWS_ACCOUNT_ID = CustomAssertions.TEST_AWS_ACCOUNT_ID;
    //CHECKSTYLE.OFF: LineLength
    public static final String DETAILED_LINE_ITEMS_2015_05 = "111111111111-aws-billing-detailed-line-items-with-resources-and-tags-2015-05.csv.zip";
    public static final String DETAILED_LINE_ITEMS_2015_06 = "111111111111-aws-billing-detailed-line-items-with-resources-and-tags-2015-06.csv.zip";
    public static final String DETAILED_LINE_ITEMS_2015_07 = "111111111111-aws-billing-detailed-line-items-with-resources-and-tags-2015-07.csv.zip";
    public static final String DETAILED_LINE_ITEMS_2016_06 = "111111111111-aws-billing-detailed-line-items-with-resources-and-tags-2016-06.csv.zip";
    //CHECKSTYLE.ON: LineLength
    public static final String CSV_2015_06 = "111111111111-aws-billing-csv-2015-06.csv";
    public static final String LINE_ITEMS_2015_06 = "111111111111-aws-billing-detailed-line-items-2015-06.csv.zip";
    public static final String COST_ALLOCATION_2015_06 = "111111111111-aws-cost-allocation-2015-06.csv";

    @Mock
    private AmazonS3 amazonS3;

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testScan_ZeroS3ObjectSummaries() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithNoPrefix();
        givenAmazonS3ListObjectsReturningObjectListing(givenObjectListing(false));

        // Execute
        Stream<S3BillingRecordFile> stream = billingRecordFileScanner.scan();

        // Verify
        assertEquals(0, stream.collect(Collectors.toList()).size());
    }

    @Test
    public void testScan_OneS3ObjectSummaries() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithNoPrefix();
        S3ObjectSummary s3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2015_06);
        givenAmazonS3ListObjectsReturningObjectListing(givenObjectListing(false, s3ObjectSummary));

        // Execute
        Stream<S3BillingRecordFile> stream = billingRecordFileScanner.scan();

        // Verify
        List<S3BillingRecordFile> recordFiles = stream.collect(Collectors.toList());
        assertEquals(1, recordFiles.size());
        assertS3BillingRecordFile(recordFiles.get(0), FileType.DETAILED_LINE_ITEMS, 2015, 6, true);
    }

    @Test
    public void testScan_WithPrefix() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithPrefix("dir");
        S3ObjectSummary s3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2015_06);
        givenAmazonS3ListObjectsReturningObjectListing(givenObjectListing(false, s3ObjectSummary), "dir");

        // Execute
        Stream<S3BillingRecordFile> stream = billingRecordFileScanner.scan();

        // Verify
        List<S3BillingRecordFile> recordFiles = stream.collect(Collectors.toList());
        assertEquals(1, recordFiles.size());
        assertS3BillingRecordFile(recordFiles.get(0), FileType.DETAILED_LINE_ITEMS, 2015, 6, true);
    }

    @Test
    public void testScan_WithPrefixWithTrailingDelimeter() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithPrefix("dir/");
        S3ObjectSummary s3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2015_06);
        givenAmazonS3ListObjectsReturningObjectListing(givenObjectListing(false, s3ObjectSummary), "dir");

        // Execute
        Stream<S3BillingRecordFile> stream = billingRecordFileScanner.scan();

        // Verify
        List<S3BillingRecordFile> recordFiles = stream.collect(Collectors.toList());
        assertEquals(1, recordFiles.size());
        assertS3BillingRecordFile(recordFiles.get(0), FileType.DETAILED_LINE_ITEMS, 2015, 6, true);
    }

    @Test
    public void testScan_CanHandleTruncatedObjectListings() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithNoPrefix();
        S3ObjectSummary firstS3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2015_06);
        S3ObjectSummary secondS3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2015_07);
        ObjectListing firstObjectListing = givenObjectListing(true, firstS3ObjectSummary);
        givenAmazonS3ListObjectsReturningObjectListing(firstObjectListing);
        givenAmazonS3ListNextBatchOfObjectsReturningObjectListing(
                firstObjectListing,
                givenObjectListing(false, secondS3ObjectSummary));

        // Execute
        Stream<S3BillingRecordFile> stream = billingRecordFileScanner.scan();

        // Verify
        List<S3BillingRecordFile> recordFiles = stream.collect(Collectors.toList());
        assertEquals(2, recordFiles.size());
        assertS3BillingRecordFile(recordFiles.get(0), FileType.DETAILED_LINE_ITEMS, 2015, 6, true);
        assertS3BillingRecordFile(recordFiles.get(1), FileType.DETAILED_LINE_ITEMS, 2015, 7, true);
    }

    @Test
    public void testScan_ManyS3ObjectSummaries() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithNoPrefix();
        S3ObjectSummary firstS3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2015_06);
        S3ObjectSummary secondS3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2015_07);
        givenAmazonS3ListObjectsReturningObjectListing(
                givenObjectListing(false, firstS3ObjectSummary, secondS3ObjectSummary));

        // Execute
        Stream<S3BillingRecordFile> stream = billingRecordFileScanner.scan();

        // Verify
        List<S3BillingRecordFile> recordFiles = stream.collect(Collectors.toList());
        assertEquals(2, recordFiles.size());
        assertS3BillingRecordFile(recordFiles.get(0), FileType.DETAILED_LINE_ITEMS, 2015, 6, true);
        assertS3BillingRecordFile(recordFiles.get(1), FileType.DETAILED_LINE_ITEMS, 2015, 7, true);
    }

    @Test
    public void testScan_IgnoresS3ObjectsThatAreNotBillingFiles() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithNoPrefix();
        S3ObjectSummary firstS3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2015_06);
        S3ObjectSummary secondS3ObjectSummary = givenS3ObjectSummary("otherstuff.txt");
        givenAmazonS3ListObjectsReturningObjectListing(
                givenObjectListing(false, firstS3ObjectSummary, secondS3ObjectSummary));

        // Execute
        Stream<S3BillingRecordFile> stream = billingRecordFileScanner.scan();

        // Verify
        List<S3BillingRecordFile> recordFiles = stream.collect(Collectors.toList());
        assertEquals(1, recordFiles.size());
        assertS3BillingRecordFile(recordFiles.get(0), FileType.DETAILED_LINE_ITEMS, 2015, 6, true);
    }

    @Test
    public void testScan_CanHandleCsvFiles() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithNoPrefix();
        S3ObjectSummary s3ObjectSummary = givenS3ObjectSummary(CSV_2015_06);
        givenAmazonS3ListObjectsReturningObjectListing(givenObjectListing(false, s3ObjectSummary));

        // Execute
        Stream<S3BillingRecordFile> stream = billingRecordFileScanner.scan();

        // Verify
        List<S3BillingRecordFile> recordFiles = stream.collect(Collectors.toList());
        assertEquals(1, recordFiles.size());
        assertS3BillingRecordFile(recordFiles.get(0), FileType.CSV, 2015, 6, false);
    }

    @Test
    public void testScan_CanHandleLineItemFiles() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithNoPrefix();
        S3ObjectSummary s3ObjectSummary = givenS3ObjectSummary(LINE_ITEMS_2015_06);
        givenAmazonS3ListObjectsReturningObjectListing(givenObjectListing(false, s3ObjectSummary));

        // Execute
        Stream<S3BillingRecordFile> stream = billingRecordFileScanner.scan();

        // Verify
        List<S3BillingRecordFile> recordFiles = stream.collect(Collectors.toList());
        assertEquals(1, recordFiles.size());
        assertS3BillingRecordFile(recordFiles.get(0), FileType.LINE_ITEMS, 2015, 6, true);
    }

    @Test
    public void testScan_CanHandleDetailedLineItemFiles() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithNoPrefix();
        S3ObjectSummary s3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2015_06);
        givenAmazonS3ListObjectsReturningObjectListing(givenObjectListing(false, s3ObjectSummary));

        // Execute
        Stream<S3BillingRecordFile> stream = billingRecordFileScanner.scan();

        // Verify
        List<S3BillingRecordFile> recordFiles = stream.collect(Collectors.toList());
        assertEquals(1, recordFiles.size());
        assertS3BillingRecordFile(recordFiles.get(0), FileType.DETAILED_LINE_ITEMS, 2015, 6, true);
    }

    @Test
    public void testScan_CanHandleCostAllocationFiles() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithNoPrefix();
        S3ObjectSummary s3ObjectSummary = givenS3ObjectSummary(COST_ALLOCATION_2015_06);
        givenAmazonS3ListObjectsReturningObjectListing(givenObjectListing(false, s3ObjectSummary));

        // Execute
        Stream<S3BillingRecordFile> stream = billingRecordFileScanner.scan();

        // Verify
        List<S3BillingRecordFile> recordFiles = stream.collect(Collectors.toList());
        assertEquals(1, recordFiles.size());
        assertS3BillingRecordFile(recordFiles.get(0), FileType.COST_ALLOCATION, 2015, 6, false);
    }

    @Test
    public void testScan_WithFileType() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithNoPrefix();
        S3ObjectSummary firstS3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2015_06);
        S3ObjectSummary secondS3ObjectSummary = givenS3ObjectSummary(COST_ALLOCATION_2015_06);
        givenAmazonS3ListObjectsReturningObjectListing(
                givenObjectListing(false, firstS3ObjectSummary, secondS3ObjectSummary));

        // Execute
        Stream<S3BillingRecordFile> stream = billingRecordFileScanner.scan(FileType.DETAILED_LINE_ITEMS);

        // Verify
        List<S3BillingRecordFile> recordFiles = stream.collect(Collectors.toList());
        assertEquals(1, recordFiles.size());
        assertS3BillingRecordFile(recordFiles.get(0), FileType.DETAILED_LINE_ITEMS, 2015, 6, true);
    }

    @Test
    public void testScan_WithFileTypeYearAndMonth() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithNoPrefix();
        S3ObjectSummary firstS3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2015_05);
        S3ObjectSummary secondS3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2015_06);
        S3ObjectSummary thirdS3ObjectSummary = givenS3ObjectSummary(COST_ALLOCATION_2015_06);
        givenAmazonS3ListObjectsReturningObjectListing(
                givenObjectListing(false, firstS3ObjectSummary, secondS3ObjectSummary, thirdS3ObjectSummary));

        // Execute
        Stream<S3BillingRecordFile> stream = billingRecordFileScanner.scan(FileType.DETAILED_LINE_ITEMS, 2015, 6);

        // Verify
        List<S3BillingRecordFile> recordFiles = stream.collect(Collectors.toList());
        assertEquals(1, recordFiles.size());
        assertS3BillingRecordFile(recordFiles.get(0), FileType.DETAILED_LINE_ITEMS, 2015, 6, true);
    }

    @Test
    public void testScan_WithPredicate() throws Exception {
        // Setup
        S3BillingRecordFileScanner billingRecordFileScanner = givenScannerWithNoPrefix();
        S3ObjectSummary firstS3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2015_05);
        S3ObjectSummary secondS3ObjectSummary = givenS3ObjectSummary(DETAILED_LINE_ITEMS_2016_06);
        S3ObjectSummary thirdS3ObjectSummary = givenS3ObjectSummary(COST_ALLOCATION_2015_06);
        givenAmazonS3ListObjectsReturningObjectListing(
                givenObjectListing(false, firstS3ObjectSummary, secondS3ObjectSummary, thirdS3ObjectSummary));

        // Execute
        Stream<S3BillingRecordFile> stream =
                billingRecordFileScanner.scan(s3BillingRecordFile -> s3BillingRecordFile.getMonth() == 6);

        // Verify
        List<S3BillingRecordFile> recordFiles = stream.collect(Collectors.toList());
        assertEquals(2, recordFiles.size());
        assertS3BillingRecordFile(recordFiles.get(0), FileType.DETAILED_LINE_ITEMS, 2016, 6, true);
        assertS3BillingRecordFile(recordFiles.get(1), FileType.COST_ALLOCATION, 2015, 6, false);
    }

    private void givenAmazonS3ListNextBatchOfObjectsReturningObjectListing(ObjectListing previousObjectListing,
                                                                           ObjectListing objectListing) {
        when(amazonS3.listNextBatchOfObjects(previousObjectListing)).thenReturn(objectListing);
    }

    private void givenAmazonS3ListObjectsReturningObjectListing(ObjectListing objectListing) {
        when(amazonS3.listObjects(TEST_BUCKET_NAME, TEST_AWS_ACCOUNT_ID)).thenReturn(objectListing);
    }

    private void givenAmazonS3ListObjectsReturningObjectListing(ObjectListing objectListing, String prefix) {
        if(prefix.endsWith("/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        String key = prefix + "/" + TEST_AWS_ACCOUNT_ID;
        when(amazonS3.listObjects(TEST_BUCKET_NAME, key)).thenReturn(objectListing);
    }

    private S3ObjectSummary givenS3ObjectSummary(String key) {
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setBucketName(TEST_BUCKET_NAME);
        s3ObjectSummary.setKey(key);
        return s3ObjectSummary;
    }

    private ObjectListing givenObjectListing(boolean truncated, S3ObjectSummary... s3ObjectSummaries) {
        ObjectListing objectListing = mock(ObjectListing.class);
        when(objectListing.getObjectSummaries()).thenReturn(Arrays.asList(s3ObjectSummaries));
        when(objectListing.isTruncated()).thenReturn(truncated);
        return objectListing;
    }

    private S3BillingRecordFileScanner givenScannerWithNoPrefix() {
        return new S3BillingRecordFileScanner(amazonS3, TEST_BUCKET_NAME, "", TEST_AWS_ACCOUNT_ID);
    }

    private S3BillingRecordFileScanner givenScannerWithPrefix(String prefix) {
        return new S3BillingRecordFileScanner(amazonS3, TEST_BUCKET_NAME, prefix, TEST_AWS_ACCOUNT_ID);
    }
}