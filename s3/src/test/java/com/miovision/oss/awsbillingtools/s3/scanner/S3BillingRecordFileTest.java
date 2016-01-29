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
import static org.junit.Assert.assertTrue;

import com.miovision.oss.awsbillingtools.FileType;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the S3BillingRecordFile class.
 */
public class S3BillingRecordFileTest {
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
}