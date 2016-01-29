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

package com.miovision.oss.awsbillingtools.s3.testutils;

import static org.junit.Assert.assertEquals;

import com.miovision.oss.awsbillingtools.FileType;
import com.miovision.oss.awsbillingtools.s3.scanner.S3BillingRecordFile;

/**
 * Custom assertions.
 */
public class CustomAssertions {
    public static final String TEST_AWS_ACCOUNT_ID = "111111111111";

    public static void assertS3BillingRecordFile(S3BillingRecordFile s3BillingRecordFile,
                                                 FileType fileType,
                                                 int year,
                                                 int month,
                                                 boolean zip) {
        assertEquals(TEST_AWS_ACCOUNT_ID, s3BillingRecordFile.getAccountId());
        assertEquals(fileType, s3BillingRecordFile.getType());
        assertEquals(year, s3BillingRecordFile.getYear());
        assertEquals(month, s3BillingRecordFile.getMonth());
        assertEquals(zip, s3BillingRecordFile.isZip());
    }
}
