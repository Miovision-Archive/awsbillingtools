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

import com.miovision.oss.awsbillingtools.FileType;

/**
 * A class that represents a billing record file stored within S3.
 */
public class S3BillingRecordFile {
    protected final String accountId;
    protected final FileType type;
    protected final int year;
    protected final int month;
    protected final boolean zip;
    private final String bucketName;
    private final String key;

    public S3BillingRecordFile(
            String bucketName,
            String key,
            String accountId,
            FileType type,
            int year,
            int month,
            boolean zip) {
        this.type = type;
        this.year = year;
        this.accountId = accountId;
        this.month = month;
        this.zip = zip;
        this.bucketName = bucketName;
        this.key = key;
    }

    public String getAccountId() {
        return accountId;
    }

    public FileType getType() {
        return type;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public boolean isZip() {
        return zip;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getKey() {
        return key;
    }
}
