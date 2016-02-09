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

package com.miovision.oss.awsbillingtools.s3.scanner;

import com.miovision.oss.awsbillingtools.FileType;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A class that represents a billing record file stored within S3.
 */
public class S3BillingRecordFile {
    private static final String FILE_REGEX = "(\\d+)-(.*)-(\\d{4})-(\\d{2})\\.(.*)";
    private static final Pattern FILE_PATTERN = Pattern.compile(FILE_REGEX);
    private static final Map<String, FileType> FILE_TYPE_STRING_TO_ENUM =
            Arrays.asList(FileType
                    .values())
                    .stream()
                    .collect(Collectors.toMap(type -> type.toFileString(), type1 -> type1));
    private static final String DEFAULT_DELIMITER = "/";

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

    public static boolean isS3BillingRecordFile(String s3Key) {
        return isS3BillingRecordFile(s3Key, DEFAULT_DELIMITER);
    }

    public static boolean isS3BillingRecordFile(String s3Key, String delimiter) {
        return parseS3Key(null, s3Key, delimiter).isPresent();
    }

    public static Optional<S3BillingRecordFile> parseS3Key(String bucketName, String s3Key) {
        return parseS3Key(bucketName, s3Key, DEFAULT_DELIMITER);
    }

    public static Optional<S3BillingRecordFile> parseS3Key(String bucketName, String s3Key, String delimiter) {
        if(s3Key == null || "".equals(s3Key)) {
            return Optional.empty();
        }

        String matchKey = s3Key;
        int index = s3Key.lastIndexOf(delimiter);
        if(index > -1) {
            matchKey = matchKey.substring(index + 1);
        }

        final Matcher filePatternMatcher = FILE_PATTERN.matcher(matchKey);
        if(!filePatternMatcher.matches()) {
            return Optional.empty();
        }

        final String awsAccountId = filePatternMatcher.group(1);
        final FileType fileType = parseFileType(filePatternMatcher.group(2));
        final int year = Integer.parseInt(filePatternMatcher.group(3));
        final int month = Integer.parseInt(filePatternMatcher.group(4));
        final String suffix = filePatternMatcher.group(5);
        final boolean zip = suffix.endsWith(".zip");

        return Optional.of(new S3BillingRecordFile(bucketName, s3Key, awsAccountId, fileType, year, month, zip));
    }

    private static FileType parseFileType(String fileTypeStr) {
        return FILE_TYPE_STRING_TO_ENUM.get(fileTypeStr);
    }
}
