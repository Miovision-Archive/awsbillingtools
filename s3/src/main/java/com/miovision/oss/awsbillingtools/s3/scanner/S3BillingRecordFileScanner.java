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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * The default implementation of S3BillingRecordFileScanner.
 */
public class S3BillingRecordFileScanner {
    private static final String FILE_REGEX = "(\\d+)-(.*)-(\\d{4})-(\\d{2})\\.(.*)";
    private static final Pattern FILE_PATTERN = Pattern.compile(FILE_REGEX);
    private static final Map<String, FileType> FILE_TYPE_STRING_TO_ENUM =
            Arrays.asList(FileType
                    .values())
                    .stream()
                    .collect(Collectors.toMap(type -> type.toFileString(), type1 -> type1));
    private static final String DELIMITER = "/";
    private final AmazonS3 amazonS3;
    private final String bucketName;
    private final String prefix;
    private final String awsAccountId;

    public S3BillingRecordFileScanner(AmazonS3 amazonS3, String bucketName, String prefix, String awsAccountId) {
        this.amazonS3 = amazonS3;
        this.bucketName = bucketName;
        this.prefix = prefix;
        this.awsAccountId = awsAccountId;
    }

    public Stream<S3BillingRecordFile> scan() {
        final String resolvedPrefix = resolvePrefix(prefix, DELIMITER, awsAccountId);
        final ObjectListing objectListing = amazonS3.listObjects(bucketName, resolvedPrefix);
        return StreamSupport
                .stream(new S3ObjectSpliterator(amazonS3, objectListing), false)
                .map(s3ObjectSummary -> parse(s3ObjectSummary))
                .filter(s3BillingRecordFile -> s3BillingRecordFile != null);
    }

    public Stream<S3BillingRecordFile> scan(FileType type) {
        return scan(s3BillingRecordFile -> s3BillingRecordFile.getType() == type);
    }

    public Stream<S3BillingRecordFile> scan(FileType fileType, int year, int month) {
        return scan(s3BillingRecordFile ->
                s3BillingRecordFile.getType() == fileType &&
                s3BillingRecordFile.getYear() == year &&
                s3BillingRecordFile.getMonth() == month);
    }

    public Stream<S3BillingRecordFile> scan(Predicate<S3BillingRecordFile> predicate) {
        return scan().filter(predicate);
    }

    protected String resolvePrefix(String prefix, String delimiter, String awsAccountId) {
        if(prefix == null || "".equals(prefix)) {
            return awsAccountId;
        }
        else {
            if(prefix.endsWith(delimiter)) {
                prefix = prefix.substring(0, prefix.length() - 1);
            }
            return String.format("%s%s%s", prefix, delimiter, awsAccountId);
        }
    }

    protected S3BillingRecordFile parse(S3ObjectSummary s3ObjectSummary) {
        final String bucketName = s3ObjectSummary.getBucketName();
        final String key = s3ObjectSummary.getKey();

        String matchKey = key;
        int index = key.indexOf(DELIMITER);
        if(index > -1) {
            matchKey = matchKey.substring(index + 1);
        }

        Matcher filePatternMatcher = FILE_PATTERN.matcher(matchKey);
        if(!filePatternMatcher.matches()) {
            return null;
        }

        final String awsAccountId = filePatternMatcher.group(1);
        final FileType fileType = parseFileType(filePatternMatcher.group(2));
        final int year = Integer.parseInt(filePatternMatcher.group(3));
        final int month = Integer.parseInt(filePatternMatcher.group(4));
        final String suffix = filePatternMatcher.group(5);
        final boolean zip = suffix.endsWith(".zip");

        return new S3BillingRecordFile(bucketName, key, awsAccountId, fileType, year, month, zip);
    }

    private FileType parseFileType(String fileTypeStr) {
        return FILE_TYPE_STRING_TO_ENUM.get(fileTypeStr);
    }

    private static class S3ObjectSpliterator implements Spliterator<S3ObjectSummary> {
        private final AmazonS3 amazonS3;
        private ObjectListing objectListing;
        private Iterator<S3ObjectSummary> objectSummaryIterator;

        public S3ObjectSpliterator(AmazonS3 amazonS3, ObjectListing objectListing) {
            this.amazonS3 = amazonS3;
            this.objectListing = objectListing;
            resetObjectSumaryIterator();
        }

        @Override
        public boolean tryAdvance(Consumer<? super S3ObjectSummary> action) {
            boolean result = objectSummaryIterator.hasNext();
            if(result) {
                action.accept(objectSummaryIterator.next());
            }
            else {
                if(objectListing.isTruncated()) {
                    result = true;
                    objectListing = amazonS3.listNextBatchOfObjects(objectListing);
                    resetObjectSumaryIterator();
                }
            }

            return result;
        }

        @Override
        public Spliterator<S3ObjectSummary> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return ORDERED;
        }

        private void resetObjectSumaryIterator() {
            objectSummaryIterator = objectListing.getObjectSummaries().iterator();
        }
    }
}
