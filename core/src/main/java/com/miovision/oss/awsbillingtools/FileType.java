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

package com.miovision.oss.awsbillingtools;

import com.miovision.oss.awsbillingtools.parser.DetailedLineItem;

/**
 * The billing record file type enumeration.
 */
public enum FileType {
    CSV("aws-billing-csv"),
    LINE_ITEMS("aws-billing-detailed-line-items"),
    DETAILED_LINE_ITEMS("aws-billing-detailed-line-items-with-resources-and-tags"),
    COST_ALLOCATION("aws-cost-allocation");

    private final String fileString;

    FileType(String fileString) {
        this.fileString = fileString;
    }

    public String toFileString() {
        return fileString;
    }

    public static FileType forClass(Class<?> clazz) {
        if(DetailedLineItem.class.equals(clazz)) {
            return DETAILED_LINE_ITEMS;
        }
        else {
            throw new UnsupportedRecordTypeException(clazz);
        }
    }

    private static class UnsupportedRecordTypeException extends RuntimeException {
        public UnsupportedRecordTypeException(Class<?> clazz) {
            super(String.format("The class %s is not a supported record type class", clazz.getCanonicalName()));
        }
    }
}
