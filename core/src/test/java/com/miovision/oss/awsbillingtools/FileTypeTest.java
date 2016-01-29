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

import static org.junit.Assert.assertEquals;

import com.miovision.oss.awsbillingtools.parser.DetailedLineItem;
import org.junit.Test;

/**
 * Unit tests for the FileType class.
 */
public class FileTypeTest {
    @Test
    public void testToFileString_Csv() {
        // Execute
        String fileString = FileType.CSV.toFileString();

        // Verify
        assertEquals("aws-billing-csv", fileString);
    }

    @Test
    public void testToFileString_LineItems() {
        // Execute
        String fileString = FileType.LINE_ITEMS.toFileString();

        // Verify
        assertEquals("aws-billing-detailed-line-items", fileString);
    }

    @Test
    public void testToFileString_DetailedLineItems() {
        // Execute
        String fileString = FileType.DETAILED_LINE_ITEMS.toFileString();

        // Verify
        assertEquals("aws-billing-detailed-line-items-with-resources-and-tags", fileString);
    }

    @Test
    public void testToFileString_CostAllocation() {
        // Execute
        String fileString = FileType.COST_ALLOCATION.toFileString();

        // Verify
        assertEquals("aws-cost-allocation", fileString);
    }

    @Test(expected = Exception.class)
    public void testForClass_NotSupported() throws Exception {
        // Execute
        FileType.forClass(NotSupportedRecordType.class);
    }

    @Test
    public void testForClass_DetailedLineItem() {
        // Execute
        FileType fileType = FileType.forClass(DetailedLineItem.class);

        // Verify
        assertEquals(FileType.DETAILED_LINE_ITEMS, fileType);
    }

    private class NotSupportedRecordType {
    }
}