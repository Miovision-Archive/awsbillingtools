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

package com.miovision.oss.awsbillingtools.parser;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Unit tests for DetailedLineItemParser.
 */
public class DetailedLineItemParserTest {
    // CHECKSTYLE.OFF: LineLengthCheck
    private static final String HEADER = "\"InvoiceID\",\"PayerAccountId\",\"LinkedAccountId\",\"RecordType\",\"RecordId\",\"ProductName\",\"RateId\",\"SubscriptionId\",\"PricingPlanId\",\"UsageType\",\"Operation\",\"AvailabilityZone\",\"ReservedInstance\",\"ItemDescription\",\"UsageStartDate\",\"UsageEndDate\",\"UsageQuantity\",\"Rate\",\"Cost\",\"ResourceId\",\"user:foo\"\n";
    private static final String RECORD_1 = "\"12345678\",\"111111111111\",\"222222222222\",\"LineItem\",\"12345678901234567890123456\",\"Product Name Here\",\"9191919\",\"723456723\",\"515253\",\"Usage Type Here\",\"Hourly\",\"us-east-1\",\"Y\",\"$0.05 per hour for a thing\",\"2015-12-01 00:00:00\",\"2015-12-01 01:00:00\",\"1.02007083\",\"0.0500000000\",\"0.05100354\",\"Resource ID\",\"bar\"\n";
    // CHECKSTYLE.ON: LineLengthCheck
    private DetailedLineItemParser detailedLineItemParser;

    @Before
    public void setUp() throws Exception {
        detailedLineItemParser = new DetailedLineItemParser();
    }

    @Test
    public void testGetRecordTypeClass() {
        // Execute
        Class<DetailedLineItem> recordTypeClass = detailedLineItemParser.getRecordTypeClass();

        // Verify
        assertEquals(DetailedLineItem.class, recordTypeClass);
    }

    @Test
    public void testParse_NoContent() throws IOException {
        // Execute
        try(Stream<DetailedLineItem> stream = detailedLineItemParser.parse(new StringReader(""))) {
            // Verify
            assertEquals(0, stream.collect(Collectors.toList()).size());
        }
    }

    @Test
    public void testParse_ZeroRecords() throws IOException {
        // Setup
        String fileContents = HEADER;

        // Execute
        try(Stream<DetailedLineItem> stream = detailedLineItemParser.parse(new StringReader(fileContents))) {
            // Verify
            assertEquals(0, stream.collect(Collectors.toList()).size());
        }
    }

    @Test
    public void testParse_OneRecord() throws IOException {
        // Setup
        String fileContents = HEADER + RECORD_1;

        // Execute
        try(Stream<DetailedLineItem> stream = detailedLineItemParser.parse(new StringReader(fileContents))) {
            // Verify
            List<DetailedLineItem> records = stream.collect(Collectors.toList());
            assertEquals(1, records.size());
            assertEquals("12345678", records.get(0).getInvoiceId());
            assertEquals("bar", records.get(0).getTag("user:foo"));
        }
    }
}