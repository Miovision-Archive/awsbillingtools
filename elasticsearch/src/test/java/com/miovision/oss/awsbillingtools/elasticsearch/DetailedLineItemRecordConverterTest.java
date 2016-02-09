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

package com.miovision.oss.awsbillingtools.elasticsearch;

import static org.junit.Assert.assertEquals;

import com.miovision.oss.awsbillingtools.parser.DetailedLineItem;
import com.miovision.oss.awsbillingtools.parser.DetailedLineItemParser;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

/**
 * Unit tests for the DetailedLineItemRecordConverter class.
 */
public class DetailedLineItemRecordConverterTest {
    // CHECKSTYLE.OFF: LineLengthCheck
    private static final String HEADER = "\"InvoiceID\",\"PayerAccountId\",\"LinkedAccountId\",\"RecordType\",\"RecordId\",\"ProductName\",\"RateId\",\"SubscriptionId\",\"PricingPlanId\",\"UsageType\",\"Operation\",\"AvailabilityZone\",\"ReservedInstance\",\"ItemDescription\",\"UsageStartDate\",\"UsageEndDate\",\"UsageQuantity\",\"Rate\",\"Cost\",\"ResourceId\",\"user:foo\"\n";
    private static final String RECORD_1 = "\"12345678\",\"111111111111\",\"222222222222\",\"LineItem\",\"12345678901234567890123456\",\"Product Name Here\",\"9191919\",\"723456723\",\"515253\",\"Usage Type Here\",\"Hourly\",\"us-east-1\",\"Y\",\"$0.05 per hour for a thing\",\"2015-12-01 00:00:00\",\"2015-12-01 01:00:00\",\"1.02007083\",\"0.0500000000\",\"0.05100354\",\"Resource ID\",\"bar\"\n";
    // CHECKSTYLE.ON: LineLengthCheck
    private static final DetailedLineItem detailedLineItem;

    static {
        DetailedLineItemParser parser = new DetailedLineItemParser();
        try {
            detailedLineItem = parser.parse(new StringReader(HEADER + RECORD_1))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Unexpected end of stream"));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DetailedLineItemRecordConverter recordConverter;

    @Before
    public void setUp() throws Exception {
        recordConverter = new DetailedLineItemRecordConverter();
    }

    @Test
    public void testGetRecordId() throws Exception {
        // Exercise
        final String recordId = recordConverter.getRecordId(detailedLineItem);

        // Verify
        assertEquals("12345678901234567890123456", recordId);
    }

    @Test
    public void testGetRecordType() throws Exception {
        // Exercise
        final String recordType = recordConverter.getRecordType(detailedLineItem);

        // Verify
        assertEquals("LineItem", recordType);
    }

    @Test
    public void testGetRecordFields() throws Exception {
        // Exercise
        Map<String, ?> fields = recordConverter.getRecordFields(detailedLineItem);

        // Verify
        assertEquals("12345678", fields.get("invoiceId"));
        assertEquals("111111111111", fields.get("payerAccountId"));
        assertEquals("222222222222", fields.get("linkedAccountId"));
        assertEquals("Product Name Here", fields.get("productName"));
        assertEquals("9191919", fields.get("rateId"));
        assertEquals("723456723", fields.get("subscriptionId"));
        assertEquals("515253", fields.get("pricingPlanId"));
        assertEquals("Usage Type Here", fields.get("usageType"));
        assertEquals("Hourly", fields.get("operation"));
        assertEquals("us-east-1", fields.get("availabilityZone"));
        assertEquals(true, fields.get("reserveInstance"));
        assertEquals("$0.05 per hour for a thing", fields.get("itemDescription"));
        assertEquals(1.02007083, (Double)fields.get("usageQuantity"), 0.0000000001);
        assertEquals("2015-12-01T00:00:00.000-05:00", fields.get("usageStartDate").toString());
        assertEquals("2015-12-01T01:00:00.000-05:00", fields.get("usageEndDate").toString());
        assertEquals(0.05, (Double)fields.get("rate"), 0.000001);
        assertEquals(0.05100354, (Double)fields.get("cost"), 0.0000000001);
        assertEquals("Resource ID", fields.get("resourceId"));
        assertEquals("bar", fields.get("user:foo"));
    }
}