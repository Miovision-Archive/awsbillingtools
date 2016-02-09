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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.junit.Test;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the DetailedLineItem class.
 */
public class DetailedLineItemTest {
    //CHECKSTYLE.OFF: LineLength
    private static final String TEST_RECORD_WITHOUT_TAGS = "\"12345678\",\"111111111111\",\"222222222222\",\"LineItem\",\"12345678901234567890123456\",\"Product Name Here\",\"9191919\",\"723456723\",\"515253\",\"Usage Type Here\",\"Hourly\",\"us-east-1\",\"N\",\"$0.05 per hour for a thing\",\"2015-12-01 00:00:00\",\"2015-12-01 01:00:00\",\"1.02007083\",\"0.0500000000\",\"0.05100354\",\"Resource ID\",\n";
    private static final String TEST_RECORD_WITH_TAGS = "\"12345678\",\"111111111111\",\"222222222222\",\"LineItem\",\"12345678901234567890123456\",\"Product Name Here\",\"9191919\",\"723456723\",\"515253\",\"Usage Type Here\",\"Hourly\",\"us-east-1\",\"Y\",\"$0.05 per hour for a thing\",\"2015-12-01 00:00:00\",\"2015-12-01 01:00:00\",\"1.02007083\",\"0.0500000000\",\"0.05100354\",\"Resource ID\",\"bar\"\n";
    //CHECKSTYLE.OFF: LineLength

    @Test
    public void testGetInvoiceId() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String invoiceId = detailedLineItem.getInvoiceId();

        // Verify
        assertEquals("12345678", invoiceId);
    }

    @Test
    public void testGetPayerAccountId() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String payerAccountId = detailedLineItem.getPayerAccountId();

        // Verify
        assertEquals("111111111111", payerAccountId);
    }

    @Test
    public void testGetLinkedAccountId() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String linkedAccountId = detailedLineItem.getLinkedAccountId();

        // Verify
        assertEquals("222222222222", linkedAccountId);
    }

    @Test
    public void testGetRecordType() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String recordType = detailedLineItem.getRecordType();

        // Verify
        assertEquals("LineItem", recordType);
    }

    @Test
    public void testGetRecordId() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String recordId = detailedLineItem.getRecordId();

        // Verify
        assertEquals("12345678901234567890123456", recordId);
    }

    @Test
    public void testGetProductName() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String productName = detailedLineItem.getProductName();

        // Verify
        assertEquals("Product Name Here", productName);
    }

    @Test
    public void testGetRateId() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String rateId = detailedLineItem.getRateId();

        // Verify
        assertEquals("9191919", rateId);
    }

    @Test
    public void testGetSubscriptionId() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String subscriptionId = detailedLineItem.getSubscriptionId();

        // Verify
        assertEquals("723456723", subscriptionId);
    }

    @Test
    public void testGetPricingPlanId() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String pricingPlanId = detailedLineItem.getPricingPlanId();

        // Verify
        assertEquals("515253", pricingPlanId);
    }

    @Test
    public void testGetUsageType() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String usageType = detailedLineItem.getUsageType();

        // Verify
        assertEquals("Usage Type Here", usageType);
    }

    @Test
    public void testGetOperation() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String operation = detailedLineItem.getOperation();

        // Verify
        assertEquals("Hourly", operation);
    }

    @Test
    public void testGetAvailabilityZone() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String availabilityZone = detailedLineItem.getAvailabilityZone();

        // Verify
        assertEquals("us-east-1", availabilityZone);
    }

    @Test
    public void testIsReserveInstance_False() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        boolean reserveInstance = detailedLineItem.isReserveInstance();

        // Verify
        assertFalse(reserveInstance);
    }

    @Test
    public void testIsReserveInstance_True() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithTags();

        // Execute
        boolean reserveInstance = detailedLineItem.isReserveInstance();

        // Verify
        assertTrue(reserveInstance);
    }

    @Test
    public void testGetItemDescription() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String itemDescription = detailedLineItem.getItemDescription();

        // Verify
        assertEquals("$0.05 per hour for a thing", itemDescription);
    }

    @Test
    public void testGetUsageStartDate() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        DateTime usageStartDate = detailedLineItem.getUsageStartDate();

        // Verify
        assertEquals(DetailedLineItem.DATE_TIME_FORMATTER.parseDateTime("2015-12-01 00:00:00"), usageStartDate);
    }

    @Test
    public void testGetUsageEndDate() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        DateTime usageEndDate = detailedLineItem.getUsageEndDate();

        // Verify
        assertEquals(DetailedLineItem.DATE_TIME_FORMATTER.parseDateTime("2015-12-01 01:00:00"), usageEndDate);
    }

    @Test
    public void testGetUsageQuantity() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        double usageQuantity = detailedLineItem.getUsageQuantity();

        // Verify
        assertEquals(1.02, usageQuantity, 0.001);
    }

    @Test
    public void testGetRate() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        double rate = detailedLineItem.getRate();

        // Verify
        assertEquals(0.05, rate, 0.001);
    }

    @Test
    public void testGetCost() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        double cost = detailedLineItem.getCost();

        // Verify
        assertEquals(0.051, cost, 0.001);
    }

    @Test
    public void testGetResourceId() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Execute
        String resourceId = detailedLineItem.getResourceId();

        // Verify
        assertEquals("Resource ID", resourceId);
    }

    @Test
    public void testGetTag_NotExists() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithTags();

        // Exercise
        String value = detailedLineItem.getTag("Nope");

        // Verify
        assertNull(value);
    }

    @Test
    public void testGetTag_Exists() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithTags();

        // Exercise
        String value = detailedLineItem.getTag("foo");

        // Verify
        assertEquals("bar", value);
    }

    @Test
    public void testGetTags_WithNoTags() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithoutTags();

        // Exercise
        Map<String, String> tags = detailedLineItem.getTags();

        // Verify
        assertEquals(0, tags.size());
    }

    @Test
    public void testGetTags_WithTags() throws Exception {
        // Setup
        DetailedLineItem detailedLineItem = givenDetailedLineItemWithTags();

        // Exercise
        Map<String, String> tags = detailedLineItem.getTags();

        // Verify
        assertEquals(1, tags.size());
        assertEquals("bar", tags.get("foo"));
    }

    private DetailedLineItem givenDetailedLineItemWithTags() {
        try {
            CSVParser csvParser = CSVFormat.EXCEL.parse(new StringReader(TEST_RECORD_WITH_TAGS));
            CSVRecord csvRecord = csvParser.getRecords().get(0);
            return givenDetailedLineItem(csvRecord, Arrays.asList("foo"));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DetailedLineItem givenDetailedLineItemWithoutTags() {
        try {
            CSVParser csvParser = CSVFormat.EXCEL.parse(new StringReader(TEST_RECORD_WITHOUT_TAGS));
            CSVRecord csvRecord = csvParser.getRecords().get(0);
            return givenDetailedLineItem(csvRecord, new ArrayList<>(0));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DetailedLineItem givenDetailedLineItem(CSVRecord csvRecord, List<String> tags) {
        return new DetailedLineItem(csvRecord, tags);
    }
}