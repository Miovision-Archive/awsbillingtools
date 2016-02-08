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

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A detailed line item billing record.
 */
public class DetailedLineItem {
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    private final CSVRecord csvRecord;
    private final List<String> tags;
    private transient LazyInitializer<DateTime> usageStartDate;
    private transient LazyInitializer<DateTime> usageEndDate;
    private transient Map<String, String> extractedTags;

    public DetailedLineItem(CSVRecord csvRecord, List<String> tags) {
        this.csvRecord = csvRecord;
        this.tags = tags;
    }

    public String getInvoiceId() {
        return csvRecord.get(0);
    }

    public String getPayerAccountId() {
        return csvRecord.get(1);
    }

    public String getLinkedAccountId() {
        return csvRecord.get(2);
    }

    public String getRecordType() {
        return csvRecord.get(3);
    }

    public String getRecordId() {
        return csvRecord.get(4);
    }

    public String getProductName() {
        return csvRecord.get(5);
    }

    public String getRateId() {
        return csvRecord.get(6);
    }

    public String getSubscriptionId() {
        return csvRecord.get(7);
    }

    public String getPricingPlanId() {
        return csvRecord.get(8);
    }

    public String getUsageType() {
        return csvRecord.get(9);
    }

    public String getOperation() {
        return csvRecord.get(10);
    }

    public String getAvailabilityZone() {
        return csvRecord.get(11);
    }

    public Boolean isReserveInstance() {
        return parseYn(csvRecord.get(12));
    }

    public String getItemDescription() {
        return csvRecord.get(13);
    }

    public DateTime getUsageStartDate() {
        if(usageStartDate == null) {
            usageStartDate = new DateTimeLazyInitializer(csvRecord, 14);
        }

        try {
            return usageStartDate.get();
        }
        catch (ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }

    public DateTime getUsageEndDate() {
        if(usageEndDate == null) {
            usageEndDate = new DateTimeLazyInitializer(csvRecord, 15);
        }

        try {
            return usageEndDate.get();
        }
        catch (ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }

    public Double getUsageQuantity() {
        return parseDouble(csvRecord.get(16));
    }

    public Double getRate() {
        return parseDouble(csvRecord.get(17));
    }

    public Double getCost() {
        return parseDouble(csvRecord.get(18));
    }

    public String getResourceId() {
        return csvRecord.get(19);
    }

    public String getTag(String tagName) {
        return getTags().get(tagName);
    }

    public Map<String, String> getTags() {
        if (extractedTags == null) {
            extractedTags = extractTags(csvRecord, tags);
        }
        assert extractedTags != null;
        return extractedTags;
    }

    private static Map<String, String> extractTags(CSVRecord csvRecord, List<String> tags) {
        Map<String, String> mappedTags = new HashMap<>();
        for (int i = 0; i < tags.size(); ++i) {
            mappedTags.put(tags.get(i), csvRecord.get(20 + i));
        }
        return mappedTags;
    }

    private static boolean parseYn(String str) {
        return "Y".equals(str) ? true : false;
    }

    private static Double parseDouble(String str) {
        return str == null || "".equals(str) ? null : Double.parseDouble(str);
    }

    private static class DateTimeLazyInitializer extends LazyInitializer<DateTime> {
        private final CSVRecord csvRecord;
        private final int fieldIndex;

        private DateTimeLazyInitializer(CSVRecord csvRecord, int fieldIndex) {
            this.csvRecord = csvRecord;
            this.fieldIndex = fieldIndex;
        }

        @Override
        protected DateTime initialize() throws ConcurrentException {
            String fieldStr = csvRecord.get(fieldIndex);
            return StringUtils.isEmpty(fieldStr) ? null : DATE_TIME_FORMATTER.parseDateTime(fieldStr);
        }
    }
}
