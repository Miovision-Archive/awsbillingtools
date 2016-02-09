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

import com.miovision.oss.awsbillingtools.parser.DetailedLineItem;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of ElasticsearchBillingRecordConverter for DetailedLineItem objects.
 */
public class DetailedLineItemRecordConverter
        implements ElasticsearchBillingRecordConverter<DetailedLineItem> {

    @Override
    public String getRecordId(DetailedLineItem record) {
        return record.getRecordId();
    }

    @Override
    public String getRecordType(DetailedLineItem record) {
        return record.getRecordType();
    }

    @Override
    public Map<String, ?> getRecordFields(DetailedLineItem record) {
        HashMap<String, Object> fields = new HashMap<>();
        fields.put("invoiceId", record.getInvoiceId());
        fields.put("payerAccountId", record.getPayerAccountId());
        fields.put("linkedAccountId", record.getLinkedAccountId());
        fields.put("productName", record.getProductName());
        fields.put("rateId", record.getRateId());
        fields.put("subscriptionId", record.getSubscriptionId());
        fields.put("pricingPlanId", record.getPricingPlanId());
        fields.put("usageType", record.getUsageType());
        fields.put("operation", record.getOperation());
        fields.put("availabilityZone", record.getAvailabilityZone());
        fields.put("reserveInstance", record.isReserveInstance());
        fields.put("itemDescription", record.getItemDescription());
        fields.put("usageQuantity", record.getUsageQuantity());
        fields.put("usageStartDate", record.getUsageStartDate());
        fields.put("usageEndDate", record.getUsageEndDate());
        fields.put("rate", record.getRate());
        fields.put("cost", record.getCost());
        fields.put("resourceId", record.getResourceId());
        record.getTags().forEach((key, value) -> {
            fields.put(key, value);
        });
        return fields;
    }
}
