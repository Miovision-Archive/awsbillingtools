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

package com.miovision.oss.awsbillingtools.examples.parser;

import com.miovision.oss.awsbillingtools.parser.BillingRecordParser;
import com.miovision.oss.awsbillingtools.parser.DetailedLineItem;
import com.miovision.oss.awsbillingtools.parser.DetailedLineItemParser;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.stream.Stream;

/**
 * An example for how to use the DetailedLineItemParser class.
 */
public class DetailedLineItemParserExampleApplication {
    //CHECKSTYLE.OFF: LineLength
    private static final String HEADER = "\"InvoiceID\",\"PayerAccountId\",\"LinkedAccountId\",\"RecordType\",\"RecordId\",\"ProductName\",\"RateId\",\"SubscriptionId\",\"PricingPlanId\",\"UsageType\",\"Operation\",\"AvailabilityZone\",\"ReservedInstance\",\"ItemDescription\",\"UsageStartDate\",\"UsageEndDate\",\"UsageQuantity\",\"Rate\",\"Cost\",\"ResourceId\",\"user:foo\"\n";
    private static final String RECORD_1 = "\"12345678\",\"111111111111\",\"222222222222\",\"LineItem\",\"12345678901234567890123456\",\"EC2 Instance\",\"9191919\",\"723456723\",\"515253\",\"Usage Type Here\",\"Hourly\",\"us-east-1\",\"Y\",\"$0.05 per hour for a thing\",\"2015-12-01 00:00:00\",\"2015-12-01 01:00:00\",\"1.02007083\",\"0.0500000000\",\"0.05100354\",\"Resource ID\",\"bar\"\n";
    //CHECKSTYLE.ON: LineLength
    private static final String EXAMPLE_FILE_CONTENT = HEADER + RECORD_1;

    public static void main(String[] args) throws IOException {
        // DetailedLineItemParser implements the BillingRecordParser<T> interface.
        BillingRecordParser<DetailedLineItem> detailedLineItemParser = new DetailedLineItemParser();

        // All BillingRecordParser classes read their data from a java.io.Reader instance.
        Reader reader = new StringReader(EXAMPLE_FILE_CONTENT);

        System.out.println("Product Name\t\tCost");

        // The BillingRecordParser takes ownership of the reader instance and therefore when you are done with the
        // stream you must close (to close the underlying reader instance). Java's try-with-resources is an excellent
        // way to accomplish this.
        try(Stream<DetailedLineItem> billingRecords = detailedLineItemParser.parse(reader)) {

            // Now that you have a Stream of DetailedLineItem objects you can map & filter to your hearts content. In
            // this example we will just print out some information using a forEach method.

            billingRecords.forEach(billingRecord -> {
                System.out.println(String.format("%s\t\t$%f", billingRecord.getProductName(), billingRecord.getCost()));
            });
        }
    }
}
