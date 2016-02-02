# AWS Billing Processor

*Java libraries for processing AWS billing files.*

This library supports the following features:
* Parsing AWS billing record files.
* Listing the AWS billing record files stored in S3.
* Pulling AWS billing records directly from S3.

# Getting Started
TODO

# Usage

## Parsing AWS Billing Files

The most basic usage of this library is to parse AWS billing files. AWS provides billing reports in 4 different formats. 
All parsers implement the `BillingRecordParser` interface where T (the parameterized type) is the billing record class.

```java
public interface BillingRecordParser<T> {
    Stream<T> parse(Reader reader) throws IOException;
}
```

_Currently only the "detailed line-item" parser is supported_.

The following is an example for how to use the `DetailedLineItemParser`.

```java
/**
 * An example for how to use the DetailedLineItemParser class.
 */
public class DetailedLineItemParserExampleApplication {
    private static final String HEADER = "\"InvoiceID\",\"PayerAccountId\",\"LinkedAccountId\",\"RecordType\",\"RecordId\",\"ProductName\",\"RateId\",\"SubscriptionId\",\"PricingPlanId\",\"UsageType\",\"Operation\",\"AvailabilityZone\",\"ReservedInstance\",\"ItemDescription\",\"UsageStartDate\",\"UsageEndDate\",\"UsageQuantity\",\"Rate\",\"Cost\",\"ResourceId\",\"user:foo\"\n";
    private static final String RECORD_1 = "\"12345678\",\"111111111111\",\"222222222222\",\"LineItem\",\"12345678901234567890123456\",\"EC2 Instance\",\"9191919\",\"723456723\",\"515253\",\"Usage Type Here\",\"Hourly\",\"us-east-1\",\"Y\",\"$0.05 per hour for a thing\",\"2015-12-01 00:00:00\",\"2015-12-01 01:00:00\",\"1.02007083\",\"0.0500000000\",\"0.05100354\",\"Resource ID\",\"bar\"\n";
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
```

## Listing billing record files stored in S3

TODO

## Pulling billing records directly from S3

TODO
