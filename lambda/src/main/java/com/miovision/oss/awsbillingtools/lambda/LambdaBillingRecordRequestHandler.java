package com.miovision.oss.awsbillingtools.lambda;

import com.miovision.oss.awsbillingtools.s3.scanner.S3BillingRecordFile;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Optional;

/**
 * An AWS Lambda request handler that uses a factory to delegate to S3BillingRecordFileProcessor instances.
 */
public class LambdaBillingRecordRequestHandler implements RequestHandler<S3Event, String> {
    private static final String DEFAULT_DELIMITER = "/";
    private final S3BillingRecordFileProcessorFactory processorFactory;
    private String delimiter = DEFAULT_DELIMITER;

    public LambdaBillingRecordRequestHandler(S3BillingRecordFileProcessorFactory processorFactory) {
        this.processorFactory = processorFactory;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public String handleRequest(S3Event input, Context context) {
        S3BillingRecordFileProcessor processor = processorFactory.create(context);
        try {
            processor.begin();
            handleRecords(input.getRecords(), context, processor);
            processor.completeSuccess();
        }
        catch(Throwable e) {
            logThrowable(context, e);
            processor.completeError(e);
        }
        return "";
    }

    protected void handleRecords(List<S3EventNotification.S3EventNotificationRecord> records,
                                 Context context,
                                 S3BillingRecordFileProcessor processor) {
        records.stream().forEach(record -> handleRecord(record, context, processor));
    }

    protected void handleRecord(S3EventNotification.S3EventNotificationRecord record,
                                Context context,
                                S3BillingRecordFileProcessor processor) {
        S3EventNotification.S3Entity s3 = record.getS3();
        handleS3Entity(s3.getBucket().getName(), s3.getObject().getKey(), context, processor);
    }

    protected void handleS3Entity(String bucketName, String key,
                                  Context context,
                                  S3BillingRecordFileProcessor processor) {
        Optional<S3BillingRecordFile> s3BillingRecordFile = S3BillingRecordFile.parseS3Key(bucketName, key, delimiter);
        if(s3BillingRecordFile.isPresent()) {
            processor.apply(s3BillingRecordFile.get());
        }
        else {
            context.getLogger().log(
                    String.format("%s/%s is not a billing record file; S3 entity will be ignored", bucketName, key));
        }
    }

    private static void logThrowable(Context context, Throwable throwable) {
        final LambdaLogger logger = context.getLogger();
        logger.log(throwable.getMessage());
        try {
            try(StringWriter out = new StringWriter()) {
                try (PrintWriter printWriter = new PrintWriter(out)) {
                    throwable.printStackTrace(printWriter);
                    logger.log(out.getBuffer().toString());
                }
            }
        }
        catch (IOException e1) {
            logger.log("Unable to print stack trace");
        }
    }
}
