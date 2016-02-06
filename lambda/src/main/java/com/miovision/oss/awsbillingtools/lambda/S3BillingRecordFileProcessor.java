package com.miovision.oss.awsbillingtools.lambda;

import com.miovision.oss.awsbillingtools.s3.scanner.S3BillingRecordFile;

/**
 * An interface for objects that can process S3 billing record files.
 */
public interface S3BillingRecordFileProcessor {
    /**
     * Begin processing.
     *
     * <p>
     * This method is called once before any calls to apply().
     * </p>
     */
    void begin();

    /**
     * Apply an S3 billing record file to the processor.
     *
     * @param s3BillingRecordFile The billing record file.
     */
    void apply(S3BillingRecordFile s3BillingRecordFile);

    /**
     * Complete Processing with Success.
     *
     * <p>
     * Called when all apply() methods have completed successfully.
     * </p>
     */
    void completeSuccess();

    /**
     * Complete Processing with failure.
     *
     * @param throwable The error condition.
     */
    void completeError(Throwable throwable);
}
