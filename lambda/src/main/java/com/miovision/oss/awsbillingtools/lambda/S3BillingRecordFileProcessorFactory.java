package com.miovision.oss.awsbillingtools.lambda;

import com.amazonaws.services.lambda.runtime.Context;

/**
 * A factory of S3BillingRecordFileProcessor objects.
 */
public interface S3BillingRecordFileProcessorFactory {
    S3BillingRecordFileProcessor create(Context context);
}
