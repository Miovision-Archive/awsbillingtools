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

package com.miovision.oss.awsbillingtools.lambda.logging;

import com.miovision.oss.awsbillingtools.lambda.LambdaContextHolder;
import com.amazonaws.services.lambda.runtime.Context;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.locks.ReentrantLock;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.DeferredProcessingAware;
import ch.qos.logback.core.status.ErrorStatus;

/**
 * A logback appender that writes to a Lambda context.
 */
public class LambdaAppender<E> extends UnsynchronizedAppenderBase<E> {
    private final ReentrantLock lock = new ReentrantLock(true);
    private Encoder<E> encoder;

    public Encoder<E> getEncoder() {
        return encoder;
    }

    public void setEncoder(Encoder<E> encoder) {
        this.encoder = encoder;
    }

    @Override
    public void start() {
        if (encoder == null) {
            addStatus(new ErrorStatus("No encoder set for the appender named \"" + name + "\".", this));
        }
        else {
            super.start();
        }
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            super.stop();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    protected void append(E eventObject) {
        if (!isStarted()) {
            return;
        }

        subAppend(eventObject);
    }

    protected void subAppend(E eventObject) {
        if (!isStarted()) {
            return;
        }
        try {
            // this step avoids LBCLASSIC-139
            if (eventObject instanceof DeferredProcessingAware) {
                ((DeferredProcessingAware) eventObject).prepareForDeferredProcessing();
            }
            // the synchronization prevents the OutputStream from being closed while we
            // are writing. It also prevents multiple threads from entering the same
            // converter. Converters assume that they are in a synchronized block.
            lock.lock();
            try {
                try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                    encoder.init(outputStream);
                    try {
                        encoder.doEncode(eventObject);
                        final String encodedMessage = new String(outputStream.toByteArray(), Charset.defaultCharset());
                        final Context context = LambdaContextHolder.getInstance().getContext();
                        if(context == null) {
                            addStatus(new ErrorStatus("Lambda context is not available", this));
                        }
                        else {
                            context.getLogger().log(encodedMessage);
                        }
                    }
                    finally {
                        encoder.close();
                    }
                }
            }
            finally {
                lock.unlock();
            }
        } catch (IOException ioe) {
            // as soon as an exception occurs, move to non-started state
            // and add a single ErrorStatus to the SM.
            started = false;
            addStatus(new ErrorStatus("IO failure in appender", this, ioe));
        }
    }
}
