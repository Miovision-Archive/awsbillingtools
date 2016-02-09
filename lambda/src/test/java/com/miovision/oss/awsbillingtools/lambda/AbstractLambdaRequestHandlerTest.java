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

package com.miovision.oss.awsbillingtools.lambda;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import com.amazonaws.services.lambda.runtime.Context;
import org.junit.Before;
import org.junit.Test;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Unit tests for the AbstractLambdaRequestHandler class.
 */
public class AbstractLambdaRequestHandlerTest {
    private AbstractLambdaRequestHandlerTss requestHandler;

    @Before
    public void setUp() {
        requestHandler = new AbstractLambdaRequestHandlerTss();
    }

    @Test
    public void testHandleRequest_Success() {
        // Setup
        requestHandler.stream = Arrays.asList("Foo", "Bar").stream();
        OutputType outputType = new OutputType();
        requestHandler.output = () -> outputType;

        // Execute
        OutputType response = requestHandler.handleRequest(new InputType(), mock(Context.class));

        // Verify
        assertSame(outputType, response);
    }

    @Test(expected = RuntimeException.class)
    public void testHandleRequest_Exception() {
        // Setup
        requestHandler.stream = Arrays.asList("Foo", "Bar").stream();
        requestHandler.output = () ->  { throw new RuntimeException("Oops!"); };

        // Execute
        requestHandler.handleRequest(new InputType(), mock(Context.class));
    }

    private static class InputType {

    }

    private static class OutputType {

    }

    private static class AbstractLambdaRequestHandlerTss
            extends AbstractLambdaRequestHandler<InputType, String, OutputType> {
        public Stream<String> stream;
        public Supplier<OutputType> output;

        @Override
        protected Stream<String> loadStream(InputType input) throws UnableToLoadStreamException {
            assertNotNull("Expected stream to be set", stream);
            return stream;
        }

        @Override
        protected OutputType handleStream(Stream<String> stream, Context context) {
            assertNotNull("Expected output supplier to be set", output);
            assertNotNull("Expected stream to be set", stream);
            assertSame("Expected stream to be same as given", this.stream, stream);
            return output.get();
        }
    }
}