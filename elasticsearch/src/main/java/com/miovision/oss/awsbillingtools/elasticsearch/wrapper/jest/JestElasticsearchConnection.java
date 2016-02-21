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

package com.miovision.oss.awsbillingtools.elasticsearch.wrapper.jest;

import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchBulkRequest;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchConnection;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchIndexRequestBuilder;
import java.io.IOException;
import io.searchbox.client.JestClient;
import io.searchbox.indices.DeleteIndex;

/**
 * A Jest implementation of ElasticsearchConnection.
 */
public class JestElasticsearchConnection implements ElasticsearchConnection {
    private final JestClient jestClient;

    public JestElasticsearchConnection(JestClient jestClient) {
        this.jestClient = jestClient;
    }

    @Override
    public ElasticsearchIndexRequestBuilder prepareIndex(String index, String type) {
        return new JestElasticsearchBulkIndexRequestBuilder(index, type);
    }

    @Override
    public ElasticsearchBulkRequest prepareBulk() {
        return new JestElasticsearchBulkRequest(jestClient);
    }

    @Override
    public void deleteIndex(String index) {
        try {
            jestClient.execute(new DeleteIndex.Builder(index).build());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        jestClient.shutdownClient();
    }
}
