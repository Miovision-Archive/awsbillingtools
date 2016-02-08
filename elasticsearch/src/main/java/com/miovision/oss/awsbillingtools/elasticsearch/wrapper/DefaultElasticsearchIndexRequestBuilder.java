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

package com.miovision.oss.awsbillingtools.elasticsearch.wrapper;

import org.elasticsearch.action.index.IndexRequestBuilder;
import java.util.Map;

/**
 * The default implementation of ElasticsearchIndexRequestBuilder.
 */
public class DefaultElasticsearchIndexRequestBuilder implements ElasticsearchIndexRequestBuilder {
    private IndexRequestBuilder indexRequestBuilder;

    public DefaultElasticsearchIndexRequestBuilder(IndexRequestBuilder indexRequestBuilder) {
        this.indexRequestBuilder = indexRequestBuilder;
    }

    @Override
    public ElasticsearchIndexRequestBuilder setId(String id) {
        indexRequestBuilder = indexRequestBuilder.setId(id);
        return this;
    }

    @Override
    public ElasticsearchIndexRequestBuilder setSource(Map<String, ?> fields) {
        indexRequestBuilder = indexRequestBuilder.setSource(fields);
        return this;
    }

    @Override
    public ElasticsearchIndexRequest request() {
        return new DefaultElasticsearchIndexRequest(indexRequestBuilder.request());
    }
}
