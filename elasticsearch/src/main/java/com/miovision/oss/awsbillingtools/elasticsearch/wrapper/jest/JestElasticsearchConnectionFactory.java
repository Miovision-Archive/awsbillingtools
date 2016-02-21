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

import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchConnection;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchConnectionFactory;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

/**
 * The Jest implementation of ElasticsearchConnectionFactory.
 */
public class JestElasticsearchConnectionFactory implements ElasticsearchConnectionFactory {
    private final JestClientFactory jestClientFactory;

    public JestElasticsearchConnectionFactory(JestClientFactory jestClientFactory) {
        this.jestClientFactory = jestClientFactory;
    }

    public JestElasticsearchConnectionFactory(HttpClientConfig httpClientConfig) {
        this(createJestClientFactory(httpClientConfig));
    }

    public JestElasticsearchConnectionFactory(String elasticsearchUri) {
        this(createJestClientFactory(createHttpClientConfig(elasticsearchUri)));
    }

    @Override
    public ElasticsearchConnection create() {
        return new JestElasticsearchConnection(jestClientFactory.getObject());
    }

    private static JestClientFactory createJestClientFactory(HttpClientConfig httpClientConfig) {
        JestClientFactory jestClientFactory = new JestClientFactory();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        return jestClientFactory;
    }

    private static HttpClientConfig createHttpClientConfig(String elasticsearchUri) {
        return new HttpClientConfig.Builder(elasticsearchUri).multiThreaded(true).build();
    }
}
