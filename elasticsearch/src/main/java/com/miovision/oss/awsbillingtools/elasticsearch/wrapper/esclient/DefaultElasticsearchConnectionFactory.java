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

package com.miovision.oss.awsbillingtools.elasticsearch.wrapper.esclient;

import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchConnection;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchConnectionFactory;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The default implementation of ElasticsearchConnectionFactory.
 */
public class DefaultElasticsearchConnectionFactory implements ElasticsearchConnectionFactory {
    private final List<TransportAddress> transportAddresses;

    public DefaultElasticsearchConnectionFactory(List<TransportAddress> transportAddresses) {
        this.transportAddresses = transportAddresses;
    }

    public DefaultElasticsearchConnectionFactory(int port, InetAddress... inetAddresses) {
        this(Arrays
                .asList(inetAddresses)
                .stream()
                .map(inetAddress -> new InetSocketTransportAddress(inetAddress, port))
                .collect(Collectors.toList()));
    }

    public DefaultElasticsearchConnectionFactory(InetSocketAddress... inetSocketAddresses) {
        this(Arrays
                .asList(inetSocketAddresses)
                .stream()
                .map(inetSocketAddress -> new InetSocketTransportAddress(inetSocketAddress))
                .collect(Collectors.toList()));
    }

    @Override
    public ElasticsearchConnection create() {
        TransportClient transportClient = createTransportClient();
        configureTransportClient(transportClient);
        return createElasticsearchConnection(transportClient);
    }

    protected TransportClient createTransportClient() {
        return TransportClient.builder().build();
    }

    protected void configureTransportClient(TransportClient transportClient) {
        transportClient.addTransportAddresses(transportAddresses.toArray(new TransportAddress[0]));
    }

    protected ElasticsearchConnection createElasticsearchConnection(TransportClient transportClient) {
        return new DefaultElasticsearchConnection(transportClient);
    }
}
