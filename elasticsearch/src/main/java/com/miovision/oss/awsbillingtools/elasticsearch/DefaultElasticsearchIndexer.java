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

package com.miovision.oss.awsbillingtools.elasticsearch;

import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchBulkRequest;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchConnection;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchConnectionFactory;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * The default implementation of ElasticsearchIndexer.
 *
 * @param <RecordTypeT> The billing record type.
 */
public class DefaultElasticsearchIndexer<RecordTypeT> implements ElasticsearchIndexer<RecordTypeT> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultElasticsearchIndexer.class);
    private final ElasticsearchConnectionFactory connectionFactory;
    private final ElasticsearchBillingRecordConverter<RecordTypeT> recordConverter;
    private final String index;
    private int batchSize;

    public DefaultElasticsearchIndexer(
            ElasticsearchConnectionFactory connectionFactory,
            ElasticsearchBillingRecordConverter<RecordTypeT> recordConverter,
            String index,
            int batchSize) {
        this.connectionFactory = connectionFactory;
        this.recordConverter = recordConverter;
        this.index = index;
        this.batchSize = batchSize;
    }

    public String getIndex() {
        return index;
    }

    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void index(Stream<RecordTypeT> stream) throws Exception {
        LOG.debug("Indexing record stream into index {} in batches of {}", index, batchSize);
        try(ElasticsearchConnection connection = connectionFactory.create()) {
            index(connection, stream);
        }
        catch (Exception e) {
            LOG.error("Indexing failed", e);
            throw e;
        }
        finally {
            LOG.debug("Indexing complete");
        }
    }

    protected void index(ElasticsearchConnection connection, Stream<RecordTypeT> stream) {
        final Iterator<RecordTypeT> streamIterator = stream.iterator();
        int batchIndex = 0;
        while(streamIterator.hasNext()) {
            indexBatch(connection, streamIterator, batchIndex++);
        }
    }

    protected void indexBatch(ElasticsearchConnection connection,
                              Iterator<RecordTypeT> streamIterator,
                              int batchIndex) {
        LOG.debug("Preparing batch #{}", batchIndex);

        ElasticsearchBulkRequest bulkRequestBuilder = prepareBatch(connection);

        for(int i = 0; i < batchSize && streamIterator.hasNext(); ++i) {
            final RecordTypeT record = streamIterator.next();
            final ElasticsearchIndexRequest indexRequestBuilder = createIndexRequestForRecord(connection, record);
            bulkRequestBuilder.add(indexRequestBuilder);
        }

        executeBatch(bulkRequestBuilder);

        LOG.debug("Batch #{} complete", batchIndex);
    }

    protected ElasticsearchIndexRequest createIndexRequestForRecord(
            ElasticsearchConnection connection,
            RecordTypeT record) {
        final Map<String, ?> fields = recordConverter.getRecordFields(record);
        return connection
                .prepareIndex(index, recordConverter.getRecordType(record))
                .setId(recordConverter.getRecordId(record))
                .setSource(fields)
                .request();
    }

    protected void executeBatch(ElasticsearchBulkRequest bulkRequestBuilder) {
        LOG.debug("Indexing batch");
        bulkRequestBuilder.execute();
    }

    protected ElasticsearchBulkRequest prepareBatch(ElasticsearchConnection connection) {
        return connection.prepareBulk();
    }

}
