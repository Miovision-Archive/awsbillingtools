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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchBulkRequest;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchConnection;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchConnectionFactory;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchIndexRequest;
import com.miovision.oss.awsbillingtools.elasticsearch.wrapper.ElasticsearchIndexRequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the DefaultElasticsearchIndexer class.
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultElasticsearchIndexerTest {
    private static final String TEST_INDEX = "my-index";
    private static final String TEST_RECORD_TYPE = "LineItem";
    private static final int TEST_BATCH_SIZE = 2;

    @Mock
    private ElasticsearchConnectionFactory connectionFactory;

    @Mock
    private ElasticsearchConnection connection;

    private List<TestElasticsearchBulkRequest> bulkRequests = new ArrayList<>();

    private DefaultElasticsearchIndexer<TestRecordType> indexer;

    @Before
    public void setUp() throws Exception {
        final TestRecordConverter recordConverter = new TestRecordConverter();
        indexer = new DefaultElasticsearchIndexer<>(connectionFactory, recordConverter, TEST_INDEX, TEST_BATCH_SIZE);

        when(connectionFactory.create()).thenReturn(connection);
        when(connection.prepareBulk()).thenAnswer(invocation -> {
            TestElasticsearchBulkRequest bulkRequest = new TestElasticsearchBulkRequest();
            bulkRequests.add(bulkRequest);
            return bulkRequest;
        });
        when(connection.prepareIndex(TEST_INDEX, TEST_RECORD_TYPE))
                .thenReturn(new TestElasticsearchIndexRequestBuilder());
    }

    @Test
    public void testGetIndex() throws Exception {
        // Execute
        final String index = indexer.getIndex();

        // Verify
        assertEquals(TEST_INDEX, index);
    }

    @Test
    public void testGetBatchSize() throws Exception {
        // Execute
        final int batchSize = indexer.getBatchSize();

        // Verify
        assertEquals(TEST_BATCH_SIZE, batchSize);
    }

    @Test
    public void testIndex_ZeroRecords() throws Exception {
        // Setup
        List<TestRecordType> recordsToIndex = new ArrayList<>();

        // Execute
        indexer.index(recordsToIndex.stream());

        // Verify
        assertEquals(0, bulkRequests.size());
    }

    @Test
    public void testIndex_LessThanOneBatchOfRecords() throws Exception {
        // Setup
        TestRecordType record1 = new TestRecordType("id1", "LineItem", "fieldA1", "fieldB1");
        List<TestRecordType> recordsToIndex = Arrays.asList(record1);

        // Execute
        indexer.index(recordsToIndex.stream());

        // Verify
        assertEquals(1, bulkRequests.size());
        assertEquals(1, bulkRequests.get(0).getExecutedIndexRequest().size());
        assertIndexRequest(record1, bulkRequests.get(0).getExecutedIndexRequest().get(0));
    }

    @Test
    public void testIndex_ExactlyOneBatchOfRecords() throws Exception {
        // Setup
        TestRecordType record1 = new TestRecordType("id1", "LineItem", "fieldA1", "fieldB1");
        TestRecordType record2 = new TestRecordType("id2", "LineItem", "fieldA2", "fieldB2");
        List<TestRecordType> recordsToIndex = Arrays.asList(record1, record2);

        // Execute
        indexer.index(recordsToIndex.stream());

        // Verify
        assertEquals(2, bulkRequests.get(0).getExecutedIndexRequest().size());
        assertIndexRequest(record1, bulkRequests.get(0).getExecutedIndexRequest().get(0));
        assertIndexRequest(record2, bulkRequests.get(0).getExecutedIndexRequest().get(1));
    }

    @Test
    public void testIndex_MoreThanOneBatchOfRecords() throws Exception {
        // Setup
        TestRecordType record1 = new TestRecordType("id1", "LineItem", "fieldA1", "fieldB1");
        TestRecordType record2 = new TestRecordType("id2", "LineItem", "fieldA2", "fieldB2");
        TestRecordType record3 = new TestRecordType("id3", "LineItem", "fieldA3", "fieldB3");
        List<TestRecordType> recordsToIndex = Arrays.asList(record1, record2, record3);

        // Execute
        indexer.index(recordsToIndex.stream());

        // Verify
        assertEquals(2, bulkRequests.size());
        assertEquals(2, bulkRequests.get(0).getExecutedIndexRequest().size());
        assertIndexRequest(record1, bulkRequests.get(0).getExecutedIndexRequest().get(0));
        assertIndexRequest(record2, bulkRequests.get(0).getExecutedIndexRequest().get(1));
        assertEquals(1, bulkRequests.get(1).getExecutedIndexRequest().size());
        assertIndexRequest(record3, bulkRequests.get(1).getExecutedIndexRequest().get(0));
    }

    private static void assertIndexRequest(TestRecordType record, TestElasticsearchIndexRequest indexRequest) {
        assertEquals(record.getRecordId(), indexRequest.getId());
        Map<String, ?> fields = indexRequest.getFields();
        assertEquals(2, fields.size());
        assertEquals(record.getFieldA(), fields.get("fieldA"));
        assertEquals(record.getFieldB(), fields.get("fieldB"));
    }

    private static class TestRecordType {
        private String recordId;
        private String recordType;
        private String fieldA;
        private String fieldB;

        public TestRecordType(String recordId, String recordType, String fieldA, String fieldB) {
            this.recordId = recordId;
            this.recordType = recordType;
            this.fieldA = fieldA;
            this.fieldB = fieldB;
        }

        public String getRecordId() {
            return this.recordId;
        }

        public String getRecordType() {
            return this.recordType;
        }

        public String getFieldA() {
            return this.fieldA;
        }

        public String getFieldB() {
            return this.fieldB;
        }

        public void setRecordId(String recordId) {
            this.recordId = recordId;
        }

        public void setRecordType(String recordType) {
            this.recordType = recordType;
        }

        public void setFieldA(String fieldA) {
            this.fieldA = fieldA;
        }

        public void setFieldB(String fieldB) {
            this.fieldB = fieldB;
        }
    }

    private static class TestRecordConverter implements ElasticsearchBillingRecordConverter<TestRecordType> {
        @Override
        public String getRecordId(TestRecordType record) {
            return record.getRecordId();
        }

        @Override
        public String getRecordType(TestRecordType record) {
            return record.getRecordType();
        }

        @Override
        public Map<String, ?> getRecordFields(TestRecordType record) {
            Map<String, Object> fields = new HashMap<>();
            fields.put("fieldA", record.getFieldA());
            fields.put("fieldB", record.getFieldB());
            return fields;
        }
    }

    private static class TestElasticsearchIndexRequestBuilder implements ElasticsearchIndexRequestBuilder {
        private String id;
        private Map<String, ?> fields;

        @Override
        public ElasticsearchIndexRequestBuilder setId(String recordId) {
            this.id = recordId;
            return this;
        }

        @Override
        public ElasticsearchIndexRequestBuilder setSource(Map<String, ?> fields) {
            this.fields = fields;
            return this;
        }

        @Override
        public ElasticsearchIndexRequest request() {
            return new TestElasticsearchIndexRequest(id, fields);
        }
    }

    private static class TestElasticsearchIndexRequest implements ElasticsearchIndexRequest {
        private final String id;
        private final Map<String, ?> fields;

        private TestElasticsearchIndexRequest(String id, Map<String, ?> fields) {
            this.id = id;
            this.fields = fields;
        }

        public String getId() {
            return id;
        }

        public Map<String, ?> getFields() {
            return fields;
        }
    }

    private static class TestElasticsearchBulkRequest implements ElasticsearchBulkRequest {
        private final List<TestElasticsearchIndexRequest> addedIndexRequests = new ArrayList<>();
        private final List<TestElasticsearchIndexRequest> executedIndexRequest = new ArrayList<>();

        public List<TestElasticsearchIndexRequest> getExecutedIndexRequest() {
            return executedIndexRequest;
        }

        @Override
        public void add(ElasticsearchIndexRequest indexRequest) {
            addedIndexRequests.add((TestElasticsearchIndexRequest)indexRequest);
        }

        @Override
        public void execute() {
            executedIndexRequest.addAll(addedIndexRequests);
        }
    }
}