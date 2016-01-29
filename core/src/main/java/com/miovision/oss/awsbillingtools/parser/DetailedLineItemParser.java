/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 the original author or authors.
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
 */

package com.miovision.oss.awsbillingtools.parser;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A class that parses detailed line-item content.
 */
public class DetailedLineItemParser implements BillingRecordParser<DetailedLineItem> {
    private static final CSVFormat CSV_FORMAT = CSVFormat.EXCEL;

    @Override
    public Stream<DetailedLineItem> parse(Reader reader) throws IOException {
        final CSVParser csvParser = CSV_FORMAT.parse(reader);
        try {
            final Iterator<CSVRecord> iterator = csvParser.iterator();
            final List<String> tags = readTags(iterator);

            return StreamSupport
                    .stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                    .map(csvRecord -> createDetailedLineItem(csvRecord, tags))
                    .onClose(() -> {
                        try {
                            csvParser.close();
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        catch(Exception e) {
            csvParser.close();
            throw e;
        }
    }

    @Override
    public Class<DetailedLineItem> getRecordTypeClass() {
        return DetailedLineItem.class;
    }

    private static List<String> readTags(Iterator<CSVRecord> iterator) {
        return iterator.hasNext() ?
                StreamSupport
                        .stream(iterator.next().spliterator(), false)
                        .skip(20)
                        .collect(Collectors.toList()) :
                new ArrayList<>(0);
    }

    private static DetailedLineItem createDetailedLineItem(CSVRecord csvRecord, List<String> tags) {
        return new DetailedLineItem(csvRecord, tags);
    }

}
