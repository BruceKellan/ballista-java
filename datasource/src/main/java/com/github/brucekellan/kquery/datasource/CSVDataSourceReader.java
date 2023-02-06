package com.github.brucekellan.kquery.datasource;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.schema.Schema;
import com.github.brucekellan.kquery.vector.FieldColumnVector;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class CSVDataSourceReader implements Iterable<RecordBatch> {

    private Schema schema;

    private CsvParser parser;

    private int batchSize;

    public CSVDataSourceReader(Schema schema, CsvParser parser, int batchSize) {
        this.schema = schema;
        this.parser = parser;
        this.batchSize = batchSize;
    }

    @Override
    public Iterator<RecordBatch> iterator() {
        return new CSVDataSourceReaderIterator(schema, parser, batchSize);
    }

    public class CSVDataSourceReaderIterator implements Iterator<RecordBatch> {

        private Schema schema;

        private CsvParser parser;

        private int batchSize;

        private RecordBatch next = null;

        private Boolean started = false;

        public CSVDataSourceReaderIterator(Schema schema, CsvParser parser, int batchSize) {
            this.schema = schema;
            this.parser = parser;
            this.batchSize = batchSize;
        }

        @Override
        public boolean hasNext() {
            if (!started) {
                started = true;
                next = nextBatch();
            }
            return next != null;
        }

        @Override
        public RecordBatch next() {
            if (!started) {
                hasNext();
            }
            RecordBatch out = next;
            if (out == null) {
                throw new NoSuchElementException("Cannot read past the end of " + getClass().getSimpleName());
            }
            return out;
        }

        private RecordBatch nextBatch() {
            List<Record> records = new ArrayList<>(batchSize);
            Record line = null;
            do {
                line = parser.parseNextRecord();
                if (line != null) {
                    records.add(line);
                }
            } while (line != null && records.size() < batchSize);
            if (records.isEmpty()) {
                return null;
            }
            return createBatch(records);
        }

        private RecordBatch createBatch(List<Record> records) {
            VectorSchemaRoot root = VectorSchemaRoot.create(schema.toArrowSchema(), new RootAllocator(Long.MAX_VALUE));
            List<FieldVector> fieldVectors = root.getFieldVectors();
            for (FieldVector fieldVector : fieldVectors) {
                fieldVector.setInitialCapacity(records.size());
            }
            root.allocateNew();
            for (int i = 0; i < fieldVectors.size(); i++) {
                FieldVector vector = fieldVectors.get(i);
                if (vector instanceof BitVector) {
                    for (int j = 0; j < records.size(); j++) {
                        Record record = records.get(j);
                        String value = record.getValue(vector.getName(), "").trim();
                        if (StringUtils.isEmpty(value)) {
                            ((BitVector) vector).setNull(j);
                        } else {
                            ((BitVector) vector).set(j, Boolean.parseBoolean(value) ? 1 : 0);
                        }
                    }
                } else if (vector instanceof TinyIntVector) {
                    for (int j = 0; j < records.size(); j++) {
                        Record record = records.get(j);
                        String value = record.getValue(vector.getName(), "").trim();
                        if (StringUtils.isEmpty(value)) {
                            ((TinyIntVector) vector).setNull(j);
                        } else {
                            ((TinyIntVector) vector).set(j, Byte.parseByte(value));
                        }
                    }
                } else if (vector instanceof SmallIntVector) {
                    for (int j = 0; j < records.size(); j++) {
                        Record record = records.get(j);
                        String value = record.getValue(vector.getName(), "").trim();
                        if (StringUtils.isEmpty(value)) {
                            ((SmallIntVector) vector).setNull(j);
                        } else {
                            ((SmallIntVector) vector).set(j, Short.parseShort(value));
                        }
                    }
                } else if (vector instanceof IntVector) {
                    for (int j = 0; j < records.size(); j++) {
                        Record record = records.get(j);
                        String value = record.getValue(vector.getName(), "").trim();
                        if (StringUtils.isEmpty(value)) {
                            ((IntVector) vector).setNull(j);
                        } else {
                            ((IntVector) vector).set(j, Integer.parseInt(value));
                        }
                    }
                } else if (vector instanceof BigIntVector) {
                    for (int j = 0; j < records.size(); j++) {
                        Record record = records.get(j);
                        String value = record.getValue(vector.getName(), "").trim();
                        if (StringUtils.isEmpty(value)) {
                            ((BigIntVector) vector).setNull(j);
                        } else {
                            ((BigIntVector) vector).set(j, Long.parseLong(value));
                        }
                    }
                } else if (vector instanceof Float4Vector) {
                    for (int j = 0; j < records.size(); j++) {
                        Record record = records.get(j);
                        String value = record.getValue(vector.getName(), "").trim();
                        if (StringUtils.isEmpty(value)) {
                            ((Float4Vector) vector).setNull(j);
                        } else {
                            ((Float4Vector) vector).set(j, Float.parseFloat(value));
                        }
                    }
                } else if (vector instanceof Float8Vector) {
                    for (int j = 0; j < records.size(); j++) {
                        Record record = records.get(j);
                        String value = record.getValue(vector.getName(), "").trim();
                        if (StringUtils.isEmpty(value)) {
                            ((Float8Vector) vector).setNull(j);
                        } else {
                            ((Float8Vector) vector).set(j, Double.parseDouble(value));
                        }
                    }
                } else if (vector instanceof VarCharVector) {
                    for (int j = 0; j < records.size(); j++) {
                        Record record = records.get(j);
                        String value = record.getValue(vector.getName(), "").trim();
                        ((VarCharVector) vector).setSafe(j, value.getBytes());
                    }
                } else {
                    throw new IllegalStateException("No support for reading CSV columns with data type " + vector);
                }
                vector.setValueCount(records.size());
            }
            return new RecordBatch(schema, fieldVectors.stream().map(FieldColumnVector::new).collect(Collectors.toList()));
        }
    }
}
