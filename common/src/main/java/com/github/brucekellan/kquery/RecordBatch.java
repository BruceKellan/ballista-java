package com.github.brucekellan.kquery;

import com.github.brucekellan.kquery.schema.Schema;
import com.github.brucekellan.kquery.vector.ColumnVector;

import java.util.List;
import java.util.NoSuchElementException;

public class RecordBatch {

    private Schema schema;

    private List<ColumnVector> fieldVectors;

    public RecordBatch(Schema schema, List<ColumnVector> fieldVectors) {
        this.schema = schema;
        this.fieldVectors = fieldVectors;
    }

    public int getRowCount() {
        return fieldVectors.stream()
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("List is empty."))
                .getSize();
    }

    public int getColumnCount() {
        return fieldVectors.size();
    }

    /**
     * Get specify vector by index.
     * @param i
     * @return
     */
    public ColumnVector getField(int i) {
        return fieldVectors.get(i);
    }

    public String toCsv() {
        int rowCount = getRowCount();
        int columnCount = getColumnCount();
        StringBuilder builder = new StringBuilder();
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                if (columnIndex > 0) {
                    builder.append(",");
                }
                ColumnVector vector = fieldVectors.get(columnIndex);
                Object value = vector.getValue(rowIndex);
                if (value == null) {
                    builder.append("null");
                } else if (value instanceof byte[]) {
                    builder.append(new String((byte[]) value));
                } else {
                    builder.append(value);
                }
            }
            builder.append("\n");
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        return toCsv();
    }
}
