package com.github.brucekellan.kquery.physical.exec;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import com.github.brucekellan.kquery.physical.PhysicalPlan;
import com.github.brucekellan.kquery.physical.expression.Expression;
import com.github.brucekellan.kquery.schema.Schema;
import com.github.brucekellan.kquery.vector.ArrowFieldVectorFactory;
import com.github.brucekellan.kquery.vector.ColumnVector;
import com.github.brucekellan.kquery.vector.FieldColumnVector;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SelectionExec implements PhysicalPlan {

    private PhysicalPlan input;

    private Expression expr;

    public SelectionExec(PhysicalPlan input, Expression expr) {
        this.input = input;
        this.expr = expr;
    }

    @Override
    public Schema schema() {
        return input.schema();
    }

    @Override
    public Iterable<RecordBatch> execute() {
        return StreamSupport.stream(input.execute().spliterator(), false)
                .map(batch -> {
                    BitVector selectionBitVector = ((BitVector) ((FieldColumnVector) expr.evaluate(batch)).getFieldVector());
                    List<ColumnVector> filteredColumnVectors = batch.getFieldVectors().stream()
                            .map(fieldVector -> {
                                FieldVector filteredVector = ArrowFieldVectorFactory.create(ArrowTypes.STRING_TYPE, 0);
                                FieldColumnVector.FieldColumnVectorBuilder builder =
                                        FieldColumnVector.builder()
                                                .fieldVector(filteredVector);
                                int rows = selectionBitVector.getValueCount();
                                int count = 0;
                                for (int i = 0; i < rows; i++) {
                                    if (selectionBitVector.get(i) == 1) {
                                        builder.set(count, fieldVector.getValue(i));
                                        count++;
                                    }
                                }
                                builder.valueCount(count);
                                return builder.build();
                            }).collect(Collectors.toList());
                    return new RecordBatch(batch.getSchema(), filteredColumnVectors);
                }).collect(Collectors.toList());
    }

    @Override
    public List<PhysicalPlan> children() {
        return Lists.newArrayList(input);
    }
}
