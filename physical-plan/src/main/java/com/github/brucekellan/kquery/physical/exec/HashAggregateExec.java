package com.github.brucekellan.kquery.physical.exec;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.physical.PhysicalPlan;
import com.github.brucekellan.kquery.physical.expression.Accumulator;
import com.github.brucekellan.kquery.physical.expression.AggregateExpression;
import com.github.brucekellan.kquery.physical.expression.Expression;
import com.github.brucekellan.kquery.schema.Schema;
import com.github.brucekellan.kquery.vector.ArrowFieldVectorFactory;
import com.github.brucekellan.kquery.vector.ColumnVector;
import com.github.brucekellan.kquery.vector.FieldColumnVector;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.FieldVector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HashAggregateExec implements PhysicalPlan {

    private PhysicalPlan input;

    private List<Expression> groupByExprs;

    private List<AggregateExpression> aggregateExprs;

    private Schema schema;

    public HashAggregateExec(PhysicalPlan input, List<Expression> groupByExprs, List<AggregateExpression> aggregateExprs, Schema schema) {
        this.input = input;
        this.groupByExprs = groupByExprs;
        this.aggregateExprs = aggregateExprs;
        this.schema = schema;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Iterable<RecordBatch> execute() {
        Map<List<Object>, List<Accumulator>> map = new HashMap<>();
        for (RecordBatch batch : input.execute()) {
            List<ColumnVector> groupByKeyVectors = groupByExprs.stream()
                    .map(expr -> expr.evaluate(batch))
                    .collect(Collectors.toList());
            List<ColumnVector> aggInputVectors = aggregateExprs.stream()
                    .map(expr -> expr.inputExpression().evaluate(batch))
                    .collect(Collectors.toList());
            int rowCount = batch.getRowCount();
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                int finalRowIndex = rowIndex;
                List<Object> rowKeys = groupByKeyVectors.stream()
                        .map(vector -> {
                            Object value = vector.getValue(finalRowIndex);
                            if (value instanceof byte[]) {
                                return String.valueOf(value);
                            } else {
                                return value;
                            }
                        }).collect(Collectors.toList());
                List<Accumulator> accumulators = map.putIfAbsent(rowKeys, aggregateExprs.stream().map(AggregateExpression::createAccumulator).collect(Collectors.toList()));
                for (int i = 0; i < accumulators.size(); i++) {
                    Object value = aggInputVectors.get(i).getValue(rowIndex);
                    accumulators.get(i).accumulate(value);
                }
            }
        }
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = this.schema.toArrowSchema();
        List<FieldVector> fieldVectors = ArrowFieldVectorFactory.create(arrowSchema, map.size());
        List<FieldColumnVector.FieldColumnVectorBuilder> builders = fieldVectors.stream()
                .map(fieldVector -> FieldColumnVector.builder().fieldVector(fieldVector))
                .collect(Collectors.toList());
        int rowIndex = 0;
        for (Map.Entry<List<Object>, List<Accumulator>> entry : map.entrySet()) {
            List<Object> groupingKey = entry.getKey();
            for (int i = 0; i < groupByExprs.size(); i++) {
                builders.get(i).set(rowIndex, groupingKey.get(i));
            }
            List<Accumulator> accumulators = entry.getValue();
            for (int i = 0; i < aggregateExprs.size(); i++) {
                builders.get(groupByExprs.size() + i).set(rowIndex, accumulators.get(i).finalValue());
            }
            rowIndex++;
        }
        RecordBatch recordBatch = new RecordBatch(schema, builders.stream()
                .map(FieldColumnVector.FieldColumnVectorBuilder::build)
                .collect(Collectors.toList()));
        return Lists.newArrayList(recordBatch);
    }

    @Override
    public List<PhysicalPlan> children() {
        return Lists.newArrayList(input);
    }

    @Override
    public String toString() {
        return String.format("HashAggregateExec: groupExpr=%s, aggrExpr=%s", groupByExprs, aggregateExprs);
    }
}
