package com.github.brucekellan.kquery.physical.exec;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.physical.PhysicalPlan;
import com.github.brucekellan.kquery.physical.expression.Expression;
import com.github.brucekellan.kquery.schema.Schema;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ProjectionExec implements PhysicalPlan {

    private PhysicalPlan input;

    private Schema schema;

    private List<Expression> expressions;

    public ProjectionExec(PhysicalPlan input, Schema schema, List<Expression> expressions) {
        this.input = input;
        this.schema = schema;
        this.expressions = expressions;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Iterable<RecordBatch> execute() {
        return StreamSupport.stream(input.execute().spliterator(), false)
                .map(batch -> new RecordBatch(schema,
                        expressions.stream()
                                .map(expr -> expr.evaluate(batch))
                                .collect(Collectors.toList())))
                .collect(Collectors.toList());
    }

    @Override
    public List<PhysicalPlan> children() {
        return Lists.newArrayList(input);
    }
}
