package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Field;

import java.sql.SQLException;
import java.util.Objects;
import java.util.stream.Collectors;

public class Column implements LogicalExpr {

    private String name;

    public Column(String name) {
        this.name = name;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return input.schema().getFields().stream()
                .filter(field -> Objects.equals(field.getName(), name))
                .findFirst()
                .orElseThrow(() -> new RuntimeException(
                        new SQLException("No column named '" + name + "' in " + input.schema().getFields().stream()
                                .map(Field::getName)
                                .collect(Collectors.joining(",")))));
    }

    @Override
    public String toString() {
        return "#" + name;
    }
}
