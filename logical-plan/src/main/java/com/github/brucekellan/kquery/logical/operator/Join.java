package com.github.brucekellan.kquery.logical.operator;

import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Field;
import com.github.brucekellan.kquery.schema.Schema;
import com.github.brucekellan.kquery.util.ListUtil;
import com.github.brucekellan.kquery.util.Tuple;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Join implements LogicalPlan {

    private LogicalPlan left;

    private LogicalPlan right;

    private JoinType joinType;

    private List<Tuple<String, String>> on;

    public Join(LogicalPlan left, LogicalPlan right, JoinType joinType, List<Tuple<String, String>> on) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.on = on;
    }

    @Override
    public Schema schema() {
        Set<String> duplicateKeys = on.stream()
                .filter(tuple -> Objects.equals(tuple.getFirst(), tuple.getSecond()))
                .map(Tuple::getFirst)
                .collect(Collectors.toSet());
        List<Field> fields = null;
        switch (joinType) {
            case LEFT:
            case INNER: {
                List<Field> leftFields = left.schema().getFields();
                List<Field> rightFields = right.schema().getFields().stream()
                        .filter(field -> !duplicateKeys.contains(field.getName()))
                        .collect(Collectors.toList());
                fields = ListUtil.concat(leftFields, rightFields);
            }
            break;
            case RIGHT: {
                List<Field> leftFields = left.schema().getFields().stream()
                        .filter(field -> !duplicateKeys.contains(field.getName()))
                        .collect(Collectors.toList());
                List<Field> rightFields = right.schema().getFields();
                fields = ListUtil.concat(leftFields, rightFields);
            }
            break;
            default:
                throw new IllegalStateException();
        }
        return new Schema(fields);
    }

    @Override
    public List<LogicalPlan> children() {
        return Lists.newArrayList(left, right);
    }

}
