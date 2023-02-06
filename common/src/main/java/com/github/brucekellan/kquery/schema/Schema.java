package com.github.brucekellan.kquery.schema;

import com.google.common.collect.Lists;
import lombok.Data;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Data
public class Schema {

    private List<Field> fields;

    public Schema(List<Field> fields) {
        this.fields = fields;
    }

    public org.apache.arrow.vector.types.pojo.Schema toArrowSchema() {
        return new org.apache.arrow.vector.types.pojo.Schema(fields.stream()
                .map(Field::toArrowField)
                .collect(Collectors.toList()));
    }

    public Schema project(List<Integer> indices) {
        return new Schema(indices.stream()
                .map(index -> fields.get(index))
                .collect(Collectors.toList()));
    }

    public Schema select(List<String> names) {
        List<Field> candidateFields = Lists.newArrayList();
        for (String name : names) {
            List<Field> filterFields = fields.stream()
                    .filter(field -> Objects.equals(field.getName(), name))
                    .collect(Collectors.toList());
            if (filterFields.size() == 1) {
                candidateFields.add(filterFields.get(0));
            } else {
                throw new IllegalArgumentException();
            }
        }
        return new Schema(candidateFields);
    }
}
