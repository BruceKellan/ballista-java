package com.github.brucekellan.kquery.sql.expression;

import lombok.Data;

@Data
public class SqlString {

    private String value;

    public SqlString(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("'%s'", value);
    }
}
