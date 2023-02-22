package com.github.brucekellan.kquery.sql.expression;

import lombok.Data;

@Data
public class SqlIdentifier {

    private String id;

    public SqlIdentifier(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return id;
    }
}
