package com.github.brucekellan.kquery.sql.expression;

import lombok.Data;

@Data
public class SqlBinaryExpr {

    private SqlExpr l;

    private String op;

    private SqlExpr r;

    public SqlBinaryExpr(SqlExpr l, String op, SqlExpr r) {
        this.l = l;
        this.op = op;
        this.r = r;
    }

    @Override
    public String toString() {
        return String.format("%s %s %s", l, op, r);
    }
}
