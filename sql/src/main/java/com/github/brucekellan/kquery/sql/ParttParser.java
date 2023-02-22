package com.github.brucekellan.kquery.sql;

import com.github.brucekellan.kquery.sql.expression.SqlExpr;

public interface ParttParser {

    default SqlExpr parse(int precedence) {
        return null;
    }

    int nextPrecedence();

    SqlExpr parsePrefix();

    SqlExpr parseInfix(SqlExpr left, int precedence);

}
