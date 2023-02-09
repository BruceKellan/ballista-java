package com.github.brucekellan.kquery.logical;

import com.github.brucekellan.kquery.datasource.CsvDataSource;
import com.github.brucekellan.kquery.datasource.DataSource;
import com.github.brucekellan.kquery.logical.operator.Projection;
import com.github.brucekellan.kquery.logical.operator.Scan;
import com.github.brucekellan.kquery.logical.operator.Selection;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Objects;

import static com.github.brucekellan.kquery.logical.expr.LogicalExprs.col;
import static com.github.brucekellan.kquery.logical.expr.LogicalExprs.lit;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LogicalPlanTest {

    String path = "/testdata/employee.csv";

    protected String getAbsolutePath() {
        return Objects.requireNonNull(getClass().getResource(path)).getPath();
    }

    @Test
    @DisplayName("test build logical plan manually")
    public void testLogicalPlanBuild() {
        DataSource dataSource = new CsvDataSource(getAbsolutePath(), null, true, 1024);
        LogicalPlan scan = new Scan(dataSource, Lists.newArrayList());
        LogicalExpr filterExpr = col("state").eq(lit("CO"));
        LogicalPlan selection = new Selection(scan, filterExpr);
        LogicalPlan plan = new Projection(selection, Lists.newArrayList(col("id"), col("first_name"), col("last_name")));
        String actual = LogicalPlan.format(plan);
        String expected = "Projection: #id, #first_name, #last_name\n" +
                "\tSelection: #state = 'CO'\n" +
                "\t\tScan; projection=None\n";
        Assertions.assertEquals(expected, actual);
    }
}
