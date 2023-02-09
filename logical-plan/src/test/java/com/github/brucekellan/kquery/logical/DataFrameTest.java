package com.github.brucekellan.kquery.logical;

import com.github.brucekellan.kquery.datasource.CsvDataSource;
import com.github.brucekellan.kquery.logical.expr.Count;
import com.github.brucekellan.kquery.logical.expr.Max;
import com.github.brucekellan.kquery.logical.expr.Min;
import com.github.brucekellan.kquery.logical.operator.Scan;
import com.github.brucekellan.kquery.schema.Schema;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Objects;

import static com.github.brucekellan.kquery.logical.expr.LogicalExprs.col;
import static com.github.brucekellan.kquery.logical.expr.LogicalExprs.lit;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DataFrameTest {

    String path = "/testdata/employee.csv";

    protected String getAbsolutePath() {
        return Objects.requireNonNull(getClass().getResource(path)).getPath();
    }

    @DisplayName("test data frame build")
    @Test
    public void testDataFrameBuild() {
        DataFrame df = csv()
                .filter(col("state").eq(lit("CO")))
                .project(Lists.newArrayList(col("id"), col("first_name"), col("last_name")));
        String expected =
                "Projection: #id, #first_name, #last_name\n" +
                        "\tSelection: #state = 'CO'\n" +
                        "\t\tScan; projection=None\n";
        Assertions.assertEquals(expected, LogicalPlan.format(df.logicalPlan()));
    }

    @Test
    public void testAggregateDataFrameBuild() {
        DataFrame df = csv().aggregate(Lists.newArrayList(col("state")),
                Lists.newArrayList(
                        new Min(col("salary")),
                        new Max(col("salary")),
                        new Count(col("salary"))));
        Schema schema = df.schema();
        System.out.println(schema);
        System.out.println(df.logicalPlan().pretty());
    }

    private DataFrame csv() {
        return new DataFrameImpl(new Scan(new CsvDataSource(getAbsolutePath(), null, true, 1024), Lists.newArrayList()));
    }

}
