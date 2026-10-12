package org.itmo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** Реализация DataSet поверх Spark Dataset. */
public class SparkDataSet implements DataSet {

    private final Dataset<Row> data;

    public SparkDataSet(Dataset<Row> data) {
        this.data = data;
    }

    public Dataset<Row> getData() {
        return data;
    }
}
