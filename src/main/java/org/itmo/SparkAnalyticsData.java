package org.itmo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.sum;

public class SparkAnalyticsData implements AnalyticsData {

    /** Топ-5 самых популярных товаров по общему количеству проданных единиц. */
    @Override
    public DataSet getTopFiveOrder(DataSet dataSet) {
        Dataset<Row> result = ((SparkDataSet) dataSet).getData()
                .groupBy("StockCode")
                .agg(sum("Quantity").alias("TotalQuantity"))
                .orderBy(col("TotalQuantity").desc())
                .limit(5);
        return new SparkDataSet(result);
    }

    /** Общее число заказов для каждого клиента. */
    @Override
    public DataSet getClientOrder(DataSet dataSet) {
        Dataset<Row> result = ((SparkDataSet) dataSet).getData()
                .groupBy("CustomerID")
                .agg(countDistinct("InvoiceNo").alias("OrderCount"));
        return new SparkDataSet(result);
    }

    /** Общая потраченная сумма для каждого клиента. */
    @Override
    public DataSet getTotalCost(DataSet dataSet) {
        Dataset<Row> result = withTotalPrice(dataSet)
                .groupBy("CustomerID")
                .agg(sum("TotalPrice").alias("TotalCost"));
        return new SparkDataSet(result);
    }

    /** Средний чек (TotalPrice на заказ) для каждого клиента. */
    @Override
    public DataSet getMeanCost(DataSet dataSet) {
        Dataset<Row> result = getOrders(dataSet)
                .groupBy("CustomerID")
                .agg(avg("TotalPrice").alias("MeanCost"));
        return new SparkDataSet(result);
    }

    /** Сумма каждого заказа. */
    private Dataset<Row> getOrders(DataSet dataSet) {
        return withTotalPrice(dataSet)
                .groupBy("CustomerID", "InvoiceNo")
                .agg(sum("TotalPrice").alias("TotalPrice"));
    }

    /** Добавляет к каждой строке TotalPrice = Quantity * UnitPrice. */
    private Dataset<Row> withTotalPrice(DataSet dataSet) {
        return ((SparkDataSet) dataSet).getData()
                .withColumn("TotalPrice", col("Quantity").multiply(col("UnitPrice")));
    }
}
