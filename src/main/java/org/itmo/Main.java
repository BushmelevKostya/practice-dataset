package org.itmo;

import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkBuilder.getSparkSession();

        DataSet dataSet = new SparkDataSet(spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(args[0]));

        AnalyticsData analytics = new SparkAnalyticsData();

        ((SparkDataSet) analytics.getTopFiveOrder(dataSet)).getData().show();
        ((SparkDataSet) analytics.getClientOrder(dataSet)).getData().show();
        ((SparkDataSet) analytics.getTotalCost(dataSet)).getData().show();
        ((SparkDataSet) analytics.getMeanCost(dataSet)).getData().show();

        spark.stop();
    }
}
