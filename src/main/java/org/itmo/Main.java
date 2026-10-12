package org.itmo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Main {
    public static void main(String[] args) {
        ValidationData validation = new DatasetPreparatorSpark();

        DataSet dataSet = validation.getValidDataSet(validation.readCsv(args[0]));
        String outputDir = args[1];

        AnalyticsData analytics = new SparkAnalyticsData();

        save(analytics.getTopFiveOrder(dataSet), outputDir + "/top_five_products");
        save(analytics.getClientOrder(dataSet), outputDir + "/client_order");
        save(analytics.getTotalCost(dataSet), outputDir + "/total_cost");
        save(analytics.getMeanCost(dataSet), outputDir + "/mean_cost");

        SparkBuilder.getSparkSession().stop();
    }

    /** Выводит результат и сохраняет его в отдельный CSV-файл. */
    private static void save(DataSet result, String path) {
        Dataset<Row> data = ((SparkDataSet) result).getData();
        data.show();
        data.coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(path);
    }
}
