package org.itmo;

import org.apache.spark.sql.SparkSession;

/** Общий SparkSession для предобработки и аналитики. */
public class SparkBuilder {

    public static SparkSession getSparkSession() {
        SparkSession spark = SparkSession.builder()
                .appName("practice-dataset")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        return spark;
    }
}
