package org.itmo.lab2;

import org.apache.spark.sql.SparkSession;

/** Общий SparkSession для предобработки, хранения и аналитики. */
public class SparkBuilder {

    public static SparkSession getSparkSession() {
        SparkSession spark = SparkSession.builder()
                .appName("practice-dataset-lab2")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        return spark;
    }
}
