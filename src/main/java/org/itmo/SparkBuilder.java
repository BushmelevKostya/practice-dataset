package org.itmo;

import org.apache.spark.sql.SparkSession;

/** Общий SparkSession для предобработки и аналитики. */
public class SparkBuilder {

    public static SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("practice-dataset")
                .getOrCreate();
    }
}
