package org.itmo.lab2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Main {

    private static final String YEAR = "Year";

    public static void main(String[] args) {
        String datasetsDir = args[0];
        String outputDir = args[1];
        int n = args.length > 2 ? Integer.parseInt(args[2]) : 5;

        String preparedDir = outputDir + "/prepared";

        // 1. Предобработка
        DatasetPreparatorSpark validation = new DatasetPreparatorSpark();

        Dataset<Row> stats = validation.getValidStats(
                validation.readCsv(datasetsDir + "/Seasons_Stats.xls"));
        Dataset<Row> players = validation.getValidPlayers(
                validation.readCsv(datasetsDir + "/players.csv"));
        Dataset<Row> salaries = validation.getValidSalaries(
                validation.readCsv(datasetsDir + "/salaries_1985to2018.csv"));

        // 2. Сохранение с партиционированием по годам.
        // В справочнике игроков года нет, он сохраняется целиком.
        SparkDataStorage storage = new SparkDataStorage();

        storage.save(stats, preparedDir + "/stats", YEAR);
        storage.save(salaries, preparedDir + "/salaries", YEAR);
        storage.save(players, preparedDir + "/players");

        // 3. Аналитика на сохранённых данных
        SparkAnalyticsData analytics = new SparkAnalyticsData();

        Dataset<Row> result = analytics.getTopUnderratedPlayers(
                storage.load(preparedDir + "/stats"),
                storage.load(preparedDir + "/players"),
                storage.load(preparedDir + "/salaries"),
                n);

        save(result, outputDir + "/underrated_players");

        SparkBuilder.getSparkSession().stop();
    }

    /** Выводит результат и сохраняет его в отдельный CSV-файл. */
    private static void save(Dataset<Row> data, String path) {
        data.show(50, false);
        data.coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(path);
    }
}
