package org.itmo.lab2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.trim;

/** Загрузка и предобработка исходных датасетов. */
public class DatasetPreparatorSpark {

    public Dataset<Row> readCsv(String filePath) {
        Dataset<Row> df = SparkBuilder.getSparkSession().read()
                .option("header", "true")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("encoding", "UTF-8")
                .csv(filePath);

        return df;
    }

    /** Статистика игроков по сезонам: Year, Player, Tm, PTS, TRB, AST. */
    public Dataset<Row> getValidStats(Dataset<Row> source) {
        Dataset<Row> df = source.select(
                col("Year").cast("int").alias("Year"),
                // Звёздочкой в конце имени отмечены члены Зала славы.
                trim(regexp_replace(col("Player"), "\\*", "")).alias("Player"),
                trim(col("Tm")).alias("Tm"),
                col("PTS").cast("double").alias("PTS"),
                col("TRB").cast("double").alias("TRB"),
                col("AST").cast("double").alias("AST")
        );

        df = df.na().drop()
                .filter(col("Player").notEqual("").and(col("Tm").notEqual("")));

        return report("Seasons_Stats", source, df);
    }

    /** Справочник игроков: PlayerId, Player. */
    public Dataset<Row> getValidPlayers(Dataset<Row> source) {
        Dataset<Row> df = source.select(
                trim(col("_id")).alias("PlayerId"),
                trim(col("name")).alias("Player")
        );

        df = df.na().drop()
                .filter(col("PlayerId").notEqual("").and(col("Player").notEqual("")))
                .dropDuplicates();

        // Полные тёзки — невалидные данные: статистика связана с игроком только
        // по имени, и определить, кому из них она принадлежит, нельзя.
        long beforeNamesakes = df.count();

        df = df.withColumn("SameName", count(lit(1)).over(Window.partitionBy("Player")))
                .filter(col("SameName").equalTo(1))
                .drop("SameName");

        System.out.println("Удалено полных тёзок в players: "
                + (beforeNamesakes - df.count()));

        return report("players", source, df);
    }

    /** Зарплаты игроков по сезонам: Year, PlayerId, Salary. */
    public Dataset<Row> getValidSalaries(Dataset<Row> source) {
        // Year в статистике — год окончания сезона, ему соответствует season_end.
        Dataset<Row> df = source.select(
                col("season_end").cast("int").alias("Year"),
                trim(col("player_id")).alias("PlayerId"),
                col("salary").cast("double").alias("Salary")
        );

        df = df.na().drop()
                .filter(col("PlayerId").notEqual("").and(col("Salary").gt(0)));

        return report("salaries", source, df);
    }

    private Dataset<Row> report(String name, Dataset<Row> source, Dataset<Row> result) {
        long before = source.count();
        long after = result.count();

        System.out.println("Предобработка " + name + ": строк было " + before
                + ", осталось " + after + ", удалено " + (before - after));

        return result;
    }
}
