package org.itmo.lab2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import scala.jdk.javaapi.CollectionConverters;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.sum;

public class SparkAnalyticsData {

    /**
     * n самых недооценённых игроков каждого года.
     * Метрика: Salary / (PTS + TRB + AST); чем она меньше, тем сильнее игрок недооценён.
     */
    public Dataset<Row> getTopUnderratedPlayers(
            Dataset<Row> stats, Dataset<Row> players, Dataset<Row> salaries, int n) {
        System.out.println("Запуск аналитического запроса: n = " + n
                + " самых недооценённых игроков каждого года");

        Dataset<Row> joined = getSeasonTotals(stats)
                .join(players, "Player")
                .join(getYearSalaries(salaries), CollectionConverters.asScala(List.of("Year", "PlayerId")).toSeq());

        Dataset<Row> rated = joined
                .withColumn("Total", col("PTS").plus(col("TRB")).plus(col("AST")))
                .filter(col("Total").gt(0))
                .withColumn("Rating", col("Salary").divide(col("Total")));

        WindowSpec byYear = Window.partitionBy("Year")
                .orderBy(col("Rating").asc(), col("Player").asc());

        Dataset<Row> result = rated
                .withColumn("Rank", row_number().over(byYear))
                .filter(col("Rank").leq(n))
                .select("Year", "Rank", "Player", "Salary", "PTS", "TRB", "AST", "Rating")
                .orderBy("Year", "Rank");

        return result;
    }

    /**
     * Одна строка на игрока за сезон. У сменивших команду есть строки по каждой
     * команде и итоговая с Tm = TOT — берём итоговую.
     */
    private Dataset<Row> getSeasonTotals(Dataset<Row> stats) {
        WindowSpec player = Window.partitionBy("Year", "Player");

        return stats
                .withColumn("Rows", count(lit(1)).over(player))
                .filter(col("Tm").equalTo("TOT").or(col("Rows").equalTo(1)))
                .select("Year", "Player", "PTS", "TRB", "AST");
    }

    /** Суммарная зарплата игрока за сезон (у сменивших команду несколько записей). */
    private Dataset<Row> getYearSalaries(Dataset<Row> salaries) {
        return salaries
                .groupBy("Year", "PlayerId")
                .agg(sum("Salary").alias("Salary"));
    }
}
