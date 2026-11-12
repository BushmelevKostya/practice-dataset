package org.itmo.lab2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

/** Хранение в Parquet с партиционированием по каталогам (partition discovery). */
public class SparkDataStorage {

    /** Сохраняет датасет, раскладывая данные по каталогам вида колонка=значение. */
    public void save(Dataset<Row> data, String path, String... partitionColumns) {
        if (partitionColumns.length > 0) {
            // Одна партиция Spark на значение ключа — один файл в каждом каталоге.
            data = data.repartition(col(partitionColumns[0]));
        } else {
            data = data.coalesce(1);
        }

        data.write()
                .mode("overwrite")
                .partitionBy(partitionColumns)
                .parquet(path);
    }

    /** Читает датасет; колонки партиционирования восстанавливаются из имён каталогов. */
    public Dataset<Row> load(String path) {
        return SparkBuilder.getSparkSession().read().parquet(path);
    }
}
