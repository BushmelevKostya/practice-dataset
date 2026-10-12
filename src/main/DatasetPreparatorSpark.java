import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class DatasetPreparatorSpark {

    public static void main(String[] args) {

        // ============================================
        // НАСТРОЙКИ
        // ============================================

        String inputPath = "data/online_retail.csv";
        String outputPath = "data/prepared";

        // ============================================
        // СОЗДАНИЕ SPARK SESSION
        // ============================================

        SparkSession spark = SparkSession.builder()
                .appName("Online Retail Data Preparation")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("======================================");
        System.out.println("   ПОДГОТОВКА ДАННЫХ С APACHE SPARK");
        System.out.println("======================================\n");

        // ============================================
        // 1. ЗАГРУЗКА CSV
        // ============================================

        System.out.println("Загрузка данных...");

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("encoding", "UTF-8")
                .csv(inputPath);

        System.out.println("Исходное количество строк: "
                + df.count());

        // Показываем структуру
        System.out.println("\nСтруктура исходных данных:");

        df.printSchema();

        // ============================================
        // 2. ОЧИСТКА DESCRIPTION ОТ ПРОБЕЛОВ
        // ============================================

        System.out.println("\nОчистка Description...");

        /*
         * trim() удаляет пробелы в начале и конце.
         *
         * regexp_replace(..., "\\s+", " ")
         * заменяет несколько последовательных пробелов
         * одним пробелом.
         */

        df = df.withColumn(
                "Description",
                regexp_replace(
                        trim(col("Description")),
                        "\\s+",
                        " "
                )
        );

        // ============================================
        // 3. УДАЛЕНИЕ ПУСТОГО DESCRIPTION
        // ============================================

        long beforeDescription = df.count();

        df = df.filter(
                col("Description").isNotNull()
                        .and(
                                length(trim(col("Description"))).gt(0)
                        )
        );

        long afterDescription = df.count();

        System.out.println(
                "Удалено строк с пустым Description: "
                        + (beforeDescription - afterDescription)
        );

        // ============================================
        // 4. УДАЛЕНИЕ DESCRIPTION, НАЧИНАЮЩЕГОСЯ С ?
        // ============================================

        long beforeQuestion = df.count();

        df = df.filter(
                !col("Description").startsWith("?")
        );

        long afterQuestion = df.count();

        System.out.println(
                "Удалено строк, где Description начинается с ?: "
                        + (beforeQuestion - afterQuestion)
        );

        // ============================================
        // ОЧИСТКА STOCKCODE
        // ============================================

        long beforeStockCode = df.count();

        df = df.withColumn(
                "StockCode",
                trim(col("StockCode").cast("string"))
        );

        df = df.filter(
                col("StockCode").rlike("^[0-9]{5}$")
        );

        long afterStockCode = df.count();

        System.out.println(
                "Удалено строк с некорректным StockCode: "
                        + (beforeStockCode - afterStockCode)
        );
        // ============================================
        // 5. ЗАПОЛНЕНИЕ ПУСТОГО CUSTOMERID
        // ============================================

        long emptyCustomerId = df.filter(
                col("CustomerID").isNull()
                        .or(
                                trim(col("CustomerID").cast("string"))
                                        .equalTo("")
                        )
        ).count();

        df = df.withColumn(
                "CustomerID",
                when(
                        col("CustomerID").isNull()
                                .or(
                                        trim(
                                                col("CustomerID")
                                                        .cast("string")
                                        ).equalTo("")
                                ),
                        lit("00000")
                ).otherwise(
                        col("CustomerID").cast("string")
                )
        );

        System.out.println(
                "Заполнено CustomerID значением 00000: "
                        + emptyCustomerId
        );

        // ============================================
        // 6. УДАЛЕНИЕ ПОЛНЫХ ДУБЛИКАТОВ
        // ============================================

        long beforeDuplicates = df.count();

        df = df.dropDuplicates();

        long afterDuplicates = df.count();

        System.out.println(
                "Удалено полных дубликатов: "
                        + (beforeDuplicates - afterDuplicates)
        );

        // ============================================
        // 7. УДАЛЕНИЕ ОТРИЦАТЕЛЬНОГО QUANTITY
        // ============================================

        long beforeQuantity = df.count();

        df = df.filter(
                !(
                        col("Quantity")
                                .cast("double")
                                .lt(0)
                                .and(
                                        !upper(
                                                trim(
                                                        col("InvoiceNo")
                                                                .cast("string")
                                                )
                                        ).startsWith("C")
                                )
                )
        );

        long afterQuantity = df.count();

        System.out.println(
                "Удалено строк с Quantity < 0 "
                        + "и InvoiceNo не C: "
                        + (beforeQuantity - afterQuantity)
        );

        // ============================================
        // 8. УДАЛЕНИЕ UnitPrice <= 0
        // ============================================

        long beforePrice = df.count();

        df = df.filter(
                col("UnitPrice")
                        .cast("double")
                        .gt(0)
        );

        long afterPrice = df.count();

        System.out.println(
                "Удалено строк с UnitPrice <= 0: "
                        + (beforePrice - afterPrice)
        );

        // ============================================
        // 9. ИТОГОВАЯ ИНФОРМАЦИЯ
        // ============================================

        long finalCount = df.count();

        System.out.println("\n======================================");
        System.out.println("         РЕЗУЛЬТАТ ОБРАБОТКИ");
        System.out.println("======================================");

        System.out.println(
                "Исходное количество строк: "
                        + df.sparkSession()
                        .read()
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .csv(inputPath)
                        .count()
        );

        System.out.println(
                "Итоговое количество строк: "
                        + finalCount
        );

        System.out.println(
                "Удалено всего строк: "
                        + (
                        df.sparkSession()
                                .read()
                                .option("header", "true")
                                .option("inferSchema", "true")
                                .csv(inputPath)
                                .count()
                                - finalCount
                )
        );

        // ============================================
        // 10. ПРОСМОТР РЕЗУЛЬТАТА
        // ============================================

        System.out.println("\nПервые 20 строк:");

        df.show(20, false);

        // ============================================
        // 11. СОХРАНЕНИЕ
        // ============================================

        System.out.println(
                "\nСохранение подготовленных данных..."
        );

        df.write()
                .mode("overwrite")
                .option("header", "true")
                .option("encoding", "UTF-8")
                .csv(outputPath);

        System.out.println(
                "Данные сохранены в: "
                        + outputPath
        );

        // ============================================
        // ЗАВЕРШЕНИЕ
        // ============================================

        spark.stop();

        System.out.println("\nОбработка завершена.");
    }
}