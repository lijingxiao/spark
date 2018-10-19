package sql.dataSource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by lijingxiao on 2018/10/16.
 */
public class ParQuet {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("parquet").master("local[2]").getOrCreate();
        Dataset<Row> parquet = spark.read().parquet("part-00000-97e6fccc-5b55-4b80-9c04-8058b619a662-c000.snappy.parquet");
        parquet.show();
        spark.stop();
    }
}
