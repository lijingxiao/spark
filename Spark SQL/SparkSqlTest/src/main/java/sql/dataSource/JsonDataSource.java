package sql.dataSource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by lijingxiao on 2018/10/16.
 */
public class JsonDataSource {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("jsonsource").master("local[2]").getOrCreate();
        Dataset<Row> json = spark.read().json("part-00000-6fd84f2d-d511-4763-a072-5701fa78e6d8-c000.json");
        json.printSchema();
        json.show();
        spark.stop();
    }
}
