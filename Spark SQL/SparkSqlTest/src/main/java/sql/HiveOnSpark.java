package sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by lijingxiao on 2018/10/19.
 */
public class HiveOnSpark {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("hiveOnSpark").master("local[2]").enableHiveSupport().getOrCreate();

        //想要使用hive的元数据库，必须指定hive元数据的位置，添加一个hive-site.xml到当前程序的classpath下即可
        Dataset<Row> result = spark.sql("select * from accecc_log");
        result.show();
        spark.stop();
    }
}
