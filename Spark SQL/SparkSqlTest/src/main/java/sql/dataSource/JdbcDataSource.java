package sql.dataSource;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Properties;

/**
 * Created by lijingxiao on 2018/10/16.
 */
public class JdbcDataSource {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("JdbcDataSource").master("local[4]").getOrCreate();

        HashMap<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://localhost:3306/mydb?useUnicode=true&characterEncoding=UTF8&useSSL=false&serverTimezone=GMT");
        options.put("dbtable", "access_log");
        options.put("user", "root");
        options.put("password", "root123");
        //jdbc:mysql://localhost:3306/mydb?user=root&password=root123&useUnicode=true&characterEncoding=UTF8&useSSL=false&serverTimezone=GMT
        Dataset<Row> logs = spark.read().format("jdbc").options(options).load();

//        logs.printSchema();
        try {
            logs.createTempView("v_logs");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        Dataset<Row> sql = spark.sql("select * from v_logs where num>300");
//        sql.show();
        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "root123");

//        sql.write().mode("ignore").jdbc("jdbc:mysql://localhost:3306/mydb?useUnicode=true&characterEncoding=UTF8&useSSL=false&serverTimezone=GMT", "logs1", properties);

        //DataFrame保存成text时出错(只能保存一列, 且必须是string类型)
//        sql.write().text("log.txt");

//        sql.write().json("/json");
//        sql.write().csv("csv");
        sql.write().parquet("parquet");
        spark.stop();
    }
}
