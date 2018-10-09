package study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by lijingxiao on 2018/9/27.
 */
public class JdbcRDDDemo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("jdbcRDD").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //JdbcRDD在执行之前会先看有多少条数据，然后尽量平均分配给每个excutor，加入传入[1,5)，2个分区
        //那么，会被分成[1,2), [3,5), 会将2那条数据丢掉，所以在传入规则的时候两边都要取等
        JavaRDD<Tuple2> tpJdbcRDD = JdbcRDD.create(jsc,
                new JdbcRDD.ConnectionFactory() {
                    @Override
                    public Connection getConnection() throws Exception {
                        return DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb?user=root&password=root123&useUnicode=true&characterEncoding=UTF8&useSSL=false&serverTimezone=GMT");
                    }
                },
                "SELECT * FROM jddp_isv_app WHERE isv_app_id >= ? AND isv_app_id < ?",
                60,
                70,
                2,
                rs -> new Tuple2(rs.getInt(1), rs.getString(2))
                );

        System.out.println(tpJdbcRDD.collect());


        jsc.stop();

    }
}
