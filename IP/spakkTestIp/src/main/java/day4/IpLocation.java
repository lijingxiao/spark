package day4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Created by lijingxiao on 2018/9/25.
 */
public class IpLocation {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("IpLocation").setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //将ip规则缓存在Driver端的内存中
        List<Tuple3<Long, Long, String>> ipinfos = MyUtils.readFile();
        Broadcast<List<Tuple3<Long, Long, String>>> broadcastRef = jsc.broadcast(ipinfos);

        JavaRDD<String> lines = jsc.textFile("access.log");

        //函数内部引用了外部变量broadcastRef（闭包），该变量随着task一起呗发送到executor端
        JavaPairRDD<String, Integer> provinceAndOne = lines.mapToPair(line -> {
            String[] fileds = line.split("[|]");
            String ip = fileds[1];
            long ip2Long = MyUtils.ip2Long(ip);
            //进行二分法查找，通过Driver端的引用或取到Executor中的广播变量
            //（该函数中的代码是在Executor中别调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播的规则了）
            List<Tuple3<Long, Long, String>> rulesInExecutor = broadcastRef.value();
            String province = "未知";
            int index = MyUtils.binarySearch(rulesInExecutor, ip2Long);
//            int index = MyUtils.binarySearch(ipinfos, ip2Long);
            if (index != -1) {
                province = rulesInExecutor.get(index)._3();
//                province = ipinfos.get(index).getProvince();
            }
            return new Tuple2<>(province, 1);
        });


//        JavaPairRDD<String, Integer> provinceAndOne = lines.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String line) throws Exception {
//                String[] fileds = line.split("[|]");
//                String ip = fileds[1];
//                long ip2Long = ip2Long(ip);
////            List<Ipinfo> rulesInExecutor = broadcastRef.value();
//                String province = "未知";
//                int index = binarySearch(ipinfos, ip2Long);
//                if (index != -1) {
//                    province = ipinfos.get(index).getProvince();
//                }
//                return new Tuple2<>(province, 1);
//            }
//        });


        JavaPairRDD<String, Integer> reduced = provinceAndOne.reduceByKey((m, n) -> m+n);

//        List<Tuple2<String, Integer>> collect = reduced.collect();
//        System.out.println(collect);


        /**
         * reduced.foreachPartition(it -> {
         //一个迭代器代表一个分区，分区中有多条数据
         //先获得一个JDBC连接
         Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&password=123456&useUnicode=true&characterEncoding=UTF8&useSSL=false");
         PreparedStatement pstm = connection.prepareStatement("insert into accecc_log VALUES (?, ?)");
         while (it.hasNext()) {
         Tuple2<String, Integer> tp = it.next();
         pstm.setString(1, tp._1);
         pstm.setInt(2, tp._2);
         pstm.executeUpdate();
         }
         if(pstm != null) {
         pstm.close();
         }
         if (connection != null) {
         connection.close();
         }
         });
         */

        reduced.foreachPartition(it -> MyUtils.data2Mysql(it));
        jsc.stop();
    }
}
