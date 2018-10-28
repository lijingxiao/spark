package orders;

import common.MyUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import redis.JedisConnectionPool;
import redis.clients.jedis.Jedis;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;

/**
 * Created by lijingxiao on 2018/10/28.
 */
public class CalculateUtil {
    public static void calculateIncome(JavaRDD<String[]> fields) {
        JavaRDD<Double> priceRDD = fields.map(items -> Double.valueOf(items[4]));
        //reduce是一个Action，会把结果返回到Driver端
        //将当前批次的总金额返回了
        Double sum = priceRDD.reduce((m, n) -> m + n);

        //这个Jedis链接是在Driver端获取的
        Jedis jedis = JedisConnectionPool.getPool().getResource();
        jedis.incrByFloat("TOTAL_INCOME", sum);
        jedis.close();
    }

    /**
     * 计算商品分类金额
     * @param fields
     */
    public static void calculateItem(JavaRDD<String[]> fields) {
        //对field的map方法是在哪一端调用的呢？Driver
        JavaPairRDD<String, Double> itemAndPrice = fields.mapToPair(items -> {
            String item = items[2];
            Double price = Double.valueOf(items[4]);
            return new Tuple2<String, Double>(item, price);

        });

        JavaPairRDD<String, Double> reduced = itemAndPrice.reduceByKey((m, n) -> m + n);
        reduced.foreachPartition(part -> {
            Jedis jedis = JedisConnectionPool.getPool().getResource();
            while (part.hasNext()) {
                jedis.incrByFloat(part.next()._1, part.next()._2);
            }
            jedis.close();
        });
    }

    /**
     * 计算区域成交金额
     * @param fields
     */
    public static void calculateZone(JavaRDD<String[]> fields, Broadcast<List<Tuple3<Long, Long, String>>> broadcast) {
        JavaPairRDD<String, Double> provinceAndPrice = fields.mapToPair(items -> {
            String ip = items[1];
            Double price = Double.valueOf(items[4]);
            long ip2Long = MyUtils.ip2Long(ip);
            //进行二分法查找，通过Driver端的引用或取到Executor中的广播变量
            //（该函数中的代码是在Executor中别调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播的规则了）
            List<Tuple3<Long, Long, String>> rulesInExecutor = broadcast.value();
            String province = "未知";
            int index = MyUtils.binarySearch(rulesInExecutor, ip2Long);
//            int index = MyUtils.binarySearch(ipinfos, ip2Long);
            if (index != -1) {
                province = rulesInExecutor.get(index)._3();
//                province = ipinfos.get(index).getProvince();
            }
            return new Tuple2<>(province, price);
        });
        //按省份进行聚合
        JavaPairRDD<String, Double> reduced = provinceAndPrice.reduceByKey((m, n) -> m + n);

        reduced.foreachPartition(part -> {
            Jedis jedis = JedisConnectionPool.getPool().getResource();
            while (part.hasNext()) {
                jedis.incrByFloat(part.next()._1, part.next()._2);
            }
            jedis.close();
        });
    }
}
