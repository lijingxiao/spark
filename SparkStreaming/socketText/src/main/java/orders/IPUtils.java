package orders;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple3;

import java.util.List;

/**
 * Created by lijingxiao on 2018/10/28.
 */
public class IPUtils {
    public static Broadcast<List<Tuple3<Long, Long, String>>> broadcastIpRules(JavaStreamingContext jssc, String ipRulesPath) {
        //现获取sparkContext
        JavaSparkContext sc = jssc.sparkContext();
        JavaRDD<String> rules = sc.textFile(ipRulesPath);

        JavaRDD<Tuple3<Long, Long, String>> ipRulesRDD = rules.map(line -> {
            String[] fileds = line.split("[|]");
            Long start = Long.valueOf(fileds[2]);
            Long end = Long.valueOf(fileds[3]);
            String province = fileds[6];
            return new Tuple3<>(start, end, province);
        });
        //将分散在多个Executor中的部分IP规则收集到Driver端
        List<Tuple3<Long, Long, String>> ruleDriver = ipRulesRDD.collect();
        //将Driver端的数据广播到Executor
        //广播变量的引用（还在Driver端）
        return sc.broadcast(ruleDriver);
    }

}
