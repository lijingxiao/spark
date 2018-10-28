import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by lijingxiao on 2018/10/21.
 */
public class KafkaWordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("kafkaWordCount").setMaster("local[3]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        String zkQuorum = "node-1:2181,node-2:2181,node-3:2181";
        String groupId = "g1";
        HashMap<String, Integer> topic = new HashMap<>();
        topic.put("testTopic", 1);
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jssc, zkQuorum, groupId, topic);

        //对数据进行处理
        //Kafak的ReceiverInputDStream[(String, String)]里面装的是一个元组（key是写入的key，value是实际写入的内容）
        JavaDStream<String> values = lines.map(line -> line._2());
        JavaDStream<String> words = values.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairDStream<String, Integer> pair = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1));
        JavaPairDStream<String, Integer> result = pair.reduceByKey((m, n) -> m + 1);
        result.print();

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
