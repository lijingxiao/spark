import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by lijingxiao on 2018/10/21.
 */
public class StateFulKafkaWordCount {
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
        JavaPairDStream<String, Integer> result = pair.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            /**
             *
             * @param values
             * @param state
             * @return
             * @throws Exception
             */
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                //第一个参数就是key传进来的数据，第二个参数是曾经已有的数据
                Integer updatedValue = 0 ;//如果第一次，state没有，updatedValue为0，如果有，就获取

                if(state.isPresent()){
                    updatedValue = state.get();
                }

                //遍历batch传进来的数据可以一直加，随着时间的流式会不断去累加相同key的value的结果。
                for(Integer value: values){
                    updatedValue += value;
                }

                return Optional.of(updatedValue);//返回更新的值
            }
        }, new HashPartitioner(jssc.sparkContext().defaultParallelism()));
        result.print();

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
