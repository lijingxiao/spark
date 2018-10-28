import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by lijingxiao on 2018/10/17.
 */
public class SocketTextTest {
    //java -cp testsocketText-1.0-SNAPSHOT.jar SocketTextTest localhost 9997 10
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("socketText").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(Long.valueOf(args[2])));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(args[0], Integer.valueOf(args[1]));
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

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
