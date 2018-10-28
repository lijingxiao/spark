import com.alibaba.fastjson.JSON;
import common.ConfigContext;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by lijingxiao on 2018/10/22.
 * HasOffsetRanges报错
 */
public class KafkaDirectWordCount {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaDirectWordCount.class);
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("directWordCount").setMaster("local[3]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        String configStr = args[0];
        logger.info(configStr);
        ConfigContext configContext = JSON.parseObject(configStr, ConfigContext.class);
        logger.info(JSON.toJSONString(configStr));
        ConfigContext.setConfigContext(configContext);

        /**
         *         //指定消费的 topic 名字
         String topic = "wwcc";
         //指定kafka的broker地址(sparkStream的Task直连到kafka的分区上，用更加底层的API消费，效率更高)
         String brokerList = "node-4:9092,node-5:9092,node-6:9092";
         //指定zk的地址，后期更新消费的偏移量时使用(以后可以使用Redis、MySQL来记录偏移量)
         String zkQuorum = "node-1:2181,node-2:2181,node-3:2181";
         //指定组名
         String group = "g001";
         */

        HashMap<String, String> kafkaParam = new HashMap<>();
        kafkaParam.put("group.id", configContext.getGroupId());
        kafkaParam.put("metadata.broker.list", configContext.getBrokerList());
        kafkaParam.put("auto.offset.reset", "largest");

        //创建 stream 时使用的 topic 名字集合，SparkStreaming可同时消费多个topic
        Set<String> topicSet = new HashSet<>();
        topicSet.add(configContext.getTopic());
        //创建一个 ZKGroupTopicDirs 对象,其实是指定往zk中写入数据的目录，用于保存偏移量
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(kafkaParam.get("group.id"), configContext.getTopic());
        //获取 zookeeper 中的路径 "/g001/offsets/wordcount/"
        String zkTopicPath = topicDirs.consumerOffsetDir();//创建 stream 时使用的 topic 名字集合，SparkStreaming可同时消费多个topic
        logger.info("zkTopicPath ", zkTopicPath);

        //zookeeper 的host 和 ip，创建一个 client,用于跟新偏移量量的
        //是zookeeper的客户端，可以从zk中读取偏移量数据，并更新偏移量
        ZkClient zkClient = new ZkClient(configContext.getZkQuorum());


        JavaDStream<String> message = createDStream(jssc, kafkaParam, zkClient, topicSet, zkTopicPath);

        //必须是DStream的第一个操作，否则会报下面这个错，因为
        //Exception in thread "main" java.lang.ClassCastException: org.apache.spark.rdd.MapPartitionsRDD cannot be cast to org.apache.spark.streaming.kafka.HasOffsetRanges
        //at KafkaDirectWordCount.lambda$main$c710ef09$1(KafkaDirectWordCount.java:75)
//        OffsetRange[] offsetRanges;
        AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
        JavaDStream<String> value = message.transform(rdd -> {
            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            logger.info("offsets: ", Arrays.asList(offsets).toString());
            offsetRanges.set(offsets);
            return rdd;
        });

        value.foreachRDD(rdd-> {
            rdd.foreachPartition(it ->{
                while (it.hasNext()) {
                    logger.info(it.next());
                }
            });
                    for (OffsetRange offset : offsetRanges.get()) {
                        String zkPath = zkTopicPath + "/" + String.valueOf(offset.partition());
                        ZkUtils.updatePersistentPath(zkClient, zkPath, String.valueOf(offset.untilOffset()));
                    }
        }
        );

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static JavaDStream<String> createDStream(JavaStreamingContext jssc, HashMap<String, String> kafkaParam, ZkClient zkClient,
                                                            Set<String> topicSet, String zkTopicPath) {
        //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
        // /g001/offsets/wordcount/0/10001"
        // /g001/offsets/wordcount/1/30001"
        // /g001/offsets/wordcount/2/10001"
        //zkTopicPath  -> /g001/offsets/wordcount/
        int children = zkClient.countChildren(zkTopicPath);
        HashMap<TopicAndPartition, Long> fromOffset = new HashMap<>();
        logger.info("children ", children);

        if (children > 0) {
            for (int i=0; i < children; i++) {
                // /g001/offsets/wordcount/0
                String path = zkTopicPath + "/" + String.valueOf(i);
                logger.info("path: ", path);
                Object data = zkClient.readData(path);
                // wordcount/0
                TopicAndPartition topicAndPartition = new TopicAndPartition(topicSet.iterator().next(), i);
                // wordcount/0 -> 10001
                fromOffset.put(topicAndPartition, Long.valueOf(data.toString()));
            }

            JavaInputDStream<String> message = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
                    StringDecoder.class, String.class, kafkaParam, fromOffset, new Function<MessageAndMetadata<String, String>, String>() {
                        @Override
                        public String call(MessageAndMetadata<String, String> stringStringMessageAndMetadata) throws Exception {
                            return stringStringMessageAndMetadata.message();
                        }
                    });

            return message;
        } else {
            JavaDStream<String> message = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class,
                    kafkaParam, topicSet).map(line -> line._2());
            return message;
        }
    }
}
