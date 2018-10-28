package orders;

import com.alibaba.fastjson.JSON;
import common.ConfigContext;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by lijingxiao on 2018/10/22.
 *  //直连方式只有在KafkaDStream的RDD中才能获取偏移量，那么就不能到调用DStream的Transformation
 //所以只能子在kafkaStream调用foreachRDD，获取RDD的偏移量，然后就是对RDD进行操作了
 //依次迭代KafkaDStream中的KafkaRDD
 */
public class OrderCount {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(OrderCount.class);
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("orderCount").setMaster("local[3]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        Broadcast<List<Tuple3<Long, Long, String>>> broadcast = IPUtils.broadcastIpRules(jssc, "ip.txt");

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
         String group = "g1";
         */

        HashMap<String, String> kafkaParam = new HashMap<>();
        //"key.deserializer" -> classOf[StringDeserializer],
        //"value.deserializer" -> classOf[StringDeserializer],
//        kafkaParam.put("deserializer.encoding", "GB2312"); //配置读取Kafka中数据的编码
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

        Tuple2<JavaDStream<String>, AtomicReference<OffsetRange[]>> dStreamAndOffset = createDStream(jssc, kafkaParam, zkClient, topicSet, zkTopicPath);
        JavaDStream<String> message = dStreamAndOffset._1();
        AtomicReference<OffsetRange[]> offsetRanges = dStreamAndOffset._2();

        //直连方式只有在KafkaDStream的RDD中才能获取偏移量，那么就不能到调用DStream的Transformation
        //所以只能子在kafkaStream调用foreachRDD，获取RDD的偏移量，然后就是对RDD进行操作了
        //依次迭代KafkaDStream中的KafkaRDD
        message.foreachRDD(rdd-> {
                    //判断当前的kafkaStream中的RDD是否有数据
                    if (!rdd.isEmpty()) {
                        //整理数据
                        JavaRDD<String> line = rdd.map(lines -> lines);
                        JavaRDD<String[]> fields = line.map(items -> items.split(" "));

                        //计算成交总金额
                        CalculateUtil.calculateIncome(fields);
                        //计算成交总金额
                        CalculateUtil.calculateIncome(fields);

                        //计算区域成交金额
                        CalculateUtil.calculateZone(fields, broadcast);

                        for (OffsetRange offset : offsetRanges.get()) {
                            String zkPath = zkTopicPath + "/" + String.valueOf(offset.partition());
                            ZkUtils.updatePersistentPath(zkClient, zkPath, String.valueOf(offset.untilOffset()));
                        }
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

    /**
     *
     * @param jssc
     * @param kafkaParam
     * @param zkClient
     * @param topicSet
     * @param zkTopicPath
     * @return 需要从HasOffsetRanges中获取offset，要将RDD强转为HasOffsetRanges格式，只有KafkaRDD实现了HasOffsetRanges这个接口
     * 因此必须是createDirectDStream返回的DStream的第一个操作，又要统一JavaInputDStream和JavaPairInputDStream返回同一种类型的DStream
     * （即不能对其中任何一个进行转化操作），因此在创建DStream的时候就获取offset
     */
    private static Tuple2<JavaDStream<String>, AtomicReference<OffsetRange[]>> createDStream(JavaStreamingContext jssc, HashMap<String, String> kafkaParam, ZkClient zkClient,
                                                            Set<String> topicSet, String zkTopicPath) {
        //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
        // /g001/offsets/wordcount/0/10001"
        // /g001/offsets/wordcount/1/30001"
        // /g001/offsets/wordcount/2/10001"
        //zkTopicPath  -> /g001/offsets/wordcount/
        int children = zkClient.countChildren(zkTopicPath);
        HashMap<TopicAndPartition, Long> fromOffset = new HashMap<>();
        logger.info("children ", children);

        //必须是DStream的第一个操作，否则会报下面这个错，因为
        //Exception in thread "main" java.lang.ClassCastException: org.apache.spark.rdd.MapPartitionsRDD cannot be cast to org.apache.spark.streaming.kafka.HasOffsetRanges
        //at KafkaDirectWordCount.lambda$main$c710ef09$1(KafkaDirectWordCount.java:75)
//        OffsetRange[] offsetRanges;

        //lambda表达式中不能修改外部变量的值，为什么下面这种方式就可以呢？
        AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
//        OffsetRange[]  offsetRanges;


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

            message.transform(rdd -> {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                logger.info("offsets: ", Arrays.asList(offsets).toString());
                offsetRanges.set(offsets);
                return rdd;
            });
            return new Tuple2<>(message, offsetRanges);
        } else {
            JavaPairInputDStream<String, String> javaPairInputDStream = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class,
                    kafkaParam, topicSet);
            JavaDStream<String> message = javaPairInputDStream.transform(new Function2<JavaPairRDD<String, String>, Time, JavaRDD<String>>() {
                @Override
                public JavaRDD<String> call(JavaPairRDD<String, String> stringStringJavaPairRDD, Time time) throws Exception {
                    OffsetRange[] offsets = ((HasOffsetRanges) stringStringJavaPairRDD.rdd()).offsetRanges();
                    logger.info("offsets: ", Arrays.asList(offsets).toString());
                    offsetRanges.set(offsets);
                    return stringStringJavaPairRDD.values();
                }
            });
            return new Tuple2<>(message, offsetRanges);
        }
    }
}
