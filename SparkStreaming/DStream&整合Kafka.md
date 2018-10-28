Spark Streaming是一个基于Spark Core之上的实时计算框架，可以从很多数据源消费数据并对数据进行处理，
在Spark Streaing中有一个最基本的抽象叫DStream（代理），本质上就是一系列连续的RDD，DStream其实就是对RDD的封装
DStream可以认为是一个RDD的工厂，该DStream里面生产都是相同业务逻辑的RDD，只不过是RDD里面要读取数据的不相同。

实时计算，Driver端不停地提交任务，一定的时间间隔读取一个小的批次，然后将这个小批次的数据提交到集群，不停地生产，不停地提交。

**深入理解DStream**：它是SparkStreaming中的一个最基本的抽象，代表了一系列连续的数据流，本质上是一系列连续的RDD，对DStream进行操作，就是对RDD进行操作。

- DStream每隔一段时间生成一个RDD，你对DStream进行操作，本质上是对里面的对应时间的RDD进行操作

- DSteam和DStream之间存在依赖关系，在一个固定的时间点，多个存在依赖关系的DSrteam对应的RDD也存在依赖关系，每个一个固定的时间，其实生成了一个小的DAG，周期性的将生成的小DAG提交到集群中运行

### 读取socket流
nc -lk 8888会启动一个socketServer监听端口8888，然后启动一个client向server写数据

### 整合kafka
#### Receiver方式
Receiver是使用Kafka的高层次Consumer API来实现的。receiver从Kafka中获取的数据都是存储在Spark Executor的内存中的，然后Spark Streaming启动的job会去处理那些数据。
这种基于receiver的方式，是使用Kafka的高阶API来在ZooKeeper中保存消费过的offset的。这是消费Kafka数据的传统方式。这种方式配合着WAL机制可以保证数据零丢失的高可靠性，但是却无法保证数据被处理一次且仅一次，可能会处理两次。因为Spark和ZooKeeper之间可能是不同步的。
```java
String zkQuorum = "node-1:2181,node-2:2181,node-3:2181";
String groupId = "g1";
HashMap<String, Integer> topic = new HashMap<>();
topic.put("testTopic", 1);
JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jssc, zkQuorum, groupId, topic);
```
#### Direct方式
Kafka低阶API，这种方式会周期性地查询Kafka，来获得每个topic+partition的最新的offset，从而定义每个batch的offset的范围。当处理数据的job启动时，就会使用Kafka的简单consumer api来获取Kafka指定offset范围的数据。
这种direct方式的优点如下：
- 简化并行读取：如果要读取多个partition，不需要创建多个输入DStream然后对它们进行union操作。Spark会创建跟Kafka partition一样多的RDD partition，并且会并行从Kafka中读取数据。所以**在Kafka partition和RDD partition之间，有一个一对一的映射关系**。
- 一次且仅一次的事务机制：基于receiver的方式，在spark和zk中通信，很有可能导致数据的不一致。
- 高效率：在receiver的情况下，如果要保证数据的不丢失，需要开启wal机制，这种方式下，为、数据实际上被复制了两份，一份在kafka自身的副本中，另外一份要复制到wal中， direct方式下是不需要副本的。
```java
JavaInputDStream<String> message = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
                    StringDecoder.class, String.class, kafkaParam, fromOffset, new Function<MessageAndMetadata<String, String>, String>() {
                        @Override
                        public String call(MessageAndMetadata<String, String> stringStringMessageAndMetadata) throws Exception {
                            return stringStringMessageAndMetadata.message();
                        }
                    });
```
注意：直连方式只有在KafkaDStream的RDD中才能获取偏移量，所以必须在获取到DStream之后的第一个操作中获取offset


### 代码块在哪一端执行
```java
kafkaStream.foreachRDD(rdd -> {
	//foreachRDD这个函数是在Driver端调用的，获取DStream中的rdd

	//调用RDD的算子，在Driver端
	mapRDD = rdd.map(...)
	mapRDD.foreachPartition(part -> {
		//在Executor端执行
		...
	}
  ```

})
