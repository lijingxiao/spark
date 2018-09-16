```scala
    val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[4]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)
    //指定以后从哪里读取数据创建RDD（弹性分布式数据集）
    //sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).sortBy(_._2, false).saveAsTextFile(args(1))

    val lines: RDD[String] = sc.textFile("hdfs://node-4:9000/wc1", 1)

    //lines.partitions.length
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //按key进行聚合
    val reduced:RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //排序
    //val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //将结果保存到HDFS中
    reduced.saveAsTextFile(args(1))
```
- 第一步 sc.textFile()创建RDD
共生成两个RDD：
1. hadoopFile方法生成一个HadoopRDD[K,V]，K为文件偏移量，V为textString
2. 对生成的HadoopRDD调用maP操作，将其中的V返回，生成mapPartitionsRDD[String]
```scala
hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
```
- 第二步，flatMap生成一个mapPartitionsRDD[String]
- 第三步，map生成mapPartitionsRDD[(String,Int)]
- 第四步，reduceByKey生成shuffledRDD[(String,Int)]
shuffle先局部聚合写磁盘，然后shuffle，全局聚合
- 第五步，调用sortBy生成shuffledRDD[(String,Int)]
- 第六步，saveAsTextFile，调用mapPartitions方法生成mapPartitionsRDD[NullWritable,String]


共产生两种类型的Task
- ShuffleMapTask：一系列map操作，局部聚合，写磁盘
- ResultTask：从上游拉取数据，全局聚合，写hdfs

以shuffle为分水岭，产生2个stage，4个task（两个分区的话）
