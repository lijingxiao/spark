### mapPartitionsWithIndex
RDD的map方法，是Executor中执行时，是一条一条的将数据拿出来处理

mapPartitionsWithIndex 一次拿出一个分区（分区中并没有数据，而是记录要读取哪些数据，真正生成的Task会读取多条数据），并且可以将分区的编号取出来

功能：取分区中对应的数据时，还可以将分区的编号取出来，这样就可以知道数据是属于哪个分区的（哪个区分对应的Task的数据）

	//该函数的功能是将对应分区中的数据取出来，并且带上分区编号
	
    val func = (index: Int, it: Iterator[Int]) => {
      it.map(e => s"part: $index, ele: $e")
    }
	rdd.mapPartitionsWithIndex(fun)
  
 ### aggregate
 spark文档的定义
 ```
 def aggregate[U](zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)(implicit arg0: ClassTag[U]): U
 
-zeroValue
the initial value for the accumulated result of each partition for the seqOp operator, 
and also the initial value for the combine results from different partitions for the combOp operator - 
this will typically be the neutral element (e.g. Nil for list concatenation or 0 for summation)
-seqOp
an operator used to accumulate results within a partition
-combOp
an associative operator used to combine results from different partitions
 ```
  seqOp操作会聚合各分区中的元素，然后combOp操作把所有分区的聚合结果再次聚合，两个操作的初始值都是zeroValue.   
  seqOp的操作是遍历分区中的所有元素(T)，**第一个T跟zeroValue做操作，结果再作为与第二个T做操作的zeroValue**，直到遍历完整个分区。
  combOp操作是把各分区聚合的结果，再聚合。aggregate函数返回一个跟RDD不同类型的值。
  因此，需要一个操作seqOp来把分区中的元素T合并成一个U，另外一个操作combOp把所有U聚合。
  ```
  scala> val rdd2 = sc.parallelize(List("a","b","c","d","e","f"),2)
  scala> rdd2.aggregate("|")(_ + _, _ + _)
  res2: String = ||def|abc   //或者 ||abc|def
  
  //空字符串先和'12'比较，取较小值0，然后'0'串再和'23'比较，取较小值为1
  scala> val rdd4 = sc.parallelize(List("12","23","345",""),2)
  scala> rdd4.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
  res3: String = 10    //或者01
  ```
也就是说传入的参数zeroValue并不是挨个将自己和分区内的记录进行计算，而是不断迭代计算，将和第一条结算的结果与第二条进行计算
 ### aggregateByKey

aggregateByKey与aggregate类似。

区别是：aggregateByKey把相关key的元素进行reduce计算，并且**初始值只运用于Partition内的reduce操作**。
```
scala> val pairRDD = sc.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)
pairRDD: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect
res0: Array[(String, Int)] = Array((dog,100), (cat,200), (mouse,200))
```
aggregateByKey执行时需要通过shuffle将key相同的元素拉到同一个executor执行
### collectAsMap

 ![collect执行过程](https://github.com/lijingxiao/spark/blob/master/RDD/collect.png)
 1. 启动master，worker，worker向msater注册，并定期发送心跳报活；
 2. 启动driver，提交任务，向master申请资源，master分配资源；
 	注：此时如果核数不够，不会影响任务提交，但是如果内存不够，任务就不能提交
 3. master下发命令，worker启动executor；
 4. executor与driver通信（通过driver和worker）
 
 5. action触发的时候（从后往前）推算rdd逻辑关系，生成task（一个分区对应一个task，记录了从哪里获取数据源），将task发往executor；
 6. executor将driver端的数据通过网络拉取到executor（边读边计算），并进行计算；
 7. 计算完成之后，driver端拉取executor的计算结果
 
 如果要持久化数据（Redis/MySQL/Hbase等）
 不要在driver端进行写（要将executor的结果通过网络传输到driver，并且只有一个链接）
应该在executor端直接写数据
 
### combineByKey
val rdd1 = sc.textFile("hdfs://node-1:9000/wc").flatMap(_.split(" ")).map((_, 1))
val rdd2 = rdd1.combineByKey(x => x, (a: Int, b: Int) => a + b, (m: Int, n: Int) => m + n)
rdd2.collect

val rdd3 = rdd1.combineByKey(x => x + 10, (a: Int, b: Int) => a + b, (m: Int, n: Int) => m + n)
rdd3.collect


val rdd4 = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
val rdd5 = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
val rdd6 = rdd5.zip(rdd4)
val rdd7 = rdd6.combineByKey(List(_), (x: List[String], y: String) => x :+ y, (m: List[String], n: List[String]) => m ++ n)


### countByKey 

val rdd1 = sc.parallelize(List(("a", 1), ("b", 2), ("b", 2), ("c", 2), ("c", 1)))
rdd1.countByKey
rdd1.countByValue


### filterByRange

val rdd1 = sc.parallelize(List(("e", 5), ("c", 3), ("d", 4), ("c", 2), ("a", 1)))
val rdd2 = rdd1.filterByRange("b", "d")
rdd2.colllect


### flatMapValues
val a = sc.parallelize(List(("a", "1 2"), ("b", "3 4")))
rdd3.flatMapValues(_.split(" "))


### foldByKey

val rdd1 = sc.parallelize(List("dog", "wolf", "cat", "bear"), 2)
val rdd2 = rdd1.map(x => (x.length, x))
val rdd3 = rdd2.foldByKey("")(_+_)

val rdd = sc.textFile("hdfs://node-1.edu360.cn:9000/wc").flatMap(_.split(" ")).map((_, 1))
rdd.foldByKey(0)(_+_)


### foreachPartition
val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
rdd1.foreachPartition(x => println(x.reduce(_ + _)))
 
 
 
