#### 通过并行化scala集合创建RDD
```
val rdd1 = sc.parallelize(Array(1,2,3,4,5,6,7,8))
```
#### 查看该rdd的分区数量
```
scala> rdd1.partitions.length
res0: Int = 32
```
#### 排序
```
scala> sc.parallelize(List(5,6,4,7,3,8,2,9,1,10)).map(_*2).sortBy(x=>x+"",true).collect
res1: Array[Int] = Array(10, 12, 14, 16, 18, 2, 20, 4, 6, 8)  
```
注意：此时只是按照字符串进行排序，并没有改变数值类型

#### map与flatMap
```
scala> val rdd4 = sc.parallelize(Array("a b c", "d e f", "h i j"))
scala> rdd4.map(_.split(" ")).collect
res4: Array[Array[String]] = Array(Array(a, b, c), Array(d, e, f), Array(h, i, j))

scala> rdd4.flatMap(_.split(' ')).collect
res5: Array[String] = Array(a, b, c, d, e, f, h, i, j)

```
##### map
将一个RDD中的每个数据项，通过map中的函数映射变为一个新的元素。

输入分区与输出分区一对一，即：有多少个输入分区，就有多少个输出分区。
##### flatMap
scala中使用flatten方法来吧一个嵌套集合转化为一个单层集合

flatmap就是先进行map操作，然后进行flat操作
```
scala> val rdd5 = sc.parallelize(List(List("a b c", "a b b"),List("e f g", "a f g"), List("h i j", "a a b")))
rdd5: org.apache.spark.rdd.RDD[List[String]] = ParallelCollectionRDD[14] at parallelize at <console>:24

scala> rdd5.flatMap(_.flatMap(_.split(" "))).collect
res7: Array[String] = Array(a, b, c, a, b, b, e, f, g, a, f, g, h, i, j, a, a, b)
```
其中，括号内的flatMap为集合的操作，外面的flatMap为RDD操作
#### union求并集，注意类型要一致
```
scala> val rdd6 = sc.parallelize(List(5,6,4,7))
scala> val rdd7 = sc.parallelize(List(1,2,3,4))
scala> val rdd8 = rdd6.union(rdd7)
scala> rdd8.collect
res9: Array[Int] = Array(5, 6, 4, 7, 1, 2, 3, 4)

scala> rdd8.distinct.sortBy(x=>x).collect
res8: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7)
```
#### intersection求交集
```
scala> val rdd9 = rdd6.intersection(rdd7)
scala> rdd9.collect
res10: Array[Int] = Array(4)
```
#### join
```
scala> val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 2), ("kitty", 3)))
scala> val rdd2 = sc.parallelize(List(("jerry", 9), ("tom", 8), ("shuke", 7), ("tom", 2)))

scala> rdd1.join(rdd2).collect
res11: Array[(String, (Int, Int))] = Array((tom,(1,8)), (tom,(1,2)), (jerry,(2,9)))

scala> rdd1.leftOuterJoin(rdd2).collect
res12: Array[(String, (Int, Option[Int]))] = Array((tom,(1,Some(8))), (tom,(1,Some(2))), (kitty,(3,None)), (jerry,(2,Some(9))))

scala> rdd1.rightOuterJoin(rdd2).collect
res13: Array[(String, (Option[Int], Int))] = Array((tom,(Some(1),8)), (tom,(Some(1),2)), (shuke,(None,7)), (jerry,(Some(2),9)))
```
#### groupByKey
```
scala> rdd1.union(rdd2).groupByKey.collec
res14: Array[(String, Iterable[Int])] = Array((tom,CompactBuffer(1, 8, 2)), (shuke,CompactBuffer(7)), (kitty,CompactBuffer(3)), (jerry,CompactBuffer(2, 9)))

scala> rdd1.union(rdd2).groupByKey.map(x=>(x._1,x._2.sum)).collect
res16: Array[(String, Int)] = Array((tom,11), (shuke,7), (kitty,3), (jerry,11))


scala> rdd1.union(rdd2).groupByKey.mapValues(_.sum).collect
res17: Array[(String, Int)] = Array((tom,11), (shuke,7), (kitty,3), (jerry,11))
```
wordCount
```
sc.textFile("/root/words.txt").flatMap(x=>x.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).collect
sc.textFile("/root/words.txt").flatMap(x=>x.split(" ")).map((_,1)).groupByKey.map(t=>(t._1, t._2.sum)).collect
```
大数据量的时候，groupby比reduceby效率低，因为groupby需要将key相同的元素分配到同一个executor执行，会产生大量的shuffle
而reduceby回先进行局部聚合，再进行全局聚合，这样shuffle的数据量会小一些
#### mapValues
顾名思义就是输入函数应用于RDD中Kev-Value的Value，原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素。因此，该函数只适用于元素为KV对的RDD。
#### cogroup
```
scala> val rdd1 = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)))
rdd1: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[52] at parallelize at <console>:24

scala> val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))
rdd2: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[53] at parallelize at <console>:24

scala> val rdd3 = rdd1.cogroup(rdd2)
rdd3: org.apache.spark.rdd.RDD[(String, (Iterable[Int], Iterable[Int]))] = MapPartitionsRDD[55] at cogroup at <console>:28

scala> rdd3.collect
res18: Array[(String, (Iterable[Int], Iterable[Int]))] = Array((tom,(CompactBuffer(1, 2),CompactBuffer(1))), (shuke,(CompactBuffer(),CompactBuffer(2))), (kitty,(CompactBuffer(2),CompactBuffer())), (jerry,(CompactBuffer(3),CompactBuffer(2))))
```
#### cartesian笛卡尔积
```
val rdd1 = sc.parallelize(List("tom", "jerry"))
val rdd2 = sc.parallelize(List("tom", "kitty", "shuke"))

scala> val rdd3 = rdd1.cartesian(rdd2)
scala> rdd3.collect
res19: Array[(String, String)] = Array((tom,tom), (tom,kitty), (tom,shuke), (jerry,tom), (jerry,kitty), (jerry,shuke))
```
### RDD的Transformation的特点
	1.lazy
	2.生成新的RDD
############################################################################
```
scala> val rdd1 = sc.parallelize(List(1,2,3,4,5), 2)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[59] at parallelize at <console>:24

scala> rdd1.partitions.length
res20: Int = 2

指定多少个分区，就会产生多少个task

一般不指定的话，有多少个输入切片，就会产生多少个分区

scala> rdd1.reduce(_+_)
res21: Int = 15

scala> rdd1.count
res22: Long = 5

scala> rdd1.top(2)
res23: Array[Int] = Array(5, 4)

scala> rdd1.take(2)
res24: Array[Int] = Array(1, 2)

scala> rdd1.first
res25: Int = 1

scala> rdd1.takeOrdered(3)
res26: Array[Int] = Array(1, 2, 3)
```


