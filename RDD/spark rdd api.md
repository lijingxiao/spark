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
 
