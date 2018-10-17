### Broadcast Join
- 适用场景：大表join小表
- 将小表以广播的形式分发到每个节点，并放到hash表中与大表进行join。避免了shuffle操作。但是广播时需要将被广播的表先collect到driver端，当频繁有广播出现时，对driver的内存也是一个考验。
- 条件：
  - 被广播的表需要小于 spark.sql.autoBroadcastJoinThreshold 所配置的值，默认是10M （或者加了broadcast join的hint）
  - 基表不能被广播，比如 left outer join 时，只能广播右表
```
== Physical Plan ==
*BroadcastHashJoin [nation#9], [ename#20], Inner, BuildRight
:- *Filter isnotnull(nation#9)
:  +- Scan ExistingRDD[id#7L,name#8,nation#9]
+- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]))
   +- *Filter isnotnull(ename#20)
      +- InMemoryTableScan [ename#20, cname#21], [isnotnull(ename#20)]
            +- InMemoryRelation [ename#20, cname#21], true, 10000, StorageLevel(disk, memory, deserialized, 1 replicas)
                  +- Scan ExistingRDD[ename#20,cname#21]
```

### Shuffle Hash Join
- 适用场景：大表join较大表
- 由于Spark是一个分布式的计算引擎，可以通过分区的形式将大批量的数据划分成n份较小的数据集进行并行计算。这种思想应用到Join上便是Shuffle Hash Join了。**利用key相同必然分区相同的这个原理**，SparkSQL将较大表的join分而治之，先将表划分成n个分区，再对两个表中相对应分区的数据分别进行Hash Join，这样即在一定程度上减少了driver广播一侧表的压力，也减少了executor端取整张被广播表的内存消耗。
- 条件：
  - 两表中的较小表总体估计大小超过spark.sql.autoBroadcastJoinThreshold设定的值，即不满足broadcast join条件
  - 开启尝试使用hash join的开关，spark.sql.join.preferSortMergeJoin=false
  - 每个分区的平均大小不超过spark.sql.autoBroadcastJoinThreshold设定的值，即shuffle read阶段每个分区来自较小表的记录要能放到内存中
  - 大表的大小是小表三倍以上
  
### Sort Merge Join
- 适用场景：大表join大表
- 当两个表都非常大时，显然无论适用哪种都会对计算内存造成很大压力。这是因为join时两者采取的都是hash join，是将一侧的数据完全加载到内存中，使用hash code取join keys值相等的记录进行连接。
首先将两张表按照join keys进行了重新shuffle，保证join keys值相同的记录会被分在相应的分区。分区后对每个分区内的数据进行排序，排序后再对相应的分区内的记录进行连接。
由于两个表都是排序的，每次处理完streamIter的一条记录后，对于streamIter的下一条记录，只需从buildIter中上一次查找结束的位置开始查找，所以说每次在buildIter中查找不必重头开始，整体上来说，查找性能还是较优的。
```
== Physical Plan ==
*Project [name#8, cname#21]
+- *SortMergeJoin [nation#9], [ename#20], Inner
   :- *Sort [nation#9 ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(nation#9, 200)
   :     +- *Project [name#8, nation#9]
   :        +- *Filter isnotnull(nation#9)
   :           +- Scan ExistingRDD[id#7L,name#8,nation#9]
   +- *Sort [ename#20 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(ename#20, 200)
         +- *Filter isnotnull(ename#20)
            +- Scan ExistingRDD[ename#20,cname#21]
```
