### 总体流程
- 构建DAG（调用RDD上的方法）
- DAGScheduler将DAG切分Stage（切分的依据是Shuffle），将Stage中生成的Task以TaskSet的形式给TaskScheduler
- TaskScheduler调度Task（根据资源情况将Task调度到相应的Executor中）
- Executor接收Task，然后将Task丢入到线程池中执行

 ![spark执行流程](https://github.com/lijingxiao/spark/blob/master/%E5%88%87%E5%88%86Stage/spark%E6%80%BB%E4%BD%93%E6%B5%81%E7%A8%8B.png)
 
 ### 构建DAG
DAG 有向无环图（数据执行过程，有方法，无闭环）

- DAG描述多个RDD的转换过程，任务执行时，可以按照DAG的描述，执行真正的计算（数据被操作的一个过程）
- **DAG是有边界的**：开始（通过SparkContext创建的RDD），结束（触发Action，调用run Job就是一个完整的DAG就形成了，一旦触发Action就形成了一个完整的DAG）
那么在第一个action触发之后，接下来的RDD操作关系形成的DAG在哪里生成的？
- 一个RDD只是描述了数据计算过程中的一个环节，而DGA由一到多个RDD组成，描述了数据计算过程中的所有环节（过程）
一个Spark Application中有多少个DAG：**一到多个（取决于触发了多少次Action）**

### 切分Stage
一个DAG中可能有产生多种不同类型和功能的Task，会有不同的阶段

DAGScheduler：将一个DAG切分成一到多个Stage，DAGScheduler切分的依据是Shuffle（宽依赖）

为什么要切分Stage？
- 一个复杂的业务逻辑（将多台机器上具有相同属性的数据聚合到一台机器上：shuffle）
- 如果有shuffle，那么就意味着前面阶段产生的结果后，才能执行下一个阶段，下一个阶段的计算要依赖上一个阶段的数据。
- 在同一个Stage中，会有多个算子，可以合并在一起，我们称其为pipeline（流水线：严格按照流程、顺序执行）

**触发action的次数决定了产生多少个DAG；每个DAG中根据shuffle切分为一到多个Stage**
一个stage中可能有多个分区，每个分区对应一个task

#### 宽依赖与窄依赖
- shuffle的定义：shuffle的意思是洗牌，将数据打散，如果父RDD一个分区的数据给了子RDD的多个分区(只要存在这种可能)，就存在shuffle
有shuffle就有网络传输，但是有网络传输不一定就有shuffle
![宽依赖与窄依赖](https://github.com/lijingxiao/spark/blob/master/%E5%88%87%E5%88%86Stage/RDD%E5%AE%BD%E4%BE%9D%E8%B5%96%E4%B8%8E%E7%AA%84%E4%BE%9D%E8%B5%96.png "宽依赖与窄依赖")
- 窄依赖：窄依赖指的是每一个父RDD的Partition最多被子RDD的一个Partition使用
总结：窄依赖我们形象的比喻为独生子女
- 宽依赖:宽依赖指的是多个子RDD的Partition会依赖同一个父RDD的Partition
总结：窄依赖我们形象的比喻为超生

#### RDD的join
-第一种情况：宽依赖
![一般情况的join](https://github.com/lijingxiao/spark/blob/master/%E5%88%87%E5%88%86Stage/RDD%E7%9A%84join.png)
2个RDD的情况下，共生成了3个Stage，stage1和stage2读取数据（每个stage包括2各task，因为各有2个分区的数据），stage3进行join
- 第二种情况：窄依赖
**先分组，再join，并且没有改变新生成的RDD的分区数量和分区器**
![一般情况的join](https://github.com/lijingxiao/spark/blob/master/%E5%88%87%E5%88%86Stage/RDD%E7%89%B9%E6%AE%8A%E7%9A%84join.png)
也是3个stage，在进行groupByKey的时候进行shuffle，切分stage；如果改变分区个数的话，就会存在shuffle，多出两个stage
```scala
  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd: RDD[_] =>
    //如果当前rdd的分区器与它的父RDD的分区器相同，就是窄依赖
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[K, Any, CoGroupCombiner](
          rdd.asInstanceOf[RDD[_ <: Product2[K, _]]], part, serializer)
      }
    }
  }
```
