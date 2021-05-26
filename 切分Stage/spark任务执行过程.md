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
那么在第一个action触发之后，接下来的RDD操作关系形成的DAG在哪里生成的？（第一个DAG执行完之后，在Driver端生成接下来的DAG）
- 一个RDD只是描述了数据计算过程中的一个环节，而DGA由一到多个RDD组成，描述了数据计算过程中的所有环节（过程）
一个Spark Application中有多少个DAG：**一到多个（取决于触发了多少次Action）**

### 切分Stage
一个DAG中可能有产生多种不同类型和功能的Task，会有不同的阶段

DAGScheduler：将一个DAG切分成一到多个Stage，DAGScheduler切分的依据是Shuffle（宽依赖）

为什么要切分Stage？
- 一个复杂的业务逻辑（将多台机器上具有相同属性的数据聚合到一台机器上：shuffle）
- 如果有shuffle，那么就意味着前面阶段产生的结果后，才能执行下一个阶段，下一个阶段的计算要依赖上一个阶段的数据。
- 在同一个Stage中，会有多个算子，可以合并在一起，我们称其为pipeline（流水线：严格按照流程、顺序执行），pipeline中的多个平行分区可以并行执行

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
### Spark任务执行流程
![](https://github.com/lijingxiao/spark/blob/master/%E5%88%87%E5%88%86Stage/%E6%89%A7%E8%A1%8C%E8%BF%87%E7%A8%8B.png)
- Driver端提交Spark任务，向master申请资源；
- master进行资源调度；
- master与worker进行RPC通信，让worker启动executor；
- worker启动executor；
- executor与driver进行通信（请求task）；
- （driver端）RDD触发action之后，会根据最后一个RDD从后往前推断依赖关系，构建DAG，遇到shuffle就切分stage；
- （driver端）DAGScheduler切分完stage之后，先提交前面的stage，执行完之后再提交后面的stage，stage生成task，一个stage会生成多个业务逻辑相同的task，以TaskSet的形式传递给TaskScheduler，TaskScheduler将Task序列化，根据资源情况发送给executor；
- executor接收到task后，先将task反序列化，然后用一个实现了Runnable接口的类将task包装起来，放到线程池中，然后包装类的run方法会被执行，进而调用task的业务逻辑。


### job task
job包含很多 task 的并行计算，可以认为是 Spark RDD 里面的 action，每个 action 的触发会生成一个job。 用户提交的 Job 会提交给 DAGScheduler，Job 会被分解成 Stage，Stage 会被细化成 Task，Task 简单的说就是在一个数据 partition 上的单个数据处理流程。

一个Job被拆分成若干个Stage，每个Stage执行一些计算，产生一些中间结果。它们的目的是最终生成这个Job的计算结果。而每个Stage是一个task set，包含若干个task。Task是Spark中最小的工作单元，在一个executor上完成一个特定的事情。

一个job就是一个DAG？


### worker  executor

- 每个Worker上存在一个或者多个ExecutorBackend 进程。每个进程包含一个Executor对象，该对象持有一个线程池，每个线程可以执行一个task。
- 每个application包含一个 driver 和多个 executors，每个 executor里面运行的tasks都属于同一个application。
- 每个Worker上存在一个或者多个ExecutorBackend 进程。
- 每个进程包含一个Executor对象，该对象持有一个线程池，每个线程可以执行一个task。
![](https://github.com/lijingxiao/spark/blob/master/%E5%88%87%E5%88%86Stage/spark_worker_excutor.png)
